//! Tests that load whole projects from temporary directories.

use std::fs;
use std::path::Path;

use infostash_model::{Error, Layer, TableRef};
use tempfile::TempDir;

use crate::{Check, Project};

const PROJECT: &str =
    "[project]\nname = \"shop\"\nformat = 1\n\n[warehouse]\npath = \".infostash/warehouse\"\n";
const ORDERS: &str = "connector = \"csv\"\npath = \"data/orders/*.csv\"\n";

/// A project directory holding `files`, each as (path, contents).
fn project(files: &[(&str, &str)]) -> TempDir {
    let dir = TempDir::new().unwrap();
    for (path, text) in files {
        let path = dir.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }
    dir
}

/// A shop project with the orders source and the given models.
fn shop(models: &[(&str, &str)]) -> TempDir {
    let mut files = vec![("infostash.toml", PROJECT), ("sources/orders.toml", ORDERS)];
    files.extend_from_slice(models);
    project(&files)
}

fn parse_error(dir: &TempDir) -> String {
    match Project::load(dir.path()) {
        Err(Error::Parse(message)) => message,
        other => panic!("expected a parse error, got {other:?}"),
    }
}

fn silver(name: &str) -> TableRef {
    TableRef::new(Layer::Clean, name)
}

fn gold(name: &str) -> TableRef {
    TableRef::new(Layer::Ready, name)
}

#[test]
fn loads_the_rfc_example() {
    let dir = shop(&[
        (
            "clean/orders.sql",
            "-- clean/orders.sql\n\
             -- @unique_key: order_id\n\
             -- @check not_negative: amount >= 0\n\
             -- @check: customer_id is not null\n\
             -- @description: One row per order, latest version wins.\n\
             select try_cast(amount as decimal(12, 2)) as amount from raw.orders\n",
        ),
        (
            "ready/daily_revenue.sql",
            "-- @full_refresh: true\nselect sum(amount) as revenue from clean.orders\n",
        ),
    ]);
    let project = Project::load(dir.path()).unwrap();
    assert_eq!(project.name(), "shop");
    assert_eq!(project.warehouse(), dir.path().join(".infostash/warehouse"));
    let source = &project.sources()[0];
    assert_eq!(source.table, TableRef::new(Layer::Raw, "orders"));
    assert_eq!(source.connector, "csv");
    assert_eq!(source.options["path"].as_str(), Some("data/orders/*.csv"));
    assert!(!source.options.contains_key("connector"));

    let [orders, revenue] = project.models() else {
        panic!("expected two models");
    };
    assert_eq!(orders.table, silver("orders"));
    assert_eq!(orders.file, Path::new("clean/orders.sql"));
    assert_eq!(orders.directives.unique_key, Some(vec!["order_id".into()]));
    assert_eq!(
        orders.directives.checks,
        vec![
            Check {
                name: Some("not_negative".into()),
                expr: "amount >= 0".into(),
                line: 3
            },
            Check {
                name: None,
                expr: "customer_id is not null".into(),
                line: 4
            },
        ]
    );
    assert_eq!(
        orders.directives.description.as_deref(),
        Some("One row per order, latest version wins.")
    );
    assert!(!orders.directives.full_refresh);
    assert_eq!(orders.inputs, [TableRef::new(Layer::Raw, "orders")].into());
    assert_eq!(revenue.table, gold("daily_revenue"));
    assert!(revenue.directives.full_refresh);
    assert_eq!(revenue.inputs, [silver("orders")].into());
}

#[test]
fn orders_models_after_what_they_read() {
    // In name order `a` would run first, but it reads `b`, which reads `c`.
    let dir = shop(&[
        ("ready/a.sql", "select * from gold.b"),
        ("ready/b.sql", "select * from ready.c join clean.d on true"),
        ("ready/c.sql", "select * from silver.d"),
        ("clean/d.sql", "select * from bronze.orders"),
        ("clean/e.sql", "select * from raw.orders"),
    ]);
    let project = Project::load(dir.path()).unwrap();
    let order: Vec<String> = project
        .models()
        .iter()
        .map(|m| m.table.to_string())
        .collect();
    assert_eq!(
        order,
        ["silver.d", "silver.e", "gold.c", "gold.b", "gold.a"]
    );
}

#[test]
fn subdirectories_do_not_change_the_name() {
    let dir = shop(&[("clean/sales/eu/orders.sql", "select * from raw.orders")]);
    let project = Project::load(dir.path()).unwrap();
    assert_eq!(project.models()[0].table, silver("orders"));
    assert_eq!(
        project.models()[0].file,
        Path::new("clean/sales/eu/orders.sql")
    );
}

#[test]
fn ignores_other_files() {
    let dir = shop(&[
        ("README.md", "# shop"),
        ("clean/notes.txt", "not a model"),
        ("sources/nested/extra.toml", "not = \"a source\""),
    ]);
    let project = Project::load(dir.path()).unwrap();
    assert_eq!(project.sources().len(), 1);
    assert!(project.models().is_empty());
}

#[test]
fn a_missing_project_file_is_not_found() {
    let dir = project(&[]);
    assert!(matches!(Project::load(dir.path()), Err(Error::NotFound(_))));
}

#[test]
fn rejects_unknown_project_keys() {
    let dir = project(&[("infostash.toml", &format!("{PROJECT}\n[extra]\nkey = 1\n"))]);
    let message = parse_error(&dir);
    assert!(message.starts_with("infostash.toml:8: "), "{message}");
    assert!(message.contains("extra"), "{message}");
}

#[test]
fn rejects_another_format() {
    let dir = project(&[(
        "infostash.toml",
        &PROJECT.replace("format = 1", "format = 2"),
    )]);
    assert!(parse_error(&dir).contains("format 2 is not supported"));
}

#[test]
fn a_source_needs_a_string_connector() {
    let missing = shop(&[("sources/items.toml", "path = \"x.csv\"")]);
    assert_eq!(
        parse_error(&missing),
        "sources/items.toml: `connector` is missing"
    );
    let number = shop(&[("sources/items.toml", "connector = 1")]);
    assert_eq!(
        parse_error(&number),
        "sources/items.toml: `connector` must be a string"
    );
}

#[test]
fn rejects_bad_table_names() {
    for name in ["Orders", "1orders", "orders__quarantine", "my-orders"] {
        let dir = shop(&[(&format!("clean/{name}.sql"), "select 1")]);
        let message = parse_error(&dir);
        assert!(
            message.starts_with(&format!("clean/{name}.sql: ")),
            "{message}"
        );
    }
}

#[test]
fn rejects_two_files_with_one_name_in_a_layer() {
    let dir = shop(&[
        ("clean/a/orders.sql", "select * from raw.orders"),
        ("clean/b/orders.sql", "select * from raw.orders"),
    ]);
    assert_eq!(
        parse_error(&dir),
        "clean/a/orders.sql and clean/b/orders.sql both define `orders`"
    );
}

#[test]
fn one_name_may_appear_in_each_layer() {
    let dir = shop(&[
        ("clean/orders.sql", "select * from raw.orders"),
        ("ready/orders.sql", "select * from clean.orders"),
    ]);
    assert_eq!(Project::load(dir.path()).unwrap().models().len(), 2);
}

#[test]
fn rejects_a_read_of_an_undefined_table() {
    let dir = shop(&[("clean/orders.sql", "-- note\nselect *\nfrom raw.items")]);
    assert_eq!(
        parse_error(&dir),
        "clean/orders.sql:3: no source or model defines `bronze.items`"
    );
}

#[test]
fn rejects_a_read_of_a_later_layer() {
    let dir = shop(&[
        ("clean/orders.sql", "select * from ready.revenue"),
        ("ready/revenue.sql", "select * from raw.orders"),
    ]);
    assert_eq!(
        parse_error(&dir),
        "clean/orders.sql:1: `silver.orders` can't read `gold.revenue`, which is in a later layer"
    );
}

#[test]
fn ready_may_read_raw() {
    let dir = shop(&[("ready/orders.sql", "select * from raw.orders")]);
    assert!(Project::load(dir.path()).is_ok());
}

#[test]
fn names_a_cycle() {
    let dir = shop(&[
        ("clean/a.sql", "select * from clean.b"),
        ("clean/b.sql", "select * from\nclean.c"),
        ("clean/c.sql", "select * from clean.b"),
    ]);
    assert_eq!(
        parse_error(&dir),
        "clean/b.sql:2: dependency cycle: silver.b -> silver.c -> silver.b"
    );
}

#[test]
fn a_model_reading_itself_is_a_cycle() {
    let dir = shop(&[("clean/a.sql", "select * from clean.a")]);
    assert_eq!(
        parse_error(&dir),
        "clean/a.sql:1: dependency cycle: silver.a -> silver.a"
    );
}

#[test]
fn rejects_a_bad_directive_with_its_line() {
    let dir = shop(&[(
        "clean/orders.sql",
        "-- top\n-- @partition_by: day\nselect * from raw.orders",
    )]);
    assert_eq!(
        parse_error(&dir),
        "clean/orders.sql:2: unknown directive `@partition_by`"
    );
}

#[test]
fn rejects_sql_that_is_not_one_query() {
    let dir = shop(&[("clean/orders.sql", "select 1; select 2")]);
    assert_eq!(
        parse_error(&dir),
        "clean/orders.sql: a model is one `select` statement"
    );
}

#[test]
fn reports_a_project_file_that_cannot_be_read() {
    let dir = project(&[("infostash.toml/x", "")]);
    assert!(matches!(Project::load(dir.path()), Err(Error::Storage(_))));
}

#[test]
fn reports_a_model_directory_that_cannot_be_read() {
    let dir = project(&[
        ("infostash.toml", PROJECT),
        ("clean", "a file, not a directory"),
    ]);
    assert!(matches!(Project::load(dir.path()), Err(Error::Storage(_))));
}
