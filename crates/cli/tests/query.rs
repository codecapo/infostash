//! `infoctl query` against a small committed table, run as a user would.

#![allow(clippy::unwrap_used)]

use std::process::{Command, Output};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use infostash_model::{Layer, TableRef};
use infostash_storage::Warehouse;

/// A warehouse with `ready.daily_revenue` holding two days, one with no
/// region.
fn warehouse() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("day", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["2026-10-01", "2026-10-02"])),
            Arc::new(StringArray::from(vec![Some("north"), None])),
            Arc::new(Int64Array::from(vec![120, 80])),
        ],
    )
    .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let wh = Warehouse::open_local(dir.path()).await.unwrap();
        let table = TableRef::new(Layer::Ready, "daily_revenue");
        wh.create_table(&table, &schema).await.unwrap();
        wh.append(&table, &[batch]).await.unwrap();
    });
    dir
}

fn infoctl(dir: &tempfile::TempDir, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_infoctl"))
        .arg("query")
        .arg("--warehouse")
        .arg(dir.path())
        .args(args)
        .output()
        .unwrap()
}

fn stdout(out: &Output) -> String {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout.clone()).unwrap()
}

const SQL: &str = "select day, region, amount from ready.daily_revenue order by day";

#[test]
fn prints_a_table_by_default() {
    let dir = warehouse();
    let text = stdout(&infoctl(&dir, &[SQL]));
    assert!(text.contains("| day        | region | amount |"), "{text}");
    assert!(text.contains("| 2026-10-01 | north  | 120    |"), "{text}");
}

#[test]
fn prints_csv() {
    let dir = warehouse();
    let text = stdout(&infoctl(&dir, &["--format", "csv", SQL]));
    assert_eq!(
        text,
        "day,region,amount\n2026-10-01,north,120\n2026-10-02,,80\n"
    );
}

#[test]
fn prints_one_json_object_per_row() {
    let dir = warehouse();
    let text = stdout(&infoctl(&dir, &["--format", "json", SQL]));
    assert_eq!(
        text,
        "{\"day\":\"2026-10-01\",\"region\":\"north\",\"amount\":120}\n\
         {\"day\":\"2026-10-02\",\"region\":null,\"amount\":80}\n"
    );
}

#[test]
fn an_empty_result_still_shows_its_columns() {
    let dir = warehouse();
    let none = "select day from gold.daily_revenue where amount < 0";
    assert!(stdout(&infoctl(&dir, &[none])).contains("| day |"));
    assert_eq!(stdout(&infoctl(&dir, &["--format", "csv", none])), "day\n");
    assert_eq!(stdout(&infoctl(&dir, &["--format", "json", none])), "");
}

#[test]
fn a_write_is_rejected_and_changes_nothing() {
    let dir = warehouse();
    let out = infoctl(
        &dir,
        &["insert into ready.daily_revenue values ('2026-10-03', 'x', 1)"],
    );
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("not supported"), "{stderr}");
    let count = "select count(*) as n from ready.daily_revenue";
    assert_eq!(
        stdout(&infoctl(&dir, &["--format", "csv", count])),
        "n\n2\n"
    );
}

#[test]
fn an_unknown_table_lists_the_tables() {
    let dir = warehouse();
    let out = infoctl(&dir, &["select * from ready.revenue"]);
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("tables: gold.daily_revenue"), "{stderr}");
}

#[test]
fn a_missing_warehouse_is_an_error_and_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope");
    let out = Command::new(env!("CARGO_BIN_EXE_infoctl"))
        .args(["query", "--warehouse"])
        .arg(&missing)
        .arg("select 1")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("no warehouse at"), "{stderr}");
    assert!(!missing.exists());
}
