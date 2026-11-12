//! The tables a model's query reads, found in its parsed SQL.

use std::collections::{BTreeMap, HashSet};
use std::ops::ControlFlow;

use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::{
    Ident, ObjectName, Query, Statement as SqlStatement, TableFactor, Visit, Visitor,
};
use infostash_model::{Error, Layer, Result, TableRef};

/// The tables a model's SQL reads, each with the first line that names it.
/// The SQL must be one query; names of its CTEs are not tables.
pub(crate) fn tables_read(file: &str, sql: &str) -> Result<BTreeMap<TableRef, usize>> {
    let statements = DFParser::parse_sql(sql).map_err(|e| Error::Parse(format!("{file}: {e}")))?;
    let query = match Vec::from(statements).as_slice() {
        [Statement::Statement(statement)] => match statement.as_ref() {
            SqlStatement::Query(query) => query.clone(),
            _ => return Err(not_one_query(file)),
        },
        _ => return Err(not_one_query(file)),
    };
    let mut names = Names::default();
    let _ = query.visit(&mut names);
    let mut tables = BTreeMap::new();
    for name in &names.tables {
        if let Some((table, line)) = resolve(file, name, &names.ctes)? {
            tables.entry(table).or_insert(line);
        }
    }
    Ok(tables)
}

fn not_one_query(file: &str) -> Error {
    Error::Parse(format!("{file}: a model is one `select` statement"))
}

/// The CTE names and table names in a query, collected in one walk.
#[derive(Default)]
struct Names {
    ctes: HashSet<String>,
    tables: Vec<ObjectName>,
}

impl Visitor for Names {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            self.ctes
                .extend(with.cte_tables.iter().map(|cte| normalise(&cte.alias.name)));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        // A table function such as `generate_series(1, 3)` reads no table.
        if let TableFactor::Table {
            name, args: None, ..
        } = factor
        {
            self.tables.push(name.clone());
        }
        ControlFlow::Continue(())
    }
}

/// The table a name refers to and its line, or `None` for a CTE.
fn resolve(
    file: &str,
    name: &ObjectName,
    ctes: &HashSet<String>,
) -> Result<Option<(TableRef, usize)>> {
    let parts: Vec<&Ident> = name.0.iter().filter_map(|part| part.as_ident()).collect();
    let line = parts.first().map_or(0, |ident| {
        usize::try_from(ident.span.start.line).unwrap_or(0)
    });
    let err = |message: String| Error::Parse(format!("{file}:{line}: {message}"));
    if parts.len() != name.0.len() {
        return Err(err(format!("`{name}` is not a table name")));
    }
    match parts.as_slice() {
        [table] if ctes.contains(&normalise(table)) => Ok(None),
        [table] => {
            let table = normalise(table);
            Err(err(format!(
                "`{table}` needs its layer, as in `raw.{table}`, `clean.{table}` or `ready.{table}`"
            )))
        }
        [layer, table] => match Layer::from_namespace(&normalise(layer)) {
            Some(layer) => Ok(Some((TableRef::new(layer, normalise(table)), line))),
            None => Err(err(format!(
                "`{name}`: unknown layer `{}`; use raw, clean or ready",
                normalise(layer)
            ))),
        },
        _ => Err(err(format!("`{name}`: name a table as `<layer>.<table>`"))),
    }
}

/// An identifier as DataFusion resolves it: unquoted names are lower case.
fn normalise(ident: &Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(sql: &str) -> Vec<(String, usize)> {
        tables_read("m.sql", sql)
            .unwrap()
            .into_iter()
            .map(|(table, line)| (table.to_string(), line))
            .collect()
    }

    fn read_err(sql: &str) -> String {
        match tables_read("m.sql", sql) {
            Err(Error::Parse(message)) => message,
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn finds_tables_in_joins_and_subqueries_with_their_first_line() {
        let sql = "select *\nfrom raw.a\njoin (select * from clean.b) b on true\n\
                   where exists (select 1 from raw.a)\nunion all select * from gold.c;";
        assert_eq!(
            read(sql),
            [
                ("bronze.a".to_string(), 2),
                ("silver.b".to_string(), 3),
                ("gold.c".to_string(), 5)
            ]
        );
    }

    #[test]
    fn cte_names_are_not_tables() {
        let sql = "with recent as (select * from raw.orders)\nselect * from recent";
        assert_eq!(read(sql), [("bronze.orders".to_string(), 1)]);
    }

    #[test]
    fn table_functions_read_no_table() {
        assert!(read("select * from generate_series(1, 3)").is_empty());
    }

    #[test]
    fn unquoted_names_are_lower_cased() {
        assert_eq!(
            read("select * from RAW.Orders"),
            [("bronze.orders".to_string(), 1)]
        );
        assert_eq!(
            read("select * from raw.\"Orders\""),
            [("bronze.Orders".to_string(), 1)]
        );
    }

    #[test]
    fn rejects_names_without_a_known_layer() {
        assert_eq!(
            read_err("select *\nfrom orders"),
            "m.sql:2: `orders` needs its layer, as in `raw.orders`, `clean.orders` or `ready.orders`"
        );
        assert_eq!(
            read_err("select * from staging.orders"),
            "m.sql:1: `staging.orders`: unknown layer `staging`; use raw, clean or ready"
        );
        assert_eq!(
            read_err("select * from cat.raw.orders"),
            "m.sql:1: `cat.raw.orders`: name a table as `<layer>.<table>`"
        );
    }

    #[test]
    fn a_model_is_one_query() {
        for sql in ["select 1; select 2", "create table x (a int)", ""] {
            assert_eq!(
                read_err(sql),
                "m.sql: a model is one `select` statement",
                "{sql}"
            );
        }
        assert!(read_err("select (").starts_with("m.sql: "));
    }
}
