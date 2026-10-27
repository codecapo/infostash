//! Compiles a project of plain SQL files into a dependency graph and
//! incremental plans. Dependencies come from the table names each query
//! reads; settings come from `-- @key: value` comments, so there is no
//! templating language to learn.

use std::collections::BTreeMap;

/// Settings parsed from `-- @key: value` comment lines.
pub fn directives(sql: &str) -> BTreeMap<String, String> {
    sql.lines()
        .filter_map(|l| l.trim().strip_prefix("--"))
        .filter_map(|l| l.trim().strip_prefix('@'))
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_directives() {
        let sql = "-- @unique_key: order_id\n-- plain comment\n-- @partition_by: day\nselect 1";
        let d = directives(sql);
        assert_eq!(d["unique_key"], "order_id");
        assert_eq!(d["partition_by"], "day");
        assert_eq!(d.len(), 2);
    }
}
