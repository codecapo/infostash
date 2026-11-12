//! The header grammar of RFC 0001: `-- @key: value` comments above the SQL.

use infostash_model::{Error, Result};

/// A model's settings, read from the `-- @key: value` comments in its
/// header. Which of them a run accepts is decided elsewhere.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Directives {
    /// `@unique_key`: the key columns, in the order written.
    pub unique_key: Option<Vec<String>>,
    /// `@check`: every check, in file order.
    pub checks: Vec<Check>,
    /// `@full_refresh: true`.
    pub full_refresh: bool,
    /// `@description`.
    pub description: Option<String>,
}

/// One `@check` directive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    /// The name written between `@check` and the colon, if any.
    pub name: Option<String>,
    /// The SQL boolean expression, as written.
    pub expr: String,
    /// The line of the model file it is on, counting from 1.
    pub line: usize,
}

/// Read the directives in the header of a model file. The header is the run
/// of blank and `--` lines at the top; a directive starts at column one with
/// `-- @`, and anything after the header is SQL, not a directive.
pub(crate) fn parse(file: &str, sql: &str) -> Result<Directives> {
    let mut directives = Directives::default();
    let mut once = Vec::new();
    for (index, line) in sql.lines().enumerate() {
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") {
            break;
        }
        let Some(rest) = line.strip_prefix("-- @") else {
            continue;
        };
        let err = |message: String| Error::Parse(format!("{file}:{}: {message}", index + 1));
        let (head, value) = rest
            .split_once(':')
            .ok_or_else(|| err(format!("`-- @{}` needs a `:` and a value", rest.trim())))?;
        let value = value.trim();
        let mut words = head.split_whitespace();
        let key = words.next().unwrap_or_default();
        let name = words.next();
        if words.next().is_some() {
            return Err(err(format!(
                "`@{}`: expected `@key` or `@check name`",
                head.trim()
            )));
        }
        if !matches!(key, "unique_key" | "check" | "full_refresh" | "description") {
            return Err(err(format!("unknown directive `@{key}`")));
        }
        if value.is_empty() {
            return Err(err(format!("`@{key}` needs a value")));
        }
        if key == "check" {
            directives
                .checks
                .push(check(name, value, index + 1).map_err(err)?);
            continue;
        }
        if name.is_some() {
            return Err(err(format!("only `@check` takes a name, not `@{key}`")));
        }
        if once.contains(&key) {
            return Err(err(format!("`@{key}` may appear only once")));
        }
        once.push(key);
        match key {
            "unique_key" => directives.unique_key = Some(columns(value).map_err(err)?),
            "full_refresh" if value == "true" => directives.full_refresh = true,
            "full_refresh" => {
                return Err(err(format!(
                    "`@full_refresh` takes only `true`, not `{value}`"
                )));
            }
            _ => directives.description = Some(value.to_string()),
        }
    }
    check_unique_names(&directives.checks)
        .map_err(|(line, message)| Error::Parse(format!("{file}:{line}: {message}")))?;
    Ok(directives)
}

fn check(name: Option<&str>, expr: &str, line: usize) -> std::result::Result<Check, String> {
    if let Some(name) = name.filter(|n| !crate::is_name(n)) {
        return Err(format!(
            "check name `{name}` must use lower case letters, digits and `_`, and start with a letter"
        ));
    }
    Ok(Check {
        name: name.map(str::to_string),
        expr: expr.to_string(),
        line,
    })
}

fn columns(value: &str) -> std::result::Result<Vec<String>, String> {
    let columns: Vec<String> = value.split(',').map(|c| c.trim().to_string()).collect();
    if columns.iter().any(String::is_empty) {
        return Err(format!("`@unique_key: {value}` has an empty column"));
    }
    Ok(columns)
}

fn check_unique_names(checks: &[Check]) -> std::result::Result<(), (usize, String)> {
    for (i, check) in checks.iter().enumerate() {
        let Some(name) = &check.name else { continue };
        if checks[..i].iter().any(|c| c.name.as_ref() == Some(name)) {
            return Err((check.line, format!("two checks are named `{name}`")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(sql: &str) -> Directives {
        parse("m.sql", sql).unwrap()
    }

    fn parse_err(sql: &str) -> String {
        match parse("m.sql", sql) {
            Err(Error::Parse(message)) => message,
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn values_may_hold_colons_and_are_trimmed() {
        let d = parse_ok("--  \n-- @check:   amount::decimal > 0  \nselect 1");
        assert_eq!(d.checks[0].expr, "amount::decimal > 0");
        assert_eq!(d.checks[0].line, 2);
    }

    #[test]
    fn splits_a_compound_unique_key() {
        let d = parse_ok("-- @unique_key: order_id, line_no\nselect 1");
        assert_eq!(
            d.unique_key,
            Some(vec!["order_id".into(), "line_no".into()])
        );
    }

    #[test]
    fn other_comment_shapes_are_ordinary_comments() {
        let d = parse_ok("--@unique_key: a\n  -- @unique_key: b\n-- plain\n\nselect 1");
        assert_eq!(d, Directives::default());
    }

    #[test]
    fn a_directive_after_the_sql_starts_is_a_comment() {
        let d = parse_ok("select 1\n-- @nonsense: here\n-- @check: x");
        assert_eq!(d, Directives::default());
    }

    #[test]
    fn rejects_malformed_directives() {
        let cases = [
            ("-- @unique_key", "`-- @unique_key` needs a `:` and a value"),
            (
                "-- @check a b: x",
                "`@check a b`: expected `@key` or `@check name`",
            ),
            ("-- @Unique_key: a", "unknown directive `@Unique_key`"),
            ("-- @description:  ", "`@description` needs a value"),
            (
                "-- @unique_key k: a",
                "only `@check` takes a name, not `@unique_key`",
            ),
            (
                "-- @unique_key: a,,b",
                "`@unique_key: a,,b` has an empty column",
            ),
            (
                "-- @full_refresh: yes",
                "`@full_refresh` takes only `true`, not `yes`",
            ),
            (
                "-- @check Bad: x",
                "check name `Bad` must use lower case letters, digits and `_`, and start with a letter",
            ),
        ];
        for (sql, message) in cases {
            assert_eq!(parse_err(sql), format!("m.sql:1: {message}"), "{sql}");
        }
    }

    #[test]
    fn rejects_a_repeated_directive() {
        let sql = "-- @description: a\n-- @description: b";
        assert_eq!(
            parse_err(sql),
            "m.sql:2: `@description` may appear only once"
        );
    }

    #[test]
    fn checks_may_repeat_but_names_may_not() {
        let d = parse_ok("-- @check: a\n-- @check: b\n-- @check x: c");
        assert_eq!(d.checks.len(), 3);
        let sql = "-- @check x: a\n-- @check: b\n-- @check x: c";
        assert_eq!(parse_err(sql), "m.sql:3: two checks are named `x`");
    }
}
