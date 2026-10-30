//! The commands behind `infoctl`. `main.rs` only passes in the arguments and
//! the output streams, so everything here can be tested in-process.

use std::io::Write;
use std::path::PathBuf;

use datafusion::arrow::util::pretty::pretty_format_batches_with_schema;
use infostash_engine::{QueryResult, query};
use infostash_storage::Warehouse;

pub const USAGE: &str = "\
infoctl: medallion pipelines on open tables

USAGE:
    infoctl <command>

COMMANDS:
    init     create a new project
    run      build Raw, Clean and Ready tables (incremental)
    query    run SQL against the lake

QUERY:
    infoctl query [--warehouse <dir>] [--format table|csv|json] \"<sql>\"

    Name tables <layer>.<table>, as raw.orders or bronze.orders. Statements
    that write are rejected. json prints one object per row.
    --warehouse defaults to .infostash/warehouse.
";

/// Where `infoctl init` puts the warehouse (RFC 0001 draft).
const DEFAULT_WAREHOUSE: &str = ".infostash/warehouse";

/// Exit code for a command line infoctl can't parse.
const USAGE_ERROR: i32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Table,
    Csv,
    Json,
}

#[derive(Debug, PartialEq, Eq)]
struct Query {
    warehouse: PathBuf,
    format: Format,
    sql: String,
}

/// Run `infoctl` with `args` (without the program name) and return the
/// exit code: 0 on success, 1 when a command fails, 2 for a usage error.
pub fn run(args: &[String], out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let code = match args.first().map(String::as_str) {
        Some(c @ ("init" | "run")) => {
            let _ = writeln!(err, "infoctl {c}: not implemented yet");
            USAGE_ERROR
        }
        Some("query") => match parse_query(&args[1..]) {
            Ok(q) => run_query(&q, out, err),
            Err(msg) => {
                let _ = writeln!(err, "infoctl query: {msg}\n\n{USAGE}");
                USAGE_ERROR
            }
        },
        _ => match write!(out, "{USAGE}") {
            Ok(()) => 0,
            Err(e) => output_error(err, &e),
        },
    };
    // stdout is buffered, so a reader that closed the pipe can show up here.
    match out.flush() {
        Err(e) if code == 0 => output_error(err, &e),
        _ => code,
    }
}

/// Report a failed write to stdout. The output is incomplete, so the
/// command fails.
fn output_error(err: &mut dyn Write, e: &std::io::Error) -> i32 {
    let _ = writeln!(err, "infoctl: output error: {e}");
    1
}

fn parse_query(args: &[String]) -> Result<Query, String> {
    let mut warehouse = PathBuf::from(DEFAULT_WAREHOUSE);
    let mut format = Format::Table;
    let mut sql = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--warehouse" => {
                warehouse = args.next().ok_or("--warehouse needs a directory")?.into();
            }
            "--format" => {
                format = match args.next().map(String::as_str) {
                    Some("table") => Format::Table,
                    Some("csv") => Format::Csv,
                    Some("json") => Format::Json,
                    _ => return Err("--format must be table, csv or json".into()),
                };
            }
            flag if flag.starts_with("--") => return Err(format!("unknown option {flag}")),
            text if sql.is_none() => sql = Some(text.to_string()),
            _ => return Err("pass the SQL as one quoted argument".into()),
        }
    }
    let sql = sql.ok_or("no SQL given")?;
    Ok(Query {
        warehouse,
        format,
        sql,
    })
}

fn run_query(q: &Query, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    match execute(q).and_then(|result| print(&result, q.format, out)) {
        Ok(()) => 0,
        Err(e) => {
            let _ = writeln!(err, "infoctl query: {e}");
            1
        }
    }
}

fn execute(q: &Query) -> Result<QueryResult, String> {
    // Opening a missing directory would create a catalog there, which a
    // read-only command must not do.
    if !q.warehouse.is_dir() {
        return Err(format!(
            "no warehouse at {} (pass --warehouse)",
            q.warehouse.display()
        ));
    }
    let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    runtime.block_on(async {
        let warehouse = Warehouse::open_local(&q.warehouse)
            .await
            .map_err(|e| e.to_string())?;
        query(&warehouse, &q.sql).await.map_err(|e| e.to_string())
    })
}

fn print(result: &QueryResult, format: Format, out: &mut dyn Write) -> Result<(), String> {
    match format {
        Format::Table => {
            let table = pretty_format_batches_with_schema(result.schema.clone(), &result.batches)
                .map_err(|e| e.to_string())?;
            writeln!(out, "{table}").map_err(|e| e.to_string())
        }
        Format::Csv => {
            let mut writer = arrow_csv::WriterBuilder::new().with_header(true).build(out);
            if result.batches.is_empty() {
                let empty = datafusion::arrow::array::RecordBatch::new_empty(result.schema.clone());
                writer.write(&empty).map_err(|e| e.to_string())?;
            }
            for batch in &result.batches {
                writer.write(batch).map_err(|e| e.to_string())?;
            }
            Ok(())
        }
        Format::Json => {
            let mut writer = arrow_json::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, arrow_json::writer::LineDelimited>(out);
            for batch in &result.batches {
                writer.write(batch).map_err(|e| e.to_string())?;
            }
            writer.finish().map_err(|e| e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn query_flags_parse() {
        let q = parse_query(&args(&["--format", "csv", "--warehouse", "w", "select 1"])).unwrap();
        assert_eq!(
            q,
            Query {
                warehouse: "w".into(),
                format: Format::Csv,
                sql: "select 1".into()
            }
        );
        let q = parse_query(&args(&["select 1"])).unwrap();
        assert_eq!(q.warehouse, PathBuf::from(DEFAULT_WAREHOUSE));
        assert_eq!(q.format, Format::Table);
        assert_eq!(
            parse_query(&args(&["--format", "json", "x"]))
                .unwrap()
                .format,
            Format::Json
        );
        assert_eq!(
            parse_query(&args(&["--format", "table", "x"]))
                .unwrap()
                .format,
            Format::Table
        );
    }

    #[test]
    fn bad_query_flags_are_usage_errors() {
        for (bad, msg) in [
            (&[][..], "no SQL"),
            (&["--warehouse"][..], "needs a directory"),
            (&["--format", "xml", "x"][..], "table, csv or json"),
            (&["--verbose", "x"][..], "unknown option --verbose"),
            (&["select", "1"][..], "one quoted argument"),
        ] {
            let e = parse_query(&args(bad)).unwrap_err();
            assert!(e.contains(msg), "{bad:?}: {e}");
        }
    }

    /// Stdout after the reader closed the pipe. With `buffered`, writes
    /// still succeed and only the flush fails.
    struct ClosedPipe {
        buffered: bool,
    }

    impl Write for ClosedPipe {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.buffered {
                Ok(buf.len())
            } else {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
    }

    #[test]
    fn output_that_cannot_be_delivered_fails_the_command() {
        for buffered in [true, false] {
            let mut err = Vec::new();
            let code = run(&[], &mut ClosedPipe { buffered }, &mut err);
            assert_eq!(code, 1, "buffered={buffered}");
            let err = String::from_utf8(err).unwrap();
            assert!(err.contains("output error"), "{err}");
        }
        // A command that already failed keeps its own exit code.
        let mut err = Vec::new();
        let code = run(
            &args(&["query"]),
            &mut ClosedPipe { buffered: true },
            &mut err,
        );
        assert_eq!(code, USAGE_ERROR);
    }
}
