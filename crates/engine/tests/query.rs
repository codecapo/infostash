//! SQL over warehouse tables, through the same path `infoctl query` uses.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use infostash_engine::query;
use infostash_model::{Error, Layer, TableRef};
use infostash_storage::Warehouse;

fn revenue() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("day", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["2026-10-01", "2026-10-02"])),
            Arc::new(Int64Array::from(vec![120, 80])),
        ],
    )
    .unwrap()
}

async fn warehouse(dir: &tempfile::TempDir) -> Warehouse {
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    let table = TableRef::new(Layer::Ready, "daily_revenue");
    wh.create_table(&table, &revenue().schema()).await.unwrap();
    wh.append(&table, &[revenue()]).await.unwrap();
    wh
}

fn text(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

#[tokio::test]
async fn a_layer_reads_the_same_by_namespace_and_alias() {
    let dir = tempfile::tempdir().unwrap();
    let wh = warehouse(&dir).await;
    for name in ["gold.daily_revenue", "ready.daily_revenue"] {
        let sql = format!("select day, amount from {name} order by day");
        let out = query(&wh, &sql).await.unwrap();
        let shown = text(&out.batches);
        assert!(
            shown.contains("2026-10-01") && shown.contains("120"),
            "{shown}"
        );
        assert_eq!(out.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }
}

#[tokio::test]
async fn sql_runs_over_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let wh = warehouse(&dir).await;
    let out = query(&wh, "select sum(amount) as total from ready.daily_revenue")
        .await
        .unwrap();
    assert!(text(&out.batches).contains("200"));
}

#[tokio::test]
async fn an_empty_result_keeps_its_schema() {
    let dir = tempfile::tempdir().unwrap();
    let wh = warehouse(&dir).await;
    let out = query(
        &wh,
        "select day from ready.daily_revenue where amount > 1000",
    )
    .await
    .unwrap();
    assert_eq!(out.schema.field(0).name(), "day");
    assert_eq!(out.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn a_table_with_no_snapshot_reads_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    let table = TableRef::new(Layer::Raw, "listings");
    wh.create_table(&table, &revenue().schema()).await.unwrap();
    let out = query(&wh, "select count(*) as n from raw.listings")
        .await
        .unwrap();
    assert!(
        text(&out.batches).contains("| 0 "),
        "{}",
        text(&out.batches)
    );
}

#[tokio::test]
async fn statements_that_write_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let wh = warehouse(&dir).await;
    let copied = dir.path().join("copied.csv");
    let copy = format!("copy (select 1) to '{}'", copied.display());
    for sql in [
        copy.as_str(),
        "insert into ready.daily_revenue values ('2026-10-03', 5)",
        "create table ready.copy as select * from ready.daily_revenue",
        "drop table ready.daily_revenue",
        "set datafusion.execution.batch_size = 1",
    ] {
        let err = query(&wh, sql).await.err().unwrap();
        assert!(matches!(err, Error::Plan(_)), "{sql}: {err}");
        assert!(err.to_string().contains("not supported"), "{sql}: {err}");
    }
    assert!(!copied.exists());
    let table = TableRef::new(Layer::Ready, "daily_revenue");
    assert_eq!(wh.read_all(&table).await.unwrap()[0].num_rows(), 2);
    assert_eq!(wh.tables().await.unwrap(), vec![table]);
}

#[tokio::test]
async fn an_unknown_table_lists_the_tables_that_exist() {
    let dir = tempfile::tempdir().unwrap();
    let wh = warehouse(&dir).await;
    let err = query(&wh, "select * from ready.missing")
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
    assert_eq!(
        err.to_string(),
        "not found: table ready.missing; tables: gold.daily_revenue"
    );
}

#[tokio::test]
async fn an_empty_warehouse_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    let err = query(&wh, "select * from raw.x").await.err().unwrap();
    assert!(err.to_string().contains("no tables"), "{err}");
}

#[tokio::test]
async fn bad_sql_is_a_parse_error() {
    let dir = tempfile::tempdir().unwrap();
    let wh = warehouse(&dir).await;
    let err = query(&wh, "select (1").await.err().unwrap();
    assert!(matches!(err, Error::Parse(_)), "{err}");
}

#[tokio::test]
async fn a_failing_expression_is_an_execution_error() {
    let dir = tempfile::tempdir().unwrap();
    let wh = warehouse(&dir).await;
    let err = query(&wh, "select cast(day as int) from ready.daily_revenue")
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::Execution(_)), "{err}");
}
