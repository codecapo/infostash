//! Week 1 proof: write an Iceberg table on local disk, commit, read it back,
//! read only the rows added since a snapshot, reopen it with no catalog, and
//! reopen the warehouse in a later run.

// Helpers outside #[test] fns unwrap too; a failure there is a test failure.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use infostash_model::{Error, Layer, SnapshotRange, TableRef};
use infostash_storage::{Warehouse, read_metadata_file};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn write_commit_read_incremental_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    let orders = TableRef::new(Layer::Raw, "orders");

    wh.create_table(&orders, &schema()).await.unwrap();
    assert!(wh.read_all(&orders).await.unwrap().is_empty());

    let first = wh
        .append(&orders, &[batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();
    assert_eq!(first.rows, 3);
    assert_eq!(ids(&wh.read_all(&orders).await.unwrap()), [1, 2, 3]);

    let second = wh
        .append(&orders, &[batch(&[4, 5], &["d", "e"])])
        .await
        .unwrap();
    assert_ne!(first.snapshot, second.snapshot);
    assert_eq!(
        wh.snapshots(&orders).await.unwrap(),
        [first.snapshot, second.snapshot]
    );
    assert_eq!(ids(&wh.read_all(&orders).await.unwrap()), [1, 2, 3, 4, 5]);

    // Incremental: only what the second commit added.
    let since = wh.read_since(&orders, Some(first.snapshot)).await.unwrap();
    assert_eq!(ids(&since), [4, 5]);
    let none = wh.read_since(&orders, Some(second.snapshot)).await.unwrap();
    assert!(ids(&none).is_empty());
    let all = wh.read_since(&orders, None).await.unwrap();
    assert_eq!(ids(&all), [1, 2, 3, 4, 5]);

    // Durable: drop the catalog and read from the metadata file alone.
    drop(wh);
    assert!(second.metadata_location.contains("bronze"));
    let reread = read_metadata_file(&orders, &second.metadata_location)
        .await
        .unwrap();
    assert_eq!(ids(&reread), [1, 2, 3, 4, 5]);
    let old = read_metadata_file(&orders, &first.metadata_location)
        .await
        .unwrap();
    assert_eq!(ids(&old), [1, 2, 3]);
}

#[tokio::test]
async fn tables_share_a_layer_and_names_are_unique() {
    let dir = tempfile::tempdir().unwrap();
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    let orders = TableRef::new(Layer::Raw, "orders");
    let customers = TableRef::new(Layer::Raw, "customers");

    wh.create_table(&orders, &schema()).await.unwrap();
    wh.create_table(&customers, &schema()).await.unwrap();
    assert!(wh.create_table(&orders, &schema()).await.is_err());

    // An empty table has nothing to read, and no snapshot to read after.
    assert!(wh.read_since(&customers, None).await.unwrap().is_empty());
    assert!(matches!(
        wh.read_since(&customers, Some(1)).await,
        Err(Error::NotFound(_))
    ));
    assert!(wh.snapshots(&customers).await.unwrap().is_empty());
}

#[tokio::test]
async fn missing_tables_and_directories_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        Warehouse::open_local(&dir.path().join("missing"))
            .await
            .is_err()
    );

    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    let ghost = TableRef::new(Layer::Ready, "ghost");
    assert!(wh.read_all(&ghost).await.is_err());
    assert!(wh.append(&ghost, &[]).await.is_err());
}

#[tokio::test]
async fn tables_survive_reopening_the_warehouse() {
    let dir = tempfile::tempdir().unwrap();
    let orders = TableRef::new(Layer::Raw, "orders");

    // First run: create the table and commit.
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    wh.create_table(&orders, &schema()).await.unwrap();
    let first = wh
        .append(&orders, &[batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();
    drop(wh);
    assert!(dir.path().join("catalog.db").exists());

    // Second run: the table is still there and takes another commit.
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    assert_eq!(ids(&wh.read_all(&orders).await.unwrap()), [1, 2]);
    let second = wh.append(&orders, &[batch(&[3], &["c"])]).await.unwrap();
    drop(wh);

    // Third run sees both commits in the snapshot log.
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    assert_eq!(
        wh.snapshots(&orders).await.unwrap(),
        [first.snapshot, second.snapshot]
    );
    assert_eq!(ids(&wh.read_all(&orders).await.unwrap()), [1, 2, 3]);
    let since = wh.read_since(&orders, Some(first.snapshot)).await.unwrap();
    assert_eq!(ids(&since), [3]);
    assert!(wh.create_table(&orders, &schema()).await.is_err());
}

#[tokio::test]
async fn odd_directory_names_keep_their_own_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let orders = TableRef::new(Layer::Raw, "orders");

    // sqlx decodes %2F as "/", so an unescaped path would open "a/b".
    for name in ["a%2Fb", "what?", "num#1", "100%"] {
        let root = dir.path().join(name);
        std::fs::create_dir(&root).unwrap();
        let wh = Warehouse::open_local(&root).await.unwrap();
        wh.create_table(&orders, &schema()).await.unwrap();
        wh.append(&orders, &[batch(&[1], &["a"])]).await.unwrap();
        drop(wh);

        assert!(root.join("catalog.db").exists(), "{name}");
        let wh = Warehouse::open_local(&root).await.unwrap();
        assert_eq!(ids(&wh.read_all(&orders).await.unwrap()), [1], "{name}");
    }
}

async fn scan(wh: &Warehouse, table: &TableRef, from: Option<i64>, to: i64) -> Vec<i64> {
    let stream = wh
        .scan_range(table, SnapshotRange { from, to })
        .await
        .unwrap();
    ids(&stream.try_collect::<Vec<_>>().await.unwrap())
}

#[tokio::test]
async fn scan_range_reads_only_the_snapshots_in_the_range() {
    let dir = tempfile::tempdir().unwrap();
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    let orders = TableRef::new(Layer::Raw, "orders");
    wh.create_table(&orders, &schema()).await.unwrap();
    let first = wh
        .append(&orders, &[batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();
    let second = wh.append(&orders, &[batch(&[3], &["c"])]).await.unwrap();
    let third = wh
        .append(&orders, &[batch(&[4, 5], &["d", "e"])])
        .await
        .unwrap();
    let (first, second, third) = (first.snapshot, second.snapshot, third.snapshot);

    // The last two appends, and nothing from the first.
    assert_eq!(scan(&wh, &orders, Some(first), third).await, [3, 4, 5]);
    // `to` is pinned: rows committed after it are not read.
    assert_eq!(scan(&wh, &orders, Some(first), second).await, [3]);
    // A full read as of a snapshot.
    assert_eq!(scan(&wh, &orders, None, second).await, [1, 2, 3]);
    assert_eq!(scan(&wh, &orders, None, third).await, [1, 2, 3, 4, 5]);
    // An empty range.
    assert!(scan(&wh, &orders, Some(third), third).await.is_empty());
}

#[tokio::test]
async fn scan_range_rejects_snapshots_outside_the_history() {
    let dir = tempfile::tempdir().unwrap();
    let wh = Warehouse::open_local(dir.path()).await.unwrap();
    let orders = TableRef::new(Layer::Raw, "orders");
    wh.create_table(&orders, &schema()).await.unwrap();
    let first = wh.append(&orders, &[batch(&[1], &["a"])]).await.unwrap();
    let second = wh.append(&orders, &[batch(&[2], &["b"])]).await.unwrap();
    let (first, second) = (first.snapshot, second.snapshot);

    let not_found = |r: infostash_model::Result<()>| matches!(r, Err(Error::NotFound(_)));
    // A made-up snapshot must not fall back to reading every row (#30).
    assert!(not_found(wh.read_since(&orders, Some(42)).await.map(drop)));
    let range = |from, to| SnapshotRange { from, to };
    assert!(not_found(
        wh.scan_range(&orders, range(Some(42), second))
            .await
            .map(drop)
    ));
    assert!(not_found(
        wh.scan_range(&orders, range(None, 42)).await.map(drop)
    ));
    assert!(not_found(
        wh.scan_range(&orders, range(Some(42), 42)).await.map(drop)
    ));
    // A range that runs backwards: `second` is not an ancestor of `first`.
    assert!(not_found(
        wh.scan_range(&orders, range(Some(second), first))
            .await
            .map(drop)
    ));
}
