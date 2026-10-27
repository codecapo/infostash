//! Week 1 proof: write an Iceberg table on local disk, commit, read it back,
//! read only the rows added since a snapshot, and reopen it with no catalog.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use infostash_model::{Layer, TableRef};
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
