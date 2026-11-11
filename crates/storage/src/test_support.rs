//! Fixtures shared by the crate's tests: a one-column `orders` table and
//! ways to look inside its snapshots.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::spec::ManifestStatus;
use infostash_model::{Layer, SnapshotId, TableRef};

use crate::{Commit, Warehouse, files_added_by};

pub(crate) fn ids_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]))
}

pub(crate) fn ids(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        ids_schema(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

pub(crate) fn position(value: &str) -> HashMap<String, String> {
    HashMap::from([("infostash.position".to_string(), value.to_string())])
}

/// The properties a job commit carries: its id and a checkpoint.
pub(crate) fn job(id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("infostash.job-id".to_string(), id.to_string()),
        ("infostash.position".to_string(), format!("after {id}")),
    ])
}

/// A warehouse in `dir` with an empty `raw.orders` table of ids.
pub(crate) async fn orders(dir: &Path) -> (Warehouse, TableRef) {
    let wh = Warehouse::open_local(dir).await.unwrap();
    let orders = TableRef::new(Layer::Raw, "orders");
    wh.create_table(&orders, &ids_schema()).await.unwrap();
    (wh, orders)
}

/// Every id in `batches`, sorted.
pub(crate) fn all_ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids: Vec<i64> = batches
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
    ids.sort_unstable();
    ids
}

/// Two appends with a position each: ids 1 and 2, then ids 3, 4 and 5.
pub(crate) async fn two_appends(wh: &Warehouse, orders: &TableRef) -> (Commit, Commit) {
    let first = wh
        .append_if_head(orders, &[ids(&[1, 2])], None, &position("p1"))
        .await
        .unwrap();
    let second = wh
        .append_if_head(
            orders,
            &[ids(&[3, 4, 5])],
            Some(first.snapshot),
            &position("p2"),
        )
        .await
        .unwrap();
    (first, second)
}

/// The data files `commit` added, sorted.
pub(crate) async fn files_of(wh: &Warehouse, orders: &TableRef, commit: &Commit) -> Vec<String> {
    let loaded = wh.load(orders).await.unwrap();
    let mut files: Vec<String> =
        files_added_by(&loaded, commit.snapshot, &HashSet::from([commit.snapshot]))
            .await
            .unwrap()
            .into_iter()
            .collect();
    files.sort();
    files
}

/// Every data file any snapshot of the table references, with any status.
pub(crate) async fn referenced_files(wh: &Warehouse, orders: &TableRef) -> HashSet<String> {
    let loaded = wh.load(orders).await.unwrap();
    let mut files = HashSet::new();
    for snapshot in loaded.metadata().snapshots() {
        let list = loaded.manifest_list_reader(snapshot).load().await.unwrap();
        for manifest in list.entries() {
            let manifest = loaded.manifest_reader().read(manifest).await.unwrap();
            for entry in manifest.entries() {
                files.insert(entry.file_path().to_string());
            }
        }
    }
    files
}

/// Status, path, owning snapshot and data sequence number of every entry
/// in `snapshot`'s manifests.
pub(crate) async fn entries_of(
    wh: &Warehouse,
    orders: &TableRef,
    snapshot: SnapshotId,
) -> Vec<(ManifestStatus, String, Option<SnapshotId>, Option<i64>)> {
    let loaded = wh.load(orders).await.unwrap();
    let snapshot = loaded.metadata().snapshot_by_id(snapshot).unwrap();
    let list = loaded.manifest_list_reader(snapshot).load().await.unwrap();
    let mut entries = Vec::new();
    for manifest in list.entries() {
        let manifest = loaded.manifest_reader().read(manifest).await.unwrap();
        for entry in manifest.entries() {
            let path = entry.file_path().to_string();
            let (by, seq) = (entry.snapshot_id(), entry.sequence_number());
            entries.push((entry.status(), path, by, seq));
        }
    }
    entries
}

/// The manifest paths in `snapshot`'s manifest list.
pub(crate) async fn manifests_of(
    wh: &Warehouse,
    orders: &TableRef,
    snapshot: SnapshotId,
) -> Vec<String> {
    let loaded = wh.load(orders).await.unwrap();
    let snapshot = loaded.metadata().snapshot_by_id(snapshot).unwrap();
    let list = loaded.manifest_list_reader(snapshot).load().await.unwrap();
    list.entries()
        .iter()
        .map(|m| m.manifest_path.clone())
        .collect()
}

/// The local path of a `file://` location.
pub(crate) fn local_path(file: &str) -> &Path {
    Path::new(file.strip_prefix("file://").unwrap())
}

/// Paths of the data files in the table's current snapshot, sorted: what a
/// replace that empties the table removes.
pub(crate) async fn live_files(wh: &Warehouse, table: &TableRef) -> Vec<String> {
    let loaded = wh.load(table).await.unwrap();
    if loaded.metadata().current_snapshot().is_none() {
        return Vec::new();
    }
    let tasks: Vec<_> = loaded
        .scan()
        .select_all()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut files: Vec<String> = tasks
        .into_iter()
        .map(|t| t.data_file_path().to_string())
        .collect();
    files.sort();
    files.dedup();
    files
}
