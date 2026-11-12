//! Reading a table: everything in a snapshot, or only the rows that
//! appends added after an earlier snapshot, which is what incremental
//! runs consume.

use std::collections::HashSet;

use arrow_array::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use iceberg::io::FileIO;
use iceberg::spec::{ManifestStatus, Operation, TableMetadata};
use iceberg::table::{StaticTable, Table};
use infostash_model::{Error, Result, SnapshotId, SnapshotRange, TableRef};

use crate::{RecordBatchStream, Warehouse, ident, storage};

impl Warehouse {
    /// Every row in the table's current snapshot.
    pub async fn read_all(&self, table: &TableRef) -> Result<Vec<RecordBatch>> {
        scan_at(&self.load(table).await?, None, None)
            .await?
            .try_collect()
            .await
    }

    /// Rows committed after `range.from` up to and including `range.to`, as
    /// a stream. `from == None` reads the whole table as of `to`. This is
    /// what incremental runs consume.
    ///
    /// Only appends can be read this way. It is an error if a snapshot in
    /// the range replaced or deleted files (a merge or a compaction), because
    /// its added files would hold rows that are not new. It is also an error
    /// if `from` is not an ancestor of `to`, rather than reading everything.
    pub async fn scan_range(
        &self,
        table: &TableRef,
        range: SnapshotRange,
    ) -> Result<RecordBatchStream> {
        let loaded = self.load(table).await?;
        let metadata = loaded.metadata();
        let only = match range.from {
            None => {
                if metadata.snapshot_by_id(range.to).is_none() {
                    return Err(Error::NotFound(format!("{table}: snapshot {}", range.to)));
                }
                None
            }
            Some(from) => {
                let snapshots = appends_between(metadata, table, from, range.to)?;
                if snapshots.is_empty() {
                    return Ok(futures::stream::empty().boxed());
                }
                Some(files_added_by(&loaded, range.to, &snapshots).await?)
            }
        };
        scan_at(&loaded, Some(range.to), only).await
    }

    /// Rows committed after snapshot `after`, up to the current snapshot.
    /// `None` reads everything. See [`Warehouse::scan_range`] for the errors.
    pub async fn read_since(
        &self,
        table: &TableRef,
        after: Option<SnapshotId>,
    ) -> Result<Vec<RecordBatch>> {
        let current = self.load(table).await?.metadata().current_snapshot_id();
        let range = match (current, after) {
            (Some(to), from) => SnapshotRange { from, to },
            (None, None) => return Ok(Vec::new()),
            (None, Some(from)) => {
                return Err(Error::NotFound(format!(
                    "{table}: snapshot {from} (the table has no snapshots)"
                )));
            }
        };
        self.scan_range(table, range).await?.try_collect().await
    }
}

/// Read a table straight from an Iceberg metadata file, with no catalog.
/// Proves the data is durable on disk, and lets other engines read our output.
pub async fn read_metadata_file(table: &TableRef, location: &str) -> Result<Vec<RecordBatch>> {
    let file_io = FileIO::new_with_fs();
    let table = StaticTable::from_metadata_file(location, ident(table), file_io)
        .await
        .map_err(storage)?
        .into_table();
    scan_at(&table, None, None).await?.try_collect().await
}

/// The snapshots after `from` up to and including `to`, walking back from
/// `to`. Fails if one of them is not an append, or if the walk never reaches
/// `from`.
fn appends_between(
    metadata: &TableMetadata,
    table: &TableRef,
    from: SnapshotId,
    to: SnapshotId,
) -> Result<HashSet<SnapshotId>> {
    let mut snapshots = HashSet::new();
    let mut cursor = Some(to);
    while let Some(id) = cursor {
        // Look the snapshot up first, so a made-up `from == to` is not found.
        let snapshot = metadata
            .snapshot_by_id(id)
            .ok_or_else(|| Error::NotFound(format!("{table}: snapshot {id}")))?;
        if id == from {
            return Ok(snapshots);
        }
        let operation = &snapshot.summary().operation;
        if *operation != Operation::Append {
            return Err(Error::Storage(format!(
                "{table}: snapshot {id} is a `{}`, not an append, so it can't be read \
                 incrementally",
                operation.as_str()
            )));
        }
        snapshots.insert(id);
        cursor = snapshot.parent_snapshot_id();
    }
    Err(Error::NotFound(format!(
        "{table}: snapshot {from} is not an ancestor of snapshot {to}"
    )))
}

/// Paths of the data files that `snapshots` added, as listed by snapshot `to`.
pub(crate) async fn files_added_by(
    table: &Table,
    to: SnapshotId,
    snapshots: &HashSet<SnapshotId>,
) -> Result<HashSet<String>> {
    let metadata = table.metadata();
    let to = metadata
        .snapshot_by_id(to)
        .ok_or_else(|| Error::NotFound(format!("snapshot {to}")))?;
    let list = table
        .manifest_list_reader(to)
        .load()
        .await
        .map_err(storage)?;
    let mut files = HashSet::new();
    for manifest in list.entries() {
        if !snapshots.contains(&manifest.added_snapshot_id) {
            continue;
        }
        let manifest = table
            .manifest_reader()
            .read(manifest)
            .await
            .map_err(storage)?;
        for entry in manifest.entries() {
            let added_here = entry.snapshot_id().is_some_and(|s| snapshots.contains(&s));
            if added_here && entry.status() == ManifestStatus::Added {
                files.insert(entry.file_path().to_string());
            }
        }
    }
    Ok(files)
}

/// Scan the table as of snapshot `at` (the current one if `None`),
/// optionally limited to some data files.
pub(crate) async fn scan_at(
    table: &Table,
    at: Option<SnapshotId>,
    only: Option<HashSet<String>>,
) -> Result<RecordBatchStream> {
    let mut scan = table.scan().select_all();
    if let Some(at) = at {
        scan = scan.snapshot_id(at);
    }
    let tasks = scan
        .build()
        .map_err(storage)?
        .plan_files()
        .await
        .map_err(storage)?;
    let tasks = match only {
        None => tasks,
        Some(only) => tasks
            .try_filter(move |t| std::future::ready(only.contains(t.data_file_path())))
            .boxed(),
    };
    Ok(table
        .reader_builder()
        .build()
        .read(tasks)
        .map_err(storage)?
        .stream()
        .map_err(storage)
        .boxed())
}

#[cfg(test)]
mod tests {
    use infostash_model::{Error, Result, SnapshotRange};

    use super::appends_between;
    use crate::Replacement;
    use crate::test_support::{all_ids, files_of, ids, job, orders, position, two_appends};

    #[tokio::test]
    async fn an_overwrite_in_the_range_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (first, second) = two_appends(&wh, &orders).await;
        let replacement = Replacement {
            remove: files_of(&wh, &orders, &second).await,
            batches: vec![ids(&[30])],
            ..Replacement::default()
        };
        let replaced = wh
            .replace_if_head(&orders, second.snapshot, replacement, &job("j"))
            .await
            .unwrap();

        let metadata = wh.load(&orders).await.unwrap().metadata().clone();
        let refused = |r: &Result<()>| matches!(r, Err(Error::Storage(m)) if m.contains("`overwrite`, not an append"));
        let err =
            appends_between(&metadata, &orders, second.snapshot, replaced.snapshot).map(|_| ());
        assert!(refused(&err), "{err:?}");
        let since_first = wh
            .read_since(&orders, Some(first.snapshot))
            .await
            .map(|_| ());
        assert!(refused(&since_first), "{since_first:?}");
        let across = SnapshotRange {
            from: Some(second.snapshot),
            to: replaced.snapshot,
        };
        let across = wh.scan_range(&orders, across).await.map(|_| ());
        assert!(refused(&across), "{across:?}");

        // An overwrite before the range doesn't matter, and reading
        // everything still works.
        let from_overwrite =
            appends_between(&metadata, &orders, replaced.snapshot, replaced.snapshot);
        assert!(from_overwrite.unwrap().is_empty());
        let since = wh.read_since(&orders, Some(replaced.snapshot)).await;
        assert!(since.unwrap().is_empty());
        let everything = wh.read_since(&orders, None).await.unwrap();
        assert_eq!(all_ids(&everything), [1, 2, 30]);

        // iceberg-rust's own append builds on the hand-made snapshot, and
        // reads incrementally from it.
        let after = Some(replaced.snapshot);
        wh.append_if_head(&orders, &[ids(&[6])], after, &position("p3"))
            .await
            .unwrap();
        let since = wh.read_since(&orders, after).await.unwrap();
        assert_eq!(all_ids(&since), [6]);
        assert_eq!(all_ids(&wh.read_all(&orders).await.unwrap()), [1, 2, 6, 30]);
    }
}
