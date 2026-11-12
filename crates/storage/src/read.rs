//! Reading a table: everything in a snapshot, or only the rows that
//! appends added after an earlier snapshot, which is what incremental
//! runs consume.

use std::collections::HashSet;

use arrow_array::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use iceberg::io::FileIO;
use iceberg::spec::{Operation, SnapshotRef, TableMetadata};
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
/// `to`. Fails if one of them is not an append, if the walk never reaches
/// `from`, or if expiry removed a snapshot it needs.
fn appends_between(
    metadata: &TableMetadata,
    table: &TableRef,
    from: SnapshotId,
    to: SnapshotId,
) -> Result<HashSet<SnapshotId>> {
    let mut snapshots = HashSet::new();
    for snapshot in ancestors_after(metadata, table, from, to) {
        let snapshot = snapshot?;
        let operation = &snapshot.summary().operation;
        if *operation != Operation::Append {
            return Err(Error::Storage(format!(
                "{table}: snapshot {} is a `{}`, not an append, so it can't be read \
                 incrementally",
                snapshot.snapshot_id(),
                operation.as_str()
            )));
        }
        snapshots.insert(snapshot.snapshot_id());
    }
    Ok(snapshots)
}

/// The snapshots after `from` up to and including `to`, newest first. `to`
/// is looked up first, so a made-up `from == to` is not found. The walk
/// stops at the child of `from` without looking `from` up, so a checkpoint
/// that expiry removed still bounds the range while its child is kept.
fn ancestors_after<'a>(
    metadata: &'a TableMetadata,
    table: &'a TableRef,
    from: SnapshotId,
    to: SnapshotId,
) -> impl Iterator<Item = Result<&'a SnapshotRef>> + 'a {
    let first = metadata
        .snapshot_by_id(to)
        .ok_or_else(|| Error::NotFound(format!("{table}: snapshot {to}")));
    let mut next = match first {
        Ok(_) if to == from => None,
        found => Some(found),
    };
    std::iter::from_fn(move || {
        let current = next.take()?;
        if let Ok(snapshot) = &current {
            next = match snapshot.parent_snapshot_id() {
                Some(parent) if parent == from => None,
                Some(parent) => Some(metadata.snapshot_by_id(parent).ok_or_else(|| {
                    Error::TruncatedHistory(format!(
                        "{table}: snapshot {parent}, between {from} and {to}, has expired"
                    ))
                })),
                None => Some(Err(Error::NotFound(format!(
                    "{table}: snapshot {from} is not an ancestor of snapshot {to}"
                )))),
            };
        }
        Some(current)
    })
}

/// Paths of the data files that `snapshots` added and that snapshot `to`
/// still lists as live.
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
        let manifest = table
            .manifest_reader()
            .read(manifest)
            .await
            .map_err(storage)?;
        // A later manifest rewrite carries a file forward as `Existing` with
        // the id of the snapshot that added it, so every manifest is read.
        for entry in manifest.entries() {
            let added_here = entry.snapshot_id().is_some_and(|s| snapshots.contains(&s));
            if added_here && entry.is_alive() {
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
    use std::collections::HashSet;

    use iceberg::spec::TableMetadata;
    use infostash_model::{Error, Result, SnapshotId, SnapshotRange};

    use super::{appends_between, files_added_by};
    use crate::Replacement;
    use crate::test_support::{all_ids, files_of, ids, job, orders, position, two_appends};
    use crate::write::write_files;

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

    /// A file that a later replace carries forward as `Existing` still
    /// counts as added by the snapshot that wrote it, and a file the range
    /// deleted does not.
    #[tokio::test]
    async fn files_added_in_the_range_are_found_in_a_rewritten_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;

        // Two prebuilt files in one replace share one manifest.
        let loaded = wh.load(&orders).await.unwrap();
        let (mut files, _) = write_files(&loaded, &orders, &[ids(&[10])]).await.unwrap();
        files.extend(
            write_files(&loaded, &orders, &[ids(&[20])])
                .await
                .unwrap()
                .0,
        );
        let (ten, twenty) = (
            files[0].file_path().to_string(),
            files[1].file_path().to_string(),
        );
        let built = Replacement {
            remove: files_of(&wh, &orders, &second).await,
            files,
            ..Replacement::default()
        };
        let built = wh
            .replace_if_head(&orders, second.snapshot, built, &job("a"))
            .await
            .unwrap();
        // Removing one of them rewrites that manifest.
        let partial = Replacement {
            remove: vec![ten],
            ..Replacement::default()
        };
        let partial = wh
            .replace_if_head(&orders, built.snapshot, partial, &job("b"))
            .await
            .unwrap();

        let loaded = wh.load(&orders).await.unwrap();
        let found = files_added_by(&loaded, partial.snapshot, &HashSet::from([built.snapshot]))
            .await
            .unwrap();
        assert_eq!(found, HashSet::from([twenty]));
    }

    /// Four appends, with expiry simulated by dropping snapshots from the
    /// metadata, as the issue allows for a hand-built overwrite.
    async fn four_appends() -> (tempfile::TempDir, TableMetadata, [SnapshotId; 4]) {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let mut head = None;
        let mut ids_ = [0; 4];
        for (i, id) in ids_.iter_mut().enumerate() {
            let value = i64::try_from(i).unwrap();
            let commit = wh
                .append_if_head(&orders, &[ids(&[value])], head, &position("p"))
                .await
                .unwrap();
            *id = commit.snapshot;
            head = Some(commit.snapshot);
        }
        let metadata = wh.load(&orders).await.unwrap().metadata().clone();
        (dir, metadata, ids_)
    }

    fn expire(metadata: &TableMetadata, gone: &[SnapshotId]) -> TableMetadata {
        metadata
            .clone()
            .into_builder(None)
            .remove_snapshots(gone)
            .build()
            .unwrap()
            .metadata
    }

    #[tokio::test]
    async fn a_range_whose_history_expired_is_truncated_not_missing() {
        let (_dir, metadata, [s1, s2, s3, s4]) = four_appends().await;
        let orders = infostash_model::TableRef::new(infostash_model::Layer::Raw, "orders");
        let after_s2 = HashSet::from([s3, s4]);

        // Snapshots older than the checkpoint, or the checkpoint alone with
        // its child kept, leave the range readable.
        let older = expire(&metadata, &[s1]);
        assert_eq!(appends_between(&older, &orders, s2, s4).unwrap(), after_s2);
        let checkpoint = expire(&metadata, &[s2]);
        assert_eq!(
            appends_between(&checkpoint, &orders, s2, s4).unwrap(),
            after_s2
        );

        // Losing the checkpoint's child too truncates the range.
        let truncated = expire(&metadata, &[s2, s3]);
        let err = appends_between(&truncated, &orders, s2, s4).unwrap_err();
        assert!(matches!(err, Error::TruncatedHistory(_)), "{err:?}");
        assert_eq!(
            err.to_string(),
            format!(
                "truncated history: bronze.orders: snapshot {s3}, between {s2} and {s4}, has expired"
            )
        );

        // A snapshot that was never an ancestor is still not found, and so
        // is a missing `to`.
        let never = appends_between(&metadata, &orders, 7, s4).unwrap_err();
        assert!(matches!(never, Error::NotFound(m) if m.contains("not an ancestor")));
        let no_to = appends_between(&truncated, &orders, s3, s3).unwrap_err();
        assert!(matches!(no_to, Error::NotFound(_)), "{no_to:?}");
        assert!(
            appends_between(&metadata, &orders, s4, s4)
                .unwrap()
                .is_empty()
        );
    }
}
