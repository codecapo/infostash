//! Replacing data files in one `overwrite` snapshot, built from public
//! iceberg-rust spec writers and committed with the catalog swap in
//! [`crate::swap`]. ADR 0007 records the route and its costs.
//!
//! A replace runs in three steps that each do one thing: [`plan`] reads
//! the base snapshot and checks the files to remove, writing nothing;
//! [`write_snapshot`] writes the data files' manifests, the manifest list
//! and the snapshot; and [`Warehouse::commit_replace`] swaps the catalog
//! pointer.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
    ManifestEntryRef, ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder,
    Operation, PartitionSpecRef, Snapshot, SnapshotSummaryCollector, Summary, TableMetadata,
};
use iceberg::table::Table;
use iceberg::{MetadataLocation, TableRequirement};
use infostash_model::{Error, Result, SnapshotId, TableRef};

use crate::{Commit, Warehouse, check_properties, ident, moved, storage, write_files};

/// What a replace takes out of a table and puts in.
#[derive(Debug, Clone, Default)]
pub struct Replacement {
    /// Paths of data files live at the expected head, to mark deleted.
    pub remove: Vec<String>,
    /// Rows to write as new data files.
    pub batches: Vec<RecordBatch>,
    /// Data files already written for this table, such as rewritten
    /// copies of removed files, to add as they are.
    pub files: Vec<DataFile>,
}

impl Replacement {
    fn is_empty(&self) -> bool {
        self.remove.is_empty()
            && self.files.is_empty()
            && self.batches.iter().all(|b| b.num_rows() == 0)
    }
}

/// A replace whose files and next metadata file are written but which the
/// catalog does not point at yet, so no reader sees it.
#[derive(Debug, Clone)]
pub(crate) struct PreparedReplace {
    table: TableRef,
    uuid: uuid::Uuid,
    expected_head: SnapshotId,
    base_location: String,
    new_location: String,
    snapshot: SnapshotId,
    rows: u64,
    /// The data files it adds, which tests check are orphaned on a loss.
    #[cfg(test)]
    added: Vec<String>,
}

impl Warehouse {
    /// Replace the live data files `replacement.remove` with the rows and
    /// files it adds, in one `overwrite` snapshot, if the table head is
    /// still `expected_head`. Removing every live file with nothing to add
    /// empties the table, which is how a full refresh starts over.
    ///
    /// `properties` land in the new snapshot's summary and in the table
    /// properties, in the same commit, as for
    /// [`Warehouse::append_if_head`], which is also the way to commit
    /// properties alone: an overwrite would stop incremental reads across
    /// it. A key outside [`crate::PROPERTY_PREFIX`], a replacement that
    /// changes nothing, a moved head, or a file to remove that is not live
    /// at the head is refused before any file is written. A head that moves after that is
    /// [`Error::Conflict`], with no retry and no rebase; the files the
    /// losing replace wrote stay on disk, referenced by no snapshot.
    ///
    /// Limits, from ADR 0007: it works only on the SQLite catalog that
    /// [`Warehouse::open_local`] opens, commits outside the `Catalog` trait
    /// through the SQL catalog's private `iceberg_tables` layout, and must
    /// be redone for the Phase 1 REST catalog. It replaces whole data files
    /// in unpartitioned format v2 tables and writes no delete files.
    pub async fn replace_if_head(
        &self,
        table: &TableRef,
        expected_head: SnapshotId,
        replacement: Replacement,
        properties: &HashMap<String, String>,
    ) -> Result<Commit> {
        let prepared = self
            .prepare_replace(table, expected_head, replacement, properties)
            .await?;
        self.commit_replace(prepared).await
    }

    /// Write everything a replace needs except the catalog pointer.
    pub(crate) async fn prepare_replace(
        &self,
        table: &TableRef,
        expected_head: SnapshotId,
        replacement: Replacement,
        properties: &HashMap<String, String>,
    ) -> Result<PreparedReplace> {
        check_properties(table, properties)?;
        if replacement.is_empty() {
            return Err(Error::Storage(format!(
                "{table}: nothing to replace (no files to remove, no rows and no files); \
                 append_if_head commits properties alone"
            )));
        }
        if let Some(file) = replacement
            .files
            .iter()
            .find(|f| f.content_type() != DataContentType::Data)
        {
            return Err(Error::Storage(format!(
                "{table}: {} is a delete file, not a data file",
                file.file_path()
            )));
        }
        let current = self.load(table).await?;
        let metadata = current.metadata();
        if metadata.format_version() != FormatVersion::V2 {
            return Err(Error::Storage(format!(
                "{table}: replacing files needs format version 2, the table is {}",
                metadata.format_version()
            )));
        }
        if !metadata.default_partition_spec().fields().is_empty() {
            return Err(Error::Storage(format!(
                "{table}: replacing files needs an unpartitioned table"
            )));
        }
        let found = metadata.current_snapshot_id();
        if found != Some(expected_head) {
            return Err(moved(table, Some(expected_head), found));
        }
        let base_location = current.metadata_location_result().map_err(storage)?;
        let plan = plan(&current, table, expected_head, &replacement.remove).await?;
        check_added(table, &plan, &replacement.files)?;

        let (mut added, written) = write_files(&current, table, &replacement.batches).await?;
        let prebuilt: u64 = replacement.files.iter().map(DataFile::record_count).sum();
        let rows = written + prebuilt;
        added.extend(replacement.files);
        let snapshot = write_snapshot(&current, plan, &added, properties).await?;
        let snapshot_id = snapshot.snapshot_id();
        let next = next_metadata(metadata, base_location, snapshot, properties)?;
        let new_location = MetadataLocation::from_str(base_location)
            .map_err(storage)?
            .with_next_version()
            .try_with_new_metadata(&next)
            .map_err(storage)?;
        next.write_to(current.file_io(), &new_location)
            .await
            .map_err(storage)?;
        Ok(PreparedReplace {
            table: table.clone(),
            uuid: metadata.uuid(),
            expected_head,
            base_location: base_location.to_string(),
            new_location: new_location.to_string(),
            snapshot: snapshot_id,
            rows,
            #[cfg(test)]
            added: added.iter().map(|f| f.file_path().to_string()).collect(),
        })
    }

    /// Point the catalog at a prepared replace's metadata file, if the table
    /// is still the one it was prepared on and its head has not moved.
    ///
    /// The requirement check gives the clearer error and catches a table
    /// dropped and created again. The swap is what makes the commit safe:
    /// it only lands while the catalog row still holds the metadata the
    /// replace was built on, so it also stops a commit that lands between
    /// the check and the swap.
    pub(crate) async fn commit_replace(&self, prepared: PreparedReplace) -> Result<Commit> {
        let table = &prepared.table;
        let current = self.load(table).await?;
        let requirements = [
            TableRequirement::UuidMatch {
                uuid: prepared.uuid,
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(prepared.expected_head),
            },
        ];
        for requirement in requirements {
            requirement
                .check(Some(current.metadata()))
                .map_err(|e| Error::Conflict(format!("{table}: {e}")))?;
        }
        let swapped = self
            .swap
            .swap(
                &ident(table),
                &prepared.base_location,
                &prepared.new_location,
            )
            .await?;
        if !swapped {
            let now = self.load(table).await?;
            if now.metadata_location() == Some(prepared.base_location.as_str()) {
                return Err(Error::Storage(format!(
                    "{table}: the catalog row still points at {} but the swap matched no \
                     row; the catalog's layout no longer fits the swap",
                    prepared.base_location
                )));
            }
            let found = now.metadata().current_snapshot_id();
            return Err(moved(table, Some(prepared.expected_head), found));
        }
        Ok(Commit {
            snapshot: prepared.snapshot,
            metadata_location: prepared.new_location,
            rows: prepared.rows,
        })
    }
}

/// How a replace changes the base snapshot's manifests. Building it reads
/// the manifests and writes nothing.
struct Plan {
    base: SnapshotId,
    /// Manifests that list no removed file, reused as they are.
    kept: Vec<ManifestFile>,
    /// Manifests that list a removed file, with their live entries.
    rewritten: Vec<(ManifestFile, Vec<ManifestEntryRef>)>,
    /// The data files removed, as their entries describe them.
    removed: Vec<DataFile>,
    /// Paths of the live data files the replace leaves in place.
    staying: HashSet<String>,
}

/// Sort the base snapshot's manifests into those a replace keeps and those
/// it rewrites, and check that every path in `remove` is live there.
async fn plan(
    current: &Table,
    table: &TableRef,
    base: SnapshotId,
    remove: &[String],
) -> Result<Plan> {
    let metadata = current.metadata();
    let snapshot = metadata
        .snapshot_by_id(base)
        .ok_or_else(|| Error::NotFound(format!("{table}: snapshot {base}")))?;
    let remove: HashSet<&str> = remove.iter().map(String::as_str).collect();
    let list = current
        .manifest_list_reader(snapshot)
        .load()
        .await
        .map_err(storage)?;
    let mut plan = Plan {
        base,
        kept: Vec::new(),
        rewritten: Vec::new(),
        removed: Vec::new(),
        staying: HashSet::new(),
    };
    for manifest_file in list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            plan.kept.push(manifest_file.clone());
            continue;
        }
        let manifest = current
            .manifest_reader()
            .read(manifest_file)
            .await
            .map_err(storage)?;
        // Entries deleted by earlier snapshots are not carried: such an
        // entry names the snapshot that deleted the file.
        let live: Vec<ManifestEntryRef> = manifest
            .entries()
            .iter()
            .filter(|e| e.is_alive())
            .cloned()
            .collect();
        let (gone, staying): (Vec<&ManifestEntryRef>, Vec<&ManifestEntryRef>) =
            live.iter().partition(|e| remove.contains(e.file_path()));
        plan.staying
            .extend(staying.iter().map(|e| e.file_path().to_string()));
        let gone: Vec<DataFile> = gone.iter().map(|e| e.data_file().clone()).collect();
        if gone.is_empty() {
            plan.kept.push(manifest_file.clone());
        } else {
            plan.removed.extend(gone);
            plan.rewritten.push((manifest_file.clone(), live));
        }
    }
    let found: HashSet<&str> = plan.removed.iter().map(DataFile::file_path).collect();
    let mut missing: Vec<&str> = remove.difference(&found).copied().collect();
    if !missing.is_empty() {
        missing.sort_unstable();
        return Err(Error::Storage(format!(
            "{table}: not live in snapshot {base}, so not removed: {}",
            missing.join(", ")
        )));
    }
    Ok(plan)
}

/// Check that no prebuilt file is listed twice or is already live and
/// staying, either of which would make readers return its rows twice.
fn check_added(table: &TableRef, plan: &Plan, files: &[DataFile]) -> Result<()> {
    let mut seen = HashSet::new();
    for path in files.iter().map(DataFile::file_path) {
        if !seen.insert(path) {
            return Err(Error::Storage(format!(
                "{table}: {path} is added more than once"
            )));
        }
        if plan.staying.contains(path) {
            return Err(Error::Storage(format!(
                "{table}: {path} is already live and not removed"
            )));
        }
    }
    Ok(())
}

/// Write the manifests and manifest list of an `overwrite` snapshot on top
/// of the plan's base, and return the snapshot. Each rewritten manifest
/// marks its removed files deleted and carries its other live files as
/// existing entries, keeping their snapshot ids and sequence numbers. The
/// added files go in one new manifest.
async fn write_snapshot(
    current: &Table,
    plan: Plan,
    added: &[DataFile],
    properties: &HashMap<String, String>,
) -> Result<Snapshot> {
    let metadata = current.metadata();
    let snapshot_id = new_snapshot_id(metadata);
    let sequence_number = metadata.next_sequence_number();
    let commit = uuid::Uuid::new_v4();
    let dir = metadata.metadata_location().map_err(storage)?;
    let avro = DataFileFormat::Avro;
    let manifest_path = |n: usize| format!("{dir}/{commit}-m{n}.{avro}");
    let removed: HashSet<&str> = plan.removed.iter().map(DataFile::file_path).collect();

    let mut manifests = plan.kept;
    for (n, (manifest_file, live)) in plan.rewritten.into_iter().enumerate() {
        let spec = spec_of(metadata, &manifest_file)?;
        let mut writer = manifest_writer(current, manifest_path(n), snapshot_id, spec)?;
        for entry in live {
            let file = entry.data_file().clone();
            // The reader fills these in from the manifest list entry; the
            // fallbacks are the values it would use.
            let seq = entry
                .sequence_number()
                .unwrap_or(manifest_file.sequence_number);
            let file_seq = entry.file_sequence_number;
            if removed.contains(entry.file_path()) {
                writer.add_delete_file(file, seq, file_seq)
            } else {
                let by = entry
                    .snapshot_id()
                    .unwrap_or(manifest_file.added_snapshot_id);
                writer.add_existing_file(file, by, seq, file_seq)
            }
            .map_err(storage)?;
        }
        manifests.push(writer.write_manifest_file().await.map_err(storage)?);
    }
    if !added.is_empty() {
        let spec = metadata.default_partition_spec();
        let path = manifest_path(manifests.len());
        let mut writer = manifest_writer(current, path, snapshot_id, spec)?;
        for file in added {
            writer
                .add_file(file.clone(), sequence_number)
                .map_err(storage)?;
        }
        manifests.push(writer.write_manifest_file().await.map_err(storage)?);
    }

    let list_path = format!("{dir}/snap-{snapshot_id}-0-{commit}.{avro}");
    let output = current.file_io().new_output(&list_path).map_err(storage)?;
    let mut list = ManifestListWriter::v2(
        output.writer().await.map_err(storage)?,
        snapshot_id,
        Some(plan.base),
        sequence_number,
    );
    list.add_manifests(manifests.into_iter()).map_err(storage)?;
    list.close().await.map_err(storage)?;

    let mut summary = overwrite_summary(metadata, plan.base, added, &plan.removed);
    summary.additional_properties.extend(properties.clone());
    Ok(Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(Some(plan.base))
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(now_ms().max(metadata.last_updated_ms()))
        .with_manifest_list(list_path)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema_id())
        .build())
}

/// The table metadata after the replace: the snapshot added, `main`
/// pointed at it with its retention kept, and the properties set.
fn next_metadata(
    metadata: &TableMetadata,
    base_location: &str,
    snapshot: Snapshot,
    properties: &HashMap<String, String>,
) -> Result<TableMetadata> {
    let mut builder = metadata
        .clone()
        .into_builder(Some(base_location.to_string()))
        .set_branch_snapshot(snapshot, MAIN_BRANCH)
        .map_err(storage)?;
    if !properties.is_empty() {
        builder = builder
            .set_properties(properties.clone())
            .map_err(storage)?;
    }
    Ok(builder.build().map_err(storage)?.metadata)
}

/// The partition spec a manifest was written with.
fn spec_of<'a>(
    metadata: &'a TableMetadata,
    manifest: &ManifestFile,
) -> Result<&'a PartitionSpecRef> {
    let id = manifest.partition_spec_id;
    metadata.partition_spec_by_id(id).ok_or_else(|| {
        Error::Storage(format!(
            "manifest {} has unknown partition spec {id}",
            manifest.manifest_path
        ))
    })
}

/// A v2 data manifest writer for `path`, owned by `snapshot_id`.
fn manifest_writer(
    current: &Table,
    path: String,
    snapshot_id: SnapshotId,
    spec: &PartitionSpecRef,
) -> Result<ManifestWriter> {
    let output = current.file_io().new_output(path).map_err(storage)?;
    Ok(ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        current.metadata().current_schema().clone(),
        spec.as_ref().clone(),
    )
    .build_v2_data())
}

/// Summary of an overwrite: `added-*` and `deleted-*` from iceberg's
/// collector, and `total-*` carried forward from the base by
/// [`carry_totals`]. iceberg-rust's helper that does this for its own
/// commits is crate-private at the pin.
fn overwrite_summary(
    metadata: &TableMetadata,
    base: SnapshotId,
    added: &[DataFile],
    removed: &[DataFile],
) -> Summary {
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    let mut collector = SnapshotSummaryCollector::default();
    for file in added {
        collector.add_file(file, schema.clone(), spec.clone());
    }
    for file in removed {
        collector.remove_file(file, schema.clone(), spec.clone());
    }
    let mut fields = collector.build();
    if let Some(previous) = metadata.snapshot_by_id(base) {
        carry_totals(&previous.summary().additional_properties, &mut fields);
    }
    Summary {
        operation: Operation::Overwrite,
        additional_properties: fields,
    }
}

/// Set each `total-*` in `fields` to the base's total plus what the
/// snapshot adds and minus what it removes. A total the base does not
/// hold as a number stays unset: carrying it from zero would publish a
/// total that leaves out the files the replace keeps.
fn carry_totals(previous: &HashMap<String, String>, fields: &mut HashMap<String, String>) {
    let delta = |m: &HashMap<String, String>, k: &str| -> i64 {
        m.get(k).and_then(|v| v.parse().ok()).unwrap_or(0)
    };
    for (total, plus, minus) in [
        ("total-records", "added-records", "deleted-records"),
        ("total-data-files", "added-data-files", "deleted-data-files"),
        ("total-files-size", "added-files-size", "removed-files-size"),
        (
            "total-delete-files",
            "added-delete-files",
            "removed-delete-files",
        ),
        (
            "total-position-deletes",
            "added-position-deletes",
            "removed-position-deletes",
        ),
        (
            "total-equality-deletes",
            "added-equality-deletes",
            "removed-equality-deletes",
        ),
    ] {
        let Some(base) = previous.get(total).and_then(|v| v.parse::<i64>().ok()) else {
            continue;
        };
        let value = base + delta(fields, plus) - delta(fields, minus);
        fields.insert(total.to_string(), value.to_string());
    }
}

/// A snapshot id no snapshot of the table has: 63 random bits, as
/// iceberg-rust's own generator makes them.
fn new_snapshot_id(metadata: &TableMetadata) -> SnapshotId {
    loop {
        let (a, b) = uuid::Uuid::new_v4().as_u64_pair();
        let id = ((a ^ b) >> 1) as i64;
        if metadata.snapshot_by_id(id).is_none() {
            return id;
        }
    }
}

/// Milliseconds since the Unix epoch, or 0 on a clock set before it.
fn now_ms() -> i64 {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH);
    elapsed.map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use std::str::FromStr;

    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, MAIN_BRANCH,
        ManifestStatus, Operation, SnapshotReference, SnapshotRetention,
    };
    use iceberg::{Catalog, MetadataLocation, NamespaceIdent, TableCreation};
    use infostash_model::{Error, TableRef};

    use super::{PreparedReplace, Replacement};
    use crate::swap::CatalogSwap;
    use crate::test_support::{
        all_ids, entries_of, files_of, ids, ids_schema, job, live_files, local_path, manifests_of,
        orders, position, referenced_files, two_appends,
    };
    use crate::{Warehouse, ident, read_metadata_file, scan_at, write_files};

    fn swap_in(remove: Vec<String>, values: &[i64]) -> Replacement {
        Replacement {
            remove,
            batches: vec![ids(values)],
            ..Replacement::default()
        }
    }

    /// Prepare a replace of `second`'s file with `values`.
    async fn prepare(
        wh: &Warehouse,
        orders: &TableRef,
        second: &crate::Commit,
        values: &[i64],
        job_id: &str,
    ) -> PreparedReplace {
        let remove = files_of(wh, orders, second).await;
        wh.prepare_replace(
            orders,
            second.snapshot,
            swap_in(remove, values),
            &job(job_id),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_replace_commits_one_overwrite_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (first, second) = two_appends(&wh, &orders).await;
        let before = wh.load(&orders).await.unwrap();
        let uuid = before.metadata().uuid();
        let log_len = before.metadata().metadata_log().len();
        let first_files = files_of(&wh, &orders, &first).await;
        let first_manifests = manifests_of(&wh, &orders, first.snapshot).await;
        assert_eq!(first_manifests.len(), 1);
        let remove = files_of(&wh, &orders, &second).await;
        assert_eq!(remove.len(), 1);

        let replaced = wh
            .replace_if_head(
                &orders,
                second.snapshot,
                swap_in(remove.clone(), &[30, 40]),
                &job("job-7"),
            )
            .await
            .unwrap();

        assert_eq!(replaced.rows, 2);
        let loaded = wh.load(&orders).await.unwrap();
        let metadata = loaded.metadata();
        assert_eq!(metadata.uuid(), uuid);
        assert_eq!(metadata.current_snapshot_id(), Some(replaced.snapshot));
        assert_eq!(
            loaded.metadata_location(),
            Some(replaced.metadata_location.as_str())
        );
        // Three commits before it, so this is the fourth metadata file.
        assert!(
            replaced.metadata_location.contains("/00003-"),
            "{}",
            replaced.metadata_location
        );
        assert_eq!(metadata.metadata_log().len(), log_len + 1);
        assert_eq!(
            wh.snapshots(&orders).await.unwrap(),
            [first.snapshot, second.snapshot, replaced.snapshot]
        );
        let rows = wh.read_all(&orders).await.unwrap();
        assert_eq!(all_ids(&rows), [1, 2, 30, 40]);

        let snapshot = metadata.current_snapshot().unwrap();
        assert_eq!(snapshot.parent_snapshot_id(), Some(second.snapshot));
        assert_eq!(snapshot.sequence_number(), metadata.last_sequence_number());
        let summary = snapshot.summary();
        assert_eq!(summary.operation, Operation::Overwrite);
        let field = |key: &str| summary.additional_properties[key].as_str();
        assert_eq!(field("total-records"), "4");
        assert_eq!(field("total-data-files"), "2");
        assert_eq!(field("added-data-files"), "1");
        assert_eq!(field("deleted-data-files"), "1");
        assert_eq!(field("added-records"), "2");
        assert_eq!(field("deleted-records"), "3");
        assert_eq!(field("total-delete-files"), "0");
        assert_eq!(field("infostash.job-id"), "job-7");
        let state = wh.state(&orders).await.unwrap();
        assert_eq!(state.head, Some(replaced.snapshot));
        assert_eq!(state.properties["infostash.job-id"], "job-7");
        assert_eq!(state.properties["infostash.position"], "after job-7");

        // The first append's manifest is carried over as it is; the second's
        // is rewritten with its file marked deleted; the new file has a
        // manifest of its own.
        let manifests = manifests_of(&wh, &orders, replaced.snapshot).await;
        assert_eq!(manifests.len(), 3, "{manifests:?}");
        assert!(manifests.contains(&first_manifests[0]), "{manifests:?}");
        let added = files_of(&wh, &orders, &replaced).await;
        assert_eq!(added.len(), 1);
        let entries = entries_of(&wh, &orders, replaced.snapshot).await;
        assert_eq!(entries.len(), 3, "{entries:?}");
        let now = Some(replaced.snapshot);
        // A deleted entry keeps the data sequence number of the file it
        // removes; the new file takes the overwrite's own.
        for expected in [
            (
                ManifestStatus::Added,
                first_files[0].clone(),
                Some(first.snapshot),
                Some(1),
            ),
            (ManifestStatus::Deleted, remove[0].clone(), now, Some(2)),
            (ManifestStatus::Added, added[0].clone(), now, Some(3)),
        ] {
            assert!(entries.contains(&expected), "{expected:?} in {entries:?}");
        }
        // Removing a file from the table does not remove it from disk.
        assert!(local_path(&remove[0]).exists());
    }

    #[tokio::test]
    async fn a_partial_replace_keeps_the_rest_of_a_manifest_as_existing_files() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (first, second) = two_appends(&wh, &orders).await;
        let first_files = files_of(&wh, &orders, &first).await;

        // Two prebuilt files in one replace share one manifest.
        let loaded = wh.load(&orders).await.unwrap();
        let (mut files, _) = write_files(&loaded, &orders, &[ids(&[10])]).await.unwrap();
        files.extend(
            write_files(&loaded, &orders, &[ids(&[20, 21])])
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
        assert_eq!(built.rows, 3);
        assert_eq!(
            all_ids(&wh.read_all(&orders).await.unwrap()),
            [1, 2, 10, 20, 21]
        );
        let deleted_only = manifests_of(&wh, &orders, built.snapshot).await;

        // Take out one of them.
        let partial = wh
            .replace_if_head(
                &orders,
                built.snapshot,
                swap_in(vec![ten.clone()], &[11]),
                &job("b"),
            )
            .await
            .unwrap();

        let rows = wh.read_all(&orders).await.unwrap();
        assert_eq!(all_ids(&rows), [1, 2, 11, 20, 21]);
        let entries = entries_of(&wh, &orders, partial.snapshot).await;
        let now = Some(partial.snapshot);
        // The other file keeps the snapshot that added it, and the earlier
        // replace's deletion is not carried into the rewritten manifest.
        // Sequence numbers are carried over too: the kept and removed files
        // keep the earlier replace's, and the new file takes this one's.
        let eleven = files_of(&wh, &orders, &partial).await;
        assert_eq!(eleven.len(), 1);
        for expected in [
            (
                ManifestStatus::Existing,
                twenty,
                Some(built.snapshot),
                Some(3),
            ),
            (ManifestStatus::Deleted, ten, now, Some(3)),
            (ManifestStatus::Added, eleven[0].clone(), now, Some(4)),
            (
                ManifestStatus::Added,
                first_files[0].clone(),
                Some(first.snapshot),
                Some(1),
            ),
        ] {
            assert!(entries.contains(&expected), "{expected:?} in {entries:?}");
        }
        // The manifest left holding only the second append's deleted file is
        // reused as it is.
        let manifests = manifests_of(&wh, &orders, partial.snapshot).await;
        let untouched: Vec<_> = deleted_only
            .iter()
            .filter(|m| manifests.contains(m))
            .collect();
        assert_eq!(untouched.len(), 2, "{deleted_only:?} {manifests:?}");
        let summary = wh
            .load(&orders)
            .await
            .unwrap()
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .clone();
        assert_eq!(summary.additional_properties["total-records"], "5");
        assert_eq!(summary.additional_properties["total-data-files"], "3");
        assert_eq!(live_files(&wh, &orders).await.len(), 3);
    }

    #[tokio::test]
    async fn two_replaces_on_one_head_give_one_commit_and_one_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        let a = prepare(&wh, &orders, &second, &[30], "a").await;
        let b = prepare(&wh, &orders, &second, &[40], "b").await;
        assert_ne!(a.new_location, b.new_location);
        assert!(local_path(&a.new_location).exists());
        // Neither is visible before its commit.
        assert_eq!(wh.state(&orders).await.unwrap().head, Some(second.snapshot));

        let won = wh.commit_replace(a.clone()).await.unwrap();
        let lost = wh.commit_replace(b.clone()).await;
        assert!(matches!(&lost, Err(Error::Conflict(_))), "{lost:?}");

        let snapshots = wh.snapshots(&orders).await.unwrap();
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[2], won.snapshot);
        assert_eq!(all_ids(&wh.read_all(&orders).await.unwrap()), [1, 2, 30]);
        let referenced = referenced_files(&wh, &orders).await;
        assert!(a.added.iter().all(|f| referenced.contains(f)));
        for file in &b.added {
            assert!(!referenced.contains(file), "{file} is referenced");
            // The loser's files are orphaned, not deleted.
            assert!(local_path(file).exists());
        }
        assert!(local_path(&b.new_location).exists());
        // The winner can't land twice either.
        let again = wh.commit_replace(a).await;
        assert!(matches!(&again, Err(Error::Conflict(_))), "{again:?}");
        assert_eq!(wh.snapshots(&orders).await.unwrap().len(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_commits_on_one_head_have_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        let a = prepare(&wh, &orders, &second, &[30], "a").await;
        let b = prepare(&wh, &orders, &second, &[40], "b").await;

        let wh = Arc::new(wh);
        let spawn = |prepared: PreparedReplace| {
            let wh = Arc::clone(&wh);
            tokio::spawn(async move { wh.commit_replace(prepared).await })
        };
        let (result_a, result_b) = tokio::join!(spawn(a), spawn(b));
        let results = [result_a.unwrap(), result_b.unwrap()];

        let won: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
        assert_eq!(won.len(), 1, "{results:?}");
        let conflicts = results
            .iter()
            .filter(|r| matches!(r, Err(Error::Conflict(_))))
            .count();
        assert_eq!(conflicts, 1, "{results:?}");
        assert_eq!(wh.state(&orders).await.unwrap().head, Some(won[0].snapshot));
        assert_eq!(wh.snapshots(&orders).await.unwrap().len(), 3);
        let rows = all_ids(&wh.read_all(&orders).await.unwrap());
        assert!(rows == [1, 2, 30] || rows == [1, 2, 40], "{rows:?}");
    }

    #[tokio::test]
    async fn a_replace_racing_an_append_that_lands_after_prepare_is_a_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        let prepared = prepare(&wh, &orders, &second, &[30], "j").await;
        let third = wh
            .append_if_head(
                &orders,
                &[ids(&[6])],
                Some(second.snapshot),
                &position("p3"),
            )
            .await
            .unwrap();

        // The swap alone refuses it, with no requirement check in front.
        let (from, to) = (&prepared.base_location, &prepared.new_location);
        assert!(!wh.swap.swap(&ident(&orders), from, to).await.unwrap());
        // The commit refuses it at the requirement check.
        let lost = wh.commit_replace(prepared.clone()).await;
        assert!(
            matches!(&lost, Err(Error::Conflict(m)) if m.contains("snapshot has changed")),
            "{lost:?}"
        );
        let loaded = wh.load(&orders).await.unwrap();
        assert_eq!(
            loaded.metadata().current_snapshot_id(),
            Some(third.snapshot)
        );
        assert!(
            loaded
                .metadata()
                .snapshots()
                .all(|s| s.summary().operation == Operation::Append)
        );
        let rows = wh.read_all(&orders).await.unwrap();
        assert_eq!(all_ids(&rows), [1, 2, 3, 4, 5, 6]);
        let referenced = referenced_files(&wh, &orders).await;
        assert!(prepared.added.iter().all(|f| !referenced.contains(f)));
    }

    #[tokio::test]
    async fn a_table_dropped_and_created_again_is_a_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        let prepared = prepare(&wh, &orders, &second, &[30], "j").await;
        wh.catalog.drop_table(&ident(&orders)).await.unwrap();
        wh.create_table(&orders, &ids_schema()).await.unwrap();

        let lost = wh.commit_replace(prepared).await;
        assert!(
            matches!(&lost, Err(Error::Conflict(m)) if m.contains("UUID")),
            "{lost:?}"
        );
        assert!(wh.snapshots(&orders).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_swap_that_matches_no_row_on_an_unmoved_table_is_a_storage_error() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        let prepared = prepare(&wh, &orders, &second, &[30], "j").await;
        // A swap keyed on the wrong catalog name stands in for a catalog
        // whose layout the statement no longer fits.
        let uri = format!(
            "sqlite:{}",
            crate::sqlite_path(&dir.path().canonicalize().unwrap().join(crate::CATALOG_FILE))
        );
        let wh = Warehouse {
            catalog: wh.catalog,
            swap: CatalogSwap::connect(&uri, "elsewhere").await.unwrap(),
        };

        let err = wh.commit_replace(prepared).await;
        assert!(
            matches!(&err, Err(Error::Storage(m)) if m.contains("no longer fits")),
            "{err:?}"
        );
        assert_eq!(wh.state(&orders).await.unwrap().head, Some(second.snapshot));
    }

    #[tokio::test]
    async fn a_reader_holding_the_old_metadata_still_reads_the_old_files() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        let reader = wh.load(&orders).await.unwrap();
        let remove = files_of(&wh, &orders, &second).await;
        let replaced = wh
            .replace_if_head(
                &orders,
                second.snapshot,
                swap_in(remove, &[30, 40]),
                &job("j"),
            )
            .await
            .unwrap();

        let old: Vec<_> = scan_at(&reader, None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(all_ids(&old), [1, 2, 3, 4, 5]);
        let old_file = read_metadata_file(&orders, &second.metadata_location)
            .await
            .unwrap();
        assert_eq!(all_ids(&old_file), [1, 2, 3, 4, 5]);
        let new_file = read_metadata_file(&orders, &replaced.metadata_location)
            .await
            .unwrap();
        assert_eq!(all_ids(&new_file), [1, 2, 30, 40]);
    }

    /// Every file under `dir`.
    fn walkdir(dir: &Path) -> Vec<std::path::PathBuf> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(walkdir(&path));
            } else {
                files.push(path);
            }
        }
        files
    }

    /// Count the entries in a directory.
    fn count(dir: &Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
    }

    #[tokio::test]
    async fn a_refused_replace_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (first, second) = two_appends(&wh, &orders).await;
        let first_files = files_of(&wh, &orders, &first).await;
        let metadata_dir = local_path(&second.metadata_location)
            .parent()
            .unwrap()
            .to_path_buf();
        let data_dir = metadata_dir.parent().unwrap().join("data");
        let before = (count(&metadata_dir), count(&data_dir));

        let stale = wh
            .prepare_replace(
                &orders,
                first.snapshot,
                swap_in(first_files.clone(), &[30]),
                &job("j"),
            )
            .await;
        assert!(matches!(&stale, Err(Error::Conflict(_))), "{stale:?}");

        let nowhere = vec!["file:///nowhere/orders.parquet".to_string()];
        let missing = wh
            .prepare_replace(&orders, second.snapshot, swap_in(nowhere, &[30]), &job("j"))
            .await;
        assert!(
            matches!(&missing, Err(Error::Storage(m)) if m.contains("not live") && m.contains("/nowhere/")),
            "{missing:?}"
        );

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("file:///nowhere/deletes.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(1)
            .build()
            .unwrap();
        let deletes = Replacement {
            files: vec![delete_file],
            ..Replacement::default()
        };
        let deletes = wh
            .prepare_replace(&orders, second.snapshot, deletes, &job("j"))
            .await;
        assert!(
            matches!(&deletes, Err(Error::Storage(m)) if m.contains("delete file")),
            "{deletes:?}"
        );

        let nothing = Replacement {
            batches: vec![ids(&[])],
            ..Replacement::default()
        };
        let nothing = wh
            .prepare_replace(&orders, second.snapshot, nothing, &job("j"))
            .await;
        assert!(
            matches!(&nothing, Err(Error::Storage(m)) if m.contains("nothing to replace")),
            "{nothing:?}"
        );
        let foreign = HashMap::from([("total-records".to_string(), "9".to_string())]);
        let foreign = wh
            .prepare_replace(
                &orders,
                second.snapshot,
                swap_in(first_files, &[30]),
                &foreign,
            )
            .await;
        assert!(
            matches!(&foreign, Err(Error::Storage(m)) if m.contains("total-records")),
            "{foreign:?}"
        );

        assert_eq!((count(&metadata_dir), count(&data_dir)), before);
        assert_eq!(
            wh.snapshots(&orders).await.unwrap(),
            [first.snapshot, second.snapshot]
        );
    }

    #[tokio::test]
    async fn a_file_removed_earlier_is_not_live() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        let remove = files_of(&wh, &orders, &second).await;
        let replaced = wh
            .replace_if_head(
                &orders,
                second.snapshot,
                swap_in(remove.clone(), &[30]),
                &job("j"),
            )
            .await
            .unwrap();

        let twice = wh
            .replace_if_head(
                &orders,
                replaced.snapshot,
                swap_in(remove, &[50]),
                &job("k"),
            )
            .await;
        assert!(
            matches!(&twice, Err(Error::Storage(m)) if m.contains("not live")),
            "{twice:?}"
        );
        assert_eq!(
            wh.state(&orders).await.unwrap().head,
            Some(replaced.snapshot)
        );
    }

    #[tokio::test]
    async fn a_full_refresh_with_no_rows_still_commits_an_empty_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        assert!(live_files(&wh, &orders).await.is_empty());
        let (_, second) = two_appends(&wh, &orders).await;
        let live = live_files(&wh, &orders).await;
        assert_eq!(live.len(), 2);

        let empty = Replacement {
            remove: live,
            ..Replacement::default()
        };
        let refreshed = wh
            .replace_if_head(&orders, second.snapshot, empty, &job("job-7"))
            .await
            .unwrap();

        assert_eq!(refreshed.rows, 0);
        assert!(all_ids(&wh.read_all(&orders).await.unwrap()).is_empty());
        assert!(live_files(&wh, &orders).await.is_empty());
        let loaded = wh.load(&orders).await.unwrap();
        let summary = loaded.metadata().current_snapshot().unwrap().summary();
        assert_eq!(summary.operation, Operation::Overwrite);
        let field = |key: &str| summary.additional_properties[key].as_str();
        assert_eq!(field("total-records"), "0");
        assert_eq!(field("total-data-files"), "0");
        assert_eq!(field("total-files-size"), "0");
        assert_eq!(field("deleted-data-files"), "2");
        assert_eq!(field("deleted-records"), "5");
        let state = wh.state(&orders).await.unwrap();
        assert_eq!(state.properties["infostash.job-id"], "job-7");
        // Both manifests were rewritten, and every entry is a deletion.
        let entries = entries_of(&wh, &orders, refreshed.snapshot).await;
        assert_eq!(entries.len(), 2, "{entries:?}");
        assert!(entries.iter().all(|(s, ..)| *s == ManifestStatus::Deleted));
        // The next append starts from an empty table.
        let after = Some(refreshed.snapshot);
        wh.append_if_head(&orders, &[ids(&[7])], after, &position("p3"))
            .await
            .unwrap();
        assert_eq!(all_ids(&wh.read_all(&orders).await.unwrap()), [7]);
        assert_eq!(all_ids(&wh.read_since(&orders, after).await.unwrap()), [7]);
    }

    #[tokio::test]
    async fn a_file_added_twice_or_already_live_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        let loaded = wh.load(&orders).await.unwrap();
        let (files, _) = write_files(&loaded, &orders, &[ids(&[10])]).await.unwrap();
        let path = files[0].file_path().to_string();

        let doubled = Replacement {
            files: vec![files[0].clone(), files[0].clone()],
            ..Replacement::default()
        };
        let doubled = wh
            .replace_if_head(&orders, second.snapshot, doubled, &job("j"))
            .await;
        assert!(
            matches!(&doubled, Err(Error::Storage(m)) if m.contains(&path) && m.contains("more than once")),
            "{doubled:?}"
        );

        let once = Replacement {
            files: files.clone(),
            ..Replacement::default()
        };
        let added = wh
            .replace_if_head(&orders, second.snapshot, once.clone(), &job("j"))
            .await
            .unwrap();
        let again = wh
            .replace_if_head(&orders, added.snapshot, once, &job("k"))
            .await;
        assert!(
            matches!(&again, Err(Error::Storage(m)) if m.contains(&path) && m.contains("already live")),
            "{again:?}"
        );

        // Removing it and adding it back in one replace is allowed.
        let back = Replacement {
            remove: vec![path],
            files,
            ..Replacement::default()
        };
        wh.replace_if_head(&orders, added.snapshot, back, &job("k"))
            .await
            .unwrap();
        assert_eq!(
            all_ids(&wh.read_all(&orders).await.unwrap()),
            [1, 2, 3, 4, 5, 10]
        );
    }

    #[tokio::test]
    async fn a_replace_keeps_the_main_branch_retention() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (_, second) = two_appends(&wh, &orders).await;
        // Give main a retention policy, committed the way a replace is.
        let loaded = wh.load(&orders).await.unwrap();
        let base = loaded.metadata_location().unwrap().to_string();
        let retention = SnapshotRetention::branch(Some(7), Some(86_400_000), None);
        let reference = SnapshotReference::new(second.snapshot, retention);
        let kept = loaded
            .metadata()
            .clone()
            .into_builder(Some(base.clone()))
            .set_ref(MAIN_BRANCH, reference)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let next = MetadataLocation::from_str(&base)
            .unwrap()
            .with_next_version()
            .try_with_new_metadata(&kept)
            .unwrap();
        kept.write_to(loaded.file_io(), &next).await.unwrap();
        let next = next.to_string();
        assert!(wh.swap.swap(&ident(&orders), &base, &next).await.unwrap());

        let prepared = prepare(&wh, &orders, &second, &[30], "j").await;
        let replaced = wh.commit_replace(prepared).await.unwrap();

        let loaded = wh.load(&orders).await.unwrap();
        let main = loaded.metadata().snapshot_for_ref(MAIN_BRANCH).unwrap();
        assert_eq!(main.snapshot_id(), replaced.snapshot);
        // iceberg keeps the refs crate-private, so read them from the file.
        let text = std::fs::read_to_string(local_path(&replaced.metadata_location)).unwrap();
        for field in [
            "\"min-snapshots-to-keep\":7",
            "\"max-snapshot-age-ms\":86400000",
        ] {
            assert!(text.contains(field), "{field} missing from {text}");
        }
    }

    #[tokio::test]
    async fn only_format_version_2_tables_are_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, _) = orders(dir.path()).await;
        let old = TableRef::new(infostash_model::Layer::Raw, "old");
        let creation = TableCreation::builder()
            .name(old.name.clone())
            .schema(iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&ids_schema()).unwrap())
            .format_version(FormatVersion::V1)
            .build();
        let raw = NamespaceIdent::new(old.layer.namespace().to_string());
        wh.catalog.create_table(&raw, creation).await.unwrap();
        let first = wh.append(&old, &[ids(&[1])]).await.unwrap();

        let refused = wh
            .replace_if_head(&old, first.snapshot, swap_in(vec![], &[2]), &job("j"))
            .await;
        assert!(
            matches!(&refused, Err(Error::Storage(m)) if m.contains("format version 2")),
            "{refused:?}"
        );
        assert_eq!(wh.snapshots(&old).await.unwrap(), [first.snapshot]);
    }

    #[tokio::test]
    async fn only_unpartitioned_tables_are_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, _) = orders(dir.path()).await;
        let parts = TableRef::new(infostash_model::Layer::Raw, "parts");
        let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&ids_schema()).unwrap();
        let spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id", iceberg::spec::Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let creation = TableCreation::builder()
            .name(parts.name.clone())
            .schema(schema)
            .partition_spec(spec)
            .build();
        let raw = NamespaceIdent::new(parts.layer.namespace().to_string());
        wh.catalog.create_table(&raw, creation).await.unwrap();

        let refused = wh
            .prepare_replace(&parts, 1, swap_in(vec![], &[2]), &job("j"))
            .await;
        assert!(
            matches!(&refused, Err(Error::Storage(m)) if m.contains("unpartitioned")),
            "{refused:?}"
        );
        let parquet = walkdir(dir.path())
            .into_iter()
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .count();
        assert_eq!(parquet, 0);
    }

    /// The catalog also reads an `iceberg_tables` in its older layout, which
    /// has no `iceberg_type` column, and appends to tables in it. The swap
    /// must fit that layout too.
    #[tokio::test]
    async fn a_replace_commits_on_a_catalog_without_the_record_type_column() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (first, second) = two_appends(&wh, &orders).await;
        sqlx::query("ALTER TABLE iceberg_tables DROP COLUMN iceberg_type")
            .execute(wh.swap.pool())
            .await
            .unwrap();
        drop(wh);

        // The catalog detects the old layout on open and still serves the
        // table.
        let wh = Warehouse::open_local(dir.path()).await.unwrap();
        assert_eq!(wh.state(&orders).await.unwrap().head, Some(second.snapshot));
        let first_files = files_of(&wh, &orders, &first).await;
        let replaced = wh
            .replace_if_head(
                &orders,
                second.snapshot,
                swap_in(first_files, &[10]),
                &job("job-8"),
            )
            .await
            .unwrap();
        assert_eq!(all_ids(&wh.read_all(&orders).await.unwrap()), [3, 4, 5, 10]);
        let state = wh.state(&orders).await.unwrap();
        assert_eq!(state.properties["infostash.job-id"], "job-8");

        // A stale replace is still a conflict, and the catalog's own commit
        // still lands on the old layout.
        let stale = wh
            .replace_if_head(
                &orders,
                second.snapshot,
                swap_in(vec![], &[11]),
                &job("job-9"),
            )
            .await;
        assert!(matches!(stale, Err(Error::Conflict(_))), "{stale:?}");
        let after = Some(replaced.snapshot);
        wh.append_if_head(&orders, &[ids(&[12])], after, &position("p3"))
            .await
            .unwrap();
        let rows = wh.read_all(&orders).await.unwrap();
        assert_eq!(all_ids(&rows), [3, 4, 5, 10, 12]);
    }

    #[test]
    fn a_total_the_base_does_not_hold_as_a_number_stays_unset() {
        let strings = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let previous = strings(&[
            ("total-records", "10"),
            ("total-data-files", "many"),
            ("total-delete-files", "0"),
        ]);
        let mut fields = strings(&[
            ("added-records", "3"),
            ("deleted-records", "4"),
            ("added-data-files", "1"),
            ("deleted-data-files", "1"),
            ("added-files-size", "100"),
        ]);

        super::carry_totals(&previous, &mut fields);

        assert_eq!(fields["total-records"], "9");
        assert_eq!(fields["total-delete-files"], "0");
        assert!(!fields.contains_key("total-data-files"), "{fields:?}");
        assert!(!fields.contains_key("total-files-size"), "{fields:?}");
    }
}
