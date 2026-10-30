//! Object storage (local disk, S3, GCS, ADLS, MinIO) and Iceberg table
//! access. Data always lives in the customer's bucket in open formats.
//!
//! Phase 0 runs against a local directory. The catalog is a SQLite file in
//! that directory, so a [`Warehouse`] opened again later finds the same
//! tables. Every table's data and metadata are real Iceberg files on disk:
//! [`read_metadata_file`] reads a table back from its metadata file alone.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::spec::{DataFileFormat, ManifestStatus, Operation, TableMetadata};
use iceberg::table::{StaticTable, Table};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use infostash_model::{Error, Result, SnapshotId, SnapshotRange, TableRef};
use parquet::file::properties::WriterProperties;

/// The result of one atomic append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub snapshot: SnapshotId,
    /// Iceberg metadata file for the table as of this commit.
    pub metadata_location: String,
    pub rows: u64,
}

/// Record batches read lazily from a table.
pub type RecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// File in the warehouse root that holds the catalog.
const CATALOG_FILE: &str = "catalog.db";

/// Escape a file path for a SQLite URL. sqlx splits the URL at the first `?`
/// and percent-decodes the path, so `%`, `?` and `#` in directory names must
/// be encoded or the catalog opens the wrong file.
fn sqlite_path(path: &Path) -> String {
    path.display()
        .to_string()
        .replace('%', "%25")
        .replace('?', "%3F")
        .replace('#', "%23")
}

/// A set of Iceberg tables under one storage root.
pub struct Warehouse {
    catalog: SqlCatalog,
}

impl Warehouse {
    /// Open a warehouse rooted at a local directory, creating its catalog
    /// on first use. Tables created by an earlier process are still there.
    pub async fn open_local(root: &Path) -> Result<Self> {
        let root = root
            .canonicalize()
            .map_err(|e| Error::Storage(format!("{}: {e}", root.display())))?;
        // mode=rwc creates the database file if it doesn't exist yet.
        let uri = format!("sqlite:{}?mode=rwc", sqlite_path(&root.join(CATALOG_FILE)));
        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "infostash",
                HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), uri),
                    (
                        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                        format!("file://{}", root.display()),
                    ),
                ]),
            )
            .await
            .map_err(storage)?;
        Ok(Self { catalog })
    }

    /// Create an empty table with the given schema.
    pub async fn create_table(&self, table: &TableRef, schema: &ArrowSchema) -> Result<()> {
        let ns = NamespaceIdent::new(table.layer.namespace().to_string());
        if !self.catalog.namespace_exists(&ns).await.map_err(storage)? {
            self.catalog
                .create_namespace(&ns, HashMap::new())
                .await
                .map_err(storage)?;
        }
        let creation = TableCreation::builder()
            .name(table.name.clone())
            .schema(arrow_schema_to_schema_auto_assign_ids(schema).map_err(storage)?)
            .build();
        self.catalog
            .create_table(&ns, creation)
            .await
            .map_err(storage)?;
        Ok(())
    }

    /// Write `batches` as new data files and commit them in one snapshot.
    pub async fn append(&self, table: &TableRef, batches: &[RecordBatch]) -> Result<Commit> {
        let current = self.load(table).await?;
        let metadata = current.metadata();
        let schema = metadata.current_schema().clone();
        // Batches must carry Iceberg field ids so columns map by id, not name.
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).map_err(storage)?);

        let files = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(WriterProperties::default(), schema),
            current.file_io().clone(),
            DefaultLocationGenerator::new(metadata).map_err(storage)?,
            // A fresh prefix per write: the generator only counts up from 0.
            DefaultFileNameGenerator::new(
                uuid::Uuid::new_v4().to_string(),
                None,
                DataFileFormat::Parquet,
            ),
        );
        let mut writer = DataFileWriterBuilder::new(files)
            .build(None)
            .await
            .map_err(storage)?;
        let mut rows = 0;
        for batch in batches {
            let batch = RecordBatch::try_new(arrow_schema.clone(), batch.columns().to_vec())
                .map_err(|e| Error::Storage(format!("batch does not match {table}: {e}")))?;
            rows += batch.num_rows() as u64;
            writer.write(batch).await.map_err(storage)?;
        }
        let data_files = writer.close().await.map_err(storage)?;

        let tx = Transaction::new(&current);
        let tx = tx
            .fast_append()
            .add_data_files(data_files)
            .apply(tx)
            .map_err(storage)?;
        let committed = tx.commit(&self.catalog).await.map_err(storage)?;

        let snapshot = committed
            .metadata()
            .current_snapshot()
            .ok_or_else(|| Error::Storage(format!("{table}: commit produced no snapshot")))?;
        Ok(Commit {
            snapshot: snapshot.snapshot_id(),
            metadata_location: committed
                .metadata_location_result()
                .map_err(storage)?
                .into(),
            rows,
        })
    }

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

    /// The table's snapshot ids, oldest first.
    pub async fn snapshots(&self, table: &TableRef) -> Result<Vec<SnapshotId>> {
        let table = self.load(table).await?;
        let mut snaps: Vec<_> = table
            .metadata()
            .snapshots()
            .map(|s| (s.sequence_number(), s.snapshot_id()))
            .collect();
        snaps.sort();
        Ok(snaps.into_iter().map(|(_, id)| id).collect())
    }

    async fn load(&self, table: &TableRef) -> Result<Table> {
        self.catalog
            .load_table(&ident(table))
            .await
            .map_err(storage)
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

fn ident(table: &TableRef) -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new(table.layer.namespace().to_string()),
        table.name.clone(),
    )
}

fn storage(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
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
async fn files_added_by(
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
async fn scan_at(
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
    #![allow(clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use iceberg::spec::{MAIN_BRANCH, Snapshot, Summary};
    use infostash_model::Layer;

    use super::*;

    #[tokio::test]
    async fn an_overwrite_in_the_range_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let wh = Warehouse::open_local(dir.path()).await.unwrap();
        let orders = TableRef::new(Layer::Raw, "orders");
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        wh.create_table(&orders, &schema).await.unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]);
        let append = wh.append(&orders, &[batch.unwrap()]).await.unwrap();

        // Nothing public in the pinned iceberg-rust makes an overwrite, so
        // add one to the metadata by hand. Its manifest list is never read.
        let metadata = wh.load(&orders).await.unwrap().metadata().clone();
        let overwrite = Snapshot::builder()
            .with_snapshot_id(append.snapshot + 1)
            .with_parent_snapshot_id(Some(append.snapshot))
            .with_sequence_number(metadata.next_sequence_number())
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list("unused.avro")
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(metadata.current_schema_id())
            .build();
        let metadata = metadata
            .into_builder(None)
            .set_branch_snapshot(overwrite, MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        let err = appends_between(&metadata, &orders, append.snapshot, append.snapshot + 1);
        assert!(
            matches!(&err, Err(Error::Storage(m)) if m.contains("`overwrite`, not an append")),
            "{err:?}"
        );
        // An overwrite before the range doesn't matter.
        let from_overwrite =
            appends_between(&metadata, &orders, append.snapshot + 1, append.snapshot + 1);
        assert!(from_overwrite.unwrap().is_empty());
    }
}
