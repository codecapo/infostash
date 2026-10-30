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
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::spec::{DataFileFormat, ManifestStatus};
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
use infostash_model::{Error, Result, SnapshotId, TableRef};
use parquet::file::properties::WriterProperties;

/// The result of one atomic append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub snapshot: SnapshotId,
    /// Iceberg metadata file for the table as of this commit.
    pub metadata_location: String,
    pub rows: u64,
}

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
        scan_current(&self.load(table).await?, None).await
    }

    /// Rows committed after snapshot `after`, up to the current snapshot.
    /// `None` reads everything. This is what incremental runs consume.
    ///
    /// Append-only for now: files added and then removed in the range are
    /// skipped, and row-level deletes are not applied.
    pub async fn read_since(
        &self,
        table: &TableRef,
        after: Option<SnapshotId>,
    ) -> Result<Vec<RecordBatch>> {
        let table = self.load(table).await?;
        let Some(after) = after else {
            return scan_current(&table, None).await;
        };
        let added = files_added_since(&table, after).await?;
        scan_current(&table, Some(&added)).await
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
    scan_current(&table, None).await
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

/// Data files added by snapshots after `after` on the current branch.
async fn files_added_since(table: &Table, after: SnapshotId) -> Result<HashSet<String>> {
    let metadata = table.metadata();
    let mut range = HashSet::new();
    let mut cursor = metadata.current_snapshot().cloned();
    while let Some(snap) = cursor {
        if snap.snapshot_id() == after {
            break;
        }
        range.insert(snap.snapshot_id());
        cursor = snap
            .parent_snapshot_id()
            .and_then(|p| metadata.snapshot_by_id(p).cloned());
    }

    let mut files = HashSet::new();
    let Some(current) = metadata.current_snapshot() else {
        return Ok(files);
    };
    let list = table
        .manifest_list_reader(current)
        .load()
        .await
        .map_err(storage)?;
    for manifest in list.entries() {
        if !range.contains(&manifest.added_snapshot_id) {
            continue;
        }
        let manifest = table
            .manifest_reader()
            .read(manifest)
            .await
            .map_err(storage)?;
        for entry in manifest.entries() {
            let in_range = entry.snapshot_id().is_some_and(|s| range.contains(&s));
            if in_range && entry.status() == ManifestStatus::Added {
                files.insert(entry.file_path().to_string());
            }
        }
    }
    Ok(files)
}

/// Scan the current snapshot, optionally limited to some data files.
async fn scan_current(table: &Table, only: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
    let scan = table.scan().select_all().build().map_err(storage)?;
    let tasks = scan.plan_files().await.map_err(storage)?;
    let tasks = match only {
        None => tasks,
        Some(only) => {
            let only = only.clone();
            tasks
                .try_filter(move |t| std::future::ready(only.contains(t.data_file_path())))
                .boxed()
        }
    };
    table
        .reader_builder()
        .build()
        .read(tasks)
        .map_err(storage)?
        .stream()
        .try_collect()
        .await
        .map_err(storage)
}
