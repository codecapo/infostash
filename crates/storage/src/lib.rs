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
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::spec::{DataFile, DataFileFormat, ManifestStatus, Operation, TableMetadata};
use iceberg::table::{StaticTable, Table};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation,
    TableIdent,
};
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

/// A table's head and properties, read together so that a position stored
/// in the properties and the snapshot it belongs to always agree.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableState {
    /// The current snapshot, or `None` for a table with no commits.
    pub head: Option<SnapshotId>,
    pub properties: HashMap<String, String>,
}

/// Prefix for the table and snapshot properties Infostash owns. Iceberg
/// computes its own summary keys (`added-*`, `total-*`) and would overwrite
/// ours if they collided.
pub const PROPERTY_PREFIX: &str = "infostash.";

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
        let (files, rows) = write_files(&current, table, batches).await?;
        let tx = Transaction::new(&current);
        let tx = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)
            .map_err(storage)?;
        let committed = tx.commit(&self.catalog).await.map_err(storage)?;
        commit_result(table, &committed, rows)
    }

    /// The table's head and properties, from one load.
    pub async fn state(&self, table: &TableRef) -> Result<TableState> {
        let loaded = self.load(table).await?;
        let metadata = loaded.metadata();
        Ok(TableState {
            head: metadata.current_snapshot_id(),
            properties: metadata.properties().clone(),
        })
    }

    /// Whether the table exists.
    pub async fn exists(&self, table: &TableRef) -> Result<bool> {
        self.catalog
            .table_exists(&ident(table))
            .await
            .map_err(storage)
    }

    /// Like [`Warehouse::append`], but commits only if the table head is
    /// still `expected_head`, and sets `properties` in the same commit, both
    /// as table properties and in the new snapshot's summary.
    ///
    /// Fails with [`Error::Conflict`] if another commit landed after the
    /// caller read `expected_head`; nothing is committed. iceberg-rust would
    /// otherwise rebase the commit onto the new head and duplicate rows.
    ///
    /// With no rows, the commit still makes a snapshot carrying
    /// `properties`. Every key must start with [`PROPERTY_PREFIX`].
    pub async fn append_if_head(
        &self,
        table: &TableRef,
        batches: &[RecordBatch],
        expected_head: Option<SnapshotId>,
        properties: &HashMap<String, String>,
    ) -> Result<Commit> {
        if let Some(key) = properties.keys().find(|k| !k.starts_with(PROPERTY_PREFIX)) {
            return Err(Error::Storage(format!(
                "{table}: property `{key}` must start with `{PROPERTY_PREFIX}`"
            )));
        }
        if properties.is_empty() && batches.iter().all(|b| b.num_rows() == 0) {
            return Err(Error::Storage(format!(
                "{table}: nothing to commit (no rows and no properties)"
            )));
        }
        let current = self.load(table).await?;
        let found = current.metadata().current_snapshot_id();
        if found != expected_head {
            return Err(moved(table, expected_head, found));
        }
        let (files, rows) = write_files(&current, table, batches).await?;

        let tx = Transaction::new(&current);
        let mut tx = tx
            .fast_append()
            .add_data_files(files)
            .set_snapshot_properties(properties.clone())
            .apply(tx)
            .map_err(storage)?;
        if !properties.is_empty() {
            let mut update = tx.update_table_properties();
            for (key, value) in properties {
                update = update.set(key.clone(), value.clone());
            }
            tx = update.apply(tx).map_err(storage)?;
        }
        let committed = commit_if_head(&self.catalog, table, expected_head, tx).await?;
        commit_result(table, &committed, rows)
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

fn moved(table: &TableRef, expected: Option<SnapshotId>, found: Option<SnapshotId>) -> Error {
    let show = |id: Option<SnapshotId>| id.map_or("no snapshot".to_string(), |id| id.to_string());
    Error::Conflict(format!(
        "{table}: expected head {}, found {}; another commit landed first",
        show(expected),
        show(found)
    ))
}

/// Write `batches` as new Parquet data files for `table`. Returns the files
/// and the number of rows in them. Nothing is committed.
async fn write_files(
    current: &Table,
    table: &TableRef,
    batches: &[RecordBatch],
) -> Result<(Vec<DataFile>, u64)> {
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
        let names = |s: &ArrowSchema| {
            s.fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        };
        if names(&batch.schema()) != names(&arrow_schema) {
            return Err(Error::Storage(format!(
                "batch does not match {table}: columns {:?}, expected {:?}",
                names(&batch.schema()),
                names(&arrow_schema)
            )));
        }
        let batch = RecordBatch::try_new(arrow_schema.clone(), batch.columns().to_vec())
            .map_err(|e| Error::Storage(format!("batch does not match {table}: {e}")))?;
        rows += batch.num_rows() as u64;
        writer.write(batch).await.map_err(storage)?;
    }
    let files = writer.close().await.map_err(storage)?;
    Ok((files, rows))
}

fn commit_result(table: &TableRef, committed: &Table, rows: u64) -> Result<Commit> {
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

/// The catalog, except that loading `table` fails unless its head is still
/// `expected`. `Transaction::commit` reloads the table before it applies its
/// actions, and would otherwise rebase a stale commit onto the new head.
///
/// A commit that lands between this load and the catalog update is caught
/// too: the SQL catalog's compare-and-swap rejects the update with a
/// retryable conflict, and the retry reloads through this guard.
struct HeadGuard<'a> {
    inner: &'a SqlCatalog,
    table: TableIdent,
    expected: Option<SnapshotId>,
    /// Set when the guard refused a load, so the caller can report a
    /// conflict rather than a storage error.
    tripped: AtomicBool,
}

impl std::fmt::Debug for HeadGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeadGuard")
            .field("table", &self.table)
            .field("expected", &self.expected)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Catalog for HeadGuard<'_> {
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let loaded = self.inner.load_table(table).await?;
        let found = loaded.metadata().current_snapshot_id();
        if *table == self.table && found != self.expected {
            self.tripped.store(true, Ordering::SeqCst);
            return Err(iceberg::Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "{table}: expected head {:?}, found {found:?}",
                    self.expected
                ),
            ));
        }
        Ok(loaded)
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }

    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }
}

/// Commit `tx`, or fail with [`Error::Conflict`] if the head is not
/// `expected` whenever iceberg loads the table, retries included.
async fn commit_if_head(
    catalog: &SqlCatalog,
    table: &TableRef,
    expected: Option<SnapshotId>,
    tx: Transaction,
) -> Result<Table> {
    let guard = HeadGuard {
        inner: catalog,
        table: ident(table),
        expected,
        tripped: AtomicBool::new(false),
    };
    match tx.commit(&guard).await {
        Ok(committed) => Ok(committed),
        Err(_) if guard.tripped.load(Ordering::SeqCst) => {
            let found = catalog.load_table(&ident(table)).await.map_err(storage)?;
            Err(moved(
                table,
                expected,
                found.metadata().current_snapshot_id(),
            ))
        }
        Err(e) => Err(storage(e)),
    }
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

    fn ids_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]))
    }

    fn ids(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            ids_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn position(value: &str) -> HashMap<String, String> {
        HashMap::from([("infostash.position".to_string(), value.to_string())])
    }

    async fn orders(dir: &Path) -> (Warehouse, TableRef) {
        let wh = Warehouse::open_local(dir).await.unwrap();
        let orders = TableRef::new(Layer::Raw, "orders");
        wh.create_table(&orders, &ids_schema()).await.unwrap();
        (wh, orders)
    }

    #[tokio::test]
    async fn rows_and_properties_land_in_one_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let before = wh
            .load(&orders)
            .await
            .unwrap()
            .metadata()
            .metadata_log()
            .len();

        let commit = wh
            .append_if_head(&orders, &[ids(&[1, 2])], None, &position("p1"))
            .await
            .unwrap();

        let loaded = wh.load(&orders).await.unwrap();
        let metadata = loaded.metadata();
        // One new metadata file holds the rows and both copies of the property.
        assert_eq!(metadata.metadata_log().len(), before + 1);
        assert_eq!(metadata.current_snapshot_id(), Some(commit.snapshot));
        assert_eq!(commit.rows, 2);
        let summary = &metadata.current_snapshot().unwrap().summary();
        assert_eq!(summary.additional_properties["infostash.position"], "p1");
        let state = wh.state(&orders).await.unwrap();
        assert_eq!(state.head, Some(commit.snapshot));
        assert_eq!(state.properties["infostash.position"], "p1");
    }

    #[tokio::test]
    async fn a_moved_head_is_a_conflict_and_commits_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let first = wh
            .append_if_head(&orders, &[ids(&[1])], None, &position("p1"))
            .await
            .unwrap();

        // A second writer that also read the empty table.
        let stale = wh
            .append_if_head(&orders, &[ids(&[1])], None, &position("p1"))
            .await;
        assert!(matches!(stale, Err(Error::Conflict(_))), "{stale:?}");
        assert_eq!(wh.snapshots(&orders).await.unwrap(), [first.snapshot]);
        assert_eq!(wh.read_all(&orders).await.unwrap()[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn the_guard_stops_a_commit_that_lands_after_the_writer_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        // The writer loads the empty table, then another commit lands.
        let stale = wh.load(&orders).await.unwrap();
        wh.append(&orders, &[ids(&[1])]).await.unwrap();

        let (files, _) = write_files(&stale, &orders, &[ids(&[2])]).await.unwrap();
        let tx = Transaction::new(&stale);
        let tx = tx.fast_append().add_data_files(files).apply(tx).unwrap();
        let lost = commit_if_head(&wh.catalog, &orders, None, tx).await;
        assert!(matches!(&lost, Err(Error::Conflict(_))), "{lost:?}");
        // Without the guard, iceberg-rust would have rebased and committed.
        assert_eq!(wh.snapshots(&orders).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_guard_passes_everything_else_through() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let guard = HeadGuard {
            inner: &wh.catalog,
            table: ident(&orders),
            expected: None,
            tripped: AtomicBool::new(false),
        };
        assert!(format!("{guard:?}").contains("HeadGuard"));
        let raw = ident(&orders).namespace().clone();
        let other = NamespaceIdent::new("other".into());
        assert!(guard.list_namespaces(None).await.unwrap().contains(&raw));
        guard
            .create_namespace(&other, HashMap::new())
            .await
            .unwrap();
        assert!(guard.namespace_exists(&other).await.unwrap());
        assert_eq!(guard.get_namespace(&other).await.unwrap().name(), &other);
        guard
            .update_namespace(&other, HashMap::from([("k".into(), "v".into())]))
            .await
            .unwrap();
        guard.drop_namespace(&other).await.unwrap();

        assert_eq!(guard.list_tables(&raw).await.unwrap(), [ident(&orders)]);
        let schema = arrow_schema_to_schema_auto_assign_ids(&ids_schema()).unwrap();
        let creation = TableCreation::builder()
            .name("scratch".into())
            .schema(schema)
            .build();
        let created = guard.create_table(&raw, creation).await.unwrap();
        let scratch = created.identifier().clone();
        let moved_to = TableIdent::new(raw.clone(), "moved".into());
        guard.rename_table(&scratch, &moved_to).await.unwrap();
        assert!(guard.table_exists(&moved_to).await.unwrap());
        guard.drop_table(&moved_to).await.unwrap();
        let location = created.metadata_location().unwrap().to_string();
        guard.register_table(&scratch, location).await.unwrap();
        guard.purge_table(&scratch).await.unwrap();
        assert!(!guard.table_exists(&scratch).await.unwrap());
        assert!(!guard.tripped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn properties_alone_make_a_snapshot_with_no_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let marker = wh
            .append_if_head(&orders, &[], None, &position("p1"))
            .await
            .unwrap();
        assert_eq!(marker.rows, 0);
        assert_eq!(wh.snapshots(&orders).await.unwrap(), [marker.snapshot]);

        let nothing = wh
            .append_if_head(&orders, &[], Some(marker.snapshot), &HashMap::new())
            .await;
        assert!(matches!(nothing, Err(Error::Storage(_))), "{nothing:?}");
        let foreign = HashMap::from([("total-records".to_string(), "9".to_string())]);
        let foreign = wh
            .append_if_head(&orders, &[ids(&[1])], Some(marker.snapshot), &foreign)
            .await;
        assert!(matches!(&foreign, Err(Error::Storage(m)) if m.contains("total-records")));
        assert_eq!(wh.snapshots(&orders).await.unwrap(), [marker.snapshot]);

        // Rows need no properties.
        let plain = wh
            .append_if_head(
                &orders,
                &[ids(&[1])],
                Some(marker.snapshot),
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(plain.rows, 1);
    }

    #[tokio::test]
    async fn a_batch_with_other_column_names_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let err = wh.append(&orders, &[batch]).await;
        assert!(
            matches!(&err, Err(Error::Storage(m)) if m.contains("amount")),
            "{err:?}"
        );
    }
}
