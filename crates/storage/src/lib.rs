//! Object storage (local disk, S3, GCS, ADLS, MinIO) and Iceberg table
//! access. Data always lives in the customer's bucket in open formats.
//!
//! Phase 0 runs against a local directory. The catalog is a SQLite file in
//! that directory, so a [`Warehouse`] opened again later finds the same
//! tables. Every table's data and metadata are real Iceberg files on disk:
//! [`read_metadata_file`] reads a table back from its metadata file alone.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use futures::stream::BoxStream;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::TableProperties;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use infostash_model::{Error, Layer, Result, SnapshotId, TableRef};

mod append;
mod read;
mod replace;
mod swap;
#[cfg(test)]
mod test_support;
mod write;

pub use read::read_metadata_file;
pub use replace::Replacement;
use swap::CatalogSwap;

/// The result of one atomic commit.
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

/// Name the warehouse registers under in its catalog.
const CATALOG_NAME: &str = "infostash";

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
    /// Commits replaces, which the catalog's own commit cannot express.
    swap: CatalogSwap,
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
                CATALOG_NAME,
                HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), uri.clone()),
                    (
                        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                        format!("file://{}", root.display()),
                    ),
                ]),
            )
            .await
            .map_err(storage)?;
        let swap = CatalogSwap::connect(&uri, CATALOG_NAME).await?;
        Ok(Self { catalog, swap })
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
        // Iceberg's default, written out so every engine sees it.
        let compression = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.to_string(),
            TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC_DEFAULT.to_string(),
        )]);
        let creation = TableCreation::builder()
            .name(table.name.clone())
            .schema(arrow_schema_to_schema_auto_assign_ids(schema).map_err(storage)?)
            .properties(compression)
            .build();
        self.catalog
            .create_table(&ns, creation)
            .await
            .map_err(storage)?;
        Ok(())
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

    /// Every table in the Raw, Clean and Ready namespaces, sorted.
    pub async fn tables(&self) -> Result<Vec<TableRef>> {
        let mut tables = Vec::new();
        for layer in Layer::ALL {
            let ns = NamespaceIdent::new(layer.namespace().to_string());
            if !self.catalog.namespace_exists(&ns).await.map_err(storage)? {
                continue;
            }
            for t in self.catalog.list_tables(&ns).await.map_err(storage)? {
                tables.push(TableRef::new(layer, t.name()));
            }
        }
        tables.sort();
        Ok(tables)
    }

    /// Load a table at its current snapshot, for engines that read it
    /// directly.
    pub async fn load(&self, table: &TableRef) -> Result<Table> {
        self.catalog
            .load_table(&ident(table))
            .await
            .map_err(storage)
    }
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

/// Refuse a property key Infostash does not own.
fn check_properties(table: &TableRef, properties: &HashMap<String, String>) -> Result<()> {
    match properties.keys().find(|k| !k.starts_with(PROPERTY_PREFIX)) {
        Some(key) => Err(Error::Storage(format!(
            "{table}: property `{key}` must start with `{PROPERTY_PREFIX}`"
        ))),
        None => Ok(()),
    }
}

fn moved(table: &TableRef, expected: Option<SnapshotId>, found: Option<SnapshotId>) -> Error {
    let show = |id: Option<SnapshotId>| id.map_or("no snapshot".to_string(), |id| id.to_string());
    Error::Conflict(format!(
        "{table}: expected head {}, found {}; another commit landed first",
        show(expected),
        show(found)
    ))
}
