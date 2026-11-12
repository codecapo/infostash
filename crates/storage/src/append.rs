//! Appending rows in one snapshot, either unconditionally or only while
//! the table head is the one the caller read, with Infostash properties
//! set in the same commit.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use iceberg_catalog_sql::SqlCatalog;
use infostash_model::{Error, Result, SnapshotId, TableRef};

use crate::write::write_files;
use crate::{Commit, Warehouse, check_properties, ident, moved, storage};

impl Warehouse {
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

    /// Like [`Warehouse::append`], but commits only if the table head is
    /// still `expected_head`, and sets `properties` in the same commit, both
    /// as table properties and in the new snapshot's summary.
    ///
    /// Fails with [`Error::Conflict`] if another commit landed after the
    /// caller read `expected_head`; nothing is committed. iceberg-rust would
    /// otherwise rebase the commit onto the new head and duplicate rows.
    ///
    /// With no rows, the commit still makes a snapshot carrying
    /// `properties`. Every key must start with [`crate::PROPERTY_PREFIX`].
    pub async fn append_if_head(
        &self,
        table: &TableRef,
        batches: &[RecordBatch],
        expected_head: Option<SnapshotId>,
        properties: &HashMap<String, String>,
    ) -> Result<Commit> {
        check_properties(table, properties)?;
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use infostash_model::Error;

    use super::{HeadGuard, commit_if_head};
    use crate::ident;
    use crate::test_support::{ids, ids_schema, orders, position};
    use crate::write::write_files;

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
}
