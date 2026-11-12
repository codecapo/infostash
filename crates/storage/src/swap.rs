//! The compare-and-swap that commits a replace on the SQL catalog.
//!
//! This is the only code that knows the catalog's private `iceberg_tables`
//! layout. A catalog that cannot run the statement, such as the Phase 1
//! REST catalog, replaces this module and keeps the snapshot building in
//! [`crate::replace`]. See ADR 0007.

use iceberg::TableIdent;
use infostash_model::Result;
use sqlx::SqlitePool;

use crate::storage;

/// Our own connections to the catalog's database, next to the catalog's.
pub(crate) struct CatalogSwap {
    pool: SqlitePool,
    /// The name the warehouse registers under in the catalog, which keys
    /// every row of `iceberg_tables`.
    catalog_name: &'static str,
}

impl CatalogSwap {
    /// Connect to the catalog database at `uri`, which the catalog has
    /// already created.
    pub(crate) async fn connect(uri: &str, catalog_name: &'static str) -> Result<Self> {
        let pool = SqlitePool::connect(uri).await.map_err(storage)?;
        Ok(Self { pool, catalog_name })
    }

    /// The SQL catalog's own commit statement, less its record type filter:
    /// point `table` at `to` only if it still points at `from`, and keep
    /// `from` as the previous location. Returns whether a row changed.
    /// SQLite runs one writer at a time, so of two swaps from one `from`
    /// exactly one changes a row.
    ///
    /// The filter adds nothing: the key is the table's primary key, and a
    /// view row never holds a table's metadata location. Without it the
    /// statement fits both layouts of `iceberg_tables` the catalog reads,
    /// with and without the `iceberg_type` column. Every pin bump must
    /// compare this statement with the catalog's `update_table`.
    pub(crate) async fn swap(&self, table: &TableIdent, from: &str, to: &str) -> Result<bool> {
        let updated = sqlx::query(
            "UPDATE iceberg_tables \
             SET metadata_location = ?, previous_metadata_location = ? \
             WHERE catalog_name = ? AND table_name = ? AND table_namespace = ? \
             AND metadata_location = ?",
        )
        .bind(to)
        .bind(from)
        .bind(self.catalog_name)
        .bind(table.name())
        .bind(table.namespace().join("."))
        .bind(from)
        .execute(&self.pool)
        .await
        .map_err(storage)?
        .rows_affected();
        Ok(updated == 1)
    }

    /// The pool, for tests that change the catalog's layout.
    #[cfg(test)]
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use iceberg::TableIdent;

    use crate::ident;
    use crate::test_support::{all_ids, orders, two_appends};

    #[tokio::test]
    async fn the_swap_moves_the_catalog_pointer_once() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let (first, second) = two_appends(&wh, &orders).await;
        let table = ident(&orders);
        let (first_file, second_file) = (&first.metadata_location, &second.metadata_location);

        // Pointing the table back at the first metadata file rolls the head
        // back, and the same swap again finds no row to update.
        assert!(wh.swap.swap(&table, second_file, first_file).await.unwrap());
        assert_eq!(wh.state(&orders).await.unwrap().head, Some(first.snapshot));
        assert!(!wh.swap.swap(&table, second_file, first_file).await.unwrap());
        // Another table's row is not touched.
        let other = TableIdent::new(table.namespace().clone(), "other".into());
        assert!(!wh.swap.swap(&other, first_file, second_file).await.unwrap());
        assert_eq!(wh.state(&orders).await.unwrap().head, Some(first.snapshot));
        // Back to where it was, and the previous location is recorded.
        assert!(wh.swap.swap(&table, first_file, second_file).await.unwrap());
        assert_eq!(wh.state(&orders).await.unwrap().head, Some(second.snapshot));
        let previous: String = sqlx::query_scalar(
            "SELECT previous_metadata_location FROM iceberg_tables WHERE table_name = ?",
        )
        .bind(table.name())
        .fetch_one(wh.swap.pool())
        .await
        .unwrap();
        assert_eq!(&previous, first_file);
        let rows = wh.read_all(&orders).await.unwrap();
        assert_eq!(all_ids(&rows), [1, 2, 3, 4, 5]);
    }
}
