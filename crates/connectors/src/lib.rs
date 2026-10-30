//! Sources that land data in the Raw layer.
//!
//! Every connector reads from a resumable position, so ingest is
//! exactly-once when paired with the position stored in the Iceberg commit:
//! [`prepare`] reads the table's position and everything after it, and
//! [`Prepared::commit`] writes the rows and the new position in one commit,
//! only if no other commit landed in between.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use infostash_model::{Result, SnapshotId, TableRef};
use infostash_storage::{Commit, Warehouse};

mod csv;

pub use csv::{CsvConnector, LOADED_AT, SOURCE_FILE};

/// Table property, mirrored in the snapshot summary, that holds the
/// position the table's rows were read up to.
pub const POSITION: &str = "infostash.position";

/// Where a source left off: a file listing, Kafka offsets, a CDC log
/// sequence number. Opaque to everything except the connector. It is stored
/// as an Iceberg table property, so it is text.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Position(pub String);

/// A chunk of rows read from a source, plus the position after it.
#[derive(Debug, Clone)]
pub struct Batch {
    pub data: RecordBatch,
    pub next: Position,
}

pub trait Connector: Send {
    /// `csv`, `kafka`, `postgres_cdc`, ...
    fn kind(&self) -> &str;

    /// Read up to `max_rows` after `from`. `None` means caught up. A batch
    /// may have no rows if only the position moved.
    fn read(&mut self, from: &Position, max_rows: usize) -> Result<Option<Batch>>;
}

/// Rows read from a source, not yet committed.
#[derive(Debug)]
pub struct Prepared {
    table: TableRef,
    head: Option<SnapshotId>,
    from: Position,
    next: Position,
    batches: Vec<RecordBatch>,
}

/// Read everything the connector has after the table's stored position.
/// Nothing is written: a crash after this loses nothing, because the
/// position only moves when [`Prepared::commit`] succeeds.
pub async fn prepare(
    warehouse: &Warehouse,
    table: &TableRef,
    connector: &mut dyn Connector,
    max_rows: usize,
) -> Result<Prepared> {
    let (head, from) = if warehouse.exists(table).await? {
        let state = warehouse.state(table).await?;
        let from = state.properties.get(POSITION).cloned().unwrap_or_default();
        (state.head, Position(from))
    } else {
        (None, Position::default())
    };
    let mut next = from.clone();
    let mut batches = Vec::new();
    while let Some(batch) = connector.read(&next, max_rows)? {
        next = batch.next;
        if batch.data.num_rows() > 0 {
            batches.push(batch.data);
        }
    }
    Ok(Prepared {
        table: table.clone(),
        head,
        from,
        next,
        batches,
    })
}

impl Prepared {
    /// Rows read.
    pub fn rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Commit the rows and the new position in one snapshot. Returns `None`
    /// if the position did not move, so there was nothing to commit.
    ///
    /// Fails with [`infostash_model::Error::Conflict`] if another commit
    /// landed on the table after [`prepare`] read it. Nothing is committed,
    /// and running again from a fresh [`prepare`] is safe.
    pub async fn commit(self, warehouse: &Warehouse) -> Result<Option<Commit>> {
        if self.next == self.from {
            return Ok(None);
        }
        if self.head.is_none() && !warehouse.exists(&self.table).await? {
            let Some(first) = self.batches.first() else {
                // Nothing to infer a schema from. The position moved past
                // files with no rows; they'll be read again next time.
                return Ok(None);
            };
            warehouse.create_table(&self.table, &first.schema()).await?;
        }
        let properties = HashMap::from([(POSITION.to_string(), self.next.0)]);
        warehouse
            .append_if_head(&self.table, &self.batches, self.head, &properties)
            .await
            .map(Some)
    }
}
