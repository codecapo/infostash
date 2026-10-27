//! Sources that land data in the Raw layer.
//!
//! Every connector reads from a resumable position, so ingest is
//! exactly-once when paired with the position stored in the Iceberg commit.

use infostash_model::Result;

/// Where a source left off: a file listing watermark, Kafka offsets, a CDC
/// log sequence number. Opaque to everything except the connector.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Position(pub Vec<u8>);

/// A chunk of rows read from a source, plus the position after it.
/// Rows will become Arrow record batches once the engine lands.
#[derive(Debug, Clone)]
pub struct Batch {
    pub rows: Vec<Vec<String>>,
    pub columns: Vec<String>,
    pub next: Position,
}

pub trait Connector: Send {
    /// `csv`, `kafka`, `postgres_cdc`, ...
    fn kind(&self) -> &str;

    /// Read up to `max_rows` after `from`. `None` means caught up.
    fn read(&mut self, from: &Position, max_rows: usize) -> Result<Option<Batch>>;
}
