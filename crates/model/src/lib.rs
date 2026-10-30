//! Core types shared by every crate.
//!
//! The unit of work in the platform is a [`Job`]: one project version, one
//! target table and a range of upstream snapshots in, an Iceberg commit and
//! metrics out. ADR 0006 extends this to one commit per table a job writes;
//! the types here still model a single target. Everything else (scheduler,
//! runners, distributed execution) is built around moving jobs and their
//! outcomes.

use std::fmt;

/// Medallion layer. Shown in the UI as Raw, Clean and Ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Layer {
    /// Bronze: data as it arrived, append only.
    Raw,
    /// Silver: typed, deduplicated, merged on a unique key, checked.
    Clean,
    /// Gold: business-ready aggregates and models.
    Ready,
}

impl Layer {
    /// Namespace used in SQL and in the catalog (`bronze.orders`).
    pub fn namespace(self) -> &'static str {
        match self {
            Layer::Raw => "bronze",
            Layer::Clean => "silver",
            Layer::Ready => "gold",
        }
    }

    pub fn from_namespace(ns: &str) -> Option<Self> {
        match ns {
            "bronze" | "raw" => Some(Layer::Raw),
            "silver" | "clean" => Some(Layer::Clean),
            "gold" | "ready" => Some(Layer::Ready),
            _ => None,
        }
    }
}

/// A table in the lake, addressed as `<layer namespace>.<name>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableRef {
    pub layer: Layer,
    pub name: String,
}

impl TableRef {
    pub fn new(layer: Layer, name: impl Into<String>) -> Self {
        Self {
            layer,
            name: name.into(),
        }
    }

    /// Parse `silver.orders`.
    pub fn parse(qualified: &str) -> Option<Self> {
        let (ns, name) = qualified.split_once('.')?;
        Some(Self::new(Layer::from_namespace(ns)?, name))
    }

    /// The quarantine table that receives rows failing this table's checks.
    pub fn quarantine(&self) -> TableRef {
        TableRef::new(self.layer, format!("{}__quarantine", self.name))
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.layer.namespace(), self.name)
    }
}

/// Iceberg snapshot id.
pub type SnapshotId = i64;

/// Upstream data a job must process: everything committed after `from`
/// up to and including `to`. `from == None` means a full build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRange {
    pub from: Option<SnapshotId>,
    pub to: SnapshotId,
}

impl SnapshotRange {
    pub fn is_full(&self) -> bool {
        self.from.is_none()
    }
}

/// Content hash of the project files a job was compiled from, so a job is
/// reproducible and cache keys change when the SQL changes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectVersion(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(pub u64);

/// The unit of work handed from the control plane to the data plane.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: JobId,
    pub project: ProjectVersion,
    pub target: TableRef,
    /// Snapshot range to read for each upstream table.
    pub inputs: Vec<(TableRef, SnapshotRange)>,
    /// Compute pool to run in (ingest, pipelines, bi, agents).
    pub pool: Pool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pool {
    Ingest,
    Pipelines,
    Bi,
    Agents,
}

/// What a job hands back. Metering and lineage are built from this.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JobMetrics {
    pub rows_read: u64,
    pub rows_written: u64,
    pub rows_quarantined: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub compute_millis: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JobOutcome {
    Committed {
        snapshot: SnapshotId,
        metrics: JobMetrics,
    },
    /// Upstream had no new snapshots; nothing to do.
    NoChange,
    Failed {
        error: String,
        retryable: bool,
    },
}

#[derive(Debug)]
pub enum Error {
    Parse(String),
    Plan(String),
    Storage(String),
    Execution(String),
    NotFound(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Parse(m) => write!(f, "parse error: {m}"),
            Error::Plan(m) => write!(f, "plan error: {m}"),
            Error::Storage(m) => write!(f, "storage error: {m}"),
            Error::Execution(m) => write!(f, "execution error: {m}"),
            Error::NotFound(m) => write!(f, "not found: {m}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_ref_round_trips() {
        let t = TableRef::parse("silver.orders").unwrap();
        assert_eq!(t.layer, Layer::Clean);
        assert_eq!(t.to_string(), "silver.orders");
        assert_eq!(TableRef::parse("clean.orders"), Some(t));
    }

    #[test]
    fn quarantine_stays_in_layer() {
        let q = TableRef::new(Layer::Clean, "orders").quarantine();
        assert_eq!(q.to_string(), "silver.orders__quarantine");
    }

    #[test]
    fn unknown_namespace_rejected() {
        assert!(TableRef::parse("platinum.orders").is_none());
        assert!(TableRef::parse("orders").is_none());
    }

    #[test]
    fn every_layer_round_trips() {
        for layer in [Layer::Raw, Layer::Clean, Layer::Ready] {
            assert_eq!(Layer::from_namespace(layer.namespace()), Some(layer));
        }
        assert_eq!(Layer::from_namespace("ready"), Some(Layer::Ready));
    }

    #[test]
    fn full_build_has_no_start() {
        assert!(SnapshotRange { from: None, to: 7 }.is_full());
        assert!(
            !SnapshotRange {
                from: Some(3),
                to: 7
            }
            .is_full()
        );
    }

    #[test]
    fn errors_name_their_kind() {
        let cases = [
            (Error::Parse("x".into()), "parse error: x"),
            (Error::Plan("x".into()), "plan error: x"),
            (Error::Storage("x".into()), "storage error: x"),
            (Error::Execution("x".into()), "execution error: x"),
            (Error::NotFound("x".into()), "not found: x"),
        ];
        for (error, text) in cases {
            assert_eq!(error.to_string(), text);
        }
    }
}
