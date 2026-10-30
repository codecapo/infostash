//! Load CSV files matching a glob.
//!
//! Every column is read as text: Raw keeps what the source said, and Clean
//! models do the typing. Two columns are added to every row:
//! [`SOURCE_FILE`] and [`LOADED_AT`].
//!
//! The position is the list of files already loaded, by path. A file is
//! loaded once: changing a loaded file, or moving the files to another
//! directory, does not load it again.

use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use infostash_model::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::{Batch, Connector, Position};

/// Column holding the path of the file a row came from.
pub const SOURCE_FILE: &str = "_source_file";
/// Column holding when the row was read, in UTC.
pub const LOADED_AT: &str = "_loaded_at";

/// What the position records, as JSON.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct Loaded {
    /// Files read to the end.
    files: BTreeSet<String>,
    /// The file being read and the rows already read from it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reading: Option<(String, usize)>,
}

impl Loaded {
    fn parse(position: &Position) -> Result<Self> {
        if position.0.is_empty() {
            return Ok(Self::default());
        }
        serde_json::from_str(&position.0)
            .map_err(|e| Error::Parse(format!("csv position `{}`: {e}", position.0)))
    }

    fn position(&self) -> Result<Position> {
        serde_json::to_string(self)
            .map(Position)
            .map_err(|e| Error::Execution(format!("csv position: {e}")))
    }
}

/// A file open for reading, so consecutive reads don't parse it again.
struct Open {
    path: String,
    rows: usize,
    schema: SchemaRef,
    reader: arrow_csv::Reader<File>,
}

/// Reads CSV files with a header row that match a glob pattern, such as
/// `data/orders/*.csv`, in path order.
pub struct CsvConnector {
    pattern: String,
    open: Option<Open>,
}

impl CsvConnector {
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            open: None,
        }
    }

    fn files(&self) -> Result<Vec<String>> {
        let paths = glob::glob(&self.pattern)
            .map_err(|e| Error::Parse(format!("csv pattern `{}`: {e}", self.pattern)))?;
        let mut files = Vec::new();
        for path in paths {
            let path = path.map_err(|e| Error::Execution(format!("csv: {e}")))?;
            if path.is_file() {
                let path = path.to_str().ok_or_else(|| {
                    Error::Execution(format!("csv: {} is not valid UTF-8", path.display()))
                })?;
                files.push(path.to_string());
            }
        }
        files.sort();
        Ok(files)
    }

    /// The open reader for `path`, positioned after `rows` rows.
    fn open(&mut self, path: &str, rows: usize, max_rows: usize) -> Result<&mut Open> {
        let reuse = self
            .open
            .as_ref()
            .is_some_and(|o| o.path == path && o.rows == rows);
        if !reuse {
            let schema = header(path)?;
            let mut builder = arrow_csv::ReaderBuilder::new(schema.clone())
                .with_header(true)
                .with_batch_size(max_rows);
            if rows > 0 {
                // Bounds count rows after the header; arrow adds the header
                // to `end`, so leave room for it.
                builder = builder.with_bounds(rows, usize::MAX - 1);
            }
            let file = File::open(path).map_err(|e| Error::Execution(format!("{path}: {e}")))?;
            let reader = builder
                .build(file)
                .map_err(|e| Error::Parse(format!("{path}: {e}")))?;
            self.open = Some(Open {
                path: path.to_string(),
                rows,
                schema,
                reader,
            });
        }
        self.open
            .as_mut()
            .ok_or_else(|| Error::Execution("csv: no open file".into()))
    }
}

impl Connector for CsvConnector {
    fn kind(&self) -> &str {
        "csv"
    }

    fn read(&mut self, from: &Position, max_rows: usize) -> Result<Option<Batch>> {
        let mut loaded = Loaded::parse(from)?;
        let (path, rows) = match loaded.reading.take() {
            Some(reading) => reading,
            None => match self
                .files()?
                .into_iter()
                .find(|f| !loaded.files.contains(f))
            {
                Some(path) => (path, 0),
                None => return Ok(None),
            },
        };
        let open = self.open(&path, rows, max_rows.max(1))?;
        let next = open
            .reader
            .next()
            .transpose()
            .map_err(|e| Error::Parse(format!("{path}: {e}")))?;

        let data = match next {
            Some(batch) => {
                open.rows += batch.num_rows();
                loaded.reading = Some((path.clone(), open.rows));
                with_load_columns(&batch, &path)?
            }
            None => {
                // The file is done. Record it, with no rows.
                let empty = RecordBatch::new_empty(open.schema.clone());
                self.open = None;
                loaded.files.insert(path.clone());
                with_load_columns(&empty, &path)?
            }
        };
        Ok(Some(Batch {
            data,
            next: loaded.position()?,
        }))
    }
}

/// The file's columns, all text, from its header row.
fn header(path: &str) -> Result<SchemaRef> {
    let mut reader =
        csv::Reader::from_path(path).map_err(|e| Error::Parse(format!("{path}: {e}")))?;
    let names = reader
        .headers()
        .map_err(|e| Error::Parse(format!("{path}: {e}")))?;
    let mut fields = Vec::with_capacity(names.len());
    for name in names {
        if name.is_empty() {
            return Err(Error::Parse(format!("{path}: a column has no name")));
        }
        if name == SOURCE_FILE || name == LOADED_AT {
            return Err(Error::Parse(format!(
                "{path}: column `{name}` is reserved for load metadata"
            )));
        }
        if fields.iter().any(|f: &Field| f.name() == name) {
            return Err(Error::Parse(format!(
                "{path}: column `{name}` appears twice"
            )));
        }
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// `batch` plus the source file and load time columns.
fn with_load_columns(batch: &RecordBatch, path: &str) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Execution(format!("clock: {e}")))?;
    let now =
        i64::try_from(now.as_micros()).map_err(|e| Error::Execution(format!("clock: {e}")))?;

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(SOURCE_FILE, DataType::Utf8, false));
    fields.push(Field::new(
        LOADED_AT,
        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        false,
    ));
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![path; rows])));
    columns.push(Arc::new(
        TimestampMicrosecondArray::from(vec![now; rows]).with_timezone("+00:00"),
    ));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| Error::Execution(format!("{path}: {e}")))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn position_round_trips() {
        let loaded = Loaded {
            files: BTreeSet::from(["a.csv".to_string()]),
            reading: Some(("b.csv".to_string(), 3)),
        };
        assert_eq!(Loaded::parse(&loaded.position().unwrap()).unwrap(), loaded);
        assert_eq!(
            Loaded::parse(&Position::default()).unwrap(),
            Loaded::default()
        );
        assert!(matches!(
            Loaded::parse(&Position("{".into())),
            Err(Error::Parse(_))
        ));
    }

    #[test]
    fn reads_resume_in_the_middle_of_a_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.csv"), "id,name\n1,a\n2,b\n3,c\n").unwrap();
        let pattern = format!("{}/*.csv", dir.path().display());

        let mut first = CsvConnector::new(&pattern);
        let batch = first.read(&Position::default(), 2).unwrap().unwrap();
        assert_eq!(batch.data.num_rows(), 2);

        // A fresh connector, as after a restart, picks up at row 3.
        let mut second = CsvConnector::new(&pattern);
        let rest = second.read(&batch.next, 2).unwrap().unwrap();
        let ids = rest.data.column(0).as_any().downcast_ref::<StringArray>();
        assert_eq!(ids.unwrap().value(0), "3");
        assert_eq!(rest.data.num_rows(), 1);
    }

    #[test]
    fn bad_headers_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        for (name, body) in [
            ("twice.csv", "id,id\n1,2\n"),
            ("reserved.csv", "id,_source_file\n1,x\n"),
            ("blank.csv", "id,\n1,2\n"),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, body).unwrap();
            let mut csv = CsvConnector::new(path.to_str().unwrap());
            let err = csv.read(&Position::default(), 10);
            assert!(matches!(err, Err(Error::Parse(_))), "{name}: {err:?}");
        }
    }
}
