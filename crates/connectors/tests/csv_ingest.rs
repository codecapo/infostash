//! CSV files land in Raw exactly once, with the position in the same commit
//! as the rows (#9, ADR 0006).

#![allow(clippy::unwrap_used)]

use std::path::Path;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, TimeUnit};
use infostash_connectors::{Connector, CsvConnector, LOADED_AT, POSITION, SOURCE_FILE, prepare};
use infostash_model::{Error, Layer, TableRef};
use infostash_storage::Warehouse;

struct Setup {
    _dir: tempfile::TempDir,
    files: std::path::PathBuf,
    wh: Warehouse,
    table: TableRef,
}

async fn setup() -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let files = dir.path().join("in");
    let warehouse = dir.path().join("wh");
    std::fs::create_dir(&files).unwrap();
    std::fs::create_dir(&warehouse).unwrap();
    let wh = Warehouse::open_local(&warehouse).await.unwrap();
    Setup {
        _dir: dir,
        files,
        wh,
        table: TableRef::new(Layer::Raw, "orders"),
    }
}

impl Setup {
    fn write(&self, name: &str, ids: &[u32]) {
        let mut body = String::from("id,amount\n");
        for id in ids {
            body.push_str(&format!("{id},{}\n", id * 10));
        }
        std::fs::write(self.files.join(name), body).unwrap();
    }

    fn connector(&self) -> CsvConnector {
        CsvConnector::new(format!("{}/*.csv", self.files.display()))
    }

    /// One full run: prepare and commit. Returns the rows committed.
    async fn run(&self) -> Option<u64> {
        let mut csv = self.connector();
        let prepared = prepare(&self.wh, &self.table, &mut csv, 2).await.unwrap();
        prepared.commit(&self.wh).await.unwrap().map(|c| c.rows)
    }

    async fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = column(&self.wh.read_all(&self.table).await.unwrap(), "id");
        ids.sort_by_key(|id| id.parse::<u32>().unwrap());
        ids
    }

    async fn snapshots(&self) -> usize {
        self.wh.snapshots(&self.table).await.unwrap().len()
    }
}

fn column(batches: &[RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name(name).unwrap();
            let col = col.as_any().downcast_ref::<StringArray>().unwrap();
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn file_name(path: &str) -> &str {
    Path::new(path).file_name().unwrap().to_str().unwrap()
}

#[tokio::test]
async fn first_run_loads_every_file_with_load_metadata() {
    let s = setup().await;
    s.write("a.csv", &[1, 2, 3]);
    s.write("b.csv", &[4]);

    assert_eq!(s.run().await, Some(4));
    assert_eq!(s.ids().await, ["1", "2", "3", "4"]);
    assert_eq!(s.snapshots().await, 1);

    let batches = s.wh.read_all(&s.table).await.unwrap();
    let mut sources: Vec<String> = column(&batches, SOURCE_FILE)
        .iter()
        .map(|p| file_name(p).to_string())
        .collect();
    sources.sort();
    assert_eq!(sources, ["a.csv", "a.csv", "a.csv", "b.csv"]);
    let schema = batches[0].schema();
    assert_eq!(
        schema.field_with_name("amount").unwrap().data_type(),
        &DataType::Utf8
    );
    let loaded_at = schema.field_with_name(LOADED_AT).unwrap();
    assert!(matches!(
        loaded_at.data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "+00:00"
    ));
    assert!(
        batches
            .iter()
            .all(|b| b.column_by_name(LOADED_AT).unwrap().null_count() == 0)
    );
}

#[tokio::test]
async fn a_run_with_no_new_files_commits_nothing() {
    let s = setup().await;
    s.write("a.csv", &[1, 2]);
    s.run().await;
    let before = s.wh.state(&s.table).await.unwrap();

    assert_eq!(s.run().await, None);
    assert_eq!(s.wh.state(&s.table).await.unwrap(), before);
    assert_eq!(s.ids().await, ["1", "2"]);
}

#[tokio::test]
async fn a_new_file_loads_only_that_file() {
    let s = setup().await;
    s.write("a.csv", &[1, 2]);
    s.run().await;
    s.write("b.csv", &[3, 4, 5]);

    assert_eq!(s.run().await, Some(3));
    assert_eq!(s.ids().await, ["1", "2", "3", "4", "5"]);
    assert_eq!(s.snapshots().await, 2);
}

#[tokio::test]
async fn a_crash_before_the_commit_loads_each_row_once_on_rerun() {
    let s = setup().await;
    s.write("a.csv", &[1, 2]);
    s.run().await;
    s.write("b.csv", &[3, 4, 5]);

    // Read b.csv, then crash before committing.
    let mut csv = s.connector();
    let crashed = prepare(&s.wh, &s.table, &mut csv, 2).await.unwrap();
    assert_eq!(crashed.rows(), 3);
    drop(crashed);
    assert_eq!(s.snapshots().await, 1);

    assert_eq!(s.run().await, Some(3));
    assert_eq!(s.ids().await, ["1", "2", "3", "4", "5"]);
    assert_eq!(s.run().await, None);
}

#[tokio::test]
async fn the_position_is_a_table_property_naming_the_loaded_files() {
    let s = setup().await;
    s.write("a.csv", &[1]);
    s.run().await;

    let state = s.wh.state(&s.table).await.unwrap();
    let position: serde_json::Value =
        serde_json::from_str(state.properties.get(POSITION).unwrap()).unwrap();
    let files: Vec<&str> = position["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| file_name(f.as_str().unwrap()))
        .collect();
    assert_eq!(files, ["a.csv"]);
    assert!(position.get("reading").is_none());
}

#[tokio::test]
async fn two_runs_from_the_same_position_conflict() {
    let s = setup().await;
    s.write("a.csv", &[1, 2]);
    s.run().await;
    s.write("b.csv", &[3]);

    let (mut one, mut two) = (s.connector(), s.connector());
    let first = prepare(&s.wh, &s.table, &mut one, 2).await.unwrap();
    let second = prepare(&s.wh, &s.table, &mut two, 2).await.unwrap();
    assert!(first.commit(&s.wh).await.unwrap().is_some());

    let lost = second.commit(&s.wh).await;
    assert!(matches!(lost, Err(Error::Conflict(_))), "{lost:?}");
    assert_eq!(s.ids().await, ["1", "2", "3"]);
    assert_eq!(s.snapshots().await, 2);
}

#[tokio::test]
async fn files_with_only_a_header_create_nothing() {
    let s = setup().await;
    std::fs::write(s.files.join("a.csv"), "id,amount\n").unwrap();

    assert_eq!(s.connector().kind(), "csv");
    assert_eq!(s.run().await, None);
    assert!(!s.wh.exists(&s.table).await.unwrap());
}
