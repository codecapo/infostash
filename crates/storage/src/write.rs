//! Writing record batches as Parquet data files for a table, with the
//! codec and sizes its `write.parquet.*` properties set. Nothing here
//! commits: [`crate::append`] and [`crate::replace`] add the files to a
//! snapshot.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use infostash_model::{Error, Result, TableRef};

use crate::storage;

/// Write `batches` as new Parquet data files for `table`. Returns the files
/// and the number of rows in them. Nothing is committed.
pub(crate) async fn write_files(
    current: &Table,
    table: &TableRef,
    batches: &[RecordBatch],
) -> Result<(Vec<DataFile>, u64)> {
    let metadata = current.metadata();
    let schema = metadata.current_schema().clone();
    // Batches must carry Iceberg field ids so columns map by id, not name.
    let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).map_err(storage)?);

    let files = RollingFileWriterBuilder::new_with_default_file_size(
        // Codec, row group and page sizes come from the table's
        // `write.parquet.*` properties.
        ParquetWriterBuilder::from_table_properties(&metadata.table_properties(), schema)
            .map_err(storage)?,
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::TableProperties;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use infostash_model::Error;

    use crate::test_support::{ids, orders};

    /// The codec of every column chunk in the Parquet files under `dir`.
    fn codecs(dir: &Path) -> HashSet<String> {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let mut found = HashSet::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                found.extend(codecs(&path));
            } else if path.extension().is_some_and(|e| e == "parquet") {
                let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap());
                for group in reader.unwrap().metadata().row_groups() {
                    for column in group.columns() {
                        let codec = format!("{:?}", column.compression());
                        found.insert(codec.split('(').next().unwrap().to_string());
                    }
                }
            }
        }
        found
    }

    #[tokio::test]
    async fn new_tables_write_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let codec = TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC;
        let state = wh.state(&orders).await.unwrap();
        assert_eq!(state.properties.get(codec).unwrap(), "zstd");

        wh.append(&orders, &[ids(&[1, 2, 3])]).await.unwrap();
        assert_eq!(codecs(dir.path()), HashSet::from(["ZSTD".to_string()]));
    }

    #[tokio::test]
    async fn the_table_codec_is_honoured() {
        let dir = tempfile::tempdir().unwrap();
        let (wh, orders) = orders(dir.path()).await;
        let table = wh.load(&orders).await.unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set(
                TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.to_string(),
                "snappy".to_string(),
            )
            .apply(tx)
            .unwrap();
        tx.commit(&wh.catalog).await.unwrap();

        wh.append(&orders, &[ids(&[1, 2, 3])]).await.unwrap();
        assert_eq!(codecs(dir.path()), HashSet::from(["SNAPPY".to_string()]));
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
