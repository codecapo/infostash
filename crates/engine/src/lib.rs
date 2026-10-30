//! Single-node vectorized execution on DataFusion and Arrow. Distributed
//! plans in `infostash-dist` run these operators inside each task.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SQLOptions, SessionContext};
use iceberg_datafusion::IcebergStaticTableProvider;
use infostash_model::{Error, Layer, Result};
use infostash_storage::Warehouse;

/// The rows a query returned, and their schema even when there are none.
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Run one read-only SQL statement against every table in the warehouse.
///
/// Tables are named `<layer>.<table>`, where the layer is either the
/// namespace (`gold`) or its Raw, Clean and Ready alias (`ready`), as in
/// ADR 0003. Each table is read at the snapshot it had when the query
/// started. Statements that write or change the session are rejected.
pub async fn query(warehouse: &Warehouse, sql: &str) -> Result<QueryResult> {
    let ctx = SessionContext::new();
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| Error::Plan("DataFusion has no default catalog".into()))?;
    let tables = warehouse.tables().await?;
    for layer in Layer::ALL {
        let schema = Arc::new(MemorySchemaProvider::new());
        for table in tables.iter().filter(|t| t.layer == layer) {
            let provider =
                IcebergStaticTableProvider::try_new_from_table(warehouse.load(table).await?)
                    .await
                    .map_err(plan)?;
            schema
                .register_table(table.name.clone(), Arc::new(provider))
                .map_err(plan)?;
        }
        for name in [layer.namespace(), layer.alias()] {
            catalog
                .register_schema(name, schema.clone())
                .map_err(plan)?;
        }
    }

    let read_only = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let run = async {
        let df = ctx.sql_with_options(sql, read_only).await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        Ok::<_, DataFusionError>(QueryResult {
            schema,
            batches: df.collect().await?,
        })
    };
    run.await.map_err(|e| match e.find_root() {
        DataFusionError::Plan(m) if m.contains("not found") && m.starts_with("table") => {
            let names: Vec<_> = tables.iter().map(ToString::to_string).collect();
            let known = if names.is_empty() {
                "the warehouse has no tables".to_string()
            } else {
                format!("tables: {}", names.join(", "))
            };
            Error::NotFound(format!("{m}; {known}"))
        }
        DataFusionError::SQL(..) => Error::Parse(e.to_string()),
        DataFusionError::Plan(..) => Error::Plan(e.to_string()),
        _ => Error::Execution(e.to_string()),
    })
}

fn plan(e: impl std::fmt::Display) -> Error {
    Error::Plan(e.to_string())
}
