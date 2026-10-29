use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

/// Execute a SQL query and return its raw Arrow batches.
// Used by the raw Arrow contracts target; the SQLLogicTest target also links
// this support module but only consumes the context helper.
#[allow(dead_code)]
pub async fn collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(sql).await?.collect().await
}
