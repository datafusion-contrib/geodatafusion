use std::sync::Arc;

use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::MemTable;
use datafusion::error::Result;

use super::constant_bytes;
use super::decode::decode_bytes;

#[derive(Debug)]
pub(super) struct FromFlatGeobuf;

impl TableFunctionImpl for FromFlatGeobuf {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let bytes = match exprs {
            [bytes] => constant_bytes(bytes, "ST_FromFlatGeobuf")?,
            [_, _] => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "ST_FromFlatGeobuf does not support the PostgreSQL row-template argument: \
                     DataFusion table functions have no composite row type, so the output \
                     schema is always derived from the FlatGeobuf header (properties in file \
                     order, then a geometry column named `geom`). Call \
                     st_fromflatgeobuf(bytes) without a template."
                        .to_string(),
                ));
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "ST_FromFlatGeobuf expects exactly one FlatGeobuf bytes argument".to_string(),
                ));
            }
        };
        let decoded = decode_bytes(&bytes, false, "geom")?;
        let table = MemTable::try_new(decoded.schema, vec![decoded.batches])?;
        Ok(Arc::new(table))
    }
}
