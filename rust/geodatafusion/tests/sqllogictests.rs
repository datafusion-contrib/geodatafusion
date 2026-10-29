//! Runs SQL smoke tests through the public UDF registry.

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use sqllogictest::{DB, DBOutput, DefaultColumnType};

const NULL: &str = "NULL";

struct DataFusionConnection {
    ctx: SessionContext,
    runtime: tokio::runtime::Runtime,
}

impl DataFusionConnection {
    fn new() -> Self {
        let ctx = SessionContext::new();
        geodatafusion::register(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build Tokio runtime");
        Self { ctx, runtime }
    }
}

impl DB for DataFusionConnection {
    type Error = DataFusionError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let (types, batches) = self.runtime.block_on(async {
            let dataframe = self.ctx.sql(sql).await?;
            let types = dataframe
                .schema()
                .as_arrow()
                .fields()
                .iter()
                .map(|field| match field.data_type() {
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64 => DefaultColumnType::Integer,
                    DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal32(_, _)
                    | DataType::Decimal64(_, _)
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _) => DefaultColumnType::FloatingPoint,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        DefaultColumnType::Text
                    }
                    _ => DefaultColumnType::Any,
                })
                .collect::<Vec<_>>();
            let batches = dataframe.collect().await?;
            Ok::<_, DataFusionError>((types, batches))
        })?;

        if types.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }

        Ok(DBOutput::Rows {
            types,
            rows: rows_from_batches(&batches)?,
        })
    }
}

fn rows_from_batches(batches: &[RecordBatch]) -> Result<Vec<Vec<String>>, DataFusionError> {
    let mut rows = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column, &FormatOptions::default()))
            .collect::<Result<Vec<_>, _>>()?;

        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .zip(&formatters)
                .map(|(column, formatter)| {
                    if column.is_null(row) {
                        NULL.to_string()
                    } else {
                        formatter.value(row).to_string()
                    }
                })
                .collect();
            rows.push(values);
        }
    }
    Ok(rows)
}

// GEOS-only cases live in the sibling `slt_geos/` directory.
#[cfg(not(feature = "geos-3_11"))]
sqllogictest::harness!(
    DataFusionConnection::new,
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/slt/**/*.slt")
);

#[cfg(feature = "geos-3_11")]
sqllogictest::harness!(
    DataFusionConnection::new,
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/slt*/**/*.slt")
);
