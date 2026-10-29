//! Minimal SQLLogicTest adapter for DataFusion SQL results.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Float16Type, Float32Type, Float64Type, Schema};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use sqllogictest::{AsyncDB, ColumnType, DBOutput, Runner, strict_column_validator};

const NULL: &str = "NULL";
const EMPTY: &str = "(empty)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSummary {
    pub executed_files: usize,
    pub executed_statements: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SltColumnType {
    Boolean,
    DateTime,
    Integer,
    Float,
    Timestamp,
    Text,
    Another,
}

impl ColumnType for SltColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Boolean),
            'D' => Some(Self::DateTime),
            'I' => Some(Self::Integer),
            'P' => Some(Self::Timestamp),
            'R' => Some(Self::Float),
            'T' => Some(Self::Text),
            _ => Some(Self::Another),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Boolean => 'B',
            Self::DateTime => 'D',
            Self::Integer => 'I',
            Self::Timestamp => 'P',
            Self::Float => 'R',
            Self::Text => 'T',
            Self::Another => '?',
        }
    }
}

struct GeoDataFusion {
    ctx: SessionContext,
    executions: Arc<AtomicUsize>,
}

#[async_trait]
impl AsyncDB for GeoDataFusion {
    type Error = DataFusionError;
    type ColumnType = SltColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.executions.fetch_add(1, Ordering::SeqCst);

        let dataframe = self.ctx.sql(sql).await?;
        let types = convert_schema(&dataframe.schema().as_arrow().clone());
        let batches = dataframe.collect().await?;
        let rows = convert_batches(&batches)?;

        if types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    async fn shutdown(&mut self) {}
}

fn convert_schema(schema: &Schema) -> Vec<SltColumnType> {
    schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Boolean => SltColumnType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => SltColumnType::Integer,
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => SltColumnType::Float,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => SltColumnType::Text,
            DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
                SltColumnType::DateTime
            }
            DataType::Timestamp(_, _) => SltColumnType::Timestamp,
            _ => SltColumnType::Another,
        })
        .collect()
}

fn convert_batches(batches: &[RecordBatch]) -> Result<Vec<Vec<String>>, DataFusionError> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                values.push(cell_to_string(column.as_ref(), row)?);
            }
            rows.push(values);
        }
    }
    Ok(rows)
}

fn cell_to_string(column: &dyn Array, row: usize) -> Result<String, DataFusionError> {
    if column.is_null(row) {
        return Ok(NULL.to_string());
    }

    match column.data_type() {
        DataType::Null => Ok(NULL.to_string()),
        DataType::Boolean => Ok(column.as_boolean().value(row).to_string()),
        DataType::Float16 => Ok(format_float(
            column.as_primitive::<Float16Type>().value(row).to_f64(),
        )),
        DataType::Float32 => Ok(format_float(f64::from(
            column.as_primitive::<Float32Type>().value(row),
        ))),
        DataType::Float64 => Ok(format_float(
            column.as_primitive::<Float64Type>().value(row),
        )),
        DataType::Utf8 => Ok(format_text(column.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => Ok(format_text(column.as_string::<i64>().value(row))),
        DataType::Utf8View => Ok(format_text(column.as_string_view().value(row))),
        _ => {
            let formatter = ArrayFormatter::try_new(column, &FormatOptions::default())?;
            Ok(formatter.value(row).to_string())
        }
    }
}

fn format_text(value: &str) -> String {
    if value.is_empty() {
        EMPTY.to_string()
    } else {
        value.trim_end_matches('\n').replace('\0', "\\0")
    }
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f64::INFINITY {
        return "Infinity".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }

    let formatted = format!("{value:.12}");
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    if trimmed == "-0" {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

#[allow(clippy::ptr_arg)]
fn normalize_value(value: &String) -> String {
    value.trim_end().to_string()
}

fn discover_slt_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    fn visit(directory: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
        let mut entries = std::fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(&path, files)?;
            } else if path.extension().is_some_and(|extension| extension == "slt") {
                files.push(path);
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    visit(root, &mut files)
        .map_err(|error| format!("failed to read {}: {error}", root.display()))?;
    files.sort();
    Ok(files)
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

pub async fn run_root(root: &Path, filters: &[String]) -> Result<RunSummary, String> {
    let files = discover_slt_files(root)?;
    if files.is_empty() {
        return Err(format!("no .slt files found under {}", root.display()));
    }

    let selected = files
        .into_iter()
        .filter(|path| {
            filters.is_empty()
                || filters.iter().any(|filter| {
                    relative_path(root, path)
                        .to_lowercase()
                        .contains(&filter.to_lowercase())
                })
        })
        .collect::<Vec<_>>();

    if selected.is_empty() {
        return Err(format!(
            "no .slt files matched filters: {}",
            filters.join(", ")
        ));
    }

    let mut summary = RunSummary {
        executed_files: 0,
        executed_statements: 0,
    };

    for path in selected {
        let relative = relative_path(root, &path);
        let ctx = SessionContext::new();
        geodatafusion::register(&ctx);
        let executions = Arc::new(AtomicUsize::new(0));
        let connection_executions = Arc::clone(&executions);
        let mut runner = Runner::new(move || {
            let ctx = ctx.clone();
            let executions = Arc::clone(&connection_executions);
            async move { Ok::<_, DataFusionError>(GeoDataFusion { ctx, executions }) }
        });
        runner.add_label("geodatafusion");
        runner.with_column_validator(strict_column_validator);
        runner.with_normalizer(normalize_value);

        runner
            .run_file_async(&path)
            .await
            .map_err(|error| format!("{relative}: {error}"))?;
        runner.shutdown_async().await;

        let executed = executions.load(Ordering::SeqCst);
        if executed == 0 {
            return Err(format!(
                "{relative}: no SQL statement or query was executed"
            ));
        }

        println!("sqllogictest: passed {relative}");
        summary.executed_files += 1;
        summary.executed_statements += executed;
    }

    Ok(summary)
}
