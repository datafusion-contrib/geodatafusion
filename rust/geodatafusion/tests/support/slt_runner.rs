//! Shared process for running GeoDataFusion SQL contracts.
//!
//! This intentionally uses `sqllogictest` directly instead of
//! `datafusion-sqllogictest`: the latter enables DataFusion's default features
//! and would pull Avro, Spark, and other test-only dependencies into the
//! workspace. The adapter below only implements the public SQL path and the
//! value normalization needed by these contract tests.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use sqllogictest::{AsyncDB, ColumnType, DBOutput, Runner, strict_column_validator};

use crate::support::context::context;

const NULL_STR: &str = "NULL";
const EMPTY_STR: &str = "(empty)";

#[derive(Debug, Clone, Copy)]
pub struct RunSummary {
    pub executed_files: usize,
    pub skipped_files: usize,
    pub executed_statements: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SltColumnType {
    Boolean,
    DateTime,
    Integer,
    Float,
    Text,
    Timestamp,
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

#[derive(Debug)]
enum SltError {
    DataFusion(DataFusionError),
    Arrow(ArrowError),
    Config(String),
}

impl fmt::Display for SltError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DataFusion(error) => write!(f, "{error}"),
            Self::Arrow(error) => write!(f, "{error}"),
            Self::Config(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for SltError {}

impl From<DataFusionError> for SltError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

impl From<ArrowError> for SltError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow(error)
    }
}

struct GeoDataFusion {
    ctx: SessionContext,
    default_config: BTreeMap<String, Option<String>>,
    config_errors: Arc<Mutex<Vec<String>>>,
    executions: Arc<AtomicUsize>,
}

impl GeoDataFusion {
    fn new(
        ctx: SessionContext,
        config_errors: Arc<Mutex<Vec<String>>>,
        executions: Arc<AtomicUsize>,
    ) -> Self {
        let default_config = ctx
            .state()
            .config()
            .options()
            .entries()
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
            .collect();

        Self {
            ctx,
            default_config,
            config_errors,
            executions,
        }
    }

    fn validate_config_unchanged(&self) -> Result<(), SltError> {
        let mut remaining = self.default_config.clone();
        let mut changed = false;
        let mut message = "SLT file left modified configuration".to_string();

        for entry in self.ctx.state().config().options().entries() {
            if let Some(default) = remaining.remove(&entry.key) {
                if default.as_ref() != entry.value.as_ref() {
                    changed = true;
                    message.push_str(&format!(
                        "\n  {}: {} -> {}",
                        entry.key,
                        default.as_deref().unwrap_or(NULL_STR),
                        entry.value.as_deref().unwrap_or(NULL_STR)
                    ));
                }
            } else {
                changed = true;
                message.push_str(&format!(
                    "\n  {}: NULL -> {}",
                    entry.key,
                    entry.value.as_deref().unwrap_or(NULL_STR)
                ));
            }
        }

        for (key, value) in remaining {
            changed = true;
            message.push_str(&format!(
                "\n  {key}: {} -> NULL",
                value.as_deref().unwrap_or(NULL_STR)
            ));
        }

        if changed {
            Err(SltError::Config(message))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl AsyncDB for GeoDataFusion {
    type Error = SltError;
    type ColumnType = SltColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        run_query(&self.ctx, sql).await
    }

    async fn shutdown(&mut self) {
        if let Err(error) = self.validate_config_unchanged() {
            self.config_errors
                .lock()
                .expect("config error lock poisoned")
                .push(error.to_string());
        }
    }
}

async fn run_query(ctx: &SessionContext, sql: &str) -> Result<DBOutput<SltColumnType>, SltError> {
    let dataframe = ctx.sql(sql).await?;
    let arrow_schema = dataframe.schema().as_arrow().clone();
    let types = convert_schema_to_types(&arrow_schema);
    let batches = dataframe.collect().await?;
    let rows = convert_batches(&batches)?;

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}

fn convert_schema_to_types(schema: &datafusion::arrow::datatypes::Schema) -> Vec<SltColumnType> {
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
            DataType::Float16 | DataType::Float32 | DataType::Float64 => SltColumnType::Float,
            DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => SltColumnType::Float,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => SltColumnType::Text,
            DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
                SltColumnType::DateTime
            }
            DataType::Timestamp(_, _) => SltColumnType::Timestamp,
            DataType::Dictionary(key_type, value_type) => {
                if key_type.is_integer()
                    && matches!(
                        value_type.as_ref(),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    )
                {
                    SltColumnType::Text
                } else {
                    SltColumnType::Another
                }
            }
            _ => SltColumnType::Another,
        })
        .collect()
}

fn convert_batches(
    batches: &[datafusion::arrow::array::RecordBatch],
) -> Result<Vec<Vec<String>>, SltError> {
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

fn cell_to_string(column: &dyn Array, row: usize) -> Result<String, SltError> {
    if column.is_null(row) {
        return Ok(NULL_STR.to_string());
    }

    match column.data_type() {
        DataType::Null => Ok(NULL_STR.to_string()),
        DataType::Boolean => Ok(if column.as_boolean().value(row) {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        DataType::Float16 => Ok(round_float(
            column
                .as_primitive::<datafusion::arrow::datatypes::Float16Type>()
                .value(row)
                .to_f64(),
        )),
        DataType::Float32 => Ok(round_float(f64::from(
            column
                .as_primitive::<datafusion::arrow::datatypes::Float32Type>()
                .value(row),
        ))),
        DataType::Float64 => Ok(round_float(
            column
                .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
                .value(row),
        )),
        DataType::Utf8 => Ok(normalize_string(column.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => Ok(normalize_string(column.as_string::<i64>().value(row))),
        DataType::Utf8View => Ok(normalize_string(column.as_string_view().value(row))),
        DataType::Dictionary(_, _) => {
            let dictionary = column.as_any_dictionary();
            let key = dictionary.normalized_keys()[row];
            cell_to_string(dictionary.values().as_ref(), key)
        }
        _ => {
            let formatter = ArrayFormatter::try_new(column, &FormatOptions::default())?;
            Ok(formatter.value(row).to_string())
        }
    }
}

fn normalize_string(value: &str) -> String {
    if value.is_empty() {
        EMPTY_STR.to_string()
    } else {
        value.trim_end_matches('\n').replace('\0', "\\0")
    }
}

fn round_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f64::INFINITY {
        return "Infinity".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }

    // Match DataFusion's sqllogictest normalization: 12 decimal places.
    let formatted = format!("{value:.12}");
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    if trimmed == "-0" {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

// `sqllogictest::Normalizer` is a function pointer over `&String`.
#[allow(clippy::ptr_arg)]
fn normalize_value(value: &String) -> String {
    value.trim_end().to_string()
}

fn discover_slt_files(root: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = std::fs::read_dir(root)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", root.display()))
        .map(|entry| entry.expect("failed to read directory entry").path())
        .collect::<Vec<_>>();
    entries.sort();

    for path in entries {
        if path.is_dir() {
            discover_slt_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "slt") {
            files.push(path);
        }
    }
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("SLT file must be under the SLT root")
        .to_string_lossy()
        .replace('\\', "/")
}

fn feature_requirement(path: &Path) -> Option<&'static str> {
    let has_component = |expected: &str| {
        path.components()
            .any(|component| component.as_os_str() == expected)
    };
    if has_component("geos_3_12") {
        Some("geos-3_12")
    } else if has_component("geos_3_11") {
        Some("geos-3_11")
    } else {
        None
    }
}

fn feature_available(feature: &str) -> bool {
    match feature {
        "geos-3_11" => cfg!(feature = "geos-3_11"),
        "geos-3_12" => cfg!(feature = "geos-3_12"),
        _ => unreachable!("unknown feature gate {feature}"),
    }
}

// GEOS 3.13 replaced the legacy relate engine with RelateNG. SLT files use
// this runtime label for contracts whose matrices differ between engines.
#[cfg(feature = "geos-3_11")]
fn geos_uses_relateng() -> bool {
    let version = geos::version().expect("failed to read the GEOS runtime version");
    let mut parts = version.split('.');
    let major: u32 = parts
        .next()
        .and_then(|part| part.parse().ok())
        .unwrap_or_else(|| panic!("unexpected GEOS version `{version}`"));
    let minor: u32 = parts
        .next()
        .and_then(|part| part.parse().ok())
        .unwrap_or_else(|| panic!("unexpected GEOS version `{version}`"));
    major > 3 || (major == 3 && minor >= 13)
}

pub async fn run_root(root: &Path, filters: &[String]) -> Result<RunSummary, String> {
    let mut files = Vec::new();
    discover_slt_files(root, &mut files);
    files.sort();

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

    let mut executed = 0;
    let mut skipped = 0;
    let mut executed_statements = 0;

    for path in selected {
        let relative = relative_path(root, &path);
        if let Some(feature) = feature_requirement(&path)
            && !feature_available(feature)
        {
            println!("sqllogictest: skipped {relative} (requires {feature})");
            skipped += 1;
            continue;
        }

        let ctx = context();
        let config_errors = Arc::new(Mutex::new(Vec::new()));
        let connection_errors = Arc::clone(&config_errors);
        let executions = Arc::new(AtomicUsize::new(0));
        let connection_executions = Arc::clone(&executions);
        let mut runner = Runner::new(move || {
            let ctx = ctx.clone();
            let errors = Arc::clone(&connection_errors);
            let executions = Arc::clone(&connection_executions);
            async move { Ok::<_, SltError>(GeoDataFusion::new(ctx, errors, executions)) }
        });
        runner.add_label("geodatafusion");
        if cfg!(feature = "geos-3_11") {
            runner.add_label("geos_3_11");
        }
        if cfg!(feature = "geos-3_12") {
            runner.add_label("geos_3_12");
        }
        #[cfg(feature = "geos-3_11")]
        {
            if geos_uses_relateng() {
                runner.add_label("geos_relateng");
            }
        }
        runner.with_column_validator(strict_column_validator);
        runner.with_normalizer(normalize_value);

        runner
            .run_file_async(&path)
            .await
            .map_err(|error| format!("{relative}: {error}"))?;
        runner.shutdown_async().await;

        let file_executions = executions.load(Ordering::SeqCst);
        if file_executions == 0 {
            return Err(format!(
                "{relative}: no SQL statement or query was executed; all records were skipped"
            ));
        }
        executed_statements += file_executions;

        let errors = config_errors.lock().expect("config error lock poisoned");
        if !errors.is_empty() {
            return Err(format!("{relative}: {}", errors.join("; ")));
        }

        println!("sqllogictest: passed {relative}");
        executed += 1;
    }

    if executed == 0 {
        return Err(format!(
            "no .slt files were executed; {skipped} feature-gated file(s) skipped"
        ));
    }

    Ok(RunSummary {
        executed_files: executed,
        skipped_files: skipped,
        executed_statements,
    })
}
