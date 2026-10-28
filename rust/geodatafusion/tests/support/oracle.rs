//! Typed, reviewed PostGIS oracle support for the `postgis` integration test.
//!
//! The checked-in baseline is generated only from a real pinned PostGIS
//! instance. This module never derives expected values from the
//! GeoDataFusion implementation under test.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::{fmt, fs};

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Float64Array};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, PointTrait, PolygonTrait,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::to_wkb;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const SCHEMA_VERSION: u32 = 1;

/// The candidate reference image. The digest is the multi-architecture index
/// digest and is also required by the checked-in compose file and CI job.
pub const PINNED_POSTGIS_IMAGE: &str = "postgis/postgis:18-3.6@sha256:4a43e3c3e5d55b9208240fc71db543b79c638766280c625a425bd0a2390989c4";

/// Case groups are independent because each group has one parameter shape.
pub const GROUPS: &[&str] = &[
    "measurement/distance",
    "measurement/dwithin",
    "dump",
    "extent",
];

#[derive(Debug, Deserialize)]
pub struct OracleInputs {
    pub schema_version: u32,
    pub cases: Vec<OracleCase>,
}

#[derive(Debug, Deserialize)]
pub struct OracleCase {
    pub id: String,
    pub function: String,
    pub arguments: Vec<OracleArgument>,
    pub result_columns: Vec<ResultColumn>,
    pub comparison: Comparison,
    pub source: String,
    #[serde(default)]
    pub skip: Option<Skip>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OracleArgument {
    Text { value: String },
    Float64 { value: f64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: ColumnType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Boolean,
    Float64,
    Int32List,
    Ewkb,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum Comparison {
    Exact,
    FloatUlp { max_ulps: u64 },
    GeometryExact,
}

impl Comparison {
    fn name(&self) -> String {
        match self {
            Self::Exact => "exact".to_string(),
            Self::FloatUlp { max_ulps } => format!("float-ulp(max_ulps={max_ulps})"),
            Self::GeometryExact => "geometry-exact".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Skip {
    pub reason: String,
    pub issue: String,
    pub review_after: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TypedValue {
    Boolean { value: bool },
    Float64 { bits: String },
    Int32List { values: Vec<i32> },
    Ewkb { hex: String },
    Null,
}

impl TypedValue {
    fn float64(value: f64) -> Self {
        Self::Float64 {
            bits: format!("{:016x}", value.to_bits()),
        }
    }

    fn ewkb(value: Vec<u8>) -> Self {
        Self::Ewkb {
            hex: hex::encode(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypedRowSet {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<TypedValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleVersions {
    pub image: String,
    pub image_digest: String,
    pub postgresql_version: String,
    pub postgresql_version_num: i32,
    pub postgis_version: String,
    pub postgis_full_version: String,
    pub geos_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Baseline {
    pub schema_version: u32,
    pub oracle: OracleVersions,
    pub inputs_sha256: String,
    pub postgis_sql_sha256: String,
    pub datafusion_sql_sha256: String,
    pub cases: BTreeMap<String, TypedRowSet>,
}

pub struct OracleGroup {
    pub name: &'static str,
    pub inputs: OracleInputs,
    pub postgis_sql: String,
    pub datafusion_sql: String,
    pub expected: Option<Baseline>,
    inputs_sha256: String,
    postgis_sql_sha256: String,
    datafusion_sql_sha256: String,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub discovered: usize,
    pub executed: usize,
    pub compared: usize,
    pub rows_compared: usize,
    pub skipped: usize,
}

impl Stats {
    pub fn add(&mut self, other: &Stats) {
        self.discovered += other.discovered;
        self.executed += other.executed;
        self.compared += other.compared;
        self.rows_compared += other.rows_compared;
        self.skipped += other.skipped;
    }

    pub fn summary(&self) -> String {
        format!(
            "discovered={} executed={} compared={} rows_compared={} skipped={}",
            self.discovered, self.executed, self.compared, self.rows_compared, self.skipped
        )
    }

    pub fn ensure_compared(&self, context: &str) -> Result<()> {
        if self.executed == 0 {
            return fail(format!(
                "{context}: zero cases executed ({})",
                self.summary()
            ));
        }
        if self.compared == 0 {
            return fail(format!(
                "{context}: zero cases compared ({})",
                self.summary()
            ));
        }
        Ok(())
    }
}

pub fn load_group(name: &'static str, require_baseline: bool) -> Result<OracleGroup> {
    load_group_inner(name, require_baseline, false)
}

pub fn load_capture_group(name: &'static str) -> Result<OracleGroup> {
    load_group_inner(name, false, true)
}

fn load_group_inner(
    name: &'static str,
    require_baseline: bool,
    ignore_existing_baseline: bool,
) -> Result<OracleGroup> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/postgis")
        .join(name);
    let inputs_bytes = fs::read(dir.join("inputs.json"))?;
    let postgis_sql_bytes = fs::read(dir.join("postgis.sql"))?;
    let datafusion_sql_bytes = fs::read(dir.join("datafusion.sql"))?;

    let inputs: OracleInputs = serde_json::from_slice(&inputs_bytes)?;
    validate_inputs(&inputs)?;

    let inputs_sha256 = sha256_hex(&inputs_bytes);
    let postgis_sql_sha256 = sha256_hex(&postgis_sql_bytes);
    let datafusion_sql_sha256 = sha256_hex(&datafusion_sql_bytes);
    let postgis_sql = String::from_utf8(postgis_sql_bytes)?;
    let datafusion_sql = String::from_utf8(datafusion_sql_bytes)?;

    let expected_path = dir.join("expected.json");
    let expected = if ignore_existing_baseline {
        None
    } else if expected_path.is_file() {
        let baseline: Baseline = serde_json::from_slice(&fs::read(&expected_path)?)?;
        validate_baseline(
            &inputs,
            &baseline,
            &inputs_sha256,
            &postgis_sql_sha256,
            &datafusion_sql_sha256,
        )?;
        Some(baseline)
    } else if require_baseline {
        return fail(format!(
            "missing reviewed oracle baseline at {}",
            expected_path.display()
        ));
    } else {
        None
    };

    Ok(OracleGroup {
        name,
        inputs,
        postgis_sql,
        datafusion_sql,
        expected,
        inputs_sha256,
        postgis_sql_sha256,
        datafusion_sql_sha256,
    })
}

fn validate_inputs(inputs: &OracleInputs) -> Result<()> {
    if inputs.schema_version != SCHEMA_VERSION {
        return fail(format!(
            "unsupported oracle inputs schema_version {}",
            inputs.schema_version
        ));
    }
    if inputs.cases.is_empty() {
        return fail("oracle inputs contain no cases");
    }

    let mut ids = HashSet::new();
    for case in &inputs.cases {
        if case.id.is_empty()
            || !case
                .id
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return fail(format!(
                "case id `{}` must contain only lowercase ASCII letters, digits, or hyphens",
                case.id
            ));
        }
        if !ids.insert(case.id.clone()) {
            return fail(format!("duplicate oracle case id `{}`", case.id));
        }
        if case.function.is_empty() {
            return fail(format!("case `{}` has no function name", case.id));
        }
        if case.arguments.is_empty() {
            return fail(format!("case `{}` has no arguments", case.id));
        }
        if case.result_columns.is_empty() {
            return fail(format!("case `{}` declares no result columns", case.id));
        }
        let mut column_names = HashSet::new();
        for column in &case.result_columns {
            if column.name.is_empty() || !column_names.insert(column.name.as_str()) {
                return fail(format!(
                    "case `{}` has an empty or duplicate result column name `{}`",
                    case.id, column.name
                ));
            }
        }
        if !case.source.starts_with("https://") {
            return fail(format!("case `{}` source must be an https URL", case.id));
        }
        if let Some(skip) = &case.skip
            && (skip.reason.is_empty() || skip.issue.is_empty() || skip.review_after.is_empty())
        {
            return fail(format!(
                "case `{}` skip must include reason, issue, and review_after",
                case.id
            ));
        }
    }

    if inputs.cases.iter().all(|case| case.skip.is_some()) {
        return fail("oracle inputs skip every case");
    }

    Ok(())
}

fn validate_baseline(
    inputs: &OracleInputs,
    baseline: &Baseline,
    inputs_sha256: &str,
    postgis_sql_sha256: &str,
    datafusion_sql_sha256: &str,
) -> Result<()> {
    if baseline.schema_version != SCHEMA_VERSION {
        return fail(format!(
            "unsupported oracle baseline schema_version {}",
            baseline.schema_version
        ));
    }
    if baseline.inputs_sha256 != inputs_sha256 {
        return fail(format!(
            "baseline inputs hash mismatch: expected {}, current {}",
            baseline.inputs_sha256, inputs_sha256
        ));
    }
    if baseline.postgis_sql_sha256 != postgis_sql_sha256 {
        return fail(format!(
            "baseline postgis.sql hash mismatch: expected {}, current {}",
            baseline.postgis_sql_sha256, postgis_sql_sha256
        ));
    }
    if baseline.datafusion_sql_sha256 != datafusion_sql_sha256 {
        return fail(format!(
            "baseline datafusion.sql hash mismatch: expected {}, current {}",
            baseline.datafusion_sql_sha256, datafusion_sql_sha256
        ));
    }
    validate_image(&baseline.oracle.image)?;
    if baseline.oracle.image_digest != format!("sha256:{}", image_digest(&baseline.oracle.image)?) {
        return fail(format!(
            "baseline oracle image_digest does not match image `{}`",
            baseline.oracle.image
        ));
    }
    if baseline.oracle.postgresql_version.is_empty()
        || baseline.oracle.postgis_version.is_empty()
        || baseline.oracle.postgis_full_version.is_empty()
        || baseline.oracle.geos_version.is_empty()
    {
        return fail("baseline oracle versions must not be empty");
    }

    let input_ids = inputs
        .cases
        .iter()
        .map(|case| case.id.as_str())
        .collect::<BTreeSet<_>>();
    let baseline_ids = baseline
        .cases
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if input_ids != baseline_ids {
        return fail(format!(
            "baseline case ids differ from inputs: inputs={input_ids:?} baseline={baseline_ids:?}"
        ));
    }

    for case in &inputs.cases {
        let result = baseline
            .cases
            .get(&case.id)
            .ok_or_else(|| format!("baseline is missing case `{}`", case.id))?;
        validate_row_set(case, result, "baseline")?;
    }

    Ok(())
}

fn validate_row_set(case: &OracleCase, row_set: &TypedRowSet, label: &str) -> Result<()> {
    if row_set.columns != case.result_columns {
        return fail(format!(
            "{label} columns for case `{}` do not match inputs: expected {:?}, found {:?}",
            case.id, case.result_columns, row_set.columns
        ));
    }

    for (row_index, row) in row_set.rows.iter().enumerate() {
        if row.len() != case.result_columns.len() {
            return fail(format!(
                "{label} row {} for case `{}` has {} cells, expected {}",
                row_index,
                case.id,
                row.len(),
                case.result_columns.len()
            ));
        }
        for (column_index, (value, column)) in row.iter().zip(&case.result_columns).enumerate() {
            validate_value(
                value,
                column.column_type,
                &case.id,
                label,
                row_index,
                column_index,
            )?;
        }
    }

    Ok(())
}

fn validate_value(
    value: &TypedValue,
    column_type: ColumnType,
    case_id: &str,
    label: &str,
    row_index: usize,
    column_index: usize,
) -> Result<()> {
    let valid = match (column_type, value) {
        (_, TypedValue::Null) => true,
        (ColumnType::Boolean, TypedValue::Boolean { .. }) => true,
        (ColumnType::Float64, TypedValue::Float64 { bits }) => parse_bits(bits).is_ok(),
        (ColumnType::Int32List, TypedValue::Int32List { .. }) => true,
        (ColumnType::Ewkb, TypedValue::Ewkb { hex }) => decode_ewkb_hex(hex).is_ok(),
        _ => false,
    };
    if !valid {
        return fail(format!(
            "{label} case `{case_id}` row {row_index} column {column_index} value {value:?} is not valid for {column_type:?}"
        ));
    }
    Ok(())
}

fn validate_image(image: &str) -> Result<()> {
    image_digest(image)?;
    Ok(())
}

fn image_digest(image: &str) -> Result<String> {
    let Some((_, digest)) = image.rsplit_once("@sha256:") else {
        return fail(format!(
            "oracle image `{image}` must be pinned by @sha256 digest"
        ));
    };
    if digest.len() != 64 || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return fail(format!(
            "oracle image `{image}` has an invalid sha256 digest"
        ));
    }
    Ok(digest.to_ascii_lowercase())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub async fn connect(dsn: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostGIS oracle connection closed with error: {error}");
        }
    });
    Ok(client)
}

pub async fn read_oracle_versions(client: &Client, image: &str) -> Result<OracleVersions> {
    let digest = image_digest(image)?;
    let no_params: &[&(dyn ToSql + Sync)] = &[];
    let row = client
        .query_one(
            "SELECT current_setting('server_version')::text, \
                    current_setting('server_version_num')::int4, \
                    (SELECT extversion FROM pg_extension WHERE extname = 'postgis'), \
                    postgis_full_version(), \
                    postgis_geos_version()",
            no_params,
        )
        .await?;

    Ok(OracleVersions {
        image: image.to_string(),
        image_digest: format!("sha256:{digest}"),
        postgresql_version: row.try_get(0)?,
        postgresql_version_num: row.try_get(1)?,
        postgis_version: row.try_get(2)?,
        postgis_full_version: row.try_get(3)?,
        geos_version: row.try_get(4)?,
    })
}

pub async fn run_offline(ctx: &SessionContext, group: &OracleGroup) -> Result<Stats> {
    let expected = group
        .expected
        .as_ref()
        .ok_or_else(|| format!("group `{}` has no reviewed baseline", group.name))?;
    let mut stats = Stats {
        discovered: group.inputs.cases.len(),
        ..Stats::default()
    };

    for case in &group.inputs.cases {
        if case.skip.is_some() {
            stats.skipped += 1;
            continue;
        }

        let actual = execute_datafusion(ctx, group, case).await?;
        stats.executed += 1;
        let expected_row_set = expected
            .cases
            .get(&case.id)
            .ok_or_else(|| format!("baseline is missing case `{}`", case.id))?;
        compare_case(case, expected_row_set, &actual)?;
        stats.compared += 1;
        stats.rows_compared += actual.rows.len();
    }

    stats.ensure_compared(group.name)?;
    Ok(stats)
}

pub async fn run_live(
    ctx: &SessionContext,
    group: &OracleGroup,
    client: &Client,
    versions: &OracleVersions,
    write_dir: Option<&Path>,
) -> Result<(Stats, Stats)> {
    let mut live_results = BTreeMap::new();
    let mut oracle_stats = Stats {
        discovered: group.inputs.cases.len(),
        ..Stats::default()
    };

    for case in &group.inputs.cases {
        if case.skip.is_some() {
            oracle_stats.skipped += 1;
            continue;
        }

        let live = execute_postgis(client, group, case).await?;
        oracle_stats.executed += 1;
        live_results.insert(case.id.clone(), live);
    }

    if let Some(expected) = &group.expected {
        if &expected.oracle != versions {
            return fail(format!(
                "group `{}` reference version drift\nreviewed: {}\nlive: {}",
                group.name,
                serde_json::to_string_pretty(&expected.oracle)?,
                serde_json::to_string_pretty(versions)?
            ));
        }

        for case in &group.inputs.cases {
            if case.skip.is_some() {
                continue;
            }
            let expected_row_set = expected
                .cases
                .get(&case.id)
                .ok_or_else(|| format!("baseline is missing case `{}`", case.id))?;
            let live_row_set = live_results
                .get(&case.id)
                .ok_or_else(|| format!("live result is missing case `{}`", case.id))?;
            compare_case(case, expected_row_set, live_row_set)?;
            oracle_stats.compared += 1;
            oracle_stats.rows_compared += live_row_set.rows.len();
        }
        oracle_stats.ensure_compared("live PostGIS vs reviewed baseline")?;
    }

    if let Some(write_dir) = write_dir {
        let baseline = Baseline {
            schema_version: SCHEMA_VERSION,
            oracle: versions.clone(),
            inputs_sha256: group.inputs_sha256.clone(),
            postgis_sql_sha256: group.postgis_sql_sha256.clone(),
            datafusion_sql_sha256: group.datafusion_sql_sha256.clone(),
            cases: live_results.clone(),
        };
        write_candidate(write_dir, group.name, &baseline)?;
    }

    let mut datafusion_stats = Stats {
        discovered: group.inputs.cases.len(),
        ..Stats::default()
    };
    for case in &group.inputs.cases {
        if case.skip.is_some() {
            datafusion_stats.skipped += 1;
            continue;
        }

        let actual = execute_datafusion(ctx, group, case).await?;
        datafusion_stats.executed += 1;
        let live_row_set = live_results
            .get(&case.id)
            .ok_or_else(|| format!("live result is missing case `{}`", case.id))?;
        compare_case(case, live_row_set, &actual)?;
        datafusion_stats.compared += 1;
        datafusion_stats.rows_compared += actual.rows.len();
    }
    datafusion_stats.ensure_compared("live DataFusion vs PostGIS")?;

    Ok((oracle_stats, datafusion_stats))
}

async fn execute_datafusion(
    ctx: &SessionContext,
    group: &OracleGroup,
    case: &OracleCase,
) -> Result<TypedRowSet> {
    let params = case
        .arguments
        .iter()
        .map(|argument| match argument {
            OracleArgument::Text { value } => ScalarValue::Utf8(Some(value.clone())),
            OracleArgument::Float64 { value } => ScalarValue::Float64(Some(*value)),
        })
        .collect::<Vec<_>>();

    let dataframe = ctx.sql(&group.datafusion_sql).await?;
    let dataframe = dataframe.with_param_values(params)?;
    let batches = dataframe.collect().await?;

    let mut columns = vec![Vec::new(); case.result_columns.len()];
    let mut rows = Vec::new();
    let mut row_offset = 0;
    for batch in &batches {
        if batch.num_columns() != case.result_columns.len() {
            return fail(format!(
                "case `{}` returned {} columns, expected {}",
                case.id,
                batch.num_columns(),
                case.result_columns.len()
            ));
        }

        for (column_index, column) in case.result_columns.iter().enumerate() {
            let schema = batch.schema();
            let field = schema.field(column_index);
            if field.name() != &column.name {
                return fail(format!(
                    "case `{}` column {} is named `{}`, expected `{}`",
                    case.id,
                    column_index,
                    field.name(),
                    column.name
                ));
            }
            let values =
                datafusion_column_values(batch.column(column_index), field, column.column_type)?;
            columns[column_index].extend(values);
        }

        for row_index in 0..batch.num_rows() {
            let row = columns
                .iter()
                .map(|column| column[row_offset + row_index].clone())
                .collect::<Vec<_>>();
            rows.push(row);
        }
        row_offset += batch.num_rows();
    }

    Ok(TypedRowSet {
        columns: case.result_columns.clone(),
        rows,
    })
}

fn datafusion_column_values(
    array: &ArrayRef,
    field: &arrow_schema::Field,
    column_type: ColumnType,
) -> Result<Vec<TypedValue>> {
    match column_type {
        ColumnType::Boolean => {
            if array.data_type() != &arrow_schema::DataType::Boolean {
                return fail(format!(
                    "DataFusion column `{}` is not Boolean: {}",
                    field.name(),
                    array.data_type()
                ));
            }
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    format!(
                        "DataFusion column `{}` is not Boolean: {}",
                        field.name(),
                        array.data_type()
                    )
                })?;
            Ok(values
                .iter()
                .map(|value| match value {
                    Some(value) => TypedValue::Boolean { value },
                    None => TypedValue::Null,
                })
                .collect())
        }
        ColumnType::Float64 => {
            if array.data_type() != &arrow_schema::DataType::Float64 {
                return fail(format!(
                    "DataFusion column `{}` is not Float64: {}",
                    field.name(),
                    array.data_type()
                ));
            }
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    format!(
                        "DataFusion column `{}` is not Float64: {}",
                        field.name(),
                        array.data_type()
                    )
                })?;
            Ok(values
                .iter()
                .map(|value| match value {
                    Some(value) => TypedValue::float64(value),
                    None => TypedValue::Null,
                })
                .collect())
        }
        ColumnType::Int32List => {
            let arrow_schema::DataType::List(item) = array.data_type() else {
                return fail(format!(
                    "DataFusion column `{}` is not List<Int32>: {}",
                    field.name(),
                    array.data_type()
                ));
            };
            if item.data_type() != &arrow_schema::DataType::Int32 {
                return fail(format!(
                    "DataFusion column `{}` is not List<Int32>: {}",
                    field.name(),
                    array.data_type()
                ));
            }
            let values = array.as_list::<i32>();
            Ok((0..values.len())
                .map(|index| {
                    if values.is_null(index) {
                        TypedValue::Null
                    } else {
                        TypedValue::Int32List {
                            values: values
                                .value(index)
                                .as_primitive::<Int32Type>()
                                .values()
                                .to_vec(),
                        }
                    }
                })
                .collect())
        }
        ColumnType::Ewkb => {
            let geometry_array = from_arrow_array(array.as_ref(), field)?;
            let values = geometry_values(geometry_array.as_ref())?;
            Ok(values
                .into_iter()
                .map(|value| match value {
                    Some(value) => TypedValue::ewkb(value),
                    None => TypedValue::Null,
                })
                .collect())
        }
    }
}

fn geometry_values(array: &dyn GeoArrowArray) -> Result<Vec<Option<Vec<u8>>>> {
    let wkb = to_wkb::<i32>(array)?;
    let wkb = wkb.into_array_ref();
    let wkb = wkb
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "independent WKB encoder did not return BinaryArray".to_string())?;
    Ok((0..wkb.len())
        .map(|index| {
            if wkb.is_null(index) {
                None
            } else {
                Some(wkb.value(index).to_vec())
            }
        })
        .collect())
}

async fn execute_postgis(
    client: &Client,
    group: &OracleGroup,
    case: &OracleCase,
) -> Result<TypedRowSet> {
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(case.arguments.len());
    for argument in &case.arguments {
        match argument {
            OracleArgument::Text { value } => params.push(value),
            OracleArgument::Float64 { value } => params.push(value),
        }
    }

    let rows = client.query(group.postgis_sql.as_str(), &params).await?;
    let mut typed_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut typed_row = Vec::with_capacity(case.result_columns.len());
        for (column_index, column) in case.result_columns.iter().enumerate() {
            let value = match column.column_type {
                ColumnType::Boolean => match row.try_get::<_, Option<bool>>(column_index)? {
                    Some(value) => TypedValue::Boolean { value },
                    None => TypedValue::Null,
                },
                ColumnType::Float64 => match row.try_get::<_, Option<f64>>(column_index)? {
                    Some(value) => TypedValue::float64(value),
                    None => TypedValue::Null,
                },
                ColumnType::Int32List => match row.try_get::<_, Option<Vec<i32>>>(column_index)? {
                    Some(values) => TypedValue::Int32List { values },
                    None => TypedValue::Null,
                },
                ColumnType::Ewkb => match row.try_get::<_, Option<Vec<u8>>>(column_index)? {
                    Some(value) => TypedValue::ewkb(value),
                    None => TypedValue::Null,
                },
            };
            typed_row.push(value);
        }
        typed_rows.push(typed_row);
    }

    Ok(TypedRowSet {
        columns: case.result_columns.clone(),
        rows: typed_rows,
    })
}

pub fn compare_case(case: &OracleCase, expected: &TypedRowSet, actual: &TypedRowSet) -> Result<()> {
    validate_row_set(case, expected, "expected")?;
    validate_row_set(case, actual, "actual")?;
    if expected.rows.len() != actual.rows.len() {
        return fail(format!(
            "case `{}` row count mismatch under {}\nexpected:\n{}\nactual:\n{}",
            case.id,
            case.comparison.name(),
            serde_json::to_string_pretty(expected)?,
            serde_json::to_string_pretty(actual)?
        ));
    }

    for (row_index, (expected_row, actual_row)) in
        expected.rows.iter().zip(&actual.rows).enumerate()
    {
        for (column_index, (expected_value, actual_value)) in
            expected_row.iter().zip(actual_row).enumerate()
        {
            if !values_match(expected_value, actual_value, &case.comparison)? {
                return fail(format!(
                    "case `{}` column `{}` row {} differs under {}\nexpected: {}\nactual:   {}",
                    case.id,
                    case.result_columns[column_index].name,
                    row_index,
                    case.comparison.name(),
                    value_display(expected_value)?,
                    value_display(actual_value)?
                ));
            }
        }
    }

    Ok(())
}

fn values_match(
    expected: &TypedValue,
    actual: &TypedValue,
    comparison: &Comparison,
) -> Result<bool> {
    match comparison {
        Comparison::Exact => Ok(expected == actual),
        Comparison::FloatUlp { max_ulps } => match (expected, actual) {
            (TypedValue::Null, TypedValue::Null) => Ok(true),
            (
                TypedValue::Float64 {
                    bits: expected_bits,
                },
                TypedValue::Float64 { bits: actual_bits },
            ) => {
                let expected_bits = parse_bits(expected_bits)?;
                let actual_bits = parse_bits(actual_bits)?;
                Ok(float_ulp_distance(expected_bits, actual_bits) <= *max_ulps)
            }
            _ => Ok(expected == actual),
        },
        Comparison::GeometryExact => match (expected, actual) {
            (TypedValue::Null, TypedValue::Null) => Ok(true),
            (TypedValue::Ewkb { hex: expected }, TypedValue::Ewkb { hex: actual }) => {
                Ok(decode_ewkb_hex(expected)? == decode_ewkb_hex(actual)?)
            }
            _ => Ok(expected == actual),
        },
    }
}

fn parse_bits(bits: &str) -> Result<u64> {
    if bits.len() != 16 || !bits.chars().all(|c| c.is_ascii_hexdigit()) {
        return fail(format!("invalid float64 bits `{bits}`"));
    }
    Ok(u64::from_str_radix(bits, 16)?)
}

fn float_ulp_distance(expected: u64, actual: u64) -> u64 {
    let expected_exponent = expected & 0x7ff0_0000_0000_0000;
    let actual_exponent = actual & 0x7ff0_0000_0000_0000;
    if expected_exponent == 0x7ff0_0000_0000_0000 || actual_exponent == 0x7ff0_0000_0000_0000 {
        return if expected == actual { 0 } else { u64::MAX };
    }

    ordered_bits(expected).abs_diff(ordered_bits(actual))
}

fn ordered_bits(bits: u64) -> u64 {
    if bits & (1 << 63) == 0 {
        bits | (1 << 63)
    } else {
        !bits
    }
}

fn value_display(value: &TypedValue) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn write_candidate(write_dir: &Path, group: &str, baseline: &Baseline) -> Result<()> {
    if !write_dir.is_absolute() {
        return fail(format!(
            "GEODATAFUSION_ORACLE_WRITE must be an absolute external directory, got {}",
            write_dir.display()
        ));
    }

    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR")).canonicalize()?;
    let repo = crate_dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| "crate must be inside the repository".to_string())?
        .canonicalize()?;
    let candidate = write_dir.join(group).join("expected.json");
    let parent = candidate
        .parent()
        .expect("candidate path must have a parent directory");
    let absolute_parent = canonicalize_allow_missing(parent)?;
    if absolute_parent.starts_with(&repo) {
        return fail(format!(
            "refusing to write oracle candidate inside the repository: {}",
            candidate.display()
        ));
    }
    if candidate.exists() {
        return fail(format!(
            "refusing to overwrite existing oracle candidate {}",
            candidate.display()
        ));
    }

    fs::create_dir_all(parent)?;
    let mut json = serde_json::to_string_pretty(baseline)?;
    json.push('\n');
    fs::write(&candidate, json)?;
    eprintln!("wrote oracle candidate {}", candidate.display());
    Ok(())
}

fn canonicalize_allow_missing(path: &Path) -> Result<PathBuf> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        if current.exists() {
            let mut resolved = current.canonicalize()?;
            for component in missing.iter().rev() {
                resolved.push(component);
            }
            return Ok(resolved);
        }
        let component = current
            .file_name()
            .ok_or_else(|| format!("cannot resolve path {}", path.display()))?;
        missing.push(component.to_os_string());
        current = current
            .parent()
            .ok_or_else(|| format!("cannot resolve path {}", path.display()))?;
    }
}

#[derive(Debug, Clone, PartialEq)]
struct GeometryValue {
    kind: GeometryKind,
    dimensions: geo_traits::Dimensions,
    srid: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
enum GeometryKind {
    Point(Option<Vec<u64>>),
    LineString(Vec<Vec<u64>>),
    Polygon(Vec<Vec<Vec<u64>>>),
}

fn decode_ewkb_hex(hex: &str) -> Result<GeometryValue> {
    let bytes = hex::decode(hex)?;
    let srid = read_ewkb_srid(&bytes)?;
    let parsed = wkb::reader::Wkb::try_new(&bytes)?;
    if parsed.buf().len() != bytes.len() {
        return fail(format!(
            "EWKB has {} trailing bytes",
            bytes.len() - parsed.buf().len()
        ));
    }
    let dimensions = parsed.dim();
    let kind = match parsed.as_type() {
        GeometryType::Point(point) => {
            GeometryKind::Point(point.coord().map(|coord| coord_bits(&coord)))
        }
        GeometryType::LineString(line_string) => {
            GeometryKind::LineString(line_string_bits(&line_string)?)
        }
        GeometryType::Polygon(polygon) => GeometryKind::Polygon(polygon_bits(&polygon)?),
        _ => {
            return fail(format!(
                "geometry-exact comparison does not support {:?}",
                parsed.geometry_type()
            ));
        }
    };

    Ok(GeometryValue {
        kind,
        dimensions,
        srid,
    })
}

fn read_ewkb_srid(bytes: &[u8]) -> Result<Option<i32>> {
    let byte_order = *bytes
        .first()
        .ok_or_else(|| "empty EWKB buffer".to_string())?;
    let little_endian = match byte_order {
        0 => false,
        1 => true,
        value => return fail(format!("invalid EWKB byte order {value}")),
    };
    let raw_type = bytes
        .get(1..5)
        .ok_or_else(|| "truncated EWKB header".to_string())?;
    let raw_type = if little_endian {
        u32::from_le_bytes(raw_type.try_into()?)
    } else {
        u32::from_be_bytes(raw_type.try_into()?)
    };
    if raw_type & 0x2000_0000 == 0 {
        return Ok(None);
    }

    let srid = bytes
        .get(5..9)
        .ok_or_else(|| "EWKB SRID flag without SRID bytes".to_string())?;
    let srid = if little_endian {
        i32::from_le_bytes(srid.try_into()?)
    } else {
        i32::from_be_bytes(srid.try_into()?)
    };
    Ok(Some(srid))
}

fn coord_bits<C: CoordTrait<T = f64>>(coord: &C) -> Vec<u64> {
    (0..coord.dim().size())
        .map(|index| coord.nth_or_panic(index).to_bits())
        .collect()
}

fn line_string_bits<L: LineStringTrait<T = f64>>(line_string: &L) -> Result<Vec<Vec<u64>>> {
    let mut coords = Vec::with_capacity(line_string.num_coords());
    for index in 0..line_string.num_coords() {
        let coord = line_string
            .coord(index)
            .ok_or_else(|| "WKB line string coord index out of range".to_string())?;
        coords.push(coord_bits(&coord));
    }
    Ok(coords)
}

fn polygon_bits<P: PolygonTrait<T = f64>>(polygon: &P) -> Result<Vec<Vec<Vec<u64>>>> {
    let mut rings = Vec::new();
    if let Some(exterior) = polygon.exterior() {
        rings.push(line_string_bits(&exterior)?);
    }
    for interior in polygon.interiors() {
        rings.push(line_string_bits(&interior)?);
    }
    Ok(rings)
}

fn fail<T>(message: impl Into<String>) -> Result<T> {
    let message = message.into();
    Err(Box::new(OracleError(message)))
}

#[derive(Debug)]
struct OracleError(String);

impl fmt::Display for OracleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for OracleError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(id: &str) -> OracleCase {
        OracleCase {
            id: id.to_string(),
            function: "ST_Distance".to_string(),
            arguments: vec![OracleArgument::Text {
                value: "POINT(0 0)".to_string(),
            }],
            result_columns: vec![ResultColumn {
                name: "value".to_string(),
                column_type: ColumnType::Float64,
            }],
            comparison: Comparison::Exact,
            source: "https://postgis.net/docs/ST_Distance.html".to_string(),
            skip: None,
        }
    }

    #[test]
    fn duplicate_case_ids_are_rejected() {
        let inputs = OracleInputs {
            schema_version: SCHEMA_VERSION,
            cases: vec![case("stable-id"), case("stable-id")],
        };
        assert!(validate_inputs(&inputs).is_err());
    }

    #[test]
    fn all_skipped_cases_are_rejected() {
        let mut skipped = case("skipped-id");
        skipped.skip = Some(Skip {
            reason: "test".to_string(),
            issue: "https://example.com/issue/1".to_string(),
            review_after: "2099-01-01".to_string(),
        });
        let inputs = OracleInputs {
            schema_version: SCHEMA_VERSION,
            cases: vec![skipped],
        };
        assert!(validate_inputs(&inputs).is_err());
    }

    #[test]
    fn malformed_baseline_row_widths_fail_comparison() {
        let case = case("stable-id");
        let expected = TypedRowSet {
            columns: case.result_columns.clone(),
            rows: vec![Vec::new()],
        };
        let actual = TypedRowSet {
            columns: case.result_columns.clone(),
            rows: vec![vec![TypedValue::float64(1.0)]],
        };
        assert!(compare_case(&case, &expected, &actual).is_err());
        assert!(validate_row_set(&case, &expected, "test").is_err());
    }

    #[test]
    fn malformed_float_bits_are_rejected() {
        let case = case("stable-id");
        let row_set = TypedRowSet {
            columns: case.result_columns.clone(),
            rows: vec![vec![TypedValue::Float64 {
                bits: "not-hex".to_string(),
            }]],
        };
        assert!(validate_row_set(&case, &row_set, "test").is_err());
    }

    #[test]
    fn baseline_hash_mismatch_is_rejected() {
        let case = case("stable-id");
        let inputs = OracleInputs {
            schema_version: SCHEMA_VERSION,
            cases: vec![case],
        };
        let baseline = Baseline {
            schema_version: SCHEMA_VERSION,
            oracle: OracleVersions {
                image: PINNED_POSTGIS_IMAGE.to_string(),
                image_digest: format!("sha256:{}", image_digest(PINNED_POSTGIS_IMAGE).unwrap()),
                postgresql_version: "test".to_string(),
                postgresql_version_num: 180000,
                postgis_version: "3.6.4".to_string(),
                postgis_full_version: "test".to_string(),
                geos_version: "3.14.1".to_string(),
            },
            inputs_sha256: "wrong".to_string(),
            postgis_sql_sha256: "wrong".to_string(),
            datafusion_sql_sha256: "wrong".to_string(),
            cases: BTreeMap::from([(
                "stable-id".to_string(),
                TypedRowSet {
                    columns: vec![ResultColumn {
                        name: "value".to_string(),
                        column_type: ColumnType::Float64,
                    }],
                    rows: Vec::new(),
                },
            )]),
        };
        assert!(validate_baseline(&inputs, &baseline, "a", "b", "c").is_err());
    }

    #[test]
    fn empty_results_can_pass_after_a_case_was_compared() {
        let stats = Stats {
            discovered: 1,
            executed: 1,
            compared: 1,
            rows_compared: 0,
            skipped: 0,
        };
        assert!(stats.ensure_compared("empty result").is_ok());
    }

    #[test]
    fn float_ulp_comparison_ignores_unrelated_types() {
        let expected = TypedValue::Boolean { value: true };
        let actual = TypedValue::Boolean { value: false };
        assert!(!values_match(&expected, &actual, &Comparison::FloatUlp { max_ulps: 8 }).unwrap());
    }

    #[test]
    fn float_ulp_comparison_uses_ieee_bits() {
        let expected = TypedValue::Float64 {
            bits: format!("{:016x}", 1.0f64.to_bits()),
        };
        let actual = TypedValue::Float64 {
            bits: format!("{:016x}", f64::from_bits(1.0f64.to_bits() + 1).to_bits()),
        };
        assert!(values_match(&expected, &actual, &Comparison::FloatUlp { max_ulps: 1 }).unwrap());
        assert!(!values_match(&expected, &actual, &Comparison::FloatUlp { max_ulps: 0 }).unwrap());
    }
}
