//! Typed, reviewed PostGIS oracle support for the `postgis` integration test.
//!
//! The checked-in baseline is generated only from a real pinned PostGIS
//! instance. This module never derives expected values from the
//! GeoDataFusion implementation under test.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, fs};

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::{GeoArrowType, Metadata, WkbType};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};
use toml_edit::DocumentMut;

#[path = "mvt.rs"]
pub(crate) mod mvt_decode;

use mvt_decode::decode_hex;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const SCHEMA_VERSION: u32 = 1;

/// The candidate reference image. The digest is the multi-architecture index
/// digest and is also required by the checked-in compose file and CI job.
pub const PINNED_POSTGIS_IMAGE: &str = "postgis/postgis:18-3.6@sha256:4a43e3c3e5d55b9208240fc71db543b79c638766280c625a425bd0a2390989c4";

/// Table name used when a case declares real Arrow input fields.
pub const ORACLE_INPUT_TABLE: &str = "oracle_input";

/// Cargo features that the test support layer knows how to evaluate.
const SUPPORTED_FEATURES: &[&str] = &["geos-3_11", "geos-3_12"];

const GROUP_FILES: &[&str] = &[
    "inputs.json",
    "postgis.sql",
    "datafusion.sql",
    "expected.json",
];

/// Recursively discovers reviewed groups under `tests/postgis`.
///
/// A directory is treated as a group when it contains any of the four
/// convention files. Loading then fails explicitly if the required files are
/// missing, so an incomplete fixture is never silently skipped.
pub fn discover_groups() -> Result<Vec<String>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/postgis");
    discover_groups_under(&root)
}

fn discover_groups_under(root: &Path) -> Result<Vec<String>> {
    let mut groups = Vec::new();
    discover_groups_in(root, root, &mut groups)?;
    groups.sort();
    if groups.is_empty() {
        return fail(format!(
            "no PostGIS oracle groups discovered under {}",
            root.display()
        ));
    }
    Ok(groups)
}

fn discover_groups_in(root: &Path, directory: &Path, groups: &mut Vec<String>) -> Result<()> {
    if GROUP_FILES
        .iter()
        .any(|name| directory.join(name).is_file())
    {
        let relative = directory.strip_prefix(root)?;
        groups.push(relative.to_string_lossy().replace('\\', "/"));
    }

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            discover_groups_in(root, &entry.path(), groups)?;
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct OracleInputs {
    pub schema_version: u32,
    #[serde(default)]
    pub required_features: Option<Vec<String>>,
    pub cases: Vec<OracleCase>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OracleCase {
    pub id: String,
    pub function: String,
    pub arguments: Vec<OracleArgument>,
    pub result_columns: Vec<ResultColumn>,
    pub comparison: Comparison,
    pub source: String,
    #[serde(default)]
    pub skip: Option<Skip>,
    #[serde(default)]
    pub runtime_error: Option<RuntimeErrorExpectation>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OracleArgument {
    Text {
        value: Option<String>,
        #[serde(default)]
        nullable: Option<bool>,
        #[serde(default)]
        storage: Option<TextStorage>,
    },
    Float64 {
        value: Option<f64>,
        #[serde(default)]
        nullable: Option<bool>,
    },
    Int64 {
        value: Option<i64>,
        #[serde(default)]
        nullable: Option<bool>,
    },
    Int32 {
        value: Option<i32>,
        #[serde(default)]
        nullable: Option<bool>,
    },
    Boolean {
        value: Option<bool>,
        #[serde(default)]
        nullable: Option<bool>,
    },
}

/// Arrow storage for a text argument that carries a geometry value.
///
/// `Text` keeps the historical WKT/UTF-8 field. `Wkb` encodes the same WKT
/// value as standard WKB and registers it with independent `WkbType`
/// metadata, so DataFusion consumers that can preserve structures GeoArrow's
/// native union cannot (notably nested `GeometryCollection`) run against a
/// real typed WKB field. PostgreSQL still receives the original text value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextStorage {
    Text,
    Wkb,
}

impl OracleArgument {
    fn declared_nullable(&self) -> Option<bool> {
        match self {
            Self::Text { nullable, .. }
            | Self::Float64 { nullable, .. }
            | Self::Int64 { nullable, .. }
            | Self::Int32 { nullable, .. }
            | Self::Boolean { nullable, .. } => *nullable,
        }
    }

    fn nullable(&self) -> bool {
        self.declared_nullable().unwrap_or(true)
    }

    fn is_null(&self) -> bool {
        match self {
            Self::Text { value, .. } => value.is_none(),
            Self::Float64 { value, .. } => value.is_none(),
            Self::Int64 { value, .. } => value.is_none(),
            Self::Int32 { value, .. } => value.is_none(),
            Self::Boolean { value, .. } => value.is_none(),
        }
    }

    fn declared_storage(&self) -> Option<TextStorage> {
        match self {
            Self::Text { storage, .. } => *storage,
            _ => None,
        }
    }
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
    Text,
    Int32,
    Int32List,
    Ewkb,
    HexList,
    Bytea,
    Mvt,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum Comparison {
    Exact,
    FloatUlp { max_ulps: u64 },
    GeometryExact,
    GeometryNormalized,
    GeometryCoordinateUlp { max_ulps: u64 },
    BinaryExact,
    GeobufSemantic,
    MvtSemantic,
    ClusterIdsEquivalent { canonical_columns: Vec<String> },
    HexSetExact,
}

impl Comparison {
    fn name(&self) -> String {
        match self {
            Self::Exact => "exact".to_string(),
            Self::FloatUlp { max_ulps } => format!("float-ulp(max_ulps={max_ulps})"),
            Self::GeometryExact => "geometry-exact".to_string(),
            Self::GeometryNormalized => "geometry-normalized".to_string(),
            Self::GeometryCoordinateUlp { max_ulps } => {
                format!("geometry-coordinate-ulp(max_ulps={max_ulps})")
            }
            Self::BinaryExact => "binary-exact".to_string(),
            Self::GeobufSemantic => "geobuf-semantic".to_string(),
            Self::MvtSemantic => "mvt-semantic".to_string(),
            Self::ClusterIdsEquivalent { canonical_columns } => format!(
                "cluster-ids-equivalent(canonical_columns=[{}])",
                canonical_columns.join(",")
            ),
            Self::HexSetExact => "hex-set-exact".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Skip {
    pub reason: String,
    pub issue: String,
    pub review_after: String,
}

/// Case-local runtime error contract for version-dependent behavior.
///
/// When the linked GEOS runtime is older than `geos_before`, executing the
/// case must fail with an error containing `contains`. At or above that
/// version the reviewed baseline is compared normally. This keeps the
/// PostGIS golden result for modern runtimes without accepting legacy output
/// or skipping the case on older ones.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeErrorExpectation {
    pub geos_before: String,
    pub contains: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TypedValue {
    Boolean { value: bool },
    Float64 { bits: String },
    Text { value: String },
    Int32 { value: i32 },
    Int32List { values: Vec<i32> },
    Ewkb { hex: String },
    HexList { values: Vec<String> },
    Bytea { hex: String },
    Mvt { hex: String },
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

    fn bytea(value: Vec<u8>) -> Self {
        Self::Bytea {
            hex: hex::encode(value),
        }
    }

    fn mvt(value: Vec<u8>) -> Self {
        Self::Mvt {
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
    pub name: String,
    pub inputs: OracleInputs,
    pub postgis_sql: String,
    pub datafusion_sql: String,
    pub expected: Option<Baseline>,
    pub missing_features: Vec<String>,
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
    pub feature_skipped_groups: usize,
    pub feature_skipped_cases: usize,
}

impl Stats {
    pub fn add(&mut self, other: &Stats) {
        self.discovered += other.discovered;
        self.executed += other.executed;
        self.compared += other.compared;
        self.rows_compared += other.rows_compared;
        self.skipped += other.skipped;
        self.feature_skipped_groups += other.feature_skipped_groups;
        self.feature_skipped_cases += other.feature_skipped_cases;
    }

    pub fn summary(&self) -> String {
        format!(
            "discovered={} executed={} compared={} rows_compared={} skipped={} feature_skipped_groups={} feature_skipped_cases={}",
            self.discovered,
            self.executed,
            self.compared,
            self.rows_compared,
            self.skipped,
            self.feature_skipped_groups,
            self.feature_skipped_cases
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

    pub fn ensure_no_feature_skips(&self, context: &str) -> Result<()> {
        if self.feature_skipped_groups != 0 || self.feature_skipped_cases != 0 {
            return fail(format!(
                "{context}: feature-gated groups were skipped ({})",
                self.summary()
            ));
        }
        Ok(())
    }
}

impl OracleGroup {
    pub fn unavailable_reason(&self) -> Option<String> {
        (!self.missing_features.is_empty()).then(|| {
            format!(
                "missing required features: {}",
                self.missing_features.join(", ")
            )
        })
    }

    pub fn feature_skip_stats(&self) -> Stats {
        Stats {
            discovered: self.inputs.cases.len(),
            feature_skipped_groups: 1,
            feature_skipped_cases: self.inputs.cases.len(),
            ..Stats::default()
        }
    }
}

pub fn load_group(name: &str, require_baseline: bool) -> Result<OracleGroup> {
    load_group_inner(name, require_baseline, false)
}

pub fn load_capture_group(name: &str) -> Result<OracleGroup> {
    load_group_inner(name, false, true)
}

fn load_group_inner(
    name: &str,
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
    let missing_features = inputs
        .required_features
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|feature| !feature_enabled(feature))
        .cloned()
        .collect();

    Ok(OracleGroup {
        name: name.to_string(),
        inputs,
        postgis_sql,
        datafusion_sql,
        expected,
        missing_features,
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
    let Some(required_features) = &inputs.required_features else {
        return fail("oracle inputs are missing `required_features`");
    };
    validate_required_features(required_features)?;
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
        validate_argument_nullability(case)?;
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
        if let Comparison::ClusterIdsEquivalent { canonical_columns } = &case.comparison {
            if canonical_columns.is_empty() {
                return fail(format!(
                    "case `{}` cluster-ids-equivalent requires at least one canonical column",
                    case.id
                ));
            }
            let mut canonical_names = HashSet::new();
            for name in canonical_columns {
                if name.is_empty() || !canonical_names.insert(name.as_str()) {
                    return fail(format!(
                        "case `{}` canonical column `{name}` is empty or duplicated",
                        case.id
                    ));
                }
                let Some(column) = case
                    .result_columns
                    .iter()
                    .find(|column| &column.name == name)
                else {
                    return fail(format!(
                        "case `{}` canonical column `{name}` does not exist in result columns",
                        case.id
                    ));
                };
                if column.column_type != ColumnType::Int32 {
                    return fail(format!(
                        "case `{}` canonical column `{name}` must be int32, found {:?}",
                        case.id, column.column_type
                    ));
                }
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
        if let Some(expectation) = &case.runtime_error {
            if case.skip.is_some() {
                return fail(format!(
                    "case `{}` cannot declare both skip and runtime_error",
                    case.id
                ));
            }
            if expectation.contains.is_empty() {
                return fail(format!(
                    "case `{}` runtime_error.contains must not be empty",
                    case.id
                ));
            }
            if GeosRuntimeVersion::parse_threshold(&expectation.geos_before).is_err() {
                return fail(format!(
                    "case `{}` runtime_error.geos_before `{}` must be a major.minor version",
                    case.id, expectation.geos_before
                ));
            }
        }
    }

    let uses_runtime_error = inputs.cases.iter().any(|case| case.runtime_error.is_some());
    if uses_runtime_error
        && !required_features
            .iter()
            .any(|feature| SUPPORTED_FEATURES.contains(&feature.as_str()))
    {
        return fail("runtime_error cases require a group with a supported GEOS feature");
    }

    if inputs.cases.iter().all(|case| case.skip.is_some()) {
        return fail("oracle inputs skip every case");
    }

    Ok(())
}

fn declared_features() -> Result<BTreeSet<String>> {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = fs::read_to_string(manifest_path)?;
    let document: DocumentMut = manifest.parse()?;
    let mut features = BTreeSet::new();
    if let Some(table) = document.get("features").and_then(|item| item.as_table()) {
        for (name, _) in table {
            features.insert(name.to_string());
        }
    }
    Ok(features)
}

fn validate_required_features(features: &[String]) -> Result<()> {
    let declared = declared_features()?;
    for feature in features {
        if !declared.contains(feature.as_str()) {
            return fail(format!(
                "required feature `{feature}` is not declared in Cargo.toml"
            ));
        }
        if !SUPPORTED_FEATURES.contains(&feature.as_str()) {
            return fail(format!(
                "required feature `{feature}` has no resolver in tests/support/oracle.rs"
            ));
        }
    }
    Ok(())
}

#[allow(unexpected_cfgs)]
#[allow(clippy::match_like_matches_macro, clippy::needless_bool)]
fn feature_enabled(name: &str) -> bool {
    // Feature values are fixed per build; clippy would collapse disabled cfg values incorrectly.
    match name {
        "geos-3_11" => cfg!(feature = "geos-3_11"),
        "geos-3_12" => cfg!(feature = "geos-3_12"),
        _ => false,
    }
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
        (ColumnType::Text, TypedValue::Text { .. }) => true,
        (ColumnType::Int32, TypedValue::Int32 { .. }) => true,
        (ColumnType::Int32List, TypedValue::Int32List { .. }) => true,
        (ColumnType::Ewkb, TypedValue::Ewkb { hex }) => decode_ewkb_hex(hex).is_ok(),
        (ColumnType::HexList, TypedValue::HexList { values }) => {
            values.iter().all(|hex| hex::decode(hex).is_ok())
        }
        (ColumnType::Bytea, TypedValue::Bytea { hex }) => hex::decode(hex).is_ok(),
        (ColumnType::Mvt, TypedValue::Mvt { hex }) => decode_hex(hex).is_ok(),
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
    if let Some(reason) = group.unavailable_reason() {
        return fail(format!("group `{}` cannot run: {reason}", group.name));
    }
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

        match execute_case(ctx, group, case).await? {
            CaseExecution::Rows(actual) => {
                stats.executed += 1;
                let expected_row_set = expected
                    .cases
                    .get(&case.id)
                    .ok_or_else(|| format!("baseline is missing case `{}`", case.id))?;
                compare_case(case, expected_row_set, &actual)?;
                stats.compared += 1;
                stats.rows_compared += actual.rows.len();
            }
            CaseExecution::RuntimeError => {
                expected
                    .cases
                    .get(&case.id)
                    .ok_or_else(|| format!("baseline is missing case `{}`", case.id))?;
                stats.executed += 1;
                stats.compared += 1;
            }
        }
    }

    stats.ensure_compared(&group.name)?;
    Ok(stats)
}

pub async fn run_live(
    ctx: &SessionContext,
    group: &OracleGroup,
    client: &Client,
    versions: &OracleVersions,
    write_dir: Option<&Path>,
) -> Result<(Stats, Stats)> {
    if let Some(reason) = group.unavailable_reason() {
        return fail(format!("group `{}` cannot run: {reason}", group.name));
    }
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
        write_candidate(write_dir, &group.name, &baseline)?;
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

        match execute_case(ctx, group, case).await? {
            CaseExecution::Rows(actual) => {
                datafusion_stats.executed += 1;
                let live_row_set = live_results
                    .get(&case.id)
                    .ok_or_else(|| format!("live result is missing case `{}`", case.id))?;
                compare_case(case, live_row_set, &actual)?;
                datafusion_stats.compared += 1;
                datafusion_stats.rows_compared += actual.rows.len();
            }
            CaseExecution::RuntimeError => {
                live_results
                    .get(&case.id)
                    .ok_or_else(|| format!("live result is missing case `{}`", case.id))?;
                datafusion_stats.executed += 1;
                datafusion_stats.compared += 1;
            }
        }
    }
    datafusion_stats.ensure_compared("live DataFusion vs PostGIS")?;

    Ok((oracle_stats, datafusion_stats))
}

async fn execute_datafusion(
    ctx: &SessionContext,
    group: &OracleGroup,
    case: &OracleCase,
) -> Result<TypedRowSet> {
    // A group has one SQL shape, so any case that needs real Arrow input
    // fields puts the whole group on the `oracle_input` table. Cases without
    // an explicit declaration still receive a regular scalar field.
    let uses_arrow_inputs = group.inputs.cases.iter().any(|group_case| {
        group_case.arguments.iter().any(|argument| {
            argument.declared_nullable().is_some() || argument.declared_storage().is_some()
        })
    });
    if uses_arrow_inputs {
        let batch = arrow_input_batch(case)?;
        let _ = ctx.deregister_table(ORACLE_INPUT_TABLE)?;
        ctx.register_batch(ORACLE_INPUT_TABLE, batch)?;
    }
    let dataframe = ctx.sql(&group.datafusion_sql).await?;
    let dataframe = if uses_arrow_inputs {
        dataframe
    } else {
        let params = case
            .arguments
            .iter()
            .map(|argument| match argument {
                OracleArgument::Text { value, .. } => ScalarValue::Utf8(value.clone()),
                OracleArgument::Float64 { value, .. } => ScalarValue::Float64(*value),
                OracleArgument::Int64 { value, .. } => ScalarValue::Int64(*value),
                OracleArgument::Int32 { value, .. } => ScalarValue::Int32(*value),
                OracleArgument::Boolean { value, .. } => ScalarValue::Boolean(*value),
            })
            .collect::<Vec<_>>();
        dataframe.with_param_values(params)?
    };
    let batches = dataframe.collect().await?;
    if uses_arrow_inputs {
        let _ = ctx.deregister_table(ORACLE_INPUT_TABLE)?;
    }

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

enum CaseExecution {
    Rows(TypedRowSet),
    RuntimeError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct GeosRuntimeVersion {
    major: u32,
    minor: u32,
}

impl GeosRuntimeVersion {
    fn parse(raw: &str) -> Result<Self> {
        let mut parts = raw.split('.');
        let (Some(major), Some(minor)) = (
            parts.next().and_then(|part| part.parse::<u32>().ok()),
            parts.next().and_then(|part| part.parse::<u32>().ok()),
        ) else {
            return fail(format!("invalid GEOS version `{raw}`"));
        };
        Ok(Self { major, minor })
    }

    /// Parse the reviewed `major.minor` threshold written in a fixture.
    fn parse_threshold(raw: &str) -> Result<Self> {
        let mut parts = raw.split('.');
        let major = parts.next().and_then(|part| part.parse::<u32>().ok());
        let minor = parts.next().and_then(|part| part.parse::<u32>().ok());
        if parts.next().is_some() {
            return fail(format!("invalid GEOS threshold `{raw}`"));
        }
        match (major, minor) {
            (Some(major), Some(minor)) => Ok(Self { major, minor }),
            _ => fail(format!("invalid GEOS threshold `{raw}`")),
        }
    }
}

impl fmt::Display for GeosRuntimeVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.major, self.minor)
    }
}

fn linked_geos_runtime() -> Result<Option<GeosRuntimeVersion>> {
    #[cfg(any(feature = "geos-3_11", feature = "geos-3_12"))]
    {
        let version = geos::version()
            .map_err(|error| format!("failed to read the linked GEOS version: {error}"))?;
        Ok(Some(GeosRuntimeVersion::parse(&version)?))
    }
    #[cfg(not(any(feature = "geos-3_11", feature = "geos-3_12")))]
    {
        Ok(None)
    }
}

/// Execute a case and enforce its optional runtime error contract.
///
/// The decision uses the version reported by the linked GEOS library, the
/// same source as the runtime guard, rather than a Cargo feature name.
async fn execute_case(
    ctx: &SessionContext,
    group: &OracleGroup,
    case: &OracleCase,
) -> Result<CaseExecution> {
    let result = execute_datafusion(ctx, group, case).await;
    let Some(expectation) = &case.runtime_error else {
        return Ok(CaseExecution::Rows(result?));
    };

    let runtime = linked_geos_runtime()?.ok_or_else(|| {
        format!(
            "case `{}` declares a runtime error expectation but no GEOS runtime is linked",
            case.id
        )
    })?;
    let threshold = GeosRuntimeVersion::parse_threshold(&expectation.geos_before)?;
    match result {
        Ok(rows) if runtime < threshold => fail(format!(
            "case `{}` must reject on GEOS {runtime} before {threshold} but returned {} rows",
            case.id,
            rows.rows.len()
        )),
        Ok(rows) => Ok(CaseExecution::Rows(rows)),
        Err(error) if runtime < threshold => {
            let message = error.to_string();
            if !message.contains(&expectation.contains) {
                return fail(format!(
                    "case `{}` error on GEOS {runtime} does not contain `{}`: {message}",
                    case.id, expectation.contains
                ));
            }
            Ok(CaseExecution::RuntimeError)
        }
        Err(error) => fail(format!(
            "case `{}` failed on GEOS {runtime} although results are compared from {threshold}: {error}",
            case.id
        )),
    }
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
        ColumnType::Text => match array.data_type() {
            arrow_schema::DataType::Utf8 => Ok(array
                .as_string::<i32>()
                .iter()
                .map(|value| {
                    value.map_or(TypedValue::Null, |value| TypedValue::Text {
                        value: value.to_string(),
                    })
                })
                .collect()),
            arrow_schema::DataType::LargeUtf8 => Ok(array
                .as_string::<i64>()
                .iter()
                .map(|value| {
                    value.map_or(TypedValue::Null, |value| TypedValue::Text {
                        value: value.to_string(),
                    })
                })
                .collect()),
            arrow_schema::DataType::Utf8View => Ok(array
                .as_string_view()
                .iter()
                .map(|value| {
                    value.map_or(TypedValue::Null, |value| TypedValue::Text {
                        value: value.to_string(),
                    })
                })
                .collect()),
            data_type => fail(format!(
                "DataFusion column `{}` is not text: {data_type}",
                field.name()
            )),
        },
        ColumnType::Int32 => {
            if array.data_type() != &arrow_schema::DataType::Int32 {
                return fail(format!(
                    "DataFusion column `{}` is not Int32: {}",
                    field.name(),
                    array.data_type()
                ));
            }
            Ok(array
                .as_primitive::<Int32Type>()
                .iter()
                .map(|value| match value {
                    Some(value) => TypedValue::Int32 { value },
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
            let values = ewkb_values(array.as_ref(), field)?;
            Ok(values
                .into_iter()
                .map(|value| match value {
                    Some(value) => TypedValue::ewkb(value),
                    None => TypedValue::Null,
                })
                .collect())
        }
        ColumnType::HexList => {
            let arrow_schema::DataType::List(item) = array.data_type() else {
                return fail(format!(
                    "DataFusion column `{}` is not a cluster list: {}",
                    field.name(),
                    array.data_type()
                ));
            };
            let arrow_schema::DataType::Struct(item_fields) = item.data_type() else {
                return fail(format!(
                    "DataFusion column `{}` is not List<Struct{{geom}}>",
                    field.name()
                ));
            };
            let (geom_index, geom_field) = item_fields
                .iter()
                .enumerate()
                .find(|(_, item_field)| item_field.name() == "geom")
                .ok_or_else(|| {
                    format!(
                        "DataFusion column `{}` cluster struct has no geom field",
                        field.name()
                    )
                })?;
            let list = array.as_list::<i32>();
            let structs = list.values().as_struct();
            let encoded = ewkb_values(structs.column(geom_index).as_ref(), geom_field)?;
            let offsets = list.value_offsets();
            let mut rows = Vec::with_capacity(list.len());
            for row in 0..list.len() {
                if list.is_null(row) {
                    rows.push(TypedValue::Null);
                    continue;
                }
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let mut hexes = Vec::with_capacity(end - start);
                for (offset, element) in encoded[start..end].iter().enumerate() {
                    let bytes = element
                        .clone()
                        .ok_or_else(|| format!("cluster geom {} is NULL", start + offset))?;
                    hexes.push(hex::encode(bytes));
                }
                rows.push(TypedValue::HexList { values: hexes });
            }
            Ok(rows)
        }
        ColumnType::Bytea => match array.data_type() {
            arrow_schema::DataType::Binary => Ok(array
                .as_binary::<i32>()
                .iter()
                .map(|value| {
                    value.map_or(TypedValue::Null, |value| TypedValue::bytea(value.to_vec()))
                })
                .collect()),
            arrow_schema::DataType::LargeBinary => Ok(array
                .as_binary::<i64>()
                .iter()
                .map(|value| {
                    value.map_or(TypedValue::Null, |value| TypedValue::bytea(value.to_vec()))
                })
                .collect()),
            arrow_schema::DataType::BinaryView => Ok(array
                .as_binary_view()
                .iter()
                .map(|value| {
                    value.map_or(TypedValue::Null, |value| TypedValue::bytea(value.to_vec()))
                })
                .collect()),
            data_type => fail(format!(
                "DataFusion column `{}` is not binary: {data_type}",
                field.name()
            )),
        },
        ColumnType::Mvt => {
            let values = match array.data_type() {
                arrow_schema::DataType::Binary => array
                    .as_binary::<i32>()
                    .iter()
                    .map(|value| value.map(|bytes| bytes.to_vec()))
                    .collect::<Vec<_>>(),
                arrow_schema::DataType::LargeBinary => array
                    .as_binary::<i64>()
                    .iter()
                    .map(|value| value.map(|bytes| bytes.to_vec()))
                    .collect::<Vec<_>>(),
                arrow_schema::DataType::BinaryView => array
                    .as_binary_view()
                    .iter()
                    .map(|value| value.map(|bytes| bytes.to_vec()))
                    .collect::<Vec<_>>(),
                other => {
                    return fail(format!(
                        "DataFusion column `{}` is not binary: {other}",
                        field.name()
                    ));
                }
            };
            Ok(values
                .into_iter()
                .map(|value| match value {
                    Some(value) => TypedValue::mvt(value),
                    None => TypedValue::Null,
                })
                .collect())
        }
    }
}

fn ewkb_values(array: &dyn Array, field: &arrow_schema::Field) -> Result<Vec<Option<Vec<u8>>>> {
    if matches!(
        field.data_type(),
        arrow_schema::DataType::Binary
            | arrow_schema::DataType::LargeBinary
            | arrow_schema::DataType::BinaryView
    ) {
        return raw_binary_values(array, field);
    }

    let geometry_array = from_arrow_array(array, field)?;
    geometry_values(geometry_array.as_ref(), field)
}

fn raw_binary_values(
    array: &dyn Array,
    field: &arrow_schema::Field,
) -> Result<Vec<Option<Vec<u8>>>> {
    let values = match field.data_type() {
        arrow_schema::DataType::Binary => array
            .as_binary::<i32>()
            .iter()
            .map(|value| value.map(<[u8]>::to_vec))
            .collect::<Vec<_>>(),
        arrow_schema::DataType::LargeBinary => array
            .as_binary::<i64>()
            .iter()
            .map(|value| value.map(<[u8]>::to_vec))
            .collect::<Vec<_>>(),
        arrow_schema::DataType::BinaryView => array
            .as_binary_view()
            .iter()
            .map(|value| value.map(<[u8]>::to_vec))
            .collect::<Vec<_>>(),
        other => {
            return fail(format!(
                "DataFusion column `{}` is not binary EWKB: {other}",
                field.name()
            ));
        }
    };
    Ok(values)
}

fn geometry_values(
    array: &dyn GeoArrowArray,
    field: &arrow_schema::Field,
) -> Result<Vec<Option<Vec<u8>>>> {
    let srid = field_srid(field)?;
    let wkb = to_wkb::<i32>(array)?;
    let wkb = wkb.into_array_ref();
    let wkb = wkb
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "independent WKB encoder did not return BinaryArray".to_string())?;
    (0..wkb.len())
        .map(|index| {
            if wkb.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(inject_ewkb_srid(wkb.value(index).to_vec(), srid)?))
            }
        })
        .collect::<Result<Vec<_>>>()
}

fn field_srid(field: &arrow_schema::Field) -> Result<Option<i32>> {
    let geo_type = GeoArrowType::from_arrow_field(field)?;
    let Some(crs_value) = geo_type.metadata().crs().crs_value() else {
        return Ok(None);
    };
    let crs_value = crs_value
        .as_str()
        .ok_or_else(|| format!("unsupported non-string CRS metadata: {crs_value}"))?;
    let srid = crs_value
        .strip_prefix("EPSG:")
        .ok_or_else(|| format!("unsupported CRS authority code `{crs_value}`"))?
        .parse::<i32>()?;
    if srid == 0 {
        Ok(None)
    } else if srid < 0 {
        fail(format!(
            "unsupported negative CRS authority code `{crs_value}`"
        ))
    } else {
        Ok(Some(srid))
    }
}

fn inject_ewkb_srid(wkb: Vec<u8>, srid: Option<i32>) -> Result<Vec<u8>> {
    let Some(srid) = srid else {
        return Ok(wkb);
    };
    if wkb.len() < 5 {
        return fail("cannot attach SRID to a truncated WKB header");
    }
    let little_endian = match wkb[0] {
        0 => false,
        1 => true,
        value => return fail(format!("invalid WKB byte order {value}")),
    };
    let raw_type = if little_endian {
        u32::from_le_bytes(wkb[1..5].try_into()?)
    } else {
        u32::from_be_bytes(wkb[1..5].try_into()?)
    };
    if raw_type & 0x2000_0000 != 0 {
        return fail("independent WKB encoder unexpectedly returned EWKB with SRID");
    }

    let raw_type = if raw_type & (0x8000_0000 | 0x4000_0000) != 0 {
        raw_type | 0x2000_0000
    } else {
        let (base_type, dimension_flags) = match raw_type / 1000 {
            0 => (raw_type, 0),
            1 => (raw_type - 1000, 0x8000_0000),
            2 => (raw_type - 2000, 0x4000_0000),
            3 => (raw_type - 3000, 0xC000_0000),
            offset => {
                return fail(format!(
                    "unsupported ISO WKB dimension offset {offset} in type {raw_type}"
                ));
            }
        };
        base_type | dimension_flags | 0x2000_0000
    };
    let mut output = Vec::with_capacity(wkb.len() + 4);
    output.push(wkb[0]);
    let type_bytes = if little_endian {
        raw_type.to_le_bytes()
    } else {
        raw_type.to_be_bytes()
    };
    output.extend_from_slice(&type_bytes);
    let srid_bytes = if little_endian {
        srid.to_le_bytes()
    } else {
        srid.to_be_bytes()
    };
    output.extend_from_slice(&srid_bytes);
    output.extend_from_slice(&wkb[5..]);
    Ok(output)
}

fn validate_argument_nullability(case: &OracleCase) -> Result<()> {
    for (index, argument) in case.arguments.iter().enumerate() {
        if !argument.nullable() && argument.is_null() {
            return fail(format!(
                "case `{}` argument {} is NULL but declared nullable=false",
                case.id,
                index + 1
            ));
        }
    }
    Ok(())
}

fn arrow_input_batch(case: &OracleCase) -> Result<RecordBatch> {
    validate_argument_nullability(case)?;

    let mut fields = Vec::with_capacity(case.arguments.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(case.arguments.len());
    for (index, argument) in case.arguments.iter().enumerate() {
        let nullable = argument.nullable();
        let name = format!("arg{}", index + 1);
        let (field, array): (FieldRef, ArrayRef) = match argument {
            OracleArgument::Text {
                value,
                storage: None | Some(TextStorage::Text),
                ..
            } => (
                Arc::new(Field::new(name, DataType::Utf8, nullable)),
                Arc::new(StringArray::from(vec![value.clone()])),
            ),
            OracleArgument::Text {
                value,
                storage: Some(TextStorage::Wkb),
                ..
            } => {
                let bytes = value.as_deref().map(wkb_from_wkt).transpose()?;
                (
                    Arc::new(
                        Field::new(name, DataType::Binary, nullable)
                            .with_extension_type(WkbType::new(Arc::new(Metadata::default()))),
                    ),
                    Arc::new(BinaryArray::from(vec![bytes.as_deref()])),
                )
            }
            OracleArgument::Float64 { value, .. } => (
                Arc::new(Field::new(name, DataType::Float64, nullable)),
                Arc::new(Float64Array::from(vec![*value])),
            ),
            OracleArgument::Int64 { value, .. } => (
                Arc::new(Field::new(name, DataType::Int64, nullable)),
                Arc::new(Int64Array::from(vec![*value])),
            ),
            OracleArgument::Int32 { value, .. } => (
                Arc::new(Field::new(name, DataType::Int32, nullable)),
                Arc::new(Int32Array::from(vec![*value])),
            ),
            OracleArgument::Boolean { value, .. } => (
                Arc::new(Field::new(name, DataType::Boolean, nullable)),
                Arc::new(BooleanArray::from(vec![*value])),
            ),
        };
        fields.push(field);
        arrays.push(array);
    }

    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

/// Encode WKT as standard little-endian WKB with the independent `wkb` writer.
///
/// The input is a reviewed geometry value, not an expected output, so this
/// does not derive any comparison baseline from the implementation under
/// test. It only gives the DataFusion query a typed WKB field.
fn wkb_from_wkt(value: &str) -> Result<Vec<u8>> {
    let geometry = value
        .parse::<wkt::Wkt<f64>>()
        .map_err(|error| format!("invalid WKT `{value}` for WKB storage: {error}"))?;
    let mut bytes = Vec::new();
    wkb::writer::write_geometry(&mut bytes, &geometry, &wkb::writer::WriteOptions::default())?;
    Ok(bytes)
}

async fn execute_postgis(
    client: &Client,
    group: &OracleGroup,
    case: &OracleCase,
) -> Result<TypedRowSet> {
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(case.arguments.len());
    for argument in &case.arguments {
        match argument {
            OracleArgument::Text { value, .. } => params.push(value),
            OracleArgument::Float64 { value, .. } => params.push(value),
            OracleArgument::Int64 { value, .. } => params.push(value),
            OracleArgument::Int32 { value, .. } => params.push(value),
            OracleArgument::Boolean { value, .. } => params.push(value),
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
                ColumnType::Text => match row.try_get::<_, Option<String>>(column_index)? {
                    Some(value) => TypedValue::Text { value },
                    None => TypedValue::Null,
                },
                ColumnType::Int32 => match row.try_get::<_, Option<i32>>(column_index)? {
                    Some(value) => TypedValue::Int32 { value },
                    None => TypedValue::Null,
                },
                ColumnType::Mvt => match row.try_get::<_, Option<Vec<u8>>>(column_index)? {
                    Some(value) => TypedValue::mvt(value),
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
                ColumnType::HexList => match row.try_get::<_, Option<Vec<String>>>(column_index)? {
                    Some(values) => TypedValue::HexList { values },
                    None => TypedValue::Null,
                },
                ColumnType::Bytea => match row.try_get::<_, Option<Vec<u8>>>(column_index)? {
                    Some(value) => TypedValue::bytea(value),
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

    let expected_rows;
    let actual_rows;
    let effective_comparison = match &case.comparison {
        Comparison::ClusterIdsEquivalent { canonical_columns } => {
            expected_rows =
                canonicalize_cluster_ids(&expected.rows, &case.result_columns, canonical_columns)?;
            actual_rows =
                canonicalize_cluster_ids(&actual.rows, &case.result_columns, canonical_columns)?;
            Comparison::Exact
        }
        Comparison::HexSetExact => {
            expected_rows = sort_hex_lists(&expected.rows)?;
            actual_rows = sort_hex_lists(&actual.rows)?;
            Comparison::Exact
        }
        other => {
            expected_rows = expected.rows.clone();
            actual_rows = actual.rows.clone();
            other.clone()
        }
    };

    if expected_rows.len() != actual_rows.len() {
        return fail(format!(
            "case `{}` row count mismatch under {}\nexpected:\n{}\nactual:\n{}",
            case.id,
            effective_comparison.name(),
            serde_json::to_string_pretty(expected)?,
            serde_json::to_string_pretty(actual)?
        ));
    }

    for (row_index, (expected_row, actual_row)) in
        expected_rows.iter().zip(&actual_rows).enumerate()
    {
        for (column_index, (expected_value, actual_value)) in
            expected_row.iter().zip(actual_row).enumerate()
        {
            if !values_match(expected_value, actual_value, &effective_comparison)? {
                return fail(format!(
                    "case `{}` column `{}` row {} differs under {}\nexpected: {}\nactual:   {}",
                    case.id,
                    case.result_columns[column_index].name,
                    row_index,
                    effective_comparison.name(),
                    value_display(expected_value)?,
                    value_display(actual_value)?
                ));
            }
        }
    }

    Ok(())
}

/// Rename the declared cluster label columns to their first-appearance order.
///
/// Only `canonical_columns` are rewritten; every other column, including an
/// input `id` of the same Arrow type, keeps its exact value and row position.
/// NULL stays NULL. Negative labels are sentinels such as
/// `ST_ClusterKMeans`'s -1 for NULL/EMPTY inputs, so they are never mapped
/// into the ordinary non-negative label space.
fn canonicalize_cluster_ids(
    rows: &[Vec<TypedValue>],
    columns: &[ResultColumn],
    canonical_columns: &[String],
) -> Result<Vec<Vec<TypedValue>>> {
    let mut label_indices = Vec::with_capacity(canonical_columns.len());
    for name in canonical_columns {
        let index = columns
            .iter()
            .position(|column| &column.name == name)
            .ok_or_else(|| format!("cluster label column `{name}` does not exist"))?;
        if columns[index].column_type != ColumnType::Int32 {
            return fail(format!(
                "cluster label column `{name}` must be int32, found {:?}",
                columns[index].column_type
            ));
        }
        if label_indices.contains(&index) {
            return fail(format!("cluster label column `{name}` is duplicated"));
        }
        label_indices.push(index);
    }

    let mut result = rows.to_vec();
    for column_index in label_indices {
        let mut mapping = BTreeMap::new();
        let mut next = 0i32;
        for row in &mut result {
            let TypedValue::Int32 { value } = &row[column_index] else {
                continue;
            };
            if *value < 0 {
                continue;
            }
            let mapped = *mapping.entry(*value).or_insert_with(|| {
                let mapped = next;
                next += 1;
                mapped
            });
            row[column_index] = TypedValue::Int32 { value: mapped };
        }
    }
    Ok(result)
}

fn sort_hex_lists(rows: &[Vec<TypedValue>]) -> Result<Vec<Vec<TypedValue>>> {
    let mut result = rows.to_vec();
    for row in &mut result {
        for value in row {
            if let TypedValue::HexList { values } = value {
                values.sort();
            }
        }
    }
    Ok(result)
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
        Comparison::GeometryNormalized => match (expected, actual) {
            (TypedValue::Null, TypedValue::Null) => Ok(true),
            (TypedValue::Ewkb { hex: expected }, TypedValue::Ewkb { hex: actual }) => Ok(
                normalize_geometry_for_comparison(&decode_ewkb_hex(expected)?)
                    == normalize_geometry_for_comparison(&decode_ewkb_hex(actual)?),
            ),
            _ => Ok(expected == actual),
        },
        Comparison::GeometryCoordinateUlp { max_ulps } => match (expected, actual) {
            (TypedValue::Null, TypedValue::Null) => Ok(true),
            (TypedValue::Ewkb { hex: expected }, TypedValue::Ewkb { hex: actual }) => {
                Ok(geometry_coordinates_within_ulp(
                    &decode_ewkb_hex(expected)?,
                    &decode_ewkb_hex(actual)?,
                    *max_ulps,
                ))
            }
            _ => Ok(expected == actual),
        },
        Comparison::BinaryExact => match (expected, actual) {
            (TypedValue::Null, TypedValue::Null) => Ok(true),
            (TypedValue::Bytea { hex: expected }, TypedValue::Bytea { hex: actual }) => {
                Ok(hex::decode(expected)? == hex::decode(actual)?)
            }
            _ => Ok(expected == actual),
        },
        Comparison::GeobufSemantic => match (expected, actual) {
            (TypedValue::Null, TypedValue::Null) => Ok(true),
            (TypedValue::Bytea { hex: expected }, TypedValue::Bytea { hex: actual }) => {
                let mut expected = decode_geobuf_hex(expected)?;
                let mut actual = decode_geobuf_hex(actual)?;
                sort_geobuf_features(&mut expected);
                sort_geobuf_features(&mut actual);
                Ok(expected == actual)
            }
            _ => Ok(expected == actual),
        },
        Comparison::MvtSemantic => match (expected, actual) {
            (TypedValue::Null, TypedValue::Null) => Ok(true),
            (TypedValue::Mvt { hex: expected }, TypedValue::Mvt { hex: actual }) => {
                Ok(decode_hex(expected)? == decode_hex(actual)?)
            }
            _ => Ok(expected == actual),
        },
        Comparison::ClusterIdsEquivalent { .. } | Comparison::HexSetExact => Ok(expected == actual),
    }
}

fn decode_geobuf_hex(hex: &str) -> Result<serde_json::Value> {
    let bytes = hex::decode(hex)?;
    geobuf_codec::decode(&bytes).map_err(|error| {
        Box::new(OracleError(format!("invalid Geobuf bytes: {error}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

fn sort_geobuf_features(value: &mut serde_json::Value) {
    if let Some(features) = value
        .get_mut("features")
        .and_then(serde_json::Value::as_array_mut)
    {
        features.sort_by_key(|feature| serde_json::to_string(feature).unwrap_or_default());
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

/// Compare decoded geometries with strict structure and coordinate ULP.
///
/// Geometry type, nesting, dimensions, SRID, and non-coordinate values must
/// match exactly. Only coordinate components may differ within `max_ulps`.
fn geometry_coordinates_within_ulp(
    expected: &GeometryValue,
    actual: &GeometryValue,
    max_ulps: u64,
) -> bool {
    expected.dimensions == actual.dimensions
        && expected.srid == actual.srid
        && geometry_kind_within_coordinate_ulp(&expected.kind, &actual.kind, max_ulps)
}

fn geometry_kind_within_coordinate_ulp(
    expected: &GeometryKind,
    actual: &GeometryKind,
    max_ulps: u64,
) -> bool {
    match (expected, actual) {
        (GeometryKind::Point(expected), GeometryKind::Point(actual)) => match (expected, actual) {
            (None, None) => true,
            (Some(expected), Some(actual)) => coordinate_within_ulp(expected, actual, max_ulps),
            _ => false,
        },
        (GeometryKind::LineString(expected), GeometryKind::LineString(actual)) => {
            coordinates_within_ulp(expected, actual, max_ulps)
        }
        (GeometryKind::LineSegments(expected), GeometryKind::LineSegments(actual)) => {
            expected.len() == actual.len()
                && expected.iter().zip(actual).all(|(expected, actual)| {
                    coordinate_within_ulp(&expected.0, &actual.0, max_ulps)
                        && coordinate_within_ulp(&expected.1, &actual.1, max_ulps)
                })
        }
        (GeometryKind::Polygon(expected), GeometryKind::Polygon(actual)) => {
            rings_within_ulp(expected, actual, max_ulps)
        }
        (GeometryKind::MultiPoint(expected), GeometryKind::MultiPoint(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| match (expected, actual) {
                        (None, None) => true,
                        (Some(expected), Some(actual)) => {
                            coordinate_within_ulp(expected, actual, max_ulps)
                        }
                        _ => false,
                    })
        }
        (GeometryKind::MultiLineString(expected), GeometryKind::MultiLineString(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| coordinates_within_ulp(expected, actual, max_ulps))
        }
        (GeometryKind::MultiPolygon(expected), GeometryKind::MultiPolygon(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| rings_within_ulp(expected, actual, max_ulps))
        }
        (GeometryKind::GeometryCollection(expected), GeometryKind::GeometryCollection(actual)) => {
            expected.len() == actual.len()
                && expected.iter().zip(actual).all(|(expected, actual)| {
                    geometry_coordinates_within_ulp(expected, actual, max_ulps)
                })
        }
        _ => false,
    }
}

fn coordinates_within_ulp(expected: &[Vec<u64>], actual: &[Vec<u64>], max_ulps: u64) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| coordinate_within_ulp(expected, actual, max_ulps))
}

fn rings_within_ulp(expected: &[Vec<Vec<u64>>], actual: &[Vec<Vec<u64>>], max_ulps: u64) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| coordinates_within_ulp(expected, actual, max_ulps))
}

fn coordinate_within_ulp(expected: &[u64], actual: &[u64], max_ulps: u64) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| float_ulp_distance(*expected, *actual) <= max_ulps)
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
    LineSegments(Vec<(Vec<u64>, Vec<u64>)>),
    Polygon(Vec<Vec<Vec<u64>>>),
    MultiPoint(Vec<Option<Vec<u64>>>),
    MultiLineString(Vec<Vec<Vec<u64>>>),
    MultiPolygon(Vec<Vec<Vec<Vec<u64>>>>),
    GeometryCollection(Vec<GeometryValue>),
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
    decode_ewkb_geometry(&parsed, srid)
}

fn normalize_geometry_for_comparison(geometry: &GeometryValue) -> GeometryValue {
    GeometryValue {
        kind: match &geometry.kind {
            GeometryKind::LineString(coords) => {
                GeometryKind::LineSegments(normalize_line_segments(coords))
            }
            GeometryKind::MultiLineString(lines) => GeometryKind::LineSegments(
                lines
                    .iter()
                    .flat_map(|coords| normalize_line_segments(coords))
                    .collect(),
            ),
            GeometryKind::Polygon(rings) => {
                GeometryKind::Polygon(rings.iter().map(|ring| normalize_ring(ring)).collect())
            }
            GeometryKind::MultiPolygon(polygons) => GeometryKind::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.iter().map(|ring| normalize_ring(ring)).collect())
                    .collect(),
            ),
            GeometryKind::GeometryCollection(geometries) => GeometryKind::GeometryCollection(
                geometries
                    .iter()
                    .map(normalize_geometry_for_comparison)
                    .collect(),
            ),
            other => other.clone(),
        },
        dimensions: geometry.dimensions,
        srid: geometry.srid,
    }
}

fn normalize_line_segments(coords: &[Vec<u64>]) -> Vec<(Vec<u64>, Vec<u64>)> {
    let mut segments = coords
        .windows(2)
        .map(|pair| {
            if pair[0] <= pair[1] {
                (pair[0].clone(), pair[1].clone())
            } else {
                (pair[1].clone(), pair[0].clone())
            }
        })
        .collect::<Vec<_>>();
    segments.sort();
    segments
}

fn normalize_ring(ring: &[Vec<u64>]) -> Vec<Vec<u64>> {
    if ring.len() < 2 {
        return ring.to_vec();
    }
    let closed = ring.first() == ring.last();
    let open = if closed {
        &ring[..ring.len() - 1]
    } else {
        ring
    };
    if open.is_empty() {
        return ring.to_vec();
    }

    let reversed = open.iter().rev().cloned().collect::<Vec<_>>();
    let mut candidates = Vec::with_capacity(open.len() * 2);
    for source in [open, reversed.as_slice()] {
        for offset in 0..source.len() {
            let mut candidate = Vec::with_capacity(source.len());
            candidate.extend_from_slice(&source[offset..]);
            candidate.extend_from_slice(&source[..offset]);
            candidates.push(candidate);
        }
    }
    let mut best = candidates
        .into_iter()
        .min()
        .expect("ring candidate list is non-empty");
    if closed {
        best.push(best[0].clone());
    }
    best
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

fn decode_ewkb_geometry<G: GeometryTrait<T = f64>>(
    geometry: &G,
    srid: Option<i32>,
) -> Result<GeometryValue> {
    let dimensions = geometry.dim();
    let kind = match geometry.as_type() {
        GeometryType::Point(point) => {
            GeometryKind::Point(point.coord().map(|coord| coord_bits(&coord)))
        }
        GeometryType::LineString(line_string) => {
            GeometryKind::LineString(line_string_bits(line_string)?)
        }
        GeometryType::Polygon(polygon) => GeometryKind::Polygon(polygon_bits(polygon)?),
        GeometryType::MultiPoint(multi_point) => {
            let mut points = Vec::with_capacity(multi_point.num_points());
            for index in 0..multi_point.num_points() {
                let point = multi_point
                    .point(index)
                    .ok_or_else(|| "WKB multi-point index out of range".to_string())?;
                points.push(point.coord().map(|coord| coord_bits(&coord)));
            }
            GeometryKind::MultiPoint(points)
        }
        GeometryType::MultiLineString(multi_line_string) => {
            let mut line_strings = Vec::with_capacity(multi_line_string.num_line_strings());
            for index in 0..multi_line_string.num_line_strings() {
                let line_string = multi_line_string
                    .line_string(index)
                    .ok_or_else(|| "WKB multi-line-string index out of range".to_string())?;
                line_strings.push(line_string_bits(&line_string)?);
            }
            GeometryKind::MultiLineString(line_strings)
        }
        GeometryType::MultiPolygon(multi_polygon) => {
            let mut polygons = Vec::with_capacity(multi_polygon.num_polygons());
            for index in 0..multi_polygon.num_polygons() {
                let polygon = multi_polygon
                    .polygon(index)
                    .ok_or_else(|| "WKB multi-polygon index out of range".to_string())?;
                polygons.push(polygon_bits(&polygon)?);
            }
            GeometryKind::MultiPolygon(polygons)
        }
        GeometryType::GeometryCollection(collection) => {
            let mut geometries = Vec::with_capacity(collection.num_geometries());
            for index in 0..collection.num_geometries() {
                let child = collection
                    .geometry(index)
                    .ok_or_else(|| "WKB geometry-collection index out of range".to_string())?;
                geometries.push(decode_ewkb_geometry(&child, None)?);
            }
            GeometryKind::GeometryCollection(geometries)
        }
        _ => {
            return fail("geometry-exact comparison does not support this geometry type");
        }
    };

    Ok(GeometryValue {
        kind,
        dimensions,
        srid,
    })
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
                value: Some("POINT(0 0)".to_string()),
                nullable: None,
                storage: None,
            }],
            result_columns: vec![ResultColumn {
                name: "value".to_string(),
                column_type: ColumnType::Float64,
            }],
            comparison: Comparison::Exact,
            source: "https://postgis.net/docs/ST_Distance.html".to_string(),
            skip: None,
            runtime_error: None,
        }
    }

    #[test]
    fn duplicate_case_ids_are_rejected() {
        let inputs = OracleInputs {
            schema_version: SCHEMA_VERSION,
            required_features: Some(Vec::new()),
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
            required_features: Some(Vec::new()),
            cases: vec![skipped],
        };
        assert!(validate_inputs(&inputs).is_err());
    }

    #[test]
    fn missing_or_unknown_required_features_are_rejected() {
        let missing = OracleInputs {
            schema_version: SCHEMA_VERSION,
            required_features: None,
            cases: vec![case("stable-id")],
        };
        let error = validate_inputs(&missing).unwrap_err().to_string();
        assert!(error.contains("required_features"), "{error}");

        let unknown = OracleInputs {
            schema_version: SCHEMA_VERSION,
            required_features: Some(vec!["geos-9_99".to_string()]),
            cases: vec![case("stable-id")],
        };
        let error = validate_inputs(&unknown).unwrap_err().to_string();
        assert!(error.contains("not declared in Cargo.toml"), "{error}");
    }

    #[test]
    fn runtime_error_contract_requires_a_geos_feature() {
        let mut runtime_case = case("runtime-error");
        runtime_case.runtime_error = Some(RuntimeErrorExpectation {
            geos_before: "3.13".to_string(),
            contains: "requires GEOS >= 3.13".to_string(),
        });

        let without_geos = OracleInputs {
            schema_version: SCHEMA_VERSION,
            required_features: Some(Vec::new()),
            cases: vec![runtime_case.clone()],
        };
        let error = validate_inputs(&without_geos).unwrap_err().to_string();
        assert!(error.contains("supported GEOS feature"), "{error}");

        let with_geos = OracleInputs {
            schema_version: SCHEMA_VERSION,
            required_features: Some(vec!["geos-3_11".to_string()]),
            cases: vec![runtime_case],
        };
        validate_inputs(&with_geos).unwrap();
    }

    #[test]
    fn runtime_error_contract_rejects_invalid_combinations() {
        let mut runtime_case = case("runtime-error");
        runtime_case.runtime_error = Some(RuntimeErrorExpectation {
            geos_before: "3.13.0".to_string(),
            contains: "requires GEOS >= 3.13".to_string(),
        });
        let inputs = OracleInputs {
            schema_version: SCHEMA_VERSION,
            required_features: Some(vec!["geos-3_11".to_string()]),
            cases: vec![runtime_case],
        };
        let error = validate_inputs(&inputs).unwrap_err().to_string();
        assert!(error.contains("must be a major.minor version"), "{error}");

        let mut skipped = case("runtime-error");
        skipped.skip = Some(Skip {
            reason: "x".to_string(),
            issue: "y".to_string(),
            review_after: "z".to_string(),
        });
        skipped.runtime_error = Some(RuntimeErrorExpectation {
            geos_before: "3.13".to_string(),
            contains: "requires GEOS >= 3.13".to_string(),
        });
        let inputs = OracleInputs {
            schema_version: SCHEMA_VERSION,
            required_features: Some(vec!["geos-3_11".to_string()]),
            cases: vec![skipped],
        };
        let error = validate_inputs(&inputs).unwrap_err().to_string();
        assert!(error.contains("skip and runtime_error"), "{error}");
    }

    #[test]
    fn geos_runtime_version_parsing_uses_major_and_minor() {
        assert_eq!(
            GeosRuntimeVersion::parse("3.12.2-CAPI-1.18.2").unwrap(),
            GeosRuntimeVersion {
                major: 3,
                minor: 12
            }
        );
        assert_eq!(
            GeosRuntimeVersion::parse_threshold("3.13").unwrap(),
            GeosRuntimeVersion {
                major: 3,
                minor: 13
            }
        );
        assert!(GeosRuntimeVersion::parse_threshold("3").is_err());
        assert!(GeosRuntimeVersion::parse_threshold("3.13.0").is_err());
        assert!(GeosRuntimeVersion::parse("not-a-version").is_err());
        assert!(
            GeosRuntimeVersion {
                major: 3,
                minor: 12
            } < GeosRuntimeVersion {
                major: 3,
                minor: 13
            }
        );
    }

    #[cfg(feature = "geos-3_11")]
    #[tokio::test]
    async fn relate_boundary_runtime_contract_uses_linked_geos() {
        assert!(
            linked_geos_runtime().unwrap().is_some(),
            "the geos feature must link a GEOS runtime"
        );
        let ctx = SessionContext::new();
        geodatafusion::register(&ctx);
        let group = load_group("relationships/relate_boundary", true).unwrap();
        let stats = run_offline(&ctx, &group).await.unwrap();
        assert_eq!(stats.discovered, group.inputs.cases.len());
        assert_eq!(stats.executed, group.inputs.cases.len());
        assert_eq!(stats.compared, group.inputs.cases.len());
        assert_eq!(stats.skipped, 0);
    }

    fn collect_complete_fixtures(
        root: &Path,
        directory: &Path,
        fixtures: &mut Vec<String>,
    ) -> Result<()> {
        if GROUP_FILES
            .iter()
            .all(|name| directory.join(name).is_file())
        {
            fixtures.push(
                directory
                    .strip_prefix(root)?
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                collect_complete_fixtures(root, &entry.path(), fixtures)?;
            }
        }
        Ok(())
    }

    #[test]
    fn discovery_is_sorted_complete_and_unique() {
        let groups = discover_groups().unwrap();
        assert!(groups.windows(2).all(|pair| pair[0] < pair[1]));

        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/postgis");
        let mut complete = Vec::new();
        collect_complete_fixtures(&root, &root, &mut complete).unwrap();
        complete.sort();
        for fixture in complete {
            assert!(
                groups.contains(&fixture),
                "complete fixture `{fixture}` was not discovered"
            );
        }
    }

    #[test]
    fn empty_discovery_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        assert!(discover_groups_under(root.path()).is_err());
    }

    #[test]
    fn nullable_false_rejects_null_argument() {
        for argument in [
            OracleArgument::Text {
                value: None,
                nullable: Some(false),
                storage: None,
            },
            OracleArgument::Float64 {
                value: None,
                nullable: Some(false),
            },
            OracleArgument::Int64 {
                value: None,
                nullable: Some(false),
            },
            OracleArgument::Int32 {
                value: None,
                nullable: Some(false),
            },
            OracleArgument::Boolean {
                value: None,
                nullable: Some(false),
            },
        ] {
            let mut case = case("nullable-false-null");
            case.arguments = vec![argument];
            let inputs = OracleInputs {
                schema_version: SCHEMA_VERSION,
                required_features: Some(Vec::new()),
                cases: vec![case],
            };
            let error = validate_inputs(&inputs).unwrap_err().to_string();
            assert!(error.contains("nullable=false"), "{error}");
        }
    }

    #[test]
    fn arrow_inputs_preserve_declared_nullability() {
        let mut case = case("arrow-input-fields");
        case.arguments = vec![
            OracleArgument::Text {
                value: Some("POINT(0 0)".to_string()),
                nullable: Some(false),
                storage: None,
            },
            OracleArgument::Float64 {
                value: None,
                nullable: Some(true),
            },
            OracleArgument::Int64 {
                value: None,
                nullable: Some(true),
            },
            OracleArgument::Int32 {
                value: None,
                nullable: Some(true),
            },
            OracleArgument::Boolean {
                value: Some(false),
                nullable: Some(false),
            },
        ];
        let batch = arrow_input_batch(&case).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(3).data_type(), &DataType::Int32);
        assert_eq!(batch.schema().field(4).data_type(), &DataType::Boolean);
        assert!(!batch.schema().field(0).is_nullable());
        assert!(batch.schema().field(1).is_nullable());
        assert!(batch.schema().field(2).is_nullable());
        assert!(batch.schema().field(3).is_nullable());
        assert!(!batch.schema().field(4).is_nullable());
        assert!(!batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
        assert!(batch.column(2).is_null(0));
        assert!(batch.column(3).is_null(0));
        assert!(!batch.column(4).is_null(0));

        let default_argument = OracleArgument::Text {
            value: None,
            nullable: None,
            storage: None,
        };
        assert!(default_argument.nullable());
    }

    #[test]
    fn wkb_storage_builds_typed_standard_wkb_input() {
        let mut case = case("wkb-storage-empty-point");
        case.arguments = vec![OracleArgument::Text {
            value: Some("POINT EMPTY".to_string()),
            nullable: Some(false),
            storage: Some(TextStorage::Wkb),
        }];
        let batch = arrow_input_batch(&case).unwrap();
        let field = batch.schema().field(0).clone();
        assert_eq!(field.data_type(), &DataType::Binary);
        assert!(!field.is_nullable());
        assert_eq!(
            field.extension_type::<WkbType>().metadata().crs(),
            &geoarrow_schema::Crs::default()
        );
        let values = batch.column(0).as_binary::<i32>();
        assert_eq!(
            hex::encode(values.value(0)),
            "0101000000000000000000f87f000000000000f87f"
        );
    }

    #[test]
    fn wkb_storage_preserves_nested_geometry_collections() {
        let mut case = case("wkb-storage-nested-collection");
        case.arguments = vec![OracleArgument::Text {
            value: Some(
                "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)),POINT(3 4))".to_string(),
            ),
            nullable: Some(false),
            storage: Some(TextStorage::Wkb),
        }];
        let batch = arrow_input_batch(&case).unwrap();
        let values = batch.column(0).as_binary::<i32>();
        let decoded = decode_ewkb_hex(&hex::encode(values.value(0))).unwrap();
        let GeometryKind::GeometryCollection(children) = decoded.kind else {
            panic!("WKB storage did not preserve the geometry collection");
        };
        assert_eq!(children.len(), 2);
        assert!(matches!(
            children[0].kind,
            GeometryKind::GeometryCollection(_)
        ));
        assert!(matches!(children[1].kind, GeometryKind::Point(_)));
    }

    #[tokio::test]
    async fn wkb_storage_feeds_real_wkb_consumers() {
        let mut case = case("wkb-storage-consumer");
        case.arguments = vec![OracleArgument::Text {
            value: Some("POINT EMPTY".to_string()),
            nullable: Some(false),
            storage: Some(TextStorage::Wkb),
        }];
        let batch = arrow_input_batch(&case).unwrap();
        let ctx = SessionContext::new();
        geodatafusion::register(&ctx);
        ctx.register_batch(ORACLE_INPUT_TABLE, batch).unwrap();
        let batches = ctx
            .sql("SELECT ST_AsText(arg1) AS text FROM oracle_input")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0].column(0).as_string::<i32>().value(0),
            "POINT EMPTY"
        );
    }

    #[test]
    fn geometry_coordinate_ulp_tolerates_only_coordinate_rounding() {
        let base = decode_ewkb_hex(&hex::encode(wkb_from_wkt("POINT(1 2)").unwrap())).unwrap();
        let one_ulp = decode_ewkb_hex(&hex::encode(
            wkb_from_wkt("POINT(1.0000000000000002 2)").unwrap(),
        ))
        .unwrap();
        let two_ulp = decode_ewkb_hex(&hex::encode(
            wkb_from_wkt("POINT(1.0000000000000004 2)").unwrap(),
        ))
        .unwrap();

        assert!(!geometry_coordinates_within_ulp(&base, &one_ulp, 0));
        assert!(geometry_coordinates_within_ulp(&base, &one_ulp, 1));
        assert!(!geometry_coordinates_within_ulp(&base, &two_ulp, 1));
        assert!(geometry_coordinates_within_ulp(&base, &two_ulp, 2));
    }

    #[test]
    fn geometry_coordinate_ulp_keeps_structure_and_metadata_strict() {
        let base = decode_ewkb_hex(&hex::encode(wkb_from_wkt("POINT(1 2)").unwrap())).unwrap();
        let one_ulp = decode_ewkb_hex(&hex::encode(
            wkb_from_wkt("POINT(1.0000000000000002 2)").unwrap(),
        ))
        .unwrap();
        let line =
            decode_ewkb_hex(&hex::encode(wkb_from_wkt("LINESTRING(1 2,3 4)").unwrap())).unwrap();
        let point_z =
            decode_ewkb_hex(&hex::encode(wkb_from_wkt("POINT Z (1 2 3)").unwrap())).unwrap();
        let with_srid = decode_ewkb_hex(&hex::encode(
            inject_ewkb_srid(
                wkb_from_wkt("POINT(1.0000000000000002 2)").unwrap(),
                Some(4326),
            )
            .unwrap(),
        ))
        .unwrap();

        assert!(!geometry_coordinates_within_ulp(&base, &line, 2));
        assert!(!geometry_coordinates_within_ulp(&base, &point_z, 2));
        assert!(!geometry_coordinates_within_ulp(&base, &with_srid, 2));
        assert!(geometry_coordinates_within_ulp(&base, &one_ulp, 1));
    }

    #[test]
    fn geometry_coordinate_ulp_deserializes_case_locally() {
        let comparison: Comparison =
            serde_json::from_str(r#"{"mode": "geometry-coordinate-ulp", "max_ulps": 2}"#).unwrap();
        assert_eq!(
            comparison,
            Comparison::GeometryCoordinateUlp { max_ulps: 2 }
        );
        assert_eq!(comparison.name(), "geometry-coordinate-ulp(max_ulps=2)");
    }

    fn cluster_case(canonical_columns: &[&str]) -> OracleCase {
        let mut cluster_case = case("cluster-labels");
        cluster_case.result_columns = vec![
            ResultColumn {
                name: "id".to_string(),
                column_type: ColumnType::Int32,
            },
            ResultColumn {
                name: "cid".to_string(),
                column_type: ColumnType::Int32,
            },
        ];
        cluster_case.comparison = Comparison::ClusterIdsEquivalent {
            canonical_columns: canonical_columns
                .iter()
                .map(|column| column.to_string())
                .collect(),
        };
        cluster_case
    }

    fn cluster_rows(rows: &[(i32, i32)]) -> TypedRowSet {
        TypedRowSet {
            columns: vec![
                ResultColumn {
                    name: "id".to_string(),
                    column_type: ColumnType::Int32,
                },
                ResultColumn {
                    name: "cid".to_string(),
                    column_type: ColumnType::Int32,
                },
            ],
            rows: rows
                .iter()
                .map(|(id, cid)| {
                    vec![
                        TypedValue::Int32 { value: *id },
                        TypedValue::Int32 { value: *cid },
                    ]
                })
                .collect(),
        }
    }

    #[test]
    fn cluster_label_renumbering_is_equivalent() {
        let case = cluster_case(&["cid"]);
        compare_case(
            &case,
            &cluster_rows(&[(1, 0), (2, 0), (3, 1)]),
            &cluster_rows(&[(1, 5), (2, 5), (3, 7)]),
        )
        .unwrap();
    }

    #[test]
    fn cluster_id_changes_and_row_swaps_are_rejected() {
        let case = cluster_case(&["cid"]);
        let expected = cluster_rows(&[(1, 0), (2, 0), (3, 1)]);
        assert!(
            compare_case(
                &case,
                &expected,
                &cluster_rows(&[(10, 0), (20, 0), (30, 1)])
            )
            .is_err()
        );
        assert!(compare_case(&case, &expected, &cluster_rows(&[(2, 0), (1, 0), (3, 1)])).is_err());
    }

    #[test]
    fn cluster_negative_labels_stay_exact() {
        let case = cluster_case(&["cid"]);
        let expected = cluster_rows(&[(1, 0), (2, 0), (3, 1)]);
        assert!(compare_case(&case, &expected, &cluster_rows(&[(1, 0), (2, 0), (3, -1)])).is_err());

        let expected = cluster_rows(&[(1, -1), (2, 0), (3, 1)]);
        compare_case(&case, &expected, &cluster_rows(&[(1, -1), (2, 0), (3, 1)])).unwrap();
        assert!(compare_case(&case, &expected, &cluster_rows(&[(1, -2), (2, 0), (3, 1)])).is_err());
    }

    #[test]
    fn cluster_canonical_columns_are_validated() {
        let inputs = |canonical_columns: Vec<&str>, result_columns: Vec<ResultColumn>| {
            let mut cluster_case = case("cluster-labels");
            cluster_case.result_columns = result_columns;
            cluster_case.comparison = Comparison::ClusterIdsEquivalent {
                canonical_columns: canonical_columns.into_iter().map(str::to_string).collect(),
            };
            OracleInputs {
                schema_version: SCHEMA_VERSION,
                required_features: Some(Vec::new()),
                cases: vec![cluster_case],
            }
        };
        let columns = || {
            vec![
                ResultColumn {
                    name: "id".to_string(),
                    column_type: ColumnType::Int32,
                },
                ResultColumn {
                    name: "cid".to_string(),
                    column_type: ColumnType::Int32,
                },
            ]
        };

        let error = validate_inputs(&inputs(Vec::new(), columns()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("at least one canonical column"), "{error}");

        let error = validate_inputs(&inputs(vec!["missing"], columns()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("does not exist"), "{error}");

        let error = validate_inputs(&inputs(vec!["cid", "cid"], columns()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("duplicated"), "{error}");

        let mut wrong_type = columns();
        wrong_type[1].column_type = ColumnType::Float64;
        let error = validate_inputs(&inputs(vec!["cid"], wrong_type))
            .unwrap_err()
            .to_string();
        assert!(error.contains("must be int32"), "{error}");
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
            required_features: Some(Vec::new()),
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
            feature_skipped_groups: 0,
            feature_skipped_cases: 0,
        };
        assert!(stats.ensure_compared("empty result").is_ok());
    }

    #[test]
    fn srid_is_injected_into_independent_wkb() {
        let wkb = hex::decode("0101000000000000000000f03f0000000000000040").unwrap();
        let ewkb = inject_ewkb_srid(wkb, Some(4326)).unwrap();
        assert_eq!(
            decode_ewkb_hex(&hex::encode(ewkb)).unwrap().srid,
            Some(4326)
        );
    }

    #[test]
    fn srid_injection_normalizes_iso_z_and_zm_wkb() {
        let point_z =
            hex::decode("01e9030000000000000000f03f00000000000000400000000000000840").unwrap();
        let decoded =
            decode_ewkb_hex(&hex::encode(inject_ewkb_srid(point_z, Some(4326)).unwrap())).unwrap();
        assert_eq!(decoded.srid, Some(4326));
        assert_eq!(decoded.dimensions, geo_traits::Dimensions::Xyz);

        let point_zm = hex::decode(
            "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040",
        )
        .unwrap();
        let decoded = decode_ewkb_hex(&hex::encode(
            inject_ewkb_srid(point_zm, Some(4326)).unwrap(),
        ))
        .unwrap();
        assert_eq!(decoded.srid, Some(4326));
        assert_eq!(decoded.dimensions, geo_traits::Dimensions::Xyzm);
    }

    #[test]
    fn ewkb_decoder_supports_multi_and_collection() {
        let multi = decode_ewkb_hex(
            "0104000000020000000101000000000000000000f03f0000000000000040010100000000000000000008400000000000001040",
        )
        .unwrap();
        assert!(matches!(multi.kind, GeometryKind::MultiPoint(_)));

        let collection =
            decode_ewkb_hex("0107000000010000000101000000000000000000f03f0000000000000040")
                .unwrap();
        assert!(matches!(
            collection.kind,
            GeometryKind::GeometryCollection(_)
        ));
    }

    #[test]
    fn srid_injection_rejects_existing_srid() {
        let ewkb = hex::decode("0101000020e6100000000000000000f03f0000000000000040").unwrap();
        assert!(inject_ewkb_srid(ewkb, Some(4326)).is_err());
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
