use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, BooleanArray, StringArray};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geos::Geom;

use super::adapter::{binary_geos_values, relate_boundary_node_rule};
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::merge_metadata;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ContainsProperly {
    signature: Signature,
}

impl ContainsProperly {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(2, any_geometry_type(), Volatility::Immutable),
        }
    }
}

impl Default for ContainsProperly {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ContainsProperly {
    fn name(&self) -> &str {
        "st_containsproperly"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(contains_properly_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        static DOCUMENTATION: std::sync::OnceLock<Documentation> = std::sync::OnceLock::new();
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true when every point of geometry B lies in the interior of geometry A. Empty inputs return false.",
                "ST_ContainsProperly(geomA, geomB)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn contains_properly_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    ensure_relate_metadata(&args, "ST_ContainsProperly")?;
    let values = binary_geos_values(&args, |_, _, _, left, _, right| {
        if left.is_empty()? || right.is_empty()? {
            Ok(Some(false))
        } else {
            Ok(Some(left.relate_pattern(right, "T**FF*FF*")?))
        }
    })?;
    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(values))))
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Relate {
    signature: Signature,
}

impl Relate {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Relate {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for Relate {
    fn name(&self) -> &str {
        "st_relate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // All accepted types are read directly by invoke_with_args. The
        // validation still has to run because user-defined signatures do not
        // provide overload matching themselves.
        self.return_type(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [left, right] if is_geometry(left) && is_geometry(right) => Ok(DataType::Utf8),
            [left, right, third]
                if is_geometry(left)
                    && is_geometry(right)
                    && (is_text(third) || matches!(third, DataType::Null)) =>
            {
                Ok(DataType::Boolean)
            }
            [left, right, third]
                if is_geometry(left)
                    && is_geometry(right)
                    && third.is_integer() =>
            {
                Ok(DataType::Utf8)
            }
            _ => Err(DataFusionError::Plan(
                "ST_Relate expects (geometry, geometry), (geometry, geometry, text), or (geometry, geometry, integer)"
                    .to_string(),
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(relate_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        static DOCUMENTATION: std::sync::OnceLock<Documentation> = std::sync::OnceLock::new();
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the DE-9IM intersection matrix for two geometries, tests it against a pattern, or computes it with a specific GEOS boundary-node rule.",
                "ST_Relate(geomA, geomB[, matrixPattern | boundaryNodeRule])",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .with_argument("matrixOrRule", "optional nine-character pattern or boundary-node rule 1-4")
            .build()
        }))
    }
}

fn relate_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    ensure_relate_metadata(&args, "ST_Relate")?;
    match args.args.len() {
        2 => {
            let values = binary_geos_values(&args, |_, _, _, left, _, right| {
                Ok(Some(left.relate(right)?))
            })?;
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(
                values,
            ))))
        }
        3 if is_text(args.arg_fields[2].data_type()) || matches!(args.arg_fields[2].data_type(), DataType::Null) => {
            let values = binary_geos_values(&args, |index, arrays, _, left, _, right| {
                let Some(pattern) = string_value(&arrays[2], index)? else {
                    return Ok(None);
                };
                let pattern = pattern
                    .chars()
                    .take(9)
                    .collect::<String>()
                    .to_ascii_uppercase();
                Ok(Some(left.relate_pattern(right, &pattern)?))
            })?;
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(
                values,
            ))))
        }
        3 if args.arg_fields[2].data_type().is_integer() => {
            let values = binary_geos_values(&args, |index, arrays, left_wkb, _, right_wkb, _| {
                let Some(rule) = integer_value(&arrays[2], index)? else {
                    return Ok(None);
                };
                let rule = i32::try_from(rule).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "GEOSRelate: Invalid boundary node rule {rule}"
                    ))
                })?;
                if !(1..=4).contains(&rule) {
                    return Err(DataFusionError::Execution(format!(
                        "GEOSRelate: Invalid boundary node rule {rule}"
                    ))
                    .into());
                }
                Ok(Some(relate_boundary_node_rule(
                    left_wkb, right_wkb, rule,
                )?))
            })?;
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(
                values,
            ))))
        }
        _ => Err(DataFusionError::Plan(
            "ST_Relate expects (geometry, geometry), (geometry, geometry, text), or (geometry, geometry, integer)"
                .to_string(),
        )
        .into()),
    }
}

fn ensure_relate_metadata(args: &ScalarFunctionArgs, name: &str) -> GeoDataFusionResult<()> {
    if args
        .arg_fields
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Null))
        || args.args.iter().any(|argument| {
            matches!(
                argument,
                ColumnarValue::Scalar(value) if value.is_null()
            )
        })
    {
        return Ok(());
    }

    merge_metadata(
        [args.arg_fields[0].as_ref(), args.arg_fields[1].as_ref()],
        name,
    )?;
    Ok(())
}

fn is_geometry(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Null) || any_geometry_type().contains(data_type)
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn string_value(array: &dyn Array, index: usize) -> GeoDataFusionResult<Option<&str>> {
    if array.is_null(index) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(index),
        DataType::LargeUtf8 => array.as_string::<i64>().value(index),
        DataType::Utf8View => array.as_string_view().value(index),
        _ => {
            return Err(DataFusionError::Internal(
                "ST_Relate matrix pattern must be text".to_string(),
            )
            .into());
        }
    }))
}

fn integer_value(array: &dyn Array, index: usize) -> GeoDataFusionResult<Option<i64>> {
    if array.is_null(index) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Int8 => i64::from(array.as_primitive::<Int8Type>().value(index)),
        DataType::Int16 => i64::from(array.as_primitive::<Int16Type>().value(index)),
        DataType::Int32 => i64::from(array.as_primitive::<Int32Type>().value(index)),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(index),
        DataType::UInt8 => i64::from(array.as_primitive::<UInt8Type>().value(index)),
        DataType::UInt16 => i64::from(array.as_primitive::<UInt16Type>().value(index)),
        DataType::UInt32 => i64::from(array.as_primitive::<UInt32Type>().value(index)),
        DataType::UInt64 => i64::try_from(array.as_primitive::<UInt64Type>().value(index))
            .map_err(|_| {
                DataFusionError::Execution(
                    "GEOSRelate: boundary node rule is outside the integer range".to_string(),
                )
            })?,
        _ => {
            return Err(DataFusionError::Internal(
                "ST_Relate boundary node rule must be an integer".to_string(),
            )
            .into());
        }
    };
    Ok(Some(value))
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(ContainsProperly::default().into());
    session_context.register_udf(Relate::default().into());
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;

    fn geos_version_at_least(major: u32, minor: u32) -> bool {
        let version = geos::version().expect("failed to read the GEOS runtime version");
        let mut parts = version.split('.');
        let version_major: u32 = parts
            .next()
            .and_then(|part| part.parse().ok())
            .unwrap_or_else(|| panic!("unexpected GEOS version `{version}`"));
        let version_minor: u32 = parts
            .next()
            .and_then(|part| part.parse().ok())
            .unwrap_or_else(|| panic!("unexpected GEOS version `{version}`"));
        version_major > major || (version_major == major && version_minor >= minor)
    }

    fn context() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register(&ctx);
        ctx
    }

    #[tokio::test]
    async fn contains_properly_interior_boundary_empty_and_null() {
        let ctx = context();
        let batch = ctx
            .sql(
                "SELECT
                    ST_ContainsProperly(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), ST_GeomFromText('POINT(2 2)')) AS interior,
                    ST_ContainsProperly(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), ST_GeomFromText('POINT(0 2)')) AS boundary,
                    ST_ContainsProperly(ST_GeomFromText('POLYGON EMPTY'), ST_GeomFromText('POINT EMPTY')) AS empty,
                    ST_ContainsProperly(ST_GeomFromText(NULL), ST_GeomFromText('POINT(0 0)')) AS null_result",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let values = batch.column(0).as_boolean();
        assert!(values.value(0));
        assert!(!batch.column(1).as_boolean().value(0));
        assert!(!batch.column(2).as_boolean().value(0));
        assert!(batch.column(3).is_null(0));
    }

    #[tokio::test]
    async fn relate_overloads_and_boundary_rules() {
        let ctx = context();
        let batch = ctx
            .sql(
                "SELECT
                    ST_Relate(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)')) AS matrix,
                    ST_Relate(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), 'FF*FF****') AS matches,
                    ST_Relate(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(1 1,2 2))'), ST_GeomFromText('POINT(1 1)'), 1) AS bnr1,
                    ST_Relate(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(1 1,2 2))'), ST_GeomFromText('POINT(1 1)'), 2) AS bnr2,
                    ST_Relate(ST_GeomFromText(NULL), ST_GeomFromText('POINT(1 1)'), 0) AS strict_null",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "FF0FFF0F2");
        assert!(batch.column(1).as_boolean().value(0));
        assert_eq!(batch.column(2).as_string::<i32>().value(0), "0F1FF0FF2");
        assert_eq!(batch.column(3).as_string::<i32>().value(0), "FF10F0FF2");
        assert!(batch.column(4).is_null(0));

        // Rules 3 and 4 depend on endpoint valence counting. The legacy
        // relate engine in GEOS < 3.13 returns non-conforming matrices for
        // those rules, so the UDF rejects them instead of reporting a wrong
        // DE-9IM matrix. GEOS 3.13 introduced RelateNG and implements both
        // rules correctly. Rules 1 and 2 remain available on every runtime.
        if geos_version_at_least(3, 13) {
            let batch = ctx
                .sql(
                    "SELECT
                        ST_Relate(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(1 1,2 2))'), ST_GeomFromText('POINT(1 1)'), 3) AS bnr3,
                        ST_Relate(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(1 1,2 2),(1 1,2 0))'), ST_GeomFromText('POINT(1 1)'), 3) AS junction3,
                        ST_Relate(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(1 1,2 2),(1 1,2 0))'), ST_GeomFromText('POINT(1 1)'), 4) AS junction4",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            assert_eq!(batch.column(0).as_string::<i32>().value(0), "FF10FFFF2");
            assert_eq!(batch.column(1).as_string::<i32>().value(0), "FF10FFFF2");
            assert_eq!(batch.column(2).as_string::<i32>().value(0), "0F1FF0FF2");
        } else {
            for rule in [3, 4] {
                let error = ctx
                    .sql(&format!(
                        "SELECT ST_Relate(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(1 1,2 2),(1 1,2 0))'), ST_GeomFromText('POINT(1 1)'), {rule})"
                    ))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains(&format!("boundary node rule {rule} requires GEOS >= 3.13")),
                    "{error}"
                );
            }
        }
    }

    #[tokio::test]
    async fn relate_invalid_pattern_and_rule_fail() {
        let ctx = context();
        let invalid_pattern = ctx
            .sql("SELECT ST_Relate(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), 'FF*FF***')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(invalid_pattern.contains("length 9"), "{invalid_pattern}");

        let invalid_rule = ctx
            .sql(
                "SELECT ST_Relate(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), 5)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            invalid_rule.contains("Invalid boundary node rule 5"),
            "{invalid_rule}"
        );
    }
}
