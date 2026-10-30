use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::Float64Builder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::{CoordTrait, PointTrait};
use geoarrow_array::array::{PointArray, from_arrow_array};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Distance;

impl Distance {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Distance {
    fn default() -> Self {
        Self::new()
    }
}

static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| Signature::any(2, Volatility::Immutable));
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Distance {
    fn name(&self) -> &str {
        "st_distance"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(distance_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, "For geometry types returns the minimum 2D Cartesian (planar) distance between two geometries, in projected units (spatial ref units).", "ST_Distance(geomA, geomB)")
                        .with_argument("geomA", "geometry")
                        .with_argument("geomB", "geometry")
                        .build()
                }))
    }
}

fn distance_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_arr = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_arr = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let result = match (left_arr.as_point_opt(), right_arr.as_point_opt()) {
        (Some(left), Some(right)) => euclidean_distance_points(left, right)?,
        // Preserve the full geo distance semantics for all other geometry combinations.
        _ => geoarrow_expr_geo::euclidean_distance(&left_arr, &right_arr)?,
    };
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn euclidean_distance_points(
    left: &PointArray,
    right: &PointArray,
) -> GeoArrowResult<arrow_array::Float64Array> {
    if left.len() != right.len() {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Arrays must have the same length".to_string(),
        ));
    }

    let mut builder = Float64Builder::with_capacity(left.len());
    for (left, right) in left.iter().zip(right.iter()) {
        match (left, right) {
            (Some(left), Some(right)) => {
                let left = left?.coord().ok_or_else(empty_point_error)?;
                let right = right?.coord().ok_or_else(empty_point_error)?;
                builder.append_value((left.x() - right.x()).hypot(left.y() - right.y()));
            }
            _ => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

fn empty_point_error() -> GeoArrowError {
    GeoArrowError::IncorrectGeometryType("geo crate does not support empty points.".to_string())
}

#[cfg(test)]
mod test {
    use approx::assert_relative_eq;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use arrow_array::{Array, Float64Array};
    use datafusion::prelude::SessionContext;
    use geoarrow_array::test;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;
    use crate::udf::native::io::GeomFromText;

    fn assert_results_eq(
        actual: GeoArrowResult<Float64Array>,
        expected: GeoArrowResult<Float64Array>,
    ) {
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(actual.len(), expected.len());
                for i in 0..actual.len() {
                    assert_eq!(actual.is_null(i), expected.is_null(i), "row {i}");
                    if actual.is_valid(i) {
                        assert_eq!(
                            actual.value(i).to_bits(),
                            expected.value(i).to_bits(),
                            "row {i}"
                        );
                    }
                }
            }
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            (actual, expected) => panic!("result mismatch: {actual:?} vs {expected:?}"),
        }
    }

    #[test]
    fn point_fast_path_matches_geoarrow_expr() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            for dim in [
                Dimension::XY,
                Dimension::XYZ,
                Dimension::XYM,
                Dimension::XYZM,
            ] {
                let left = test::point::array(coord_type, dim).slice(0, 3);
                let right = test::point::array(coord_type, dim).slice(0, 3);
                assert_results_eq(
                    euclidean_distance_points(&left, &right),
                    geoarrow_expr_geo::euclidean_distance(&left, &right),
                );
            }
        }
    }

    #[test]
    fn point_fast_path_preserves_empty_point_error() {
        let left = test::point::array(CoordType::Interleaved, Dimension::XY);
        let right = test::point::array(CoordType::Interleaved, Dimension::XY);
        assert_results_eq(
            euclidean_distance_points(&left, &right),
            geoarrow_expr_geo::euclidean_distance(&left, &right),
        );
    }

    #[tokio::test]
    async fn test_distance() {
        let ctx = SessionContext::new();

        ctx.register_udf(Distance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Distance(ST_GeomFromText('POINT(-72.1235 42.3521)'), ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        assert_relative_eq!(
            col.as_primitive::<Float64Type>().value(0),
            0.00150567726382282
        );
    }
}
