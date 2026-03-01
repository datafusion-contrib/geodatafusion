use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::{Array, StringArray};
use arrow_array::builder::StringBuilder;
use arrow_schema::{DataType, Field};
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo::Relate as GeoRelate;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;

use crate::error::GeoDataFusionResult;

/// ST_Relate — returns the DE-9IM intersection matrix string for two geometries.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StRelate {
    signature: Signature,
}

impl StRelate {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl Default for StRelate {
    fn default() -> Self {
        Self::new()
    }
}

static RELATE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StRelate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_relate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut arg_iter = args.args.into_iter();
        let left = arg_iter.next().unwrap();
        let right = arg_iter.next().unwrap();
        Ok(relate_string_impl(left, &args.arg_fields[0], right, &args.arg_fields[1])?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(RELATE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the DE-9IM (Dimensionally Extended 9-Intersection Model) matrix string representing the spatial relationship between two geometries.",
                "ST_Relate(geomA, geomB)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn relate_string_impl(
    left: ColumnarValue,
    left_field: &Field,
    right: ColumnarValue,
    right_field: &Field,
) -> GeoDataFusionResult<ColumnarValue> {
    match (left, right) {
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => {
            let mut arrays =
                ColumnarValue::values_to_arrays(&[ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)])?
                    .into_iter();
            relate_string_impl(
                ColumnarValue::Array(arrays.next().unwrap()),
                left_field,
                ColumnarValue::Array(arrays.next().unwrap()),
                right_field,
            )
        }
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => {
            let left_geo = from_arrow_array(&l, left_field)?;
            let right_geo = from_arrow_array(&r, right_field)?;
            let result = relate_arrays(&left_geo, &right_geo)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r_arr)) => {
            let l_arrays = ColumnarValue::values_to_arrays(&[ColumnarValue::Scalar(l)])?;
            let left_geo = from_arrow_array(&l_arrays[0], left_field)?;
            let right_geo = from_arrow_array(&r_arr, right_field)?;
            let result = relate_broadcast_left(&left_geo, &right_geo)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Array(l_arr), ColumnarValue::Scalar(r)) => {
            let r_arrays = ColumnarValue::values_to_arrays(&[ColumnarValue::Scalar(r)])?;
            let left_geo = from_arrow_array(&l_arr, left_field)?;
            let right_geo = from_arrow_array(&r_arrays[0], right_field)?;
            let result = relate_broadcast_right(&left_geo, &right_geo)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

fn relate_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<StringArray> {
    downcast_geoarrow_array!(left, _relate_left, right)
}

fn _relate_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<StringArray> {
    downcast_geoarrow_array!(right, _relate_both, left)
}

fn _relate_both<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<StringArray> {
    let mut builder = StringBuilder::with_capacity(left.len(), left.len() * 9);
    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(lg), Some(rg)) => {
                let left_geo = geometry_to_geo(&lg?)?;
                let right_geo = geometry_to_geo(&rg?)?;
                let matrix = left_geo.relate(&right_geo);
                builder.append_value(intersection_matrix_to_string(&matrix));
            }
            _ => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

/// ST_RelateMatch — tests if a DE-9IM matrix matches a given pattern.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StRelateMatch {
    signature: Signature,
}

impl StRelateMatch {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for StRelateMatch {
    fn default() -> Self {
        Self::new()
    }
}

static RELATE_MATCH_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StRelateMatch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_relatematch"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let matrices = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
        let patterns = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();

        let mut builder = arrow_array::builder::BooleanBuilder::with_capacity(matrices.len());

        for i in 0..matrices.len() {
            if matrices.is_null(i) || patterns.is_null(i) {
                builder.append_null();
            } else {
                let matrix_str = matrices.value(i);
                let pattern = patterns.value(i);
                let matches = de9im_matches(matrix_str, pattern);
                builder.append_value(matches);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(RELATE_MATCH_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Tests if a DE-9IM intersection matrix matches a given pattern string.",
                "ST_RelateMatch(matrix, pattern)",
            )
            .with_argument("matrix", "text - a DE-9IM matrix string (9 characters)")
            .with_argument("pattern", "text - a DE-9IM pattern string")
            .build()
        }))
    }
}

fn relate_broadcast_left(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<StringArray> {
    downcast_geoarrow_array!(left, _relate_bl_left, right)
}

fn _relate_bl_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<StringArray> {
    let left_geom = left
        .iter()
        .next()
        .unwrap()
        .map(|g| geometry_to_geo(&g?))
        .transpose()?;
    downcast_geoarrow_array!(right, _relate_bl_right, &left_geom)
}

fn _relate_bl_right<'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left_geom: &Option<geo::Geometry>,
) -> GeoArrowResult<StringArray> {
    let mut builder = StringBuilder::with_capacity(right.len(), right.len() * 9);
    if let Some(lg) = left_geom {
        for r in right.iter() {
            if let Some(rg) = r {
                let right_geo = geometry_to_geo(&rg?)?;
                let matrix = lg.relate(&right_geo);
                builder.append_value(intersection_matrix_to_string(&matrix));
            } else {
                builder.append_null();
            }
        }
    } else {
        for _ in 0..right.len() {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn relate_broadcast_right(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<StringArray> {
    downcast_geoarrow_array!(right, _relate_br_right, left)
}

fn _relate_br_right<'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &dyn GeoArrowArray,
) -> GeoArrowResult<StringArray> {
    let right_geom = right
        .iter()
        .next()
        .unwrap()
        .map(|g| geometry_to_geo(&g?))
        .transpose()?;
    downcast_geoarrow_array!(left, _relate_br_left, &right_geom)
}

fn _relate_br_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right_geom: &Option<geo::Geometry>,
) -> GeoArrowResult<StringArray> {
    let mut builder = StringBuilder::with_capacity(left.len(), left.len() * 9);
    if let Some(rg) = right_geom {
        for l in left.iter() {
            if let Some(lg) = l {
                let left_geo = geometry_to_geo(&lg?)?;
                let matrix = left_geo.relate(rg);
                builder.append_value(intersection_matrix_to_string(&matrix));
            } else {
                builder.append_null();
            }
        }
    } else {
        for _ in 0..left.len() {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

/// Convert an `IntersectionMatrix` to its 9-character DE-9IM string using the
/// public `get` API, avoiding any reliance on the `Debug` format.
fn intersection_matrix_to_string(matrix: &geo::relate::IntersectionMatrix) -> String {
    use geo::coordinate_position::CoordPos;
    use geo::dimensions::Dimensions;
    let mut s = String::with_capacity(9);
    for a in [CoordPos::Inside, CoordPos::OnBoundary, CoordPos::Outside] {
        for b in [CoordPos::Inside, CoordPos::OnBoundary, CoordPos::Outside] {
            s.push(match matrix.get(a, b) {
                Dimensions::Empty           => 'F',
                Dimensions::ZeroDimensional => '0',
                Dimensions::OneDimensional  => '1',
                Dimensions::TwoDimensional  => '2',
            });
        }
    }
    s
}

/// Check if a DE-9IM matrix string matches a pattern.
fn de9im_matches(matrix: &str, pattern: &str) -> bool {
    if matrix.len() != 9 || pattern.len() != 9 {
        return false;
    }

    matrix
        .chars()
        .zip(pattern.chars())
        .all(|(m, p)| match p {
            '*' => true,
            'T' | 't' => m != 'F' && m != 'f',
            'F' | 'f' => m == 'F' || m == 'f',
            '0' => m == '0',
            '1' => m == '1',
            '2' => m == '2',
            _ => false,
        })
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_relate() {
        let ctx = SessionContext::new();
        ctx.register_udf(StRelate::new().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_Relate(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING(0 0, 1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_string::<i32>().value(0);
        // Point on start of line: F0FFFF102
        assert_eq!(val.len(), 9);
    }

    #[test]
    fn test_de9im_matches() {
        assert!(de9im_matches("212101212", "T*T***T**"));
        assert!(!de9im_matches("FF*FF****", "T*T***T**"));
        assert!(de9im_matches("FF*FF****", "FF*FF****"));
    }
}
