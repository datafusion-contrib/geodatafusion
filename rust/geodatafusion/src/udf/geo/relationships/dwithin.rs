use std::any::Any;
use std::sync::OnceLock;

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geo::EuclideanDistance;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;

use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DWithin {
    signature: Signature,
}

impl DWithin {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl Default for DWithin {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for DWithin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_dwithin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut arg_iter = args.args.into_iter();
        let left = arg_iter.next().unwrap();
        let right = arg_iter.next().unwrap();
        let distance_arg = arg_iter.next().unwrap();

        let distance = match distance_arg.cast_to(&DataType::Float64, None)? {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(d))) => d,
            _ => {
                return Err(DataFusionError::NotImplemented(
                    "Vectorized distance not supported for ST_DWithin".to_string(),
                ));
            }
        };

        Ok(dwithin_impl(
            left,
            &args.arg_fields[0],
            right,
            &args.arg_fields[1],
            distance,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if the geometries are within a given distance of one another. The distance is specified in units of the spatial reference system.",
                "ST_DWithin(geomA, geomB, distance)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .with_argument("distance", "float8 - distance threshold")
            .build()
        }))
    }
}

fn dwithin_impl(
    left: ColumnarValue,
    left_field: &Field,
    right: ColumnarValue,
    right_field: &Field,
    distance: f64,
) -> GeoDataFusionResult<ColumnarValue> {
    match (left, right) {
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => {
            let mut arrays =
                ColumnarValue::values_to_arrays(&[ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)])?
                    .into_iter();
            dwithin_impl(
                ColumnarValue::Array(arrays.next().unwrap()),
                left_field,
                ColumnarValue::Array(arrays.next().unwrap()),
                right_field,
                distance,
            )
        }
        (ColumnarValue::Array(l_arr), ColumnarValue::Array(r_arr)) => {
            let left_geo = from_arrow_array(&l_arr, left_field)?;
            let right_geo = from_arrow_array(&r_arr, right_field)?;
            let result = dwithin_arrays(&left_geo, &right_geo, distance)?;
            Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r_arr)) => {
            let l_arrays = ColumnarValue::values_to_arrays(&[ColumnarValue::Scalar(l)])?;
            let left_geo = from_arrow_array(&l_arrays[0], left_field)?;
            let right_geo = from_arrow_array(&r_arr, right_field)?;
            let result = dwithin_broadcast_left(&left_geo, &right_geo, distance)?;
            Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
        }
        (ColumnarValue::Array(l_arr), ColumnarValue::Scalar(r)) => {
            let r_arrays = ColumnarValue::values_to_arrays(&[ColumnarValue::Scalar(r)])?;
            let left_geo = from_arrow_array(&l_arr, left_field)?;
            let right_geo = from_arrow_array(&r_arrays[0], right_field)?;
            let result = dwithin_broadcast_right(&left_geo, &right_geo, distance)?;
            Ok(ColumnarValue::Array(std::sync::Arc::new(result)))
        }
    }
}

fn dwithin_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(left, _dwithin_left, right, distance)
}

fn _dwithin_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(right, _dwithin_both, left, distance)
}

fn _dwithin_both<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(left.len());
    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(lg), Some(rg)) => {
                let left_geo = geometry_to_geo(&lg?)?;
                let right_geo = geometry_to_geo(&rg?)?;
                builder.append_value(left_geo.euclidean_distance(&right_geo) <= distance);
            }
            _ => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

fn dwithin_broadcast_left(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(left, _dwithin_bl_left, right, distance)
}

fn _dwithin_bl_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    let left_geom = left
        .iter()
        .next()
        .unwrap()
        .map(|g| geometry_to_geo(&g?))
        .transpose()?;

    downcast_geoarrow_array!(right, _dwithin_bl_right, &left_geom, distance)
}

fn _dwithin_bl_right<'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left_geom: &Option<geo::Geometry>,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(right.len());
    if let Some(lg) = left_geom {
        for r in right.iter() {
            if let Some(rg) = r {
                let right_geo = geometry_to_geo(&rg?)?;
                builder.append_value(lg.euclidean_distance(&right_geo) <= distance);
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

fn dwithin_broadcast_right(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(right, _dwithin_br_right, left, distance)
}

fn _dwithin_br_right<'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &dyn GeoArrowArray,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    let right_geom = right
        .iter()
        .next()
        .unwrap()
        .map(|g| geometry_to_geo(&g?))
        .transpose()?;

    downcast_geoarrow_array!(left, _dwithin_br_left, &right_geom, distance)
}

fn _dwithin_br_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right_geom: &Option<geo::Geometry>,
    distance: f64,
) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(left.len());
    if let Some(rg) = right_geom {
        for l in left.iter() {
            if let Some(lg) = l {
                let left_geo = geometry_to_geo(&lg?)?;
                builder.append_value(left_geo.euclidean_distance(rg) <= distance);
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

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_dwithin_true() {
        let ctx = SessionContext::new();
        ctx.register_udf(DWithin::new().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 0)'), 1.5);")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(batch.column(0).as_boolean().value(0));
    }

    #[tokio::test]
    async fn test_dwithin_false() {
        let ctx = SessionContext::new();
        ctx.register_udf(DWithin::new().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(10 0)'), 1.5);")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(!batch.column(0).as_boolean().value(0));
    }
}
