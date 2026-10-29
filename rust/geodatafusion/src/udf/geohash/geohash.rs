use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_array::builder::StringViewBuilder;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use super::geohash_codec::{MAX_GEOHASH_CHARS, cell_prefix_chars, encode};
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::{GeometryBounds, geometry_bounds};
use crate::udf::native::geometry::{expand_rect_to_geometry, geometry_array_from_arrow};
use crate::udf::native::io::format::{is_plain_wkt_field, plain_wkt_array};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeoHash;

impl GeoHash {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for GeoHash {
    fn default() -> Self {
        Self::new()
    }
}

static GEOHASH_DOC: OnceLock<Documentation> = OnceLock::new();
static GEOHASH_SIGNATURE: OnceLock<Signature> = OnceLock::new();

fn signature() -> &'static Signature {
    GEOHASH_SIGNATURE.get_or_init(|| Signature::user_defined(Volatility::Immutable))
}

impl ScalarUDFImpl for GeoHash {
    fn name(&self) -> &str {
        "st_geohash"
    }

    fn signature(&self) -> &Signature {
        signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return Err(DataFusionError::Plan(
                "ST_GeoHash expects one geometry and an optional maxchars argument".to_string(),
            ));
        }

        let geometry_types = any_geometry_type();
        let mut coerced = Vec::with_capacity(arg_types.len());
        for (index, data_type) in arg_types.iter().enumerate() {
            if index == 0 {
                if data_type == &DataType::Null || geometry_types.contains(data_type) {
                    coerced.push(data_type.clone());
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "ST_GeoHash expects a geometry argument, got {data_type}"
                    )));
                }
            } else if data_type == &DataType::Null || data_type.is_integer() {
                coerced.push(DataType::Int64);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "ST_GeoHash expects an integer maxchars argument, got {data_type}"
                )));
            }
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_GeoHash output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(args.arg_fields[0].name().clone(), DataType::Utf8View, true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOHASH_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a GeoHash representation of a geometry. Non-point geometries use the center of their bounding box and the bounding-box precision. maxchars defaults to the PostGIS computed precision; an explicit value selects that many characters. Non-finite coordinates return an explicit error, and explicit maxchars above 1,000,000 return an error instead of attempting an unbounded allocation.",
                "ST_GeoHash(geometry[, maxchars])",
            )
            .with_argument("geom", "geometry")
            .with_argument("maxchars", "optional GeoHash length")
            .build()
        }))
    }
}

fn geohash_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let length = args.number_rows;

    if arrays
        .iter()
        .any(|array| array.data_type() == &DataType::Null)
    {
        let mut builder = StringViewBuilder::with_capacity(length);
        for _ in 0..length {
            builder.append_null();
        }
        return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
    }

    let input: Arc<dyn GeoArrowArray> = if is_plain_wkt_field(&args.arg_fields[0]) {
        plain_wkt_array(&arrays[0])?
    } else {
        geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?
    };
    let expanded = expand_rect_to_geometry(input.as_ref())?;
    let geometry_array: &dyn GeoArrowArray = match expanded.as_ref() {
        Some(value) => value,
        None => input.as_ref(),
    };
    let wkb = to_wkb::<i32>(geometry_array)?;
    let mut builder = StringViewBuilder::with_capacity(length);

    for row in 0..length {
        if arrays.iter().any(|array| array.is_null(row)) {
            builder.append_null();
            continue;
        }

        let precision = arrays
            .get(1)
            .and_then(|array| int_value(array, row))
            .unwrap_or(0);
        let geometry = wkb
            .value(row)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;

        match geometry_bounds(&geometry) {
            GeometryBounds::Empty => builder.append_null(),
            GeometryBounds::NonFinite => {
                return Err(DataFusionError::Execution(
                    "ST_GeoHash requires finite coordinates".to_string(),
                )
                .into());
            }
            GeometryBounds::Finite(bounds) => {
                let (min_x, min_y, max_x, max_y) =
                    (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy());
                if min_x < -180.0 || min_y < -90.0 || max_x > 180.0 || max_y > 90.0 {
                    return Err(DataFusionError::Execution(format!(
                        "Geohash requires inputs in decimal degrees, got \
                         ({min_x} {min_y}, {max_x} {max_y})."
                    ))
                    .into());
                }

                let precision = if precision > 0 {
                    usize::try_from(precision).unwrap_or(usize::MAX)
                } else {
                    cell_prefix_chars(min_x, min_y, max_x, max_y)
                };
                if precision > MAX_GEOHASH_CHARS {
                    return Err(DataFusionError::Execution(format!(
                        "ST_GeoHash maxchars {precision} exceeds the supported range"
                    ))
                    .into());
                }

                let longitude = min_x + (max_x - min_x) / 2.0;
                let latitude = min_y + (max_y - min_y) / 2.0;
                builder.append_value(encode(longitude, latitude, precision));
            }
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn int_value(array: &arrow_array::ArrayRef, row: usize) -> Option<i64> {
    match array.data_type() {
        DataType::Int64 => Some(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .expect("coerced maxchars is Int64")
                .value(row),
        ),
        DataType::Int32 => Some(i64::from(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .expect("maxchars Int32")
                .value(row),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::constructors::Point;

    #[tokio::test]
    async fn test_geohash() {
        let ctx = SessionContext::new();
        ctx.register_udf(GeoHash.into());
        ctx.register_udf(Point::default().into());

        let df = ctx
            .sql("SELECT ST_GeoHash(ST_Point(-126,48));")
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let column = batches[0].column(0);
        let string_arr = column.as_string_view();

        assert_eq!(string_arr.value(0), "c0w3hf1s70w3hf1s70w3");
    }
}
