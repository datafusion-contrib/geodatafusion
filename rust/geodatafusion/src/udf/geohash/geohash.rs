use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringViewBuilder;
use arrow_array::{StringViewArray, new_null_array};
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::{GeometryBounds, geometry_bounds};

/// `ST_GeoHash` scalar UDF implementation: encodes a geometry into a GeoHash string.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeoHash;

impl GeoHash {
    /// Creates a new `ST_GeoHash` UDF instance.
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

impl ScalarUDFImpl for GeoHash {
    fn name(&self) -> &str {
        "st_geohash"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.arg_fields[0].data_type() == &DataType::Null
            || matches!(args.args[0], ColumnarValue::Scalar(ScalarValue::Null))
        {
            return Ok(match &args.args[0] {
                ColumnarValue::Array(array) => {
                    ColumnarValue::Array(new_null_array(&DataType::Utf8View, array.len()))
                }
                _ => ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
            });
        }
        Ok(geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOHASH_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a GeoHash representation of a geometry using the center of its XY bounding box. Empty geometries return NULL.",
                "ST_GeoHash(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn geohash_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let geo_array = from_arrow_array(&array, &args.arg_fields[0])?;
    let geo_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_ref, geohash_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn geohash_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<StringViewArray> {
    let mut builder = StringViewBuilder::with_capacity(array.len());
    for geom in array.iter() {
        let Some(geom) = geom else {
            builder.append_null();
            continue;
        };
        let geom = geom?;
        let center = match geometry_bounds(&geom) {
            GeometryBounds::Finite(bounds) => Some((
                (bounds.minx() + bounds.maxx()) / 2.0,
                (bounds.miny() + bounds.maxy()) / 2.0,
            )),
            GeometryBounds::Empty | GeometryBounds::NonFinite => None,
        };
        if let Some((x, y)) = center {
            let s = geohash::encode(geo::Coord { x, y }, 12)?;
            builder.append_value(s);
        } else {
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
    use crate::udf::native::constructors::Point;

    #[tokio::test]
    async fn test_geohash() {
        let ctx = SessionContext::new();
        ctx.register_udf(GeoHash.into());
        ctx.register_udf(Point::default().into());

        let df = ctx
            .sql("SELECT ST_GeoHash( ST_Point(-126,48) );")
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let column = batches[0].column(0);
        let string_arr = column.as_string_view();

        assert_eq!(string_arr.value(0), "c0w3hf1s70w3");
    }
}
