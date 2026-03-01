use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use geo::Buffer as GeoBuffer;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StBuffer {
    signature: Signature,
    coord_type: CoordType,
}

impl StBuffer {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for StBuffer {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StBuffer {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_buffer"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(buffer_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a geometry that represents all points whose distance from this geometry is less than or equal to the given distance. Positive distance creates an expanded polygon, negative shrinks it.",
                "ST_Buffer(geometry, distance)",
            )
            .with_argument("geom", "geometry")
            .with_argument("distance", "float8 - buffer distance in the units of the geometry's coordinate system")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn buffer_impl(
    args: ScalarFunctionArgs,
    _coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let distance = args.args[1].cast_to(&DataType::Float64, None)?;
    let distance = match distance {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Float64(Some(val)) => val,
            ScalarValue::Float64(None) => {
                return Err(DataFusionError::Plan(
                    "ST_Buffer distance cannot be NULL".into(),
                )
                .into());
            }
            _ => unreachable!(),
        },
        ColumnarValue::Array(_) => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized distance not yet implemented".to_string(),
            )
            .into());
        }
    };

    let result = buffer_array(&geo_array, distance)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn buffer_array(
    array: &dyn GeoArrowArray,
    distance: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _buffer_impl, distance)
}

fn _buffer_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    distance: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let buffered = geo_geom.buffer(distance);
            builder.push_geometry(Some(&geo::Geometry::MultiPolygon(buffered)))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::geo::measurement::Area;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_buffer_point() {
        let ctx = SessionContext::new();

        ctx.register_udf(StBuffer::default().into());
        ctx.register_udf(Area::new().into());
        ctx.register_udf(GeomFromText::default().into());

        // Buffer a point by 1.0 should create a circle-like polygon with area ~π
        let df = ctx
            .sql("SELECT ST_Area(ST_Buffer(ST_GeomFromText('POINT(0 0)'), 1.0));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let area = col.as_primitive::<Float64Type>().value(0);
        // Area of a circle with r=1 is π ≈ 3.14159; buffer approximation should be close
        assert!(area > 3.0 && area < 3.3, "Buffer area was {}", area);
    }
}
