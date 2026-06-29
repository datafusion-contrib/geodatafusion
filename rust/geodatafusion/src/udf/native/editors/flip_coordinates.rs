use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::GeometryTrait;
use geoarrow_array::array::{
    CoordBuffer, InterleavedCoordBuffer, LineStringArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray, RectArray, SeparatedCoordBuffer, from_arrow_array,
};
use geoarrow_array::builder::{GeometryBuilder, InterleavedCoordBufferBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, GeometryType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct FlipCoordinates {
    signature: Signature,
    coord_type: CoordType,
}

impl FlipCoordinates {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for FlipCoordinates {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for FlipCoordinates {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_flipcoordinates"
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
        Ok(invoke_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the number of points in a geometry. Works for all geometries.",
                "ST_FlipCoordinates(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let field = args.arg_fields[0].as_ref();
    let geo_type = GeoArrowType::from_arrow_field(field)?;
    let new_type = match geo_type {
        GeoArrowType::Point(_)
        | GeoArrowType::LineString(_)
        | GeoArrowType::Polygon(_)
        | GeoArrowType::MultiPoint(_)
        | GeoArrowType::MultiLineString(_)
        | GeoArrowType::MultiPolygon(_)
        | GeoArrowType::Rect(_) => geo_type,
        _ => GeoArrowType::Geometry(
            GeometryType::new(geo_type.metadata().clone()).with_coord_type(coord_type),
        ),
    };
    Ok(Arc::new(
        new_type.to_field(field.name(), field.is_nullable()),
    ))
}

fn invoke_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = flip_coordinates_impl(&geo_array, coord_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn flip_coordinates_impl(
    array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    match array.data_type() {
        GeoArrowType::Point(_) => Ok(Arc::new(flip_point_array(array.as_point()))),
        GeoArrowType::LineString(_) => Ok(Arc::new(flip_line_string_array(array.as_line_string()))),
        GeoArrowType::Polygon(_) => Ok(Arc::new(flip_polygon_array(array.as_polygon()))),
        GeoArrowType::MultiPoint(_) => Ok(Arc::new(flip_multipoint_array(array.as_multi_point()))),
        GeoArrowType::MultiLineString(_) => Ok(Arc::new(flip_multi_line_string_array(
            array.as_multi_line_string(),
        ))),
        GeoArrowType::MultiPolygon(_) => {
            Ok(Arc::new(flip_multi_polygon_array(array.as_multi_polygon())))
        }
        GeoArrowType::Rect(_) => Ok(Arc::new(flip_rect_array(array.as_rect()))),
        _ => downcast_geoarrow_array!(array, flip_generic_array, coord_type),
    }
}

fn flip_point_array(array: &PointArray) -> PointArray {
    PointArray::new(
        flip_coords(array.coords()),
        array.logical_nulls(),
        array.extension_type().metadata().clone(),
    )
}

fn flip_line_string_array(array: &LineStringArray) -> LineStringArray {
    LineStringArray::new(
        flip_coords(array.coords()),
        array.geom_offsets().clone(),
        array.logical_nulls(),
        array.extension_type().metadata().clone(),
    )
}

fn flip_polygon_array(array: &PolygonArray) -> PolygonArray {
    PolygonArray::new(
        flip_coords(array.coords()),
        array.geom_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.extension_type().metadata().clone(),
    )
}

fn flip_multipoint_array(array: &MultiPointArray) -> MultiPointArray {
    MultiPointArray::new(
        flip_coords(array.coords()),
        array.geom_offsets().clone(),
        array.logical_nulls(),
        array.extension_type().metadata().clone(),
    )
}

fn flip_multi_line_string_array(array: &MultiLineStringArray) -> MultiLineStringArray {
    MultiLineStringArray::new(
        flip_coords(array.coords()),
        array.geom_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.extension_type().metadata().clone(),
    )
}

fn flip_multi_polygon_array(array: &MultiPolygonArray) -> MultiPolygonArray {
    MultiPolygonArray::new(
        flip_coords(array.coords()),
        array.geom_offsets().clone(),
        array.polygon_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.extension_type().metadata().clone(),
    )
}

fn flip_rect_array(array: &RectArray) -> RectArray {
    RectArray::new(
        flip_separated_coords(array.lower()),
        flip_separated_coords(array.upper()),
        array.logical_nulls(),
        array.extension_type().metadata().clone(),
    )
}

fn flip_generic_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let typ = GeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut output_builder = GeometryBuilder::new(typ);

    for geom in array.iter() {
        if let Some(g) = geom {
            output_builder.push_geometry(Some(&flip_geometry(&g?)?))?;
        } else {
            output_builder.push_null();
        }
    }

    Ok(Arc::new(output_builder.finish()))
}

fn flip_geometry(geom: &impl GeometryTrait<T = f64>) -> GeoArrowResult<wkt::Wkt> {
    todo!()
    // match geom.as_type() {
    //     geo_traits::GeometryType::Point()
    // }
}

fn flip_coords(coords: &CoordBuffer) -> CoordBuffer {
    match coords {
        CoordBuffer::Separated(separated) => {
            CoordBuffer::Separated(flip_separated_coords(separated))
        }
        CoordBuffer::Interleaved(interleaved) => {
            CoordBuffer::Interleaved(flip_interleaved_coords(interleaved))
        }
    }
}

fn flip_separated_coords(coords: &SeparatedCoordBuffer) -> SeparatedCoordBuffer {
    let mut buffers = coords.buffers().to_vec();
    buffers.swap(0, 1);
    SeparatedCoordBuffer::from_vec(buffers, coords.dim()).unwrap()
}

fn flip_interleaved_coords(coords: &InterleavedCoordBuffer) -> InterleavedCoordBuffer {
    let mut builder = InterleavedCoordBufferBuilder::with_capacity(coords.len(), coords.dim());
    let raw_coord_buffer = coords.coords();
    for coord_idx in 0..coords.len() {
        match coords.dim() {
            Dimension::XY => {
                let x = raw_coord_buffer.get(coord_idx * 2).unwrap();
                let y = raw_coord_buffer.get(coord_idx * 2 + 1).unwrap();
                let flipped = geo::Coord { x: *y, y: *x };
                builder.push_coord(&flipped);
            }
            Dimension::XYZ => {
                let x = raw_coord_buffer.get(coord_idx * 3).unwrap();
                let y = raw_coord_buffer.get(coord_idx * 3 + 1).unwrap();
                let z = raw_coord_buffer.get(coord_idx * 3 + 2).unwrap();
                let flipped = wkt::types::Coord {
                    x: *y,
                    y: *x,
                    z: Some(*z),
                    m: None,
                };
                builder.push_coord(&flipped);
            }
            Dimension::XYM => {
                let x = raw_coord_buffer.get(coord_idx * 3).unwrap();
                let y = raw_coord_buffer.get(coord_idx * 3 + 1).unwrap();
                let m = raw_coord_buffer.get(coord_idx * 3 + 2).unwrap();
                let flipped = wkt::types::Coord {
                    x: *y,
                    y: *x,
                    z: None,
                    m: Some(*m),
                };
                builder.push_coord(&flipped);
            }
            Dimension::XYZM => {
                let x = raw_coord_buffer.get(coord_idx * 4).unwrap();
                let y = raw_coord_buffer.get(coord_idx * 4 + 1).unwrap();
                let z = raw_coord_buffer.get(coord_idx * 4 + 2).unwrap();
                let m = raw_coord_buffer.get(coord_idx * 4 + 3).unwrap();
                let flipped = wkt::types::Coord {
                    x: *y,
                    y: *x,
                    z: Some(*z),
                    m: Some(*m),
                };
                builder.push_coord(&flipped);
            }
        }
    }
    builder.finish()
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use datafusion::prelude::SessionContext;
    use geoarrow_array::array::WktArray;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();

        ctx.register_udf(FlipCoordinates::new(Default::default()).into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(AsText::new().into());

        let df = ctx
            .sql("SELECT ST_AsText(ST_FlipCoordinates(ST_GeomFromText('POINT(1 2)')));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let wkt_arr =
            WktArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
        let val = wkt_arr.value(0).unwrap();
        assert_eq!(val, wkt::Wkt::from_str("POINT(2 1)").unwrap());
    }
}
