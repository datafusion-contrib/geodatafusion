use std::sync::{Arc, OnceLock};

use arrow_array::builder::{ArrayBuilder, Int32Builder, ListBuilder};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::common::{coord_to_wkt, output_geometry_type};
use crate::udf::native::accessors::dump::{
    finish_dump_list, output_field, path_values_field, push_dump_item,
};
use crate::udf::native::accessors::is_empty::is_geometry_topologically_empty;
use crate::udf::native::geometry::geometry_array_from_arrow;

/// Returns one point per coordinate, preserving the parent geometry path.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DumpPoints {
    coord_type: CoordType,
}

impl DumpPoints {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for DumpPoints {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DUMP_POINTS_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for DumpPoints {
    fn name(&self) -> &str {
        "st_dumppoints"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from geometry metadata in return_field_from_args."
                .to_string(),
        ))
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> Result<std::sync::Arc<arrow_schema::Field>> {
        let geometry_type = output_geometry_type(&args.arg_fields[0], self.coord_type)?;
        Ok(output_field(&geometry_type))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(dump_points_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DUMP_POINTS_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a list of (path, geom) records for every coordinate in a geometry. \
                 The path is 1-based for collection members and coordinate positions, and \
                 polygon paths additionally identify the 1-based ring number.",
                "ST_DumpPoints(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn dump_points_impl(
    args: ScalarFunctionArgs,
    fallback_coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geometry_type = output_geometry_type(&args.arg_fields[0], fallback_coord_type)?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, dump_points_array, &geometry_type)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn dump_points_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    geometry_type: &GeometryType,
) -> GeoDataFusionResult<arrow_array::ListArray> {
    let mut geom_builder = GeometryBuilder::new(geometry_type.clone());
    let mut path_builder = ListBuilder::new(Int32Builder::new()).with_field(path_values_field());
    let mut path = Vec::new();
    let mut row_lengths = Vec::with_capacity(array.len());
    let mut row_validity = NullBufferBuilder::new(array.len());

    for item in array.iter() {
        let start = path_builder.len();
        match item.transpose()? {
            Some(geometry) => {
                row_validity.append_non_null();
                if !is_geometry_topologically_empty(&geometry) {
                    visit_points(&geometry, &mut path, &mut geom_builder, &mut path_builder)?;
                }
            }
            None => row_validity.append_null(),
        }
        row_lengths.push(path_builder.len() - start);
    }

    Ok(finish_dump_list(
        geom_builder,
        path_builder,
        row_lengths,
        row_validity,
        geometry_type,
    ))
}

fn visit_points(
    geometry: &impl GeometryTrait<T = f64>,
    path: &mut Vec<i32>,
    geom_builder: &mut GeometryBuilder,
    path_builder: &mut ListBuilder<Int32Builder>,
) -> GeoDataFusionResult<()> {
    use geo_traits::GeometryType::*;

    match geometry.as_type() {
        Point(point) => {
            if let Some(coord) = point.coord() {
                emit_point(&coord, 1, path, geom_builder, path_builder)?;
            }
        }
        LineString(line_string) => {
            emit_coords(line_string.coords(), path, geom_builder, path_builder)?;
        }
        Polygon(polygon) => {
            let mut ring_number = 1;
            if let Some(ring) = polygon.exterior() {
                path.push(ring_number);
                emit_coords(ring.coords(), path, geom_builder, path_builder)?;
                path.pop();
                ring_number += 1;
            }
            for ring in polygon.interiors() {
                path.push(ring_number);
                emit_coords(ring.coords(), path, geom_builder, path_builder)?;
                path.pop();
                ring_number += 1;
            }
        }
        MultiPoint(multi_point) => {
            for (index, point) in multi_point.points().enumerate() {
                path.push(index as i32 + 1);
                visit_points(&point, path, geom_builder, path_builder)?;
                path.pop();
            }
        }
        MultiLineString(multi_line_string) => {
            for (index, line_string) in multi_line_string.line_strings().enumerate() {
                path.push(index as i32 + 1);
                visit_points(&line_string, path, geom_builder, path_builder)?;
                path.pop();
            }
        }
        MultiPolygon(multi_polygon) => {
            for (index, polygon) in multi_polygon.polygons().enumerate() {
                path.push(index as i32 + 1);
                visit_points(&polygon, path, geom_builder, path_builder)?;
                path.pop();
            }
        }
        GeometryCollection(collection) => {
            for (index, child) in collection.geometries().enumerate() {
                path.push(index as i32 + 1);
                visit_points(&child, path, geom_builder, path_builder)?;
                path.pop();
            }
        }
        Triangle(triangle) => {
            path.push(1);
            let coords = [triangle.first(), triangle.second(), triangle.third()];
            for (index, coord) in coords.iter().enumerate() {
                emit_point(coord, index as i32 + 1, path, geom_builder, path_builder)?;
            }
            emit_rect_or_triangle_closing_coord(&coords[0], path, geom_builder, path_builder)?;
            path.pop();
        }
        Rect(rect) => {
            path.push(1);
            emit_rect_ring(rect, path, geom_builder, path_builder)?;
            path.pop();
        }
        Line(line) => {
            let coords = line.coords();
            emit_point(&coords[0], 1, path, geom_builder, path_builder)?;
            emit_point(&coords[1], 2, path, geom_builder, path_builder)?;
        }
    }
    Ok(())
}

fn emit_coords(
    coords: impl Iterator<Item = impl CoordTrait<T = f64>>,
    path: &mut Vec<i32>,
    geom_builder: &mut GeometryBuilder,
    path_builder: &mut ListBuilder<Int32Builder>,
) -> GeoDataFusionResult<()> {
    for (index, coord) in coords.enumerate() {
        emit_point(&coord, index as i32 + 1, path, geom_builder, path_builder)?;
    }
    Ok(())
}

fn emit_point(
    coord: &impl CoordTrait<T = f64>,
    index: i32,
    path: &mut Vec<i32>,
    geom_builder: &mut GeometryBuilder,
    path_builder: &mut ListBuilder<Int32Builder>,
) -> GeoDataFusionResult<()> {
    path.push(index);
    let point = wkt::types::Point::from_coord(coord_to_wkt(coord));
    push_dump_item(geom_builder, path_builder, path, &point)?;
    path.pop();
    Ok(())
}

fn emit_rect_or_triangle_closing_coord(
    first: &impl CoordTrait<T = f64>,
    path: &mut Vec<i32>,
    geom_builder: &mut GeometryBuilder,
    path_builder: &mut ListBuilder<Int32Builder>,
) -> GeoDataFusionResult<()> {
    emit_point(first, 4, path, geom_builder, path_builder)
}

fn emit_rect_ring<R: RectTrait<T = f64>>(
    rect: &R,
    path: &mut Vec<i32>,
    geom_builder: &mut GeometryBuilder,
    path_builder: &mut ListBuilder<Int32Builder>,
) -> GeoDataFusionResult<()> {
    let min = coord_to_wkt(&rect.min());
    let max = coord_to_wkt(&rect.max());
    let coords = [
        wkt::types::Coord {
            x: min.x,
            y: min.y,
            z: min.z,
            m: min.m,
        },
        wkt::types::Coord {
            x: max.x,
            y: min.y,
            z: max.z,
            m: max.m,
        },
        wkt::types::Coord {
            x: max.x,
            y: max.y,
            z: max.z,
            m: max.m,
        },
        wkt::types::Coord {
            x: min.x,
            y: max.y,
            z: min.z,
            m: min.m,
        },
        wkt::types::Coord {
            x: min.x,
            y: min.y,
            z: min.z,
            m: min.m,
        },
    ];
    for (index, coord) in coords.iter().enumerate() {
        emit_point(coord, index as i32 + 1, path, geom_builder, path_builder)?;
    }
    Ok(())
}
