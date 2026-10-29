use std::sync::{Arc, OnceLock};

use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait, TriangleTrait,
};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::common::{coord_to_wkt, output_geometry_type, wkt_dimension};
use crate::udf::native::geometry::geometry_array_from_arrow;

/// Returns a MultiPoint containing every coordinate of a geometry, preserving duplicates.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Points {
    coord_type: CoordType,
}

impl Points {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Points {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINTS_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Points {
    fn name(&self) -> &str {
        "st_points"
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<arrow_schema::Field>> {
        let geometry_type = output_geometry_type(&args.arg_fields[0], self.coord_type)?;
        Ok(geometry_type.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(points_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINTS_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a MultiPoint containing all coordinates of a geometry. Duplicate \
                 points, including ring closure points, are preserved. Z and M coordinates \
                 are preserved.",
                "ST_Points(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn points_impl(
    args: ScalarFunctionArgs,
    fallback_coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geometry_type = output_geometry_type(&args.arg_fields[0], fallback_coord_type)?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, points_array, &geometry_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn points_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    geometry_type: &GeometryType,
) -> GeoDataFusionResult<geoarrow_array::array::GeometryArray> {
    let mut builder = GeometryBuilder::new(geometry_type.clone());

    for item in array.iter() {
        let Some(geometry) = item.transpose()? else {
            builder.push_null();
            continue;
        };
        let mut coords = Vec::new();
        collect_points(&geometry, &mut coords);
        let points = wkt::types::MultiPoint::new(coords, wkt_dimension(geometry.dim()));
        builder.push_geometry(Some(&points))?;
    }

    Ok(builder.finish())
}

fn collect_points(geometry: &impl GeometryTrait<T = f64>, coords: &mut Vec<wkt::types::Point>) {
    use geo_traits::GeometryType::*;

    match geometry.as_type() {
        Point(point) => {
            if let Some(coord) = point.coord() {
                coords.push(wkt::types::Point::from_coord(coord_to_wkt(&coord)));
            }
        }
        LineString(line_string) => collect_line_string(line_string, coords),
        Polygon(polygon) => {
            if let Some(ring) = polygon.exterior() {
                collect_line_string(&ring, coords);
            }
            for ring in polygon.interiors() {
                collect_line_string(&ring, coords);
            }
        }
        MultiPoint(multi_point) => {
            for point in multi_point.points() {
                if let Some(coord) = point.coord() {
                    coords.push(wkt::types::Point::from_coord(coord_to_wkt(&coord)));
                }
            }
        }
        MultiLineString(multi_line_string) => {
            for line_string in multi_line_string.line_strings() {
                collect_line_string(&line_string, coords);
            }
        }
        MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon.polygons() {
                if let Some(ring) = polygon.exterior() {
                    collect_line_string(&ring, coords);
                }
                for ring in polygon.interiors() {
                    collect_line_string(&ring, coords);
                }
            }
        }
        GeometryCollection(collection) => {
            for child in collection.geometries() {
                collect_points(&child, coords);
            }
        }
        Triangle(triangle) => {
            let first = triangle.first();
            coords.push(wkt::types::Point::from_coord(coord_to_wkt(&first)));
            coords.push(wkt::types::Point::from_coord(coord_to_wkt(
                &triangle.second(),
            )));
            coords.push(wkt::types::Point::from_coord(coord_to_wkt(
                &triangle.third(),
            )));
            coords.push(wkt::types::Point::from_coord(coord_to_wkt(&first)));
        }
        Rect(rect) => {
            let min = coord_to_wkt(&rect.min());
            let max = coord_to_wkt(&rect.max());
            let ring = [
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
            for coord in ring {
                coords.push(wkt::types::Point::from_coord(coord));
            }
        }
        Line(line) => {
            let start = line.start();
            let end = line.end();
            coords.push(wkt::types::Point::from_coord(coord_to_wkt(&start)));
            coords.push(wkt::types::Point::from_coord(coord_to_wkt(&end)));
        }
    }
}

fn collect_line_string<L: LineStringTrait<T = f64>>(
    line_string: &L,
    coords: &mut Vec<wkt::types::Point>,
) {
    for coord in line_string.coords() {
        coords.push(wkt::types::Point::from_coord(coord_to_wkt(&coord)));
    }
}
