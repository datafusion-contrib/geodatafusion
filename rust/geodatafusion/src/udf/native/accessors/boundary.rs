//! `ST_Boundary` following the OGC Simple Features boundary contract.
//!
//! The observable result shapes are matched against reviewed PostGIS
//! behavior: point boundaries keep the point type, curve boundaries are the
//! modulo-2 endpoint set, polygon boundaries are their rings, and collection
//! boundaries are passed through the repository's shared homogenizer. The
//! construction is expressed directly over `geo-traits` and does not
//! translate PostGIS source code.

use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPolygonTrait, PolygonTrait, RectTrait,
};
use geoarrow_array::array::GeometryArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType as GeoArrowGeometryType};
use wkt::Wkt;
use wkt::types::{Coord, GeometryCollection, LineString, MultiLineString, MultiPoint, Point};

use super::common::wkt_dimension;
use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::editors::collection_homogenize;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata_or_default, geometry_array_from_arrow,
    is_null_geometry_input, null_geometry_array,
};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Boundary {
    coord_type: CoordType,
}

impl Boundary {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Boundary {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Boundary {
    fn name(&self) -> &str {
        "st_boundary"
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = args.arg_fields[0].as_ref();
        let coord_type = if input_field.data_type() == &DataType::Null {
            self.coord_type
        } else {
            GeoArrowType::from_arrow_field(input_field)
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                .coord_type()
                .unwrap_or(self.coord_type)
        };
        let output_type = GeoArrowGeometryType::new(field_metadata_or_default(input_field)?)
            .with_coord_type(coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(boundary_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the boundary of a geometry as a geometry with the same coordinate dimension and CRS metadata.",
                "ST_Boundary(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn boundary_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let input_field = args.arg_fields[0].as_ref();
    let GeoArrowType::Geometry(output_type) =
        GeoArrowType::from_arrow_field(args.return_field.as_ref())?
    else {
        return Err(DataFusionError::Internal(
            "ST_Boundary result field is not a generic Geometry".to_string(),
        )
        .into());
    };

    if is_null_geometry_input(input_field, arrays[0].as_ref()) {
        return Ok(null_geometry_array(output_type, arrays[0].len())?.into());
    }

    let geometry = geometry_array_from_arrow(arrays[0].as_ref(), input_field)?;
    let geometry_ref = geometry.as_ref();
    let output = downcast_geoarrow_array!(geometry_ref, boundary_array, &output_type)?;
    Ok(output.into_array_ref().into())
}

fn boundary_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeoArrowGeometryType,
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = StrictGeometryBuilder::new(output_type.clone());
    for item in array.iter() {
        match item {
            None => builder.push_geometry(None::<&Wkt<f64>>)?,
            Some(geometry) => {
                let geometry = geometry?;
                let boundary = boundary_geometry(&geometry)?;
                builder.push_geometry(Some(&boundary))?;
            }
        }
    }
    Ok(builder.finish())
}

fn boundary_geometry<G>(geometry: &G) -> GeoDataFusionResult<Wkt<f64>>
where
    G: GeometryTrait<T = f64>,
{
    Ok(match geometry.as_type() {
        GeometryType::Point(point) => empty_point_boundary(point.dim()),
        GeometryType::LineString(line_string) => single_curve_boundary(line_string),
        GeometryType::Polygon(polygon) => polygon_boundary(polygon),
        GeometryType::MultiPoint(multi_point) => empty_multi_point_boundary(multi_point.dim()),
        GeometryType::MultiLineString(multi_line_string) => multi_curve_boundary(multi_line_string),
        GeometryType::MultiPolygon(multi_polygon) => {
            let dimension = wkt_dimension(multi_polygon.dim());
            let mut boundaries = Vec::with_capacity(multi_polygon.num_polygons());
            for index in 0..multi_polygon.num_polygons() {
                if let Some(polygon) = multi_polygon.polygon(index) {
                    boundaries.push(polygon_boundary(&polygon));
                }
            }
            collection_homogenize(&Wkt::GeometryCollection(GeometryCollection::new(
                boundaries, dimension,
            )))
        }
        GeometryType::GeometryCollection(collection) => {
            let dimension = wkt_dimension(collection.dim());
            let mut boundaries = Vec::with_capacity(collection.num_geometries());
            for child in collection.geometries() {
                boundaries.push(boundary_geometry(&child)?);
            }
            collection_homogenize(&Wkt::GeometryCollection(GeometryCollection::new(
                boundaries, dimension,
            )))
        }
        GeometryType::Rect(rect) => rect_boundary(rect)?,
        GeometryType::Triangle(_) | GeometryType::Line(_) => {
            return Err(DataFusionError::Execution(
                "ST_Boundary does not support this geometry type".to_string(),
            )
            .into());
        }
    })
}

fn empty_point_boundary(dimension: Dimensions) -> Wkt<f64> {
    Wkt::Point(Point::empty(wkt_dimension(dimension)))
}

fn empty_multi_point_boundary(dimension: Dimensions) -> Wkt<f64> {
    Wkt::MultiPoint(MultiPoint::empty(wkt_dimension(dimension)))
}

fn rect_boundary(rect: &impl RectTrait<T = f64>) -> GeoDataFusionResult<Wkt<f64>> {
    if !matches!(rect.dim(), Dimensions::Xy | Dimensions::Unknown(2)) {
        return Err(DataFusionError::Execution(
            "ST_Boundary supports only 2D Box/Rect inputs".to_string(),
        )
        .into());
    }

    let min = rect.min();
    let max = rect.max();
    let ring = LineString::new(
        vec![
            Coord {
                x: min.x(),
                y: min.y(),
                z: None,
                m: None,
            },
            Coord {
                x: max.x(),
                y: min.y(),
                z: None,
                m: None,
            },
            Coord {
                x: max.x(),
                y: max.y(),
                z: None,
                m: None,
            },
            Coord {
                x: min.x(),
                y: max.y(),
                z: None,
                m: None,
            },
            Coord {
                x: min.x(),
                y: min.y(),
                z: None,
                m: None,
            },
        ],
        wkt::types::Dimension::XY,
    );
    Ok(Wkt::LineString(ring))
}

fn single_curve_boundary(line_string: &impl LineStringTrait<T = f64>) -> Wkt<f64> {
    let dimension = wkt_dimension(line_string.dim());
    let mut endpoints = EndpointToggleSet::new(line_string.dim());
    if let Some((first, last)) = open_curve_endpoints(line_string) {
        endpoints.toggle(first);
        endpoints.toggle(last);
    }
    endpoints.into_multi_point(dimension)
}

fn multi_curve_boundary(multi_line_string: &impl MultiLineStringTrait<T = f64>) -> Wkt<f64> {
    let dimension = wkt_dimension(multi_line_string.dim());
    let mut endpoints = EndpointToggleSet::new(multi_line_string.dim());
    for index in 0..multi_line_string.num_line_strings() {
        let Some(line_string) = multi_line_string.line_string(index) else {
            continue;
        };
        if let Some((first, last)) = open_curve_endpoints(&line_string) {
            endpoints.toggle(first);
            endpoints.toggle(last);
        }
    }
    endpoints.into_multi_point(dimension)
}

/// Boundary of a curve is the modulo-2 endpoint set: endpoints shared by an
/// even number of open curve components cancel.
struct EndpointToggleSet {
    dimension: Dimensions,
    points: Vec<(CoordinateKey, Coord)>,
}

impl EndpointToggleSet {
    fn new(dimension: Dimensions) -> Self {
        Self {
            dimension,
            points: Vec::new(),
        }
    }

    fn toggle(&mut self, coordinate: Coord) {
        let key = coordinate_key(&coordinate, self.dimension);
        if let Some(index) = self
            .points
            .iter()
            .position(|(existing, _)| *existing == key)
        {
            self.points.remove(index);
        } else {
            self.points.push((key, coordinate));
        }
    }

    fn into_multi_point(self, dimension: wkt::types::Dimension) -> Wkt<f64> {
        Wkt::MultiPoint(MultiPoint::new(
            self.points
                .into_iter()
                .map(|(_, coordinate)| Point::new(Some(coordinate), dimension))
                .collect(),
            dimension,
        ))
    }
}

fn open_curve_endpoints(line_string: &impl LineStringTrait<T = f64>) -> Option<(Coord, Coord)> {
    let num_coords = line_string.num_coords();
    if num_coords < 2 {
        return None;
    }

    let first = line_string
        .coord(0)
        .expect("a line string with coordinates has a first coordinate");
    let last = line_string
        .coord(num_coords - 1)
        .expect("a line string with coordinates has a last coordinate");
    if coordinates_closed(&first, &last, line_string.dim()) {
        return None;
    }
    Some((to_wkt_coord(&first), to_wkt_coord(&last)))
}

fn polygon_boundary(polygon: &impl PolygonTrait<T = f64>) -> Wkt<f64> {
    let dimension = wkt_dimension(polygon.dim());
    let mut rings = Vec::with_capacity(1 + polygon.num_interiors());
    if let Some(exterior) = polygon.exterior() {
        rings.push(line_string_from(exterior));
    }
    for index in 0..polygon.num_interiors() {
        if let Some(interior) = polygon.interior(index) {
            rings.push(line_string_from(interior));
        }
    }

    collection_homogenize(&Wkt::MultiLineString(MultiLineString::new(
        rings, dimension,
    )))
}

fn line_string_from(line_string: impl LineStringTrait<T = f64>) -> LineString {
    let dimension = wkt_dimension(line_string.dim());
    LineString::new(
        line_string
            .coords()
            .map(|coord| to_wkt_coord(&coord))
            .collect(),
        dimension,
    )
}

fn to_wkt_coord(coord: &impl CoordTrait<T = f64>) -> Coord {
    let mut output = Coord {
        x: coord.x(),
        y: coord.y(),
        z: None,
        m: None,
    };
    match coord.dim() {
        Dimensions::Xy => {}
        Dimensions::Xyz | Dimensions::Unknown(3) => output.z = coord.nth(2),
        Dimensions::Xym => output.m = coord.nth(2),
        Dimensions::Xyzm | Dimensions::Unknown(4) => {
            output.z = coord.nth(2);
            output.m = coord.nth(3);
        }
        Dimensions::Unknown(_) => {}
    }
    output
}

fn coordinates_closed(
    first: &impl CoordTrait<T = f64>,
    last: &impl CoordTrait<T = f64>,
    dimension: Dimensions,
) -> bool {
    if first.x().to_bits() != last.x().to_bits() || first.y().to_bits() != last.y().to_bits() {
        return false;
    }

    match dimension {
        Dimensions::Xyz | Dimensions::Xyzm | Dimensions::Unknown(3 | 4) => {
            first.nth(2).map(f64::to_bits) == last.nth(2).map(f64::to_bits)
        }
        _ => true,
    }
}

type CoordinateKey = (u64, u64, u64, u64);

fn coordinate_key(coordinate: &impl CoordTrait<T = f64>, dimension: Dimensions) -> CoordinateKey {
    let z = match dimension {
        Dimensions::Xyz | Dimensions::Xyzm | Dimensions::Unknown(3 | 4) => coordinate.nth(2),
        _ => None,
    };
    let m = match dimension {
        Dimensions::Xym => coordinate.nth(2),
        Dimensions::Xyzm | Dimensions::Unknown(4) => coordinate.nth(3),
        _ => None,
    };
    (
        coordinate.x().to_bits(),
        coordinate.y().to_bits(),
        z.unwrap_or(0.0).to_bits(),
        m.unwrap_or(0.0).to_bits(),
    )
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn context() -> SessionContext {
        let ctx = SessionContext::new();
        crate::udf::native::register(&ctx);
        ctx
    }

    #[tokio::test]
    async fn boundary_contract() {
        let ctx = context();
        let batch = ctx
            .sql(
                "SELECT
                    ST_AsText(ST_Boundary(ST_GeomFromText('POINT(1 2)'))) AS point,
                    ST_AsText(ST_Boundary(ST_GeomFromText('LINESTRING(0 0,1 1)'))) AS line,
                    ST_AsText(ST_Boundary(ST_GeomFromText('LINESTRING(0 0,1 0,1 1,0 0)'))) AS closed,
                    ST_AsText(ST_Boundary(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))'))) AS polygon,
                    ST_AsText(ST_Boundary(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(1 1,2 2))'))) AS multi_line,
                    ST_AsEWKB(ST_Boundary(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))'))) AS collection,
                    ST_AsText(ST_Boundary(ST_GeomFromText(NULL))) AS null_input",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let text = |index: usize| batch.column(index).as_string::<i32>().value(0).to_string();
        assert_eq!(text(0), "POINT EMPTY");
        assert_eq!(text(1), "MULTIPOINT((0 0),(1 1))");
        assert_eq!(text(2), "MULTIPOINT EMPTY");
        assert_eq!(text(3), "LINESTRING(0 0,1 0,1 1,0 0)");
        assert_eq!(text(4), "MULTIPOINT((0 0),(2 2))");
        assert_eq!(
            hex::encode(batch.column(5).as_binary::<i32>().value(0)),
            "0104000000030000000101000000000000000000f87f000000000000f87f0101000000000000000000000000000000000000000101000000000000000000f03f000000000000f03f"
        );
        assert!(batch.column(6).is_null(0));
    }
}
