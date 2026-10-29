//! WKB to `wkt` conversion used by geometry builders.
//!
//! GeoArrow builders construct collections from `geo-traits` values. WKB is
//! used as the partition-safe aggregate state, so aggregates need to turn each
//! stored value back into a `wkt` geometry before building their result.

use datafusion::error::DataFusionError;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use wkb::reader::Wkb;
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

use crate::error::GeoDataFusionResult;

pub(crate) fn wkb_to_wkt(bytes: &[u8]) -> GeoDataFusionResult<Wkt<f64>> {
    let geometry = Wkb::try_new(bytes).map_err(|error| {
        DataFusionError::Execution(format!("invalid WKB in geometry aggregate state: {error}"))
    })?;
    geometry_to_wkt(&geometry)
}

pub(crate) fn geometry_to_wkt(
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoDataFusionResult<Wkt<f64>> {
    let dimension = wkt_dimension(geometry.dim())?;
    Ok(match geometry.as_type() {
        GeometryType::Point(point) => Wkt::Point(point_to_wkt(point, dimension)),
        GeometryType::LineString(line_string) => {
            Wkt::LineString(line_string_to_wkt(line_string, dimension))
        }
        GeometryType::Polygon(polygon) => Wkt::Polygon(polygon_to_wkt(polygon, dimension)),
        GeometryType::MultiPoint(multi_point) => Wkt::MultiPoint(MultiPoint::new(
            multi_point
                .points()
                .map(|point| point_to_wkt(&point, dimension))
                .collect(),
            dimension,
        )),
        GeometryType::MultiLineString(multi_line_string) => {
            Wkt::MultiLineString(MultiLineString::new(
                multi_line_string
                    .line_strings()
                    .map(|line_string| line_string_to_wkt(&line_string, dimension))
                    .collect(),
                dimension,
            ))
        }
        GeometryType::MultiPolygon(multi_polygon) => Wkt::MultiPolygon(MultiPolygon::new(
            multi_polygon
                .polygons()
                .map(|polygon| polygon_to_wkt(&polygon, dimension))
                .collect(),
            dimension,
        )),
        GeometryType::GeometryCollection(geometry_collection) => {
            Wkt::GeometryCollection(GeometryCollection::new(
                geometry_collection
                    .geometries()
                    .map(|geometry| geometry_to_wkt(&geometry))
                    .collect::<GeoDataFusionResult<Vec<_>>>()?,
                dimension,
            ))
        }
        _ => {
            return Err(DataFusionError::Execution(
                "unsupported geometry type in aggregate state".to_string(),
            )
            .into());
        }
    })
}

pub(crate) fn wkt_dimension(
    dimension: geo_traits::Dimensions,
) -> GeoDataFusionResult<WktDimension> {
    Ok(match dimension {
        geo_traits::Dimensions::Xy => WktDimension::XY,
        geo_traits::Dimensions::Xyz => WktDimension::XYZ,
        geo_traits::Dimensions::Xym => WktDimension::XYM,
        geo_traits::Dimensions::Xyzm => WktDimension::XYZM,
        geo_traits::Dimensions::Unknown(value) => {
            return Err(DataFusionError::Execution(format!(
                "unsupported geometry dimension {value} in aggregate state"
            ))
            .into());
        }
    })
}

pub(crate) fn coord_to_wkt(coord: &impl CoordTrait<T = f64>) -> Coord<f64> {
    let (z, m) = match coord.dim() {
        geo_traits::Dimensions::Xy => (None, None),
        geo_traits::Dimensions::Xyz => (coord.nth(2), None),
        geo_traits::Dimensions::Xym => (None, coord.nth(2)),
        geo_traits::Dimensions::Xyzm => (coord.nth(2), coord.nth(3)),
        geo_traits::Dimensions::Unknown(_) => (None, None),
    };
    Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    }
}

fn point_to_wkt(point: &impl PointTrait<T = f64>, dimension: WktDimension) -> Point<f64> {
    Point::new(point.coord().map(|coord| coord_to_wkt(&coord)), dimension)
}

fn line_string_to_wkt(
    line_string: &impl LineStringTrait<T = f64>,
    dimension: WktDimension,
) -> LineString<f64> {
    LineString::new(
        line_string
            .coords()
            .map(|coord| coord_to_wkt(&coord))
            .collect(),
        dimension,
    )
}

fn polygon_to_wkt(polygon: &impl PolygonTrait<T = f64>, dimension: WktDimension) -> Polygon<f64> {
    let mut rings = Vec::new();
    if let Some(exterior) = polygon.exterior() {
        rings.push(line_string_to_wkt(&exterior, dimension));
    }
    rings.extend(
        polygon
            .interiors()
            .map(|ring| line_string_to_wkt(&ring, dimension)),
    );
    Polygon::new(rings, dimension)
}
