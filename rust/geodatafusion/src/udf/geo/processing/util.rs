use geo::kernels::RobustKernel;
use geo::{Coord as GeoCoord, Kernel, Orientation};
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, LineStringTrait};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

pub(super) use crate::udf::native::geometry::is_empty_geometry;

/// Native planar processing is defined over XY. Empty outputs are therefore
/// always materialized as 2D empties even when the input carries Z or M.
pub(super) const OUTPUT_DIMENSION: WktDimension = WktDimension::XY;

/// Build the 2D empty geometry that PostGIS exposes for an empty input family.
pub(super) fn empty_wkt_like(geometry: &impl GeometryTrait<T = f64>) -> Wkt<f64> {
    match geometry.as_type() {
        GeometryType::Point(_) => Wkt::Point(Point::empty(OUTPUT_DIMENSION)),
        GeometryType::LineString(_) => Wkt::LineString(LineString::empty(OUTPUT_DIMENSION)),
        GeometryType::Polygon(_) => Wkt::Polygon(Polygon::empty(OUTPUT_DIMENSION)),
        GeometryType::MultiPoint(_) => Wkt::MultiPoint(MultiPoint::empty(OUTPUT_DIMENSION)),
        GeometryType::MultiLineString(_) => {
            Wkt::MultiLineString(MultiLineString::empty(OUTPUT_DIMENSION))
        }
        GeometryType::MultiPolygon(_) => Wkt::MultiPolygon(MultiPolygon::empty(OUTPUT_DIMENSION)),
        GeometryType::GeometryCollection(_) => {
            Wkt::GeometryCollection(GeometryCollection::empty(OUTPUT_DIMENSION))
        }
        GeometryType::Rect(_) | GeometryType::Triangle(_) | GeometryType::Line(_) => {
            Wkt::GeometryCollection(GeometryCollection::empty(OUTPUT_DIMENSION))
        }
    }
}

pub(super) fn point_wkt(coord: GeoCoord<f64>) -> Wkt<f64> {
    Wkt::Point(Point::new(
        Some(Coord {
            x: coord.x,
            y: coord.y,
            z: None,
            m: None,
        }),
        OUTPUT_DIMENSION,
    ))
}

pub(super) fn empty_polygon_wkt() -> Wkt<f64> {
    Wkt::Polygon(Polygon::empty(OUTPUT_DIMENSION))
}

pub(super) fn line_wkt(coords: &[GeoCoord<f64>]) -> Wkt<f64> {
    Wkt::LineString(LineString::new(
        coords
            .iter()
            .map(|coord| Coord {
                x: coord.x,
                y: coord.y,
                z: None,
                m: None,
            })
            .collect(),
        OUTPUT_DIMENSION,
    ))
}

pub(super) fn polygon_wkt(polygon: &geo::Polygon<f64>) -> Wkt<f64> {
    let mut rings = Vec::with_capacity(1 + polygon.interiors().len());
    rings.push(ring_wkt(polygon.exterior()));
    rings.extend(polygon.interiors().iter().map(ring_wkt));
    Wkt::Polygon(Polygon::new(rings, OUTPUT_DIMENSION))
}

/// Return a Point or LineString when a hull has no two-dimensional interior.
///
/// `geo::ConvexHull` and `geo::MinimumRotatedRect` both represent degenerate
/// results as polygons with repeated vertices. PostGIS exposes those results
/// as Point or LineString, so classify the hull before serializing it.
pub(super) fn degenerate_hull(polygon: &geo::Polygon<f64>) -> Option<Wkt<f64>> {
    let points = unique_coords(polygon.exterior().coords());
    match points.len() {
        0 => Some(Wkt::Polygon(Polygon::empty(OUTPUT_DIMENSION))),
        1 => Some(point_wkt(points[0])),
        _ if collinear(&points) => {
            let (first, last) = lexicographic_extremes(&points);
            Some(line_wkt(&[first, last]))
        }
        _ => None,
    }
}

fn ring_wkt(line_string: &impl LineStringTrait<T = f64>) -> LineString {
    LineString::new(
        line_string
            .coords()
            .map(|coord| Coord {
                x: coord.x(),
                y: coord.y(),
                z: None,
                m: None,
            })
            .collect(),
        OUTPUT_DIMENSION,
    )
}

fn unique_coords<C>(coords: impl Iterator<Item = C>) -> Vec<GeoCoord<f64>>
where
    C: geo_traits::CoordTrait<T = f64>,
{
    let mut output = Vec::new();
    for coord in coords {
        let coord = GeoCoord {
            x: coord.x(),
            y: coord.y(),
        };
        if output.last() != Some(&coord) {
            output.push(coord);
        }
    }
    output
}

fn collinear(points: &[GeoCoord<f64>]) -> bool {
    if points.len() < 3 {
        return true;
    }

    let first = points[0];
    let second = points[1];
    points
        .iter()
        .all(|point| RobustKernel::orient2d(first, second, *point) == Orientation::Collinear)
}

fn lexicographic_extremes(points: &[GeoCoord<f64>]) -> (GeoCoord<f64>, GeoCoord<f64>) {
    let mut points = points.to_vec();
    points.sort_by(|left, right| {
        left.x
            .partial_cmp(&right.x)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.y
                    .partial_cmp(&right.y)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    (points[0], points[points.len() - 1])
}
