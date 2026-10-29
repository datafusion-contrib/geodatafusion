//! GEOS coordinate normalization.
//!
//! The `geos-3_11` binding API cannot read M ordinates. PostGIS's GEOS-backed
//! functions drop M before calling GEOS while keeping Z, so `strip_m` rewrites
//! only the GEOS-facing WKB and leaves the original bytes available for
//! functions that return the input unchanged.
//!
//! `restore_z` is the complement for results whose Z ordinates were lost: it
//! rebuilds an XY GEOS result as XYZ with a caller-provided elevation rule.

use datafusion::error::DataFusionError;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use geos::CoordDimensions;
use wkb::reader::Wkb;
use wkb::writer::{WriteOptions, geometry_wkb_size, write_geometry};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

use crate::error::GeoDataFusionResult;

pub(super) fn strip_m(wkb: &[u8]) -> GeoDataFusionResult<Vec<u8>> {
    rewrite_wkb(
        wkb,
        &coord_without_m,
        &wkt_dimension_without_m,
        "could not normalize GEOS input WKB",
    )
}

/// Rewrite an empty GEOS result to the requested coordinate dimension.
///
/// GEOS's WKB writer does not always raise the dimension of an empty geometry
/// (and some releases report a Z dimension for an XY empty input). Rebuilding
/// the empty WKB makes the type flags follow the dimension PostGIS derives from
/// the inputs without touching non-empty results.
pub(super) fn force_dimension(
    wkb: &[u8],
    dimension: CoordDimensions,
) -> GeoDataFusionResult<Vec<u8>> {
    let target = match dimension {
        CoordDimensions::TwoD => WktDimension::XY,
        CoordDimensions::ThreeD => WktDimension::XYZ,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "GEOS results support only 2D and 3D coordinate dimensions".to_string(),
            )
            .into());
        }
    };
    rewrite_wkb(
        wkb,
        &coord_without_m,
        &|_| target,
        "could not normalize an empty GEOS result dimension",
    )
}

/// Rebuilds a GEOS result as XYZ, filling in every missing Z ordinate.
///
/// The `geos-3_11` binding clips without Z ordinates, so callers restore them
/// from the input geometry before the result is converted to GeoArrow. Z
/// ordinates that are already present - including the preserved Z of a
/// collection member that never needed clipping - are left untouched.
pub(super) fn restore_z(
    wkb: &[u8],
    z_of: &impl Fn(f64, f64) -> f64,
) -> GeoDataFusionResult<Vec<u8>> {
    rewrite_wkb(
        wkb,
        &|coord| {
            let x = coord.x();
            let y = coord.y();
            Coord {
                x,
                y,
                z: Some(
                    coord_z(coord)
                        .filter(|z| !z.is_nan())
                        .unwrap_or_else(|| z_of(x, y)),
                ),
                m: None,
            }
        },
        &|_| WktDimension::XYZ,
        "could not restore clipped Z ordinates",
    )
}

fn rewrite_wkb(
    wkb: &[u8],
    map: &impl Fn(&dyn CoordTrait<T = f64>) -> Coord<f64>,
    dimension: &impl Fn(Dimensions) -> WktDimension,
    context: &str,
) -> GeoDataFusionResult<Vec<u8>> {
    let parsed = Wkb::try_new(wkb).map_err(|error| {
        DataFusionError::Execution(format!("GEOS input WKB is invalid: {error}"))
    })?;
    let geometry = map_geometry(&parsed, map, dimension)?;
    let mut output = Vec::with_capacity(geometry_wkb_size(&geometry));
    write_geometry(&mut output, &geometry, &WriteOptions::default())
        .map_err(|error| DataFusionError::Execution(format!("{context}: {error}")))?;
    Ok(output)
}

fn map_geometry(
    geometry: &impl GeometryTrait<T = f64>,
    map: &impl Fn(&dyn CoordTrait<T = f64>) -> Coord<f64>,
    dimension: &impl Fn(Dimensions) -> WktDimension,
) -> GeoDataFusionResult<Wkt<f64>> {
    Ok(match geometry.as_type() {
        GeometryType::Point(point) => Wkt::Point(Point::new(
            point.coord().map(|coord| map(&coord)),
            dimension(point.dim()),
        )),
        GeometryType::LineString(line_string) => {
            Wkt::LineString(line_string_mapped(line_string, map, dimension))
        }
        GeometryType::Polygon(polygon) => Wkt::Polygon(polygon_mapped(polygon, map, dimension)),
        GeometryType::MultiPoint(multi_point) => Wkt::MultiPoint(MultiPoint::new(
            multi_point
                .points()
                .map(|point| {
                    Point::new(
                        point.coord().map(|coord| map(&coord)),
                        dimension(point.dim()),
                    )
                })
                .collect(),
            dimension(multi_point.dim()),
        )),
        GeometryType::MultiLineString(multi_line_string) => {
            Wkt::MultiLineString(MultiLineString::new(
                multi_line_string
                    .line_strings()
                    .map(|line_string| line_string_mapped(&line_string, map, dimension))
                    .collect(),
                dimension(multi_line_string.dim()),
            ))
        }
        GeometryType::MultiPolygon(multi_polygon) => Wkt::MultiPolygon(MultiPolygon::new(
            multi_polygon
                .polygons()
                .map(|polygon| polygon_mapped(&polygon, map, dimension))
                .collect(),
            dimension(multi_polygon.dim()),
        )),
        GeometryType::GeometryCollection(collection) => {
            Wkt::GeometryCollection(GeometryCollection::new(
                collection
                    .geometries()
                    .map(|geometry| map_geometry(&geometry, map, dimension))
                    .collect::<GeoDataFusionResult<Vec<_>>>()?,
                dimension(collection.dim()),
            ))
        }
        GeometryType::Rect(_) | GeometryType::Triangle(_) | GeometryType::Line(_) => {
            return Err(DataFusionError::Execution(
                "GEOS input contains a geometry type that cannot be normalized".to_string(),
            )
            .into());
        }
    })
}

fn line_string_mapped(
    line_string: &impl LineStringTrait<T = f64>,
    map: &impl Fn(&dyn CoordTrait<T = f64>) -> Coord<f64>,
    dimension: &impl Fn(Dimensions) -> WktDimension,
) -> LineString<f64> {
    LineString::new(
        line_string.coords().map(|coord| map(&coord)).collect(),
        dimension(line_string.dim()),
    )
}

fn polygon_mapped(
    polygon: &impl PolygonTrait<T = f64>,
    map: &impl Fn(&dyn CoordTrait<T = f64>) -> Coord<f64>,
    dimension: &impl Fn(Dimensions) -> WktDimension,
) -> Polygon<f64> {
    let mut rings = Vec::with_capacity(1 + polygon.num_interiors());
    if let Some(exterior) = polygon.exterior() {
        rings.push(line_string_mapped(&exterior, map, dimension));
    }
    rings.extend(
        polygon
            .interiors()
            .map(|interior| line_string_mapped(&interior, map, dimension)),
    );
    Polygon::new(rings, dimension(polygon.dim()))
}

fn coord_without_m(coord: &dyn CoordTrait<T = f64>) -> Coord<f64> {
    Coord {
        x: coord.x(),
        y: coord.y(),
        z: coord_z(coord),
        m: None,
    }
}

fn coord_z(coord: &dyn CoordTrait<T = f64>) -> Option<f64> {
    has_z(coord.dim()).then(|| coord.nth_or_panic(2))
}

fn has_z(dimension: Dimensions) -> bool {
    matches!(
        dimension,
        Dimensions::Xyz | Dimensions::Xyzm | Dimensions::Unknown(3) | Dimensions::Unknown(4)
    )
}

fn wkt_dimension_without_m(dimension: Dimensions) -> WktDimension {
    match dimension {
        Dimensions::Xy | Dimensions::Unknown(2) => WktDimension::XY,
        Dimensions::Xyz | Dimensions::Unknown(3) => WktDimension::XYZ,
        Dimensions::Xym => WktDimension::XY,
        Dimensions::Xyzm | Dimensions::Unknown(4) => WktDimension::XYZ,
        Dimensions::Unknown(_) => WktDimension::XY,
    }
}

#[cfg(test)]
mod tests {
    use geo_traits::{CoordTrait, GeometryTrait, GeometryType};
    use wkb::reader::Dimension;
    use wkt::Wkt;

    use super::*;

    fn encode(wkt: &str) -> Vec<u8> {
        let geometry: Wkt<f64> = wkt.parse().unwrap();
        let mut bytes = Vec::new();
        write_geometry(&mut bytes, &geometry, &WriteOptions::default()).unwrap();
        bytes
    }

    #[test]
    fn strip_m_keeps_xyz_coordinates() {
        let input = encode("LINESTRING ZM (0 0 1 2,4 0 5 6)");
        assert_eq!(Wkb::try_new(&input).unwrap().dimension(), Dimension::Xyzm);

        let output = strip_m(&input).unwrap();
        let parsed = Wkb::try_new(&output).unwrap();
        assert_eq!(parsed.dimension(), Dimension::Xyz);
        let GeometryType::LineString(line) = parsed.as_type() else {
            panic!("normalized geometry is not a LineString");
        };
        assert_eq!(line.num_coords(), 2);
        let first = line.coord(0).unwrap();
        assert_eq!(
            (first.x(), first.y(), first.nth_or_panic(2)),
            (0.0, 0.0, 1.0)
        );
        let second = line.coord(1).unwrap();
        assert_eq!(
            (second.x(), second.y(), second.nth_or_panic(2)),
            (4.0, 0.0, 5.0)
        );
    }

    #[test]
    fn strip_m_removes_measure_only_coordinates() {
        let input = encode("POINT M (1 2 3)");
        assert_eq!(Wkb::try_new(&input).unwrap().dimension(), Dimension::Xym);

        let output = strip_m(&input).unwrap();
        let parsed = Wkb::try_new(&output).unwrap();
        assert_eq!(parsed.dimension(), Dimension::Xy);
        let GeometryType::Point(point) = parsed.as_type() else {
            panic!("normalized geometry is not a Point");
        };
        let coord = point.coord().unwrap();
        assert_eq!((coord.x(), coord.y()), (1.0, 2.0));
        assert!(coord.nth(2).is_none());
    }

    #[test]
    fn restore_z_rebuilds_xyz_coordinates() {
        let input = encode("LINESTRING(0 0,10 0)");
        let output = restore_z(&input, &|x, _y| x / 10.0).unwrap();
        let parsed = Wkb::try_new(&output).unwrap();
        assert_eq!(parsed.dimension(), Dimension::Xyz);
        let GeometryType::LineString(line) = parsed.as_type() else {
            panic!("restored geometry is not a LineString");
        };
        assert_eq!(line.coord(0).unwrap().nth_or_panic(2), 0.0);
        assert_eq!(line.coord(1).unwrap().nth_or_panic(2), 1.0);
    }

    #[test]
    fn restore_z_keeps_existing_ordinates() {
        let input = encode("POINT Z (1 2 3)");
        let output = restore_z(&input, &|_x, _y| 9.0).unwrap();
        let parsed = Wkb::try_new(&output).unwrap();
        assert_eq!(parsed.dimension(), Dimension::Xyz);
        let GeometryType::Point(point) = parsed.as_type() else {
            panic!("restored geometry is not a Point");
        };
        assert_eq!(point.coord().unwrap().nth_or_panic(2), 3.0);
    }
}
