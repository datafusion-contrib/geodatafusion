//! Helpers to convert an `impl GeometryTrait` to a `WKT` type.
//!
//! This is **not** a WKT string but rather a geometry representation in the `wkt` crate. We use it
//! because it additionally supports Z and M dimensions.
use geo_traits::*;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use wkt::WktNum;
use wkt::types::*;

fn dim_to_wkt_dim(dim: geo_traits::Dimensions) -> GeoArrowResult<Dimension> {
    match dim {
        geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => Ok(Dimension::XY),
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => Ok(Dimension::XYZ),
        geo_traits::Dimensions::Xym => Ok(Dimension::XYM),
        geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(4) => Ok(Dimension::XYZM),
        _ => Err(GeoArrowError::InvalidGeoArrow(
            "Unsupported coordinate dimension".to_string(),
        )),
    }
}

fn coord_to_wkt_coord<T: WktNum>(coord: &impl CoordTrait<T = T>) -> GeoArrowResult<Coord<T>> {
    match dim_to_wkt_dim(coord.dim())? {
        Dimension::XY => Ok(Coord {
            x: coord.x(),
            y: coord.y(),
            z: None,
            m: None,
        }),
        Dimension::XYZ => Ok(Coord {
            x: coord.x(),
            y: coord.y(),
            z: coord.nth(2),
            m: None,
        }),
        Dimension::XYM => Ok(Coord {
            x: coord.x(),
            y: coord.y(),
            z: None,
            m: coord.nth(2),
        }),
        Dimension::XYZM => Ok(Coord {
            x: coord.x(),
            y: coord.y(),
            z: coord.nth(2),
            m: coord.nth(3),
        }),
        _ => Err(GeoArrowError::InvalidGeoArrow(
            "Unsupported coordinate dimension".to_string(),
        )),
    }
}

fn point_to_wkt_point<T: WktNum>(point: &impl PointTrait<T = T>) -> GeoArrowResult<Point<T>> {
    Ok(Point::new(
        point.coord().map(|c| coord_to_wkt_coord(&c)).transpose()?,
        dim_to_wkt_dim(point.dim())?,
    ))
}

fn line_string_to_wkt_line_string<T: WktNum>(
    ls: &impl LineStringTrait<T = T>,
) -> GeoArrowResult<LineString<T>> {
    let coords = ls
        .coords()
        .map(|c| coord_to_wkt_coord(&c))
        .collect::<GeoArrowResult<Vec<_>>>()?;
    Ok(LineString::new(coords, dim_to_wkt_dim(ls.dim())?))
}

fn polygon_to_wkt_polygon<T: WktNum>(
    poly: &impl PolygonTrait<T = T>,
) -> GeoArrowResult<Polygon<T>> {
    let mut rings = Vec::with_capacity(1 + poly.num_interiors());
    let exterior = if let Some(ext) = poly.exterior() {
        line_string_to_wkt_line_string(&ext)?
    } else {
        LineString::new(vec![], Dimension::XY)
    };
    rings.push(exterior);

    let interiors = poly
        .interiors()
        .map(|int| line_string_to_wkt_line_string(&int))
        .collect::<GeoArrowResult<Vec<_>>>()?;
    rings.extend(interiors);

    Ok(Polygon::new(rings, dim_to_wkt_dim(poly.dim())?))
}

fn multi_point_to_wkt_multi_point<T: WktNum>(
    mp: &impl MultiPointTrait<T = T>,
) -> GeoArrowResult<MultiPoint<T>> {
    let points = mp
        .points()
        .map(|pt| point_to_wkt_point(&pt))
        .collect::<GeoArrowResult<Vec<_>>>()?;
    Ok(MultiPoint::new(points, dim_to_wkt_dim(mp.dim())?))
}

fn multi_line_string_to_wkt_multi_line_string<T: WktNum>(
    mls: &impl MultiLineStringTrait<T = T>,
) -> GeoArrowResult<MultiLineString<T>> {
    let lines = mls
        .line_strings()
        .map(|ls| line_string_to_wkt_line_string(&ls))
        .collect::<GeoArrowResult<Vec<_>>>()?;
    Ok(MultiLineString::new(lines, dim_to_wkt_dim(mls.dim())?))
}

fn multi_polygon_to_wkt_multi_polygon<T: WktNum>(
    mp: &impl MultiPolygonTrait<T = T>,
) -> GeoArrowResult<MultiPolygon<T>> {
    let polys = mp
        .polygons()
        .map(|poly| polygon_to_wkt_polygon(&poly))
        .collect::<GeoArrowResult<Vec<_>>>()?;
    Ok(MultiPolygon::new(polys, dim_to_wkt_dim(mp.dim())?))
}

pub(crate) fn geometry_to_wkt_geometry<T: WktNum>(
    geom: &impl GeometryTrait<T = T>,
) -> GeoArrowResult<wkt::Wkt<T>> {
    match geom.as_type() {
        geo_traits::GeometryType::Point(pt) => Ok(wkt::Wkt::Point(point_to_wkt_point(pt)?)),
        geo_traits::GeometryType::LineString(ls) => {
            Ok(wkt::Wkt::LineString(line_string_to_wkt_line_string(ls)?))
        }
        geo_traits::GeometryType::Polygon(poly) => {
            Ok(wkt::Wkt::Polygon(polygon_to_wkt_polygon(poly)?))
        }
        geo_traits::GeometryType::MultiPoint(mp) => {
            Ok(wkt::Wkt::MultiPoint(multi_point_to_wkt_multi_point(mp)?))
        }
        geo_traits::GeometryType::MultiLineString(mls) => Ok(wkt::Wkt::MultiLineString(
            multi_line_string_to_wkt_multi_line_string(mls)?,
        )),
        geo_traits::GeometryType::MultiPolygon(mp) => Ok(wkt::Wkt::MultiPolygon(
            multi_polygon_to_wkt_multi_polygon(mp)?,
        )),
        geo_traits::GeometryType::GeometryCollection(gc) => Ok(wkt::Wkt::GeometryCollection(
            geometry_collection_to_wkt_geometry_collection(gc)?,
        )),
        _ => Err(GeoArrowError::InvalidGeoArrow(
            "Unsupported geometry type".to_string(),
        )),
    }
}

fn geometry_collection_to_wkt_geometry_collection<T: WktNum>(
    gc: &impl GeometryCollectionTrait<T = T>,
) -> GeoArrowResult<GeometryCollection<T>> {
    let geoms = gc
        .geometries()
        .map(|g| geometry_to_wkt_geometry(&g))
        .collect::<GeoArrowResult<Vec<_>>>()?;
    Ok(GeometryCollection::new(geoms, dim_to_wkt_dim(gc.dim())?))
}
