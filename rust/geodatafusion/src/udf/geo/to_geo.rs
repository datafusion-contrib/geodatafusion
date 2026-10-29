//! Conversion from GeoArrow data to `geo-types` for planimetric work.
//!
//! `geo-types` cannot represent empty points, so the conversion below keeps the
//! point set of a geometry instead of its exact structure: an empty point
//! becomes an empty collection and empty points are removed from
//! multi-geometries and collections. Planar predicates and 2D validity checks
//! only observe the point set, while an empty point contributes no points.

use geo::{CoordNum, Geometry, GeometryCollection, MultiPoint};
use geo_traits::to_geo::ToGeoPoint;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, MultiPointTrait, PointTrait,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;

/// Iterator over optional `geo::Geometry` values converted from GeoArrow data.
///
/// Conversion happens inside the iterator, so a geometry that is never paired
/// with a non-null geometry is never converted.
pub(crate) type GeoGeometryIter<'a> =
    Box<dyn Iterator<Item = Option<GeoArrowResult<Geometry<f64>>>> + 'a>;

pub(crate) fn to_geo_iter(array: &dyn GeoArrowArray) -> GeoArrowResult<GeoGeometryIter<'_>> {
    downcast_geoarrow_array!(array, to_geo_iter_impl)
}

fn to_geo_iter_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<GeoGeometryIter<'a>> {
    Ok(Box::new(array.iter().map(|item| {
        item.map(|geometry| geometry.and_then(|geometry| geometry_to_geo_for_topology(&geometry)))
    })))
}

/// Convert a GeoArrow geometry to `geo::Geometry` for 2D planimetric work.
///
/// Non-empty geometries use the same conversion as the existing GeoArrow
/// helpers; only geometries that hold empty points change shape so that the
/// point set is preserved.
pub(crate) fn geometry_to_geo_for_topology<T: CoordNum>(
    geometry: &impl GeometryTrait<T = T>,
) -> GeoArrowResult<Geometry<T>> {
    match geometry.as_type() {
        GeometryType::Point(point) => {
            if point.coord().is_none() {
                Ok(Geometry::GeometryCollection(GeometryCollection::empty()))
            } else {
                geometry_to_geo(geometry)
            }
        }
        GeometryType::MultiPoint(multi_point) => {
            let points = multi_point
                .points()
                .filter_map(|point| point.try_to_point())
                .collect();
            Ok(Geometry::MultiPoint(MultiPoint::new(points)))
        }
        GeometryType::GeometryCollection(collection) => {
            let geometries = collection
                .geometries()
                .filter(|child| {
                    !matches!(child.as_type(), GeometryType::Point(point) if point.coord().is_none())
                })
                .map(|child| geometry_to_geo_for_topology(&child))
                .collect::<GeoArrowResult<_>>()?;
            Ok(Geometry::GeometryCollection(GeometryCollection::new_from(
                geometries,
            )))
        }
        _ => geometry_to_geo(geometry),
    }
}
