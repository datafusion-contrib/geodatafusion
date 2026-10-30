use geoarrow_schema::error::GeoArrowError;

pub(crate) fn empty_point_error() -> GeoArrowError {
    GeoArrowError::IncorrectGeometryType("geo crate does not support empty points.".to_string())
}

/// Normalize algorithms that return either a point or an optional point.
pub(crate) fn point_or_none(point: impl Into<Option<geo::Point<f64>>>) -> Option<geo::Point<f64>> {
    point.into()
}

/// Run an operation on the concrete `geo` type behind a borrowed GeoArrow geometry.
///
/// This avoids constructing a generic `geo::Geometry` for every row. Empty
/// points and multi-points retain the error behavior of `geometry_to_geo`.
macro_rules! with_owned_geo_geometry {
    ($geometry:expr, |$owned:ident| $operation:expr) => {{
        use geo_traits::GeometryType;
        use geo_traits::to_geo::{
            ToGeoLine, ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint, ToGeoMultiPolygon,
            ToGeoPoint, ToGeoPolygon, ToGeoRect, ToGeoTriangle,
        };

        let geometry = $geometry;
        match geo_traits::GeometryTrait::as_type(geometry) {
            GeometryType::Point(geometry) => {
                let $owned = geometry
                    .try_to_point()
                    .ok_or_else($crate::udf::geo::util::empty_point_error)?;
                $operation
            }
            GeometryType::Line(geometry) => {
                let $owned = geometry.to_line();
                $operation
            }
            GeometryType::LineString(geometry) => {
                let $owned = geometry.to_line_string();
                $operation
            }
            GeometryType::Polygon(geometry) => {
                let $owned = geometry.to_polygon();
                $operation
            }
            GeometryType::MultiPoint(geometry) => {
                let $owned = geometry
                    .try_to_multi_point()
                    .ok_or_else($crate::udf::geo::util::empty_point_error)?;
                $operation
            }
            GeometryType::MultiLineString(geometry) => {
                let $owned = geometry.to_multi_line_string();
                $operation
            }
            GeometryType::MultiPolygon(geometry) => {
                let $owned = geometry.to_multi_polygon();
                $operation
            }
            GeometryType::GeometryCollection(geometry) => {
                let $owned =
                    geo_traits::to_geo::ToGeoGeometryCollection::try_to_geometry_collection(
                        geometry,
                    )
                    .ok_or_else($crate::udf::geo::util::empty_point_error)?;
                $operation
            }
            GeometryType::Rect(geometry) => {
                let $owned = geometry.to_rect();
                $operation
            }
            GeometryType::Triangle(geometry) => {
                let $owned = geometry.to_triangle();
                $operation
            }
        }
    }};
}

pub(crate) use with_owned_geo_geometry;
