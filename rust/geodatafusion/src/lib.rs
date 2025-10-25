// Temporary
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub(crate) mod data_types;
pub(crate) mod error;
pub mod udf;

use datafusion::prelude::SessionContext;

/// Mount all UDFs defined in geodatafusion
pub fn mount(session_context: &SessionContext) {
    session_context.register_udf(crate::udf::geo::measurement::Area::default().into());
    session_context.register_udf(crate::udf::geo::measurement::Distance::default().into());
    session_context.register_udf(crate::udf::geo::measurement::Length::default().into());

    session_context.register_udf(crate::udf::geo::processing::Centroid::default().into());
    session_context.register_udf(crate::udf::geo::processing::ConvexHull::default().into());
    session_context.register_udf(crate::udf::geo::processing::OrientedEnvelope::default().into());
    session_context.register_udf(crate::udf::geo::processing::PointOnSurface::default().into());
    session_context.register_udf(crate::udf::geo::processing::Simplify::default().into());
    session_context
        .register_udf(crate::udf::geo::processing::SimplifyPreserveTopology::default().into());
    session_context.register_udf(crate::udf::geo::processing::SimplifyVW::default().into());

    session_context.register_udf(crate::udf::geo::relationships::Contains::default().into());
    session_context.register_udf(crate::udf::geo::relationships::CoveredBy::default().into());
    session_context.register_udf(crate::udf::geo::relationships::Covers::default().into());
    session_context.register_udf(crate::udf::geo::relationships::Crosses::default().into());
    session_context.register_udf(crate::udf::geo::relationships::Disjoint::default().into());
    session_context.register_udf(crate::udf::geo::relationships::Equals::default().into());
    session_context.register_udf(crate::udf::geo::relationships::Intersects::default().into());
    session_context.register_udf(crate::udf::geo::relationships::Overlaps::default().into());
    session_context.register_udf(crate::udf::geo::relationships::Touches::default().into());
    session_context.register_udf(crate::udf::geo::relationships::Within::default().into());

    session_context.register_udf(crate::udf::geo::validation::IsValid::default().into());
    session_context.register_udf(crate::udf::geo::validation::IsValidReason::default().into());

    session_context.register_udf(crate::udf::geohash::GeoHash::default().into());
    session_context.register_udf(crate::udf::geohash::Box2DFromGeoHash::default().into());
    session_context.register_udf(crate::udf::geohash::PointFromGeoHash::default().into());

    session_context.register_udf(crate::udf::native::accessors::CoordDim::default().into());
    session_context.register_udf(crate::udf::native::accessors::NDims::default().into());
    session_context.register_udf(crate::udf::native::accessors::GeometryType::default().into());
    session_context.register_udf(crate::udf::native::accessors::ST_GeometryType::default().into());
    session_context.register_udf(crate::udf::native::accessors::EndPoint::default().into());
    session_context.register_udf(crate::udf::native::accessors::StartPoint::default().into());
    session_context.register_udf(crate::udf::native::accessors::NPoints::default().into());
    session_context.register_udf(crate::udf::native::accessors::NumInteriorRings::default().into());
    session_context.register_udf(crate::udf::native::accessors::M::default().into());
    session_context.register_udf(crate::udf::native::accessors::X::default().into());
    session_context.register_udf(crate::udf::native::accessors::Y::default().into());
    session_context.register_udf(crate::udf::native::accessors::Z::default().into());

    session_context.register_udf(crate::udf::native::bounding_box::Box2D::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::Box3D::default().into());
    session_context.register_udaf(crate::udf::native::bounding_box::Extent::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::XMax::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::XMin::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::YMax::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::YMin::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::ZMax::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::ZMin::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::MakeBox2D::default().into());
    session_context.register_udf(crate::udf::native::bounding_box::MakeBox3D::default().into());

    session_context.register_udf(crate::udf::native::constructors::MakePoint::default().into());
    session_context.register_udf(crate::udf::native::constructors::MakePointM::default().into());
    session_context.register_udf(crate::udf::native::constructors::Point::default().into());
    session_context.register_udf(crate::udf::native::constructors::PointM::default().into());
    session_context.register_udf(crate::udf::native::constructors::PointZ::default().into());
    session_context.register_udf(crate::udf::native::constructors::PointZM::default().into());

    session_context.register_udf(crate::udf::native::io::AsBinary::default().into());
    session_context.register_udf(crate::udf::native::io::GeomFromWKB::default().into());
    session_context.register_udf(crate::udf::native::io::AsText::default().into());
    session_context.register_udf(crate::udf::native::io::GeomFromText::default().into());
}
