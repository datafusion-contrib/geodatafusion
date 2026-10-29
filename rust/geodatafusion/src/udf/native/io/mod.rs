//! Geometry Input and Output

mod column;
mod common;
mod coordinate;
mod ewkb;
mod ewkt;
pub(crate) mod format;
mod geobuf;
mod geojson;
mod geojson_record;
mod gml;
mod kml;
mod lat_lon;
mod polyline;
mod svg;
pub(crate) mod twkb;
mod wkb;
mod wkb_validate;
pub(crate) mod wkt;
mod xml;

pub(crate) use common::decode_geometry_array;
pub use ewkb::{AsEWKB, AsHEXEWKB, GeomFromEWKB};
pub use ewkt::{AsEWKT, GeomFromEWKT};
pub use geobuf::AsGeobuf;
pub use geojson::{AsGeoJSON, GeomFromGeoJSON};
pub use gml::{AsGML, GeomFromGML};
pub use kml::{AsKML, GeomFromKML};
pub use lat_lon::AsLatLonText;
pub use polyline::{AsEncodedPolyline, LineFromEncodedPolyline};
pub use svg::AsSVG;
pub use twkb::{AsTWKB, GeomFromTWKB};
pub use wkb::{AsBinary, GeomFromWKB};
pub(crate) use wkb_validate::validate_wkb_array;
pub use wkt::{AsText, GeomFromText};

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(AsGeoJSON::default().into());
    session_context.register_udf(AsEncodedPolyline::default().into());
    session_context.register_udf(AsGML::default().into());
    session_context.register_udf(AsKML::default().into());
    session_context.register_udf(AsLatLonText::default().into());
    session_context.register_udf(AsBinary.into());
    session_context.register_udf(AsSVG::default().into());
    session_context.register_udf(GeomFromGeoJSON.into());
    session_context.register_udf(LineFromEncodedPolyline::default().into());
    session_context.register_udf(AsEWKB::default().into());
    session_context.register_udf(AsHEXEWKB::default().into());
    session_context.register_udf(GeomFromEWKB::default().into());
    session_context.register_udf(GeomFromWKB::default().into());
    session_context.register_udf(
        wkb::TypedGeomFromWKB::new("st_linefromwkb", common::GeometryKind::LineString)
            .with_aliases(vec!["st_linestringfromwkb".to_string()])
            .into(),
    );
    session_context.register_udf(
        wkb::TypedGeomFromWKB::new("st_pointfromwkb", common::GeometryKind::Point).into(),
    );
    session_context.register_udf(
        wkb::TypedGeomFromWKB::new("st_mpolyfromwkb", common::GeometryKind::MultiPolygon)
            .with_aliases(vec!["st_multipolygonfromwkb".to_string()])
            .into(),
    );
    session_context.register_udf(AsEWKT::default().into());
    session_context.register_udaf(AsGeobuf::default().into());
    session_context.register_udf(AsTWKB::default().into());
    session_context.register_udf(AsText.into());
    session_context.register_udf(GeomFromTWKB::default().into());
    session_context.register_udf(GeomFromEWKT::default().into());
    session_context.register_udf(GeomFromGML::default().into());
    session_context.register_udf(GeomFromKML::default().into());
    session_context.register_udf(GeomFromText::default().into());
    session_context.register_udf(
        wkt::TypedGeomFromText::new(
            "st_geomcollfromtext",
            common::GeometryKind::GeometryCollection,
        )
        .into(),
    );
    session_context.register_udf(
        wkt::TypedGeomFromText::new("st_linefromtext", common::GeometryKind::LineString).into(),
    );
    session_context.register_udf(
        wkt::TypedGeomFromText::new("st_mlinefromtext", common::GeometryKind::MultiLineString)
            .into(),
    );
    session_context.register_udf(
        wkt::TypedGeomFromText::new("st_mpointfromtext", common::GeometryKind::MultiPoint).into(),
    );
    session_context.register_udf(
        wkt::TypedGeomFromText::new("st_mpolyfromtext", common::GeometryKind::MultiPolygon).into(),
    );
    session_context.register_udf(
        wkt::TypedGeomFromText::new("st_pointfromtext", common::GeometryKind::Point).into(),
    );
    session_context.register_udf(
        wkt::TypedGeomFromText::new("st_polygonfromtext", common::GeometryKind::Polygon).into(),
    );
}
