//! CRS classification and verified authority axis order.
//!
//! Geographic classification uses bundled EPSG PROJ definitions or explicit
//! GeoArrow CRS metadata. Authority axis order uses the EPSG subset verified
//! by the GML oracle. Unknown axis order is an error; unknown classification
//! lets callers such as TWKB request an explicit precision.

use datafusion::error::{DataFusionError, Result};
use geoarrow_schema::{CrsType, Metadata};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrsKind {
    /// Longitude/latitude based, matching PostGIS `srid_is_latlong`.
    Geographic,
    /// Projected, geocentric, vertical, or otherwise not longitude/latitude.
    NonGeographic,
    /// The metadata does not carry enough information to classify the CRS.
    Unknown,
}

pub(crate) fn classify_metadata(metadata: &Metadata) -> CrsKind {
    let crs = metadata.crs();
    let Some(value) = crs.crs_value() else {
        // PostGIS leaves the base precision unchanged for SRID 0.
        return CrsKind::NonGeographic;
    };
    match crs.crs_type() {
        Some(CrsType::Projjson) => classify_projjson(value),
        Some(CrsType::Wkt2_2019) => classify_wkt(value),
        Some(CrsType::AuthorityCode) => classify_authority(value),
        Some(CrsType::Srid) => classify_epsg_number(value),
        None => CrsKind::Unknown,
    }
}

pub(crate) fn classify_epsg(code: u16) -> CrsKind {
    let Some(definition) = crs_definitions::from_code(code) else {
        return CrsKind::Unknown;
    };
    let proj4 = definition.proj4.trim();
    if proj4.is_empty() {
        return CrsKind::Unknown;
    }
    // `+proj=longlat` is the PROJ token behind `srid_is_latlong`.
    if proj4
        .split_whitespace()
        .any(|token| token == "+proj=longlat" || token == "+proj=latlong")
    {
        CrsKind::Geographic
    } else {
        CrsKind::NonGeographic
    }
}

fn classify_projjson(value: &Value) -> CrsKind {
    let Some(object) = value.as_object() else {
        return CrsKind::Unknown;
    };
    let Some(kind) = object.get("type").and_then(Value::as_str) else {
        return CrsKind::Unknown;
    };
    match kind {
        "GeographicCRS" | "DerivedGeographicCRS" => CrsKind::Geographic,
        "ProjectedCRS"
        | "DerivedProjectedCRS"
        | "GeocentricCRS"
        | "VerticalCRS"
        | "EngineeringCRS"
        | "ParametricCRS"
        | "TemporalCRS" => CrsKind::NonGeographic,
        "GeodeticCRS" => object
            .get("coordinate_system")
            .and_then(Value::as_object)
            .and_then(|system| system.get("subtype"))
            .and_then(Value::as_str)
            .map_or(CrsKind::Unknown, |subtype| match subtype {
                "ellipsoidal" => CrsKind::Geographic,
                "Cartesian" => CrsKind::NonGeographic,
                _ => CrsKind::Unknown,
            }),
        _ => CrsKind::Unknown,
    }
}

fn classify_wkt(value: &Value) -> CrsKind {
    let Some(text) = value.as_str() else {
        return CrsKind::Unknown;
    };
    let text = text.trim_start().to_ascii_uppercase();
    if text.starts_with("GEOGCRS") || text.starts_with("GEOGCS") {
        CrsKind::Geographic
    } else if text.starts_with("PROJCRS") || text.starts_with("PROJCS") {
        CrsKind::NonGeographic
    } else {
        CrsKind::Unknown
    }
}

fn classify_authority(value: &Value) -> CrsKind {
    let Some(text) = value.as_str() else {
        return CrsKind::Unknown;
    };
    let Some((authority, code)) = text.split_once(':') else {
        return CrsKind::Unknown;
    };
    if (authority.eq_ignore_ascii_case("CRS") && code == "84")
        || (authority.eq_ignore_ascii_case("OGC") && code.eq_ignore_ascii_case("CRS84"))
    {
        return CrsKind::Geographic;
    }
    if !authority.eq_ignore_ascii_case("EPSG") {
        return CrsKind::Unknown;
    }
    classify_epsg_str(code)
}

fn classify_epsg_number(value: &Value) -> CrsKind {
    value.as_str().map_or(CrsKind::Unknown, classify_epsg_str)
}

fn classify_epsg_str(code: &str) -> CrsKind {
    code.parse::<u16>().map_or(CrsKind::Unknown, classify_epsg)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrsAxisOrder {
    /// Longitude/easting first, as PostGIS stores internal coordinates.
    GisFriendly,
    /// Latitude first, used by authoritative GML 3 axis order.
    LatitudeLongitude,
    /// Geocentric X/Y/Z, which is neither geographic nor projected.
    Geocentric,
}

/// Return authority axis order only for the verified EPSG subset.
pub(crate) fn epsg_axis_order(srid: i32) -> Result<CrsAxisOrder> {
    match srid {
        4258 | 4267 | 4269 | 4326 | 4490 | 4979 => Ok(CrsAxisOrder::LatitudeLongitude),
        4978 => Ok(CrsAxisOrder::Geocentric),
        3857 => Ok(CrsAxisOrder::GisFriendly),
        _ => Err(DataFusionError::Execution(format!(
            "Authority axis order for EPSG:{srid} is not in the supported table; refusing to guess axis order"
        ))),
    }
}

#[cfg(test)]
mod classification_tests {
    use geoarrow_schema::{Crs, Metadata};

    use super::{CrsKind, classify_epsg, classify_metadata};

    #[test]
    fn classifies_epsg_codes_without_numeric_ranges() {
        assert_eq!(classify_epsg(4326), CrsKind::Geographic);
        assert_eq!(classify_epsg(4269), CrsKind::Geographic);
        assert_eq!(classify_epsg(4979), CrsKind::Geographic);
        assert_eq!(classify_epsg(4978), CrsKind::NonGeographic);
        assert_eq!(classify_epsg(3857), CrsKind::NonGeographic);
        assert_eq!(classify_epsg(9999), CrsKind::Unknown);
    }

    #[test]
    fn classifies_ogc_crs84_authority_metadata() {
        for authority in ["CRS:84", "OGC:CRS84"] {
            let metadata = Metadata::new(Crs::from_authority_code(authority.to_string()), None);
            assert_eq!(classify_metadata(&metadata), CrsKind::Geographic);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CrsAxisOrder, epsg_axis_order};

    #[test]
    fn distinguishes_geocentric_from_geographic_axis_order() {
        assert_eq!(epsg_axis_order(4978).unwrap(), CrsAxisOrder::Geocentric);
        assert_eq!(
            epsg_axis_order(4979).unwrap(),
            CrsAxisOrder::LatitudeLongitude
        );
        assert_eq!(
            epsg_axis_order(4326).unwrap(),
            CrsAxisOrder::LatitudeLongitude
        );
    }

    #[test]
    fn unknown_srid_errors_instead_of_guessing() {
        assert!(epsg_axis_order(20_001).is_err());
    }
}
