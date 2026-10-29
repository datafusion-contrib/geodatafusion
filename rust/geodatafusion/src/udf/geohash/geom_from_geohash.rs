//! `ST_GeomFromGeoHash`.

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_schema::{CoordType, Dimension, Metadata, PolygonType};
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString,
    Polygon as WktPolygon,
};

use crate::udf::native::io::format::coerce_text_with_optional_ints;

const BASE32: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeomFromGeoHash {
    signature: Signature,
}

impl GeomFromGeoHash {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for GeomFromGeoHash {
    fn default() -> Self {
        Self::new()
    }
}

static GEOM_FROM_GEOHASH_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromGeoHash {
    fn name(&self) -> &str {
        "st_geomfromgeohash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_text_with_optional_ints(arg_types, 1, "ST_GeomFromGeoHash")
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_GeomFromGeoHash output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        let geo_type = PolygonType::new(Dimension::XY, Arc::new(Metadata::default()))
            .with_coord_type(CoordType::Separated);
        Ok(geo_type
            .to_field(input_field.name(), input_field.is_nullable())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
        let geo_type =
            PolygonType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
        let mut builder = PolygonBuilder::new(geo_type);

        for row in 0..arrays[0].len() {
            let text = if arrays[0].is_null(row) {
                None
            } else {
                Some(arrays[0].as_string::<i32>().value(row))
            };
            let Some(text) = text else {
                builder
                    .push_polygon(Option::<&WktPolygon<f64>>::None)
                    .map_err(crate::error::GeoDataFusionError::GeoArrow)?;
                continue;
            };

            let precision = arrays.get(1).and_then(|array| precision_value(array, row));
            let bounds = decode_bounds(text, precision).map_err(DataFusionError::Execution)?;
            let polygon = bounds_to_polygon(bounds);
            builder
                .push_polygon(Some(&polygon))
                .map_err(crate::error::GeoDataFusionError::GeoArrow)?;
        }

        Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_GEOHASH_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a polygon representing the requested GeoHash bounds. Precision selects \
                 how many hash characters to use; NULL, negative, or oversized precision uses \
                 the full hash and zero selects the whole world. The result has unknown SRID.",
                "ST_GeomFromGeoHash(geohash[, precision])",
            )
            .with_argument("geohash", "GeoHash text")
            .with_argument("precision", "optional number of hash characters")
            .build()
        }))
    }
}

fn precision_value(array: &arrow_array::ArrayRef, row: usize) -> Option<i32> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Int64 => {
            let value = array
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row);
            Some(i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX }))
        }
        DataType::Null => None,
        other => panic!("unexpected geohash precision array type {other}"),
    }
}

#[derive(Debug, Clone, Copy)]
struct GeoBounds {
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
}

fn decode_bounds(hash: &str, precision: Option<i32>) -> std::result::Result<GeoBounds, String> {
    let hash = hash.to_ascii_lowercase();
    let bytes = hash.as_bytes();
    let precision = match precision {
        Some(value) if value >= 0 => (value as usize).min(bytes.len()),
        _ => bytes.len(),
    };

    let mut bounds = GeoBounds {
        min_lon: -180.0,
        min_lat: -90.0,
        max_lon: 180.0,
        max_lat: 90.0,
    };
    let mut is_even = true;

    for byte in bytes.iter().take(precision) {
        let index = BASE32
            .iter()
            .position(|candidate| candidate == byte)
            .ok_or_else(|| {
                format!(
                    "decode_geohash_bbox: Invalid character '{}'",
                    char::from(*byte)
                )
            })? as u8;
        for mask in [16u8, 8, 4, 2, 1] {
            let lower = index & mask == 0;
            if is_even {
                let middle = (bounds.min_lon + bounds.max_lon) / 2.0;
                if lower {
                    bounds.max_lon = middle;
                } else {
                    bounds.min_lon = middle;
                }
            } else {
                let middle = (bounds.min_lat + bounds.max_lat) / 2.0;
                if lower {
                    bounds.max_lat = middle;
                } else {
                    bounds.min_lat = middle;
                }
            }
            is_even = !is_even;
        }
    }

    Ok(bounds)
}

fn bounds_to_polygon(bounds: GeoBounds) -> WktPolygon<f64> {
    let ring = WktLineString::new(
        vec![
            WktCoord {
                x: bounds.min_lon,
                y: bounds.min_lat,
                z: None,
                m: None,
            },
            WktCoord {
                x: bounds.min_lon,
                y: bounds.max_lat,
                z: None,
                m: None,
            },
            WktCoord {
                x: bounds.max_lon,
                y: bounds.max_lat,
                z: None,
                m: None,
            },
            WktCoord {
                x: bounds.max_lon,
                y: bounds.min_lat,
                z: None,
                m: None,
            },
            WktCoord {
                x: bounds.min_lon,
                y: bounds.min_lat,
                z: None,
                m: None,
            },
        ],
        WktDimension::XY,
    );
    WktPolygon::new(vec![ring], WktDimension::XY)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn polygon_text(bounds: GeoBounds) -> String {
        bounds_to_polygon(bounds).to_string()
    }

    #[test]
    fn geohash_bounds_match_postgis_examples() {
        assert_eq!(
            polygon_text(decode_bounds("9qqj7nmxncgyy4d0dbxqz0", Some(4)).unwrap()),
            "POLYGON((-115.3125 36.03515625,-115.3125 36.2109375,-114.9609375 36.2109375,-114.9609375 36.03515625,-115.3125 36.03515625))"
        );
        assert_eq!(
            polygon_text(decode_bounds("", None).unwrap()),
            "POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))"
        );
    }

    #[test]
    fn uppercase_and_invalid_characters_follow_postgis() {
        assert_eq!(
            decode_bounds("U4PRUYDQQVJ", None).unwrap().min_lat,
            decode_bounds("u4pruydqqvj", None).unwrap().min_lat
        );
        assert!(
            decode_bounds("abc", None)
                .unwrap_err()
                .contains("Invalid character")
        );
    }
}
