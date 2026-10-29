//! GeoHash base32 encoding and decoding shared by the GeoHash functions.
//!
//! The encoder follows the public GeoHash base32 algorithm; the reference
//! encoder derives from David Troy's MIT-licensed `geohash-native.c`. The
//! bbox-prefix selection below is derived independently from cell containment
//! rather than translated from the reference C implementation.

use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};

const BASE32: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Shared coercion for `text [, integer]` GeoHash arguments.
pub(super) fn coerce_text_and_precision(
    arg_types: &[DataType],
    function_name: &str,
) -> Result<Vec<DataType>> {
    if arg_types.is_empty() || arg_types.len() > 2 {
        return Err(DataFusionError::Plan(format!(
            "{function_name} expects a text argument and an optional precision argument"
        )));
    }

    let mut coerced = Vec::with_capacity(arg_types.len());
    for (index, data_type) in arg_types.iter().enumerate() {
        if index == 0 {
            if matches!(
                data_type,
                DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                coerced.push(data_type.clone());
            } else {
                return Err(DataFusionError::Plan(format!(
                    "{function_name} expects a text argument, got {data_type}"
                )));
            }
        } else if data_type == &DataType::Null || data_type.is_integer() {
            coerced.push(DataType::Int64);
        } else {
            return Err(DataFusionError::Plan(format!(
                "{function_name} expects an integer precision argument, got {data_type}"
            )));
        }
    }
    Ok(coerced)
}

/// Upper bound for explicit `ST_GeoHash` precision requests.
///
/// The bound keeps a malformed integer argument from causing an unbounded
/// allocation while covering every practical GeoHash length.
pub(crate) const MAX_GEOHASH_CHARS: usize = 1_000_000;

/// Longitude/latitude bounds produced by decoding or subdividing a GeoHash cell.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Bounds {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

impl Bounds {
    pub(crate) fn center(&self) -> (f64, f64) {
        (
            self.min_lon + (self.max_lon - self.min_lon) / 2.0,
            self.min_lat + (self.max_lat - self.min_lat) / 2.0,
        )
    }
}

/// A rectangular longitude/latitude cell addressed by a GeoHash prefix.
#[derive(Debug, Clone, Copy)]
struct Cell {
    lon: [f64; 2],
    lat: [f64; 2],
}

impl Cell {
    fn world() -> Self {
        Self {
            lon: [-180.0, 180.0],
            lat: [-90.0, 90.0],
        }
    }

    fn lon_middle(&self) -> f64 {
        (self.lon[0] + self.lon[1]) / 2.0
    }

    fn lat_middle(&self) -> f64 {
        (self.lat[0] + self.lat[1]) / 2.0
    }

    fn split_lon(self, upper: bool) -> Self {
        let middle = self.lon_middle();
        Self {
            lon: if upper {
                [middle, self.lon[1]]
            } else {
                [self.lon[0], middle]
            },
            lat: self.lat,
        }
    }

    fn split_lat(self, upper: bool) -> Self {
        let middle = self.lat_middle();
        Self {
            lon: self.lon,
            lat: if upper {
                [middle, self.lat[1]]
            } else {
                [self.lat[0], middle]
            },
        }
    }

    /// Return the half containing `min..max`, or `None` when the range touches
    /// or crosses the split.
    fn lon_half_for(&self, min: f64, max: f64) -> Option<bool> {
        let middle = self.lon_middle();
        if min > middle {
            Some(true)
        } else if max < middle {
            Some(false)
        } else {
            None
        }
    }

    /// Return the half containing `min..max`, or `None` when the range touches
    /// or crosses the split.
    fn lat_half_for(&self, min: f64, max: f64) -> Option<bool> {
        let middle = self.lat_middle();
        if min > middle {
            Some(true)
        } else if max < middle {
            Some(false)
        } else {
            None
        }
    }

    fn bounds(&self) -> Bounds {
        Bounds {
            min_lon: self.lon[0],
            min_lat: self.lat[0],
            max_lon: self.lon[1],
            max_lat: self.lat[1],
        }
    }
}

/// Decode the first `precision` characters of `hash`.
///
/// `None`, negative precision, and precision beyond the input length all
/// decode the whole string, matching the reference's default precision.
pub(crate) fn decode_bounds(hash: &str, precision: Option<i64>) -> Result<Bounds, String> {
    let character_limit = match precision {
        None => hash.len(),
        Some(value) if value < 0 => hash.len(),
        Some(value) => (value as usize).min(hash.len()),
    };

    let mut cell = Cell::world();
    let mut bit_index = 0_usize;
    for &raw in hash.as_bytes().iter().take(character_limit) {
        let character = raw.to_ascii_lowercase();
        let Some(value) = BASE32.iter().position(|&candidate| candidate == character) else {
            return Err(format!(
                "decode_geohash_bbox: Invalid character '{}'",
                raw as char
            ));
        };

        for offset in 0..5_u32 {
            let upper = value & (1_usize << (4 - offset)) != 0;
            cell = if bit_index.is_multiple_of(2) {
                cell.split_lon(upper)
            } else {
                cell.split_lat(upper)
            };
            bit_index += 1;
        }
    }

    Ok(cell.bounds())
}

/// Encode a coordinate pair into `precision` base32 characters.
///
/// The bit order is the public GeoHash order: longitude first, then latitude,
/// alternating across character boundaries.
pub(crate) fn encode(longitude: f64, latitude: f64, precision: usize) -> String {
    let mut output = String::with_capacity(precision);
    let mut cell = Cell::world();
    let mut bit_index = 0_usize;

    for _ in 0..precision {
        let mut value = 0_u8;
        for offset in 0..5_u32 {
            let upper = if bit_index.is_multiple_of(2) {
                let upper = longitude >= cell.lon_middle();
                cell = cell.split_lon(upper);
                upper
            } else {
                let upper = latitude >= cell.lat_middle();
                cell = cell.split_lat(upper);
                upper
            };
            if upper {
                value |= 1_u8 << (4 - offset);
            }
            bit_index += 1;
        }
        output.push(BASE32[value as usize] as char);
    }

    output
}

/// Longest base32 prefix whose cell still contains the whole bounds.
///
/// Each base32 character adds five alternating longitude/latitude
/// bisections. The common prefix can grow only while the bounds stay wholly
/// inside one half of each split; once an edge reaches or crosses the split,
/// the common prefix ends. Count the bits before that split and convert the
/// count to base32 characters.
pub(crate) fn cell_prefix_chars(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> usize {
    if min_x == max_x && min_y == max_y {
        // A point has no extent that could cut a cell. The reference resolves
        // a point at 20 characters, so preserve that observable precision.
        return 20;
    }

    let mut cell = Cell::world();
    let mut bits = 0_usize;

    while let Some(lon_upper) = cell.lon_half_for(min_x, max_x) {
        cell = cell.split_lon(lon_upper);
        bits += 1;

        let Some(lat_upper) = cell.lat_half_for(min_y, max_y) else {
            break;
        };
        cell = cell.split_lat(lat_upper);
        bits += 1;
    }

    bits / 5
}
