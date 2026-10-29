use arrow_array::Array;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use geoarrow_array::array::{WkbArray, WkbViewArray};

use crate::error::GeoDataFusionError;

const EWKB_FLAG_Z: u32 = 0x8000_0000;
const EWKB_FLAG_M: u32 = 0x4000_0000;
const EWKB_FLAG_SRID: u32 = 0x2000_0000;
const EWKB_FLAG_BBOX: u32 = 0x1000_0000;
const MAX_WKB_NESTING_DEPTH: usize = 128;

#[derive(Clone, Copy)]
enum Dimension {
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

impl Dimension {
    fn coord_bytes(self) -> usize {
        match self {
            Self::Xy => 16,
            Self::Xyz | Self::Xym => 24,
            Self::Xyzm => 32,
        }
    }
}

#[derive(Clone, Copy)]
struct Header {
    base_type: u8,
    dimension: Dimension,
}

/// Rejects WKB type codes that the `wkb` parser accepts but PostGIS does not.
///
/// The parser masks the geometry type to the low three bits and ignores unknown
/// flags, so e.g. type 63046 is silently treated as MultiPolygon. Validate the
/// full header and all nested headers before any geometry is built.
pub(crate) fn validate_wkb_array(array: &dyn Array, field: &Field) -> Result<()> {
    match array.data_type() {
        DataType::Binary | DataType::LargeBinary => {
            let wkb = WkbArray::try_from((array, field)).map_err(GeoDataFusionError::GeoArrow)?;
            validate_values(wkb.inner().iter())
        }
        DataType::BinaryView => {
            let view =
                WkbViewArray::try_from((array, field)).map_err(GeoDataFusionError::GeoArrow)?;
            let wkb: WkbArray = view.into();
            validate_values(wkb.inner().iter())
        }
        _ => Ok(()),
    }
}

fn validate_values<'a>(values: impl Iterator<Item = Option<&'a [u8]>>) -> Result<()> {
    for bytes in values.flatten() {
        validate_wkb_bytes(bytes).map_err(|message| {
            DataFusionError::Execution(format!("invalid WKB input: {message}"))
        })?;
    }
    Ok(())
}

fn validate_wkb_bytes(bytes: &[u8]) -> std::result::Result<(), String> {
    let mut cursor = WkbCursor {
        bytes,
        position: 0,
        depth: 0,
    };
    cursor.parse_geometry(None)?;
    Ok(())
}

struct WkbCursor<'a> {
    bytes: &'a [u8],
    position: usize,
    depth: usize,
}

impl WkbCursor<'_> {
    fn read_u8(&mut self) -> std::result::Result<u8, String> {
        let byte = *self
            .bytes
            .get(self.position)
            .ok_or_else(|| "WKB buffer ended in the middle of a header".to_string())?;
        self.position += 1;
        Ok(byte)
    }

    fn read_u32(&mut self, little_endian: bool) -> std::result::Result<u32, String> {
        let bytes = self
            .bytes
            .get(self.position..self.position + 4)
            .ok_or_else(|| "WKB buffer ended in the middle of an integer".to_string())?;
        self.position += 4;
        Ok(if little_endian {
            u32::from_le_bytes(bytes.try_into().unwrap())
        } else {
            u32::from_be_bytes(bytes.try_into().unwrap())
        })
    }

    fn skip(&mut self, count: usize) -> std::result::Result<(), String> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| "WKB length overflow".to_string())?;
        if end > self.bytes.len() {
            return Err("WKB structure does not match expected size".to_string());
        }
        self.position = end;
        Ok(())
    }

    fn parse_geometry(
        &mut self,
        expected_base_type: Option<u8>,
    ) -> std::result::Result<Header, String> {
        self.depth += 1;
        if self.depth > MAX_WKB_NESTING_DEPTH {
            return Err("WKB nesting depth exceeded".to_string());
        }

        let byte_order = self.read_u8()?;
        let little_endian = match byte_order {
            0 => false,
            1 => true,
            other => return Err(format!("invalid WKB byte order {other}")),
        };
        let code = self.read_u32(little_endian)?;
        let header = validate_header(code)?;

        if code & EWKB_FLAG_SRID != 0 {
            self.skip(4)?;
        }

        if let Some(expected) = expected_base_type
            && header.base_type != expected
        {
            return Err(format!(
                "WKB container expected geometry type {expected}, got {}",
                header.base_type
            ));
        }

        let coord_bytes = header.dimension.coord_bytes();
        match header.base_type {
            1 => self.skip(coord_bytes)?,
            2 => {
                let count = self.read_u32(little_endian)?;
                self.skip(checked_coord_count(count, coord_bytes)?)?;
            }
            3 => {
                let ring_count = self.read_u32(little_endian)?;
                for _ in 0..ring_count {
                    let coord_count = self.read_u32(little_endian)?;
                    self.skip(checked_coord_count(coord_count, coord_bytes)?)?;
                }
            }
            4 => {
                let count = self.read_u32(little_endian)?;
                for _ in 0..count {
                    self.parse_geometry(Some(1))?;
                }
            }
            5 => {
                let count = self.read_u32(little_endian)?;
                for _ in 0..count {
                    self.parse_geometry(Some(2))?;
                }
            }
            6 => {
                let count = self.read_u32(little_endian)?;
                for _ in 0..count {
                    self.parse_geometry(Some(3))?;
                }
            }
            7 => {
                let count = self.read_u32(little_endian)?;
                for _ in 0..count {
                    self.parse_geometry(None)?;
                }
            }
            _ => unreachable!("validated WKB base type"),
        }

        self.depth -= 1;
        Ok(header)
    }
}

fn checked_coord_count(count: u32, coord_bytes: usize) -> std::result::Result<usize, String> {
    usize::try_from(count)
        .ok()
        .and_then(|count| count.checked_mul(coord_bytes))
        .ok_or_else(|| "WKB coordinate count overflow".to_string())
}

fn validate_header(code: u32) -> std::result::Result<Header, String> {
    let flags = code & (EWKB_FLAG_Z | EWKB_FLAG_M | EWKB_FLAG_SRID | EWKB_FLAG_BBOX);
    let payload = code & !flags;
    let mut dimension = match payload / 1000 {
        0 => Dimension::Xy,
        1 => Dimension::Xyz,
        2 => Dimension::Xym,
        3 => Dimension::Xyzm,
        _ => return Err(format!("unknown WKB type code {code}")),
    };
    if flags & EWKB_FLAG_Z != 0 && flags & EWKB_FLAG_M != 0 {
        dimension = Dimension::Xyzm;
    } else if flags & EWKB_FLAG_Z != 0 {
        dimension = Dimension::Xyz;
    } else if flags & EWKB_FLAG_M != 0 {
        dimension = Dimension::Xym;
    }

    let base_type = payload % 1000;
    if !(1..=7).contains(&base_type) {
        return Err(format!("unknown WKB type code {code}"));
    }

    Ok(Header {
        base_type: base_type as u8,
        dimension,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn accepts_standard_iso_and_ewkb_type_codes() {
        for code in [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            1001,
            1006,
            2006,
            3006,
            0x8000_0006,
            0x4000_0006,
            0xC000_0006,
            0x2000_0006,
            0x1000_0006,
            0x8000_0001,
        ] {
            assert!(validate_header(code).is_ok(), "rejected WKB type {code}");
        }
    }

    #[test]
    fn rejects_unknown_low_bits() {
        assert!(validate_header(441).is_err());
        assert!(validate_header(63_046).is_err());
    }
}
