//! GEOS operations used by the native geometry editors.
//!
//! Most native editors stay independent from GEOS. `ST_WrapX` needs GEOS
//! overlay semantics when it has to split a crossing polygon: the Z values of
//! the new cut nodes and of a dissolved overlap follow GEOS interpolation, and
//! only a polygon overlay keeps notched and multi-part results valid.
//!
//! The operations here use the public overlay API only (`GEOSIntersection`,
//! `GEOSDifference`, and `GEOSUnaryUnion`). Callers must drop M ordinates
//! first because the `geos-3_11` binding input path cannot read them.

use geos::{Geom, Geometry};

use super::adapter::GeosWkbWriter;
use crate::error::GeoDataFusionResult;

/// Computes the unary union of a WKB geometry through GEOS.
pub(crate) fn unary_union_wkb(wkb: &[u8]) -> GeoDataFusionResult<Vec<u8>> {
    let geometry = Geometry::new_from_wkb(wkb)?;
    let union = geometry.unary_union()?;
    let mut writer = GeosWkbWriter::new()?;
    writer.write(&union)
}

/// Splits a WKB polygon into its parts left and right of the vertical line at
/// `cut`.
///
/// The left half is the intersection with a rectangle that covers the polygon
/// bbox on three sides and reaches `cut` on the fourth; the right half is the
/// difference against the same rectangle. Both halves share only the segment
/// at `cut`, so their areas add up to the input area, holes crossing the cut
/// become notches of both halves, and the GEOS results are valid polygons or
/// multi-polygons. Callers only pass polygons with `x_min < cut < x_max`;
/// polygons that do not cross the cut take the native fast paths instead.
pub(crate) fn split_polygon_wkb(wkb: &[u8], cut: f64) -> GeoDataFusionResult<Vec<Vec<u8>>> {
    let polygon = Geometry::new_from_wkb(wkb)?;
    let left_half = Geometry::create_rectangle(
        polygon.get_x_min()? - 1.0,
        polygon.get_y_min()? - 1.0,
        cut,
        polygon.get_y_max()? + 1.0,
    )?;

    let mut writer = GeosWkbWriter::new()?;
    let mut pieces = Vec::new();
    for piece in [
        polygon.intersection(&left_half)?,
        polygon.difference(&left_half)?,
    ] {
        if !piece.is_empty()? {
            pieces.push(writer.write(&piece)?);
        }
    }
    Ok(pieces)
}
