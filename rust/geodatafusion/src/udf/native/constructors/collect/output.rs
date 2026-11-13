//! Final `ST_Collect_Agg` output assembly for both WKB-backed and native member stores.

use std::sync::Arc;

use arrow_array::ArrayRef;
use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::scalar::Geometry;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, GeometryType, Metadata};
use wkb_assembly::PreparedWkbOutput;

use super::container::{CollectedGeometry, classify_members};
use super::state::{MemberBatches, MemberStorage};
use crate::error::GeoDataFusionResult;

mod wkb_assembly;

/// Assemble one output geometry per group from stored member batches.
pub(super) fn assemble_output(
    batches: &MemberBatches,
    groups: &[&[(usize, usize)]],
    metadata: Arc<Metadata>,
    coord_type: CoordType,
) -> GeoDataFusionResult<ArrayRef> {
    match batches.storage() {
        MemberStorage::Wkb(batches) => PreparedWkbOutput::prepare(batches, groups)?
            .assemble(GeometryType::new(metadata).with_coord_type(coord_type)),
        MemberStorage::Geometry(batches) => {
            let typed: Vec<&GeometryArray> = batches.iter().map(|a| a.as_ref()).collect();
            assemble_geometry(&typed, groups, metadata, coord_type)
        }
    }
}

fn assemble_geometry<'a>(
    typed: &[&'a GeometryArray],
    groups: &[&[(usize, usize)]],
    metadata: Arc<Metadata>,
    coord_type: CoordType,
) -> GeoDataFusionResult<ArrayRef> {
    let out_type = GeometryType::new(metadata).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(out_type);
    let mut members: Vec<Geometry<'a>> = Vec::new();
    for group in groups {
        if group.is_empty() {
            builder.push_null();
            continue;
        }
        members.clear();
        for &(bi, ri) in *group {
            if !typed[bi].is_null(ri) {
                members.push(typed[bi].value(ri)?);
            }
        }
        if members.is_empty() {
            builder.push_null();
            continue;
        }
        let (kind, dim) = classify_members(&members)?;
        builder.push_geometry(Some(&CollectedGeometry::new(&members, kind, dim)))?;
    }
    Ok(builder.finish().into_array_ref())
}
