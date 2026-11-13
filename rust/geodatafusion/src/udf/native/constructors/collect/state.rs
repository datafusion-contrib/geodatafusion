//! Accumulator member storage and aggregate state encoding for `ST_Collect_Agg`.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, UInt32Array, new_empty_array};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::arrow::compute::{concat, interleave, take};
use datafusion::error::DataFusionError;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{GeometryArray, WkbArray, from_arrow_array};
use geoarrow_array::cast::{AsGeoArrowArray, from_wkt, to_wkb};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata};

use super::Entry;
use crate::error::GeoDataFusionResult;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StateEncoding {
    Wkb,
    Geometry,
}

impl StateEncoding {
    pub(super) fn for_field(field: &Field) -> GeoDataFusionResult<Self> {
        let geo_type = GeoArrowType::from_arrow_field(field)?;
        Ok(if is_wkb_type(&geo_type) {
            Self::Wkb
        } else {
            Self::Geometry
        })
    }

    pub(super) fn value_field(self, metadata: &Arc<Metadata>, coord_type: CoordType) -> FieldRef {
        match self {
            Self::Wkb => Arc::new(Field::new_list_field(DataType::Binary, true)),
            Self::Geometry => Arc::new(
                GeometryType::new(metadata.clone())
                    .with_coord_type(coord_type)
                    .to_field("item", true),
            ),
        }
    }

    pub(super) fn state_field(
        self,
        name: &str,
        metadata: &Arc<Metadata>,
        coord_type: CoordType,
    ) -> FieldRef {
        let suffix = match self {
            Self::Wkb => "wkbs",
            Self::Geometry => "geoms",
        };
        Arc::new(Field::new(
            format!("{name}[{suffix}]"),
            DataType::List(self.value_field(metadata, coord_type)),
            true,
        ))
    }
}

#[derive(Debug)]
pub(super) enum MemberBatch {
    Wkb(Arc<WkbArray>),
    Geometry(Arc<GeometryArray>),
}

impl MemberBatch {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Wkb(array) => array.len(),
            Self::Geometry(array) => array.len(),
        }
    }

    pub(super) fn is_null(&self, i: usize) -> bool {
        match self {
            Self::Wkb(array) => array.is_null(i),
            Self::Geometry(array) => array.is_null(i),
        }
    }

    pub(super) fn logical_nulls(&self) -> Option<NullBuffer> {
        match self {
            Self::Wkb(array) => array.logical_nulls(),
            Self::Geometry(array) => array.logical_nulls(),
        }
    }

    pub(super) fn to_array_ref(&self) -> ArrayRef {
        match self {
            Self::Wkb(array) => array.to_array_ref(),
            Self::Geometry(array) => array.to_array_ref(),
        }
    }

    /// Materialized Arrow memory footprint of this batch.
    ///
    /// Computed once, when the batch is first stored, so the running total in [`MemberBatches`]
    /// never has to rebuild the Arrow representation during `size()` accounting
    /// (see [`MemberBatches::memory_size`]).
    fn array_memory_size(&self) -> usize {
        match self {
            Self::Wkb(array) => array.to_array_ref().get_array_memory_size(),
            Self::Geometry(array) => array.to_array_ref().get_array_memory_size(),
        }
    }

    /// Build a new batch of the same encoding containing only `rows`, in the given order.
    fn take_rows(
        &self,
        rows: Vec<u32>,
        metadata: &Arc<Metadata>,
        coord_type: CoordType,
    ) -> GeoDataFusionResult<MemberBatch> {
        let taken = take(self.to_array_ref().as_ref(), &UInt32Array::from(rows), None)?;
        Ok(match self {
            Self::Wkb(_) => MemberBatch::Wkb(Arc::new(WkbArray::new(
                taken.as_binary::<i32>().clone(),
                metadata.clone(),
            ))),
            Self::Geometry(_) => {
                let field = StateEncoding::Geometry.value_field(metadata, coord_type);
                let geo = from_arrow_array(taken.as_ref(), field.as_ref())?;
                MemberBatch::Geometry(Arc::new(geo.as_geometry().clone()))
            }
        })
    }
}

/// The encoding-specific store of accumulated member arrays.
#[derive(Debug)]
pub(super) enum MemberStorage {
    Wkb(Vec<Arc<WkbArray>>),
    Geometry(Vec<Arc<GeometryArray>>),
}

/// An accumulator with allocation size tracking.
///
/// This internally tracks allocations,
/// so that we can compute the total accumulator `size()` in constant time.
/// (DataFusion polls this once per input batch, so it's important to keep this cheap.)
#[derive(Debug)]
pub(super) struct MemberBatches {
    storage: MemberStorage,
    /// Per-batch [`MemberBatch::array_memory_size`], parallel to `storage`.
    batch_bytes: Vec<usize>,
    /// Running sum of [`MemberBatch::array_memory_size`] across every stored batch.
    array_bytes: usize,
}

impl MemberBatches {
    pub(super) fn new(encoding: StateEncoding) -> Self {
        let storage = match encoding {
            StateEncoding::Wkb => MemberStorage::Wkb(Vec::new()),
            StateEncoding::Geometry => MemberStorage::Geometry(Vec::new()),
        };
        Self {
            storage,
            batch_bytes: Vec::new(),
            array_bytes: 0,
        }
    }

    pub(super) fn storage(&self) -> &MemberStorage {
        &self.storage
    }

    pub(super) fn encoding(&self) -> StateEncoding {
        match &self.storage {
            MemberStorage::Wkb(_) => StateEncoding::Wkb,
            MemberStorage::Geometry(_) => StateEncoding::Geometry,
        }
    }

    pub(super) fn clear(&mut self) {
        match &mut self.storage {
            MemberStorage::Wkb(batches) => batches.clear(),
            MemberStorage::Geometry(batches) => batches.clear(),
        }
        self.batch_bytes.clear();
        self.array_bytes = 0;
    }

    pub(super) fn push(&mut self, batch: MemberBatch) -> GeoDataFusionResult<()> {
        let size = batch.array_memory_size();
        self.push_with_size(batch, size)
    }

    /// Push a batch whose footprint is already known, reusing `size` instead of re-measuring.
    fn push_with_size(&mut self, batch: MemberBatch, size: usize) -> GeoDataFusionResult<()> {
        match (&mut self.storage, batch) {
            (MemberStorage::Wkb(batches), MemberBatch::Wkb(batch)) => batches.push(batch),
            (MemberStorage::Geometry(batches), MemberBatch::Geometry(batch)) => batches.push(batch),
            _ => {
                return Err(DataFusionError::Internal(
                    "ST_Collect_Agg accumulator state encoding mismatch".to_string(),
                )
                .into());
            }
        }
        self.array_bytes += size;
        self.batch_bytes.push(size);
        Ok(())
    }

    /// Iterate the stored batches as encoding-agnostic [`MemberBatch`]es (cheap `Arc` clones).
    fn iter_batches(&self) -> Box<dyn Iterator<Item = MemberBatch> + '_> {
        match &self.storage {
            MemberStorage::Wkb(batches) => Box::new(batches.iter().cloned().map(MemberBatch::Wkb)),
            MemberStorage::Geometry(batches) => {
                Box::new(batches.iter().cloned().map(MemberBatch::Geometry))
            }
        }
    }

    /// Take ownership of every stored batch with its cached footprint, leaving the store empty.
    fn drain_batches(&mut self) -> Vec<(MemberBatch, usize)> {
        let sizes = std::mem::take(&mut self.batch_bytes);
        let batches: Vec<MemberBatch> = match &mut self.storage {
            MemberStorage::Wkb(batches) => std::mem::take(batches)
                .into_iter()
                .map(MemberBatch::Wkb)
                .collect(),
            MemberStorage::Geometry(batches) => std::mem::take(batches)
                .into_iter()
                .map(MemberBatch::Geometry)
                .collect(),
        };
        debug_assert_eq!(batches.len(), sizes.len());
        self.array_bytes = 0;
        batches.into_iter().zip(sizes).collect()
    }

    /// Return `(batch, row)` entries for every retained non-null member.
    pub(super) fn non_null_order(&self) -> Vec<(usize, usize)> {
        let mut entries = Vec::new();
        for (bi, batch) in self.iter_batches().enumerate() {
            for ri in 0..batch.len() {
                if !batch.is_null(ri) {
                    entries.push((bi, ri));
                }
            }
        }
        entries
    }

    /// Concatenate or reorder stored member arrays into the flat child values of a state list.
    pub(super) fn state_values(
        &self,
        order: Option<&[(usize, usize)]>,
        metadata: &Arc<Metadata>,
        coord_type: CoordType,
    ) -> GeoDataFusionResult<ArrayRef> {
        let value_field = self.encoding().value_field(metadata, coord_type);
        let arrays: Vec<ArrayRef> = self.iter_batches().map(|b| b.to_array_ref()).collect();
        if arrays.is_empty() {
            return Ok(new_empty_array(value_field.data_type()));
        }
        if matches!(order, Some(order) if order.is_empty()) {
            return Ok(new_empty_array(value_field.data_type()));
        }
        let refs: Vec<&dyn Array> = arrays.iter().map(|b| b.as_ref()).collect();
        Ok(match order {
            Some(order) => interleave(&refs, order)?,
            None => concat(&refs)?,
        })
    }

    pub(super) fn memory_size(&self) -> usize {
        let batches_array_size = match &self.storage {
            MemberStorage::Wkb(batches) => {
                batches.capacity() * std::mem::size_of::<Arc<WkbArray>>()
            }
            MemberStorage::Geometry(batches) => {
                batches.capacity() * std::mem::size_of::<Arc<GeometryArray>>()
            }
        };
        let batch_bytes_vec_size = self.batch_bytes.capacity() * std::mem::size_of::<usize>();
        // Sum of the tracking arrays + array_bytes (the Arrow memory footprint)
        batches_array_size + batch_bytes_vec_size + self.array_bytes
    }
}

/// Normalize one input array into the accumulator's fixed state encoding.
pub(super) fn normalize_input_for_encoding(
    encoding: StateEncoding,
    values: &ArrayRef,
    input_field: &FieldRef,
    metadata: &Arc<Metadata>,
    coord_type: CoordType,
) -> GeoDataFusionResult<MemberBatch> {
    Ok(match encoding {
        StateEncoding::Wkb => MemberBatch::Wkb(Arc::new(normalize_wkb_input(
            values,
            input_field,
            metadata,
        )?)),
        StateEncoding::Geometry => MemberBatch::Geometry(Arc::new(normalize_geometry_input(
            values,
            input_field,
            metadata,
            coord_type,
        )?)),
    })
}

/// Decode the flat child values of a DataFusion aggregate state list into stored members.
pub(super) fn decode_state_values(
    encoding: StateEncoding,
    values: &ArrayRef,
    metadata: &Arc<Metadata>,
    coord_type: CoordType,
) -> GeoDataFusionResult<MemberBatch> {
    Ok(match encoding {
        StateEncoding::Wkb => {
            let binary = values.as_binary::<i32>().clone();
            MemberBatch::Wkb(Arc::new(WkbArray::new(binary, metadata.clone())))
        }
        StateEncoding::Geometry => {
            let field = encoding.value_field(metadata, coord_type);
            let geo = from_arrow_array(values.as_ref(), field.as_ref())?;
            MemberBatch::Geometry(Arc::new(geo.as_geometry().clone()))
        }
    })
}

/// Build a `List` state array using the exact field shape for the selected encoding.
pub(super) fn state_list_array(
    encoding: StateEncoding,
    offsets: OffsetBuffer<i32>,
    values: ArrayRef,
    nulls: Option<NullBuffer>,
    metadata: &Arc<Metadata>,
    coord_type: CoordType,
) -> ListArray {
    let field = encoding.value_field(metadata, coord_type);
    ListArray::new(field, offsets, values, nulls)
}

/// Retain groups after `EmitTo::First`, dropping the emitted ones and compacting mixed batches.
///
/// Renumbers retained groups down by `emit_groups`, drops batches with no survivors,
/// and rebuilds any batch that straddles the cutoff via [`MemberBatch::take_rows`].
///
/// Returns the heap footprint of the rebuilt `batch_entries`,
/// so the caller can refresh its cached total without a second pass.
pub(super) fn compact_retained_batches(
    batches: &mut MemberBatches,
    batch_entries: &mut Vec<Vec<Entry>>,
    emit_groups: usize,
    metadata: &Arc<Metadata>,
    coord_type: CoordType,
) -> GeoDataFusionResult<usize> {
    let emit_groups = emit_groups as u32;
    let old_entries = std::mem::take(batch_entries);
    let mut entries_bytes = 0;
    for ((batch, size), entries) in batches.drain_batches().into_iter().zip(old_entries) {
        let retained_len = entries.iter().filter(|(g, _)| *g >= emit_groups).count();
        if retained_len == 0 {
            continue; // Whole batch emitted; drop it.
        }
        let (kept, kept_size, retained_entries) = if retained_len == entries.len() {
            // Whole batch retained: keep the array verbatim, just renumber the groups.
            let mut entries = entries;
            for (g, _) in &mut entries {
                *g -= emit_groups;
            }
            (batch, size, entries)
        } else {
            // Mixed batch: keep only retained rows, renumbering groups and re-indexing rows.
            let mut retained_entries = Vec::with_capacity(retained_len);
            let mut retained_rows = Vec::with_capacity(retained_len);
            for (g, r) in entries {
                if g >= emit_groups {
                    retained_entries.push((g - emit_groups, retained_rows.len() as u32));
                    retained_rows.push(r);
                }
            }
            let taken = batch.take_rows(retained_rows, metadata, coord_type)?;
            let taken_size = taken.array_memory_size();
            (taken, taken_size, retained_entries)
        };
        entries_bytes += retained_entries.capacity() * std::mem::size_of::<Entry>();
        batches.push_with_size(kept, kept_size)?;
        batch_entries.push(retained_entries);
    }
    Ok(entries_bytes)
}

/// Convert a boolean filter into nulls for `convert_to_state` output rows.
pub(super) fn filter_to_null_buffer(filter: &BooleanArray) -> Option<NullBuffer> {
    let (keep, filter_nulls) = filter.clone().into_parts();
    NullBuffer::union(Some(&NullBuffer::new(keep)), filter_nulls.as_ref())
}

/// Decode non-WKB inputs to mixed native `Geometry` without unnecessary coordinate re-encoding.
fn normalize_geometry_input(
    values: &ArrayRef,
    input_field: &FieldRef,
    metadata: &Arc<Metadata>,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let geo = from_arrow_array(values, input_field)?;
    let normalized = match geo.data_type() {
        GeoArrowType::Point(_) => GeometryArray::from(geo.as_point().clone()),
        GeoArrowType::LineString(_) => GeometryArray::from(geo.as_line_string().clone()),
        GeoArrowType::Polygon(_) => GeometryArray::from(geo.as_polygon().clone()),
        GeoArrowType::MultiPoint(_) => GeometryArray::from(geo.as_multi_point().clone()),
        GeoArrowType::MultiLineString(_) => GeometryArray::from(geo.as_multi_line_string().clone()),
        GeoArrowType::MultiPolygon(_) => GeometryArray::from(geo.as_multi_polygon().clone()),
        GeoArrowType::GeometryCollection(_) => {
            GeometryArray::from(geo.as_geometry_collection().clone())
        }
        GeoArrowType::Geometry(_) => geo.as_geometry().clone(),
        GeoArrowType::Wkt(_) => from_wkt(geo.as_wkt::<i32>(), geometry_type(metadata, coord_type))?
            .as_geometry()
            .clone(),
        GeoArrowType::LargeWkt(_) => {
            from_wkt(geo.as_wkt::<i64>(), geometry_type(metadata, coord_type))?
                .as_geometry()
                .clone()
        }
        GeoArrowType::WktView(_) => {
            from_wkt(geo.as_wkt_view(), geometry_type(metadata, coord_type))?
                .as_geometry()
                .clone()
        }
        GeoArrowType::Wkb(_) | GeoArrowType::LargeWkb(_) | GeoArrowType::WkbView(_) => {
            return Err(DataFusionError::Internal(
                "ST_Collect_Agg WKB input reached Geometry state normalization".to_string(),
            )
            .into());
        }
        GeoArrowType::Rect(_) => {
            return Err(DataFusionError::NotImplemented(
                "ST_Collect_Agg does not accept a Box/Rect input".to_string(),
            )
            .into());
        }
    };
    Ok(normalized)
}

/// Normalize WKB-like inputs to the i32-offset WKB representation used in state.
///
/// Member bytes are kept verbatim (geoarrow does not re-encode WKB on read).
/// ISO WKB and EWKB Z/M dimensions are supported in either byte order.
/// Embedded SRIDs are rejected during output assembly (see `read_wkb_member` in `output/wkb_assembly.rs`),
/// since the result uses array-level CRS metadata.
fn normalize_wkb_input(
    values: &ArrayRef,
    input_field: &FieldRef,
    metadata: &Arc<Metadata>,
) -> GeoDataFusionResult<WkbArray> {
    let geo = from_arrow_array(values, input_field)?;
    match geo.data_type() {
        GeoArrowType::Wkb(_) => Ok(WkbArray::new(
            geo.as_wkb::<i32>().inner().clone(),
            metadata.clone(),
        )),
        GeoArrowType::LargeWkb(_) | GeoArrowType::WkbView(_) => Ok(to_wkb::<i32>(geo.as_ref())?),
        _ => Err(DataFusionError::Internal(
            "ST_Collect_Agg non-WKB input reached WKB state normalization".to_string(),
        )
        .into()),
    }
}

fn geometry_type(metadata: &Arc<Metadata>, coord_type: CoordType) -> GeoArrowType {
    GeoArrowType::Geometry(GeometryType::new(metadata.clone()).with_coord_type(coord_type))
}

fn is_wkb_type(ty: &GeoArrowType) -> bool {
    matches!(
        ty,
        GeoArrowType::Wkb(_) | GeoArrowType::LargeWkb(_) | GeoArrowType::WkbView(_)
    )
}
