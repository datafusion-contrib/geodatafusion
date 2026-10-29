use std::sync::{Arc, OnceLock};

use arrow_array::builder::Int64Builder;
use arrow_array::cast::AsArray;
use arrow_array::{Array, Int64Array};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, UnionMode};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

/// Returns the Arrow buffer bytes referenced by one geometry row.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MemSize;

impl MemSize {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MemSize {
    fn default() -> Self {
        Self::new()
    }
}

static MEM_SIZE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MemSize {
    fn name(&self) -> &str {
        "st_memsize"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(mem_size_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MEM_SIZE_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the Arrow buffer bytes referenced by this geometry row: list offset \
                 slots, dense-union type id and offset slots, and coordinate or binary payload \
                 bytes actually addressed by the row. Validity bitmaps, shared metadata, and \
                 allocator padding are excluded; byte-view payloads count only the non-inline \
                 range referenced by the row. This is an Arrow-storage metric and is not the \
                 PostGIS GSERIALIZED or EWKB byte size. NULL input returns NULL.",
                "ST_MemSize(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn mem_size_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, mem_size_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn mem_size_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<Int64Array> {
    let array_ref = array.to_array_ref();
    let logical_nulls = array.logical_nulls();
    let mut builder = Int64Builder::with_capacity(array.len());

    for row in 0..array.len() {
        if logical_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.is_null(row))
        {
            builder.append_null();
            continue;
        }
        let row_slice = array_ref.slice(row, 1);
        let bytes = row_slice_buffer_bytes(row_slice.as_ref())?;
        builder.append_value(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    Ok(builder.finish())
}

/// Counts the Arrow buffers referenced by an already sliced array.
///
/// `ArrayData::get_slice_memory_size` charges the validity bitmap and does not
/// rebase list or union children for a one-row slice, so this walks the
/// containers and leaves with their public Arrow APIs instead.
fn row_slice_buffer_bytes(array: &dyn Array) -> GeoDataFusionResult<usize> {
    let len = array.len();
    let bytes = match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[len] as usize;
            (len + 1) * 4
                + row_slice_buffer_bytes(list.values().slice(start, end - start).as_ref())?
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[len] as usize;
            (len + 1) * 8
                + row_slice_buffer_bytes(list.values().slice(start, end - start).as_ref())?
        }
        DataType::FixedSizeList(_, _) => {
            row_slice_buffer_bytes(array.as_fixed_size_list().values().as_ref())?
        }
        DataType::Struct(_) => {
            let fields = array.as_struct();
            let mut total = 0;
            for index in 0..fields.num_columns() {
                total += row_slice_buffer_bytes(fields.column(index).as_ref())?;
            }
            total
        }
        DataType::Union(_, mode) => {
            let union = array.as_union();
            // Every union row stores one type id and a dense union also stores
            // one child offset.
            let mut total = len;
            if matches!(mode, UnionMode::Dense) {
                total += len * std::mem::size_of::<i32>();
            }
            for row in 0..len {
                total += row_slice_buffer_bytes(union.value(row).as_ref())?;
            }
            total
        }
        _ => leaf_buffer_bytes(array)?,
    };
    Ok(bytes)
}

/// Arrow stores a byte view in a 16-byte slot; values longer than 12 bytes
/// reference a range in a separate shared payload buffer.
const VIEW_INLINE_LEN: u32 = 12;

/// Counts the leaf buffers referenced by a row range, excluding its validity
/// bitmap.
fn leaf_buffer_bytes(array: &dyn Array) -> GeoDataFusionResult<usize> {
    let len = array.len();
    let bytes = match array.data_type() {
        DataType::Binary => {
            let offsets = array.as_binary::<i32>().value_offsets();
            2 * std::mem::size_of::<i32>() + (offsets[len] - offsets[0]) as usize
        }
        DataType::LargeBinary => {
            let offsets = array.as_binary::<i64>().value_offsets();
            2 * std::mem::size_of::<i64>() + (offsets[len] - offsets[0]) as usize
        }
        DataType::Utf8 => {
            let offsets = array.as_string::<i32>().value_offsets();
            2 * std::mem::size_of::<i32>() + (offsets[len] - offsets[0]) as usize
        }
        DataType::LargeUtf8 => {
            let offsets = array.as_string::<i64>().value_offsets();
            2 * std::mem::size_of::<i64>() + (offsets[len] - offsets[0]) as usize
        }
        DataType::BinaryView => {
            let view = array.as_binary_view();
            view_buffer_bytes(view.views(), view.data_buffers(), len)?
        }
        DataType::Utf8View => {
            let view = array.as_string_view();
            view_buffer_bytes(view.views(), view.data_buffers(), len)?
        }
        DataType::Boolean => len.div_ceil(8),
        DataType::Null => 0,
        DataType::FixedSizeBinary(width) => {
            let width = usize::try_from(*width).map_err(|_| {
                DataFusionError::Internal(format!("negative FixedSizeBinary width {width}"))
            })?;
            len * width
        }
        other => {
            let Some(width) = other.primitive_width() else {
                return Err(DataFusionError::NotImplemented(format!(
                    "ST_MemSize does not support Arrow storage type {other}"
                ))
                .into());
            };
            len * width
        }
    };
    Ok(bytes)
}

fn view_buffer_bytes(views: &[u128], buffers: &[Buffer], len: usize) -> GeoDataFusionResult<usize> {
    let mut total = len * std::mem::size_of::<u128>();
    for raw in &views[..len] {
        let length = *raw as u32;
        if length <= VIEW_INLINE_LEN {
            continue;
        }
        // ByteView layout: length, prefix, buffer_index, offset.
        let buffer_index = (*raw >> 64) as u32 as usize;
        let offset = (*raw >> 96) as u32 as usize;
        let buffer = buffers.get(buffer_index).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "ST_MemSize: view references missing buffer {buffer_index}"
            ))
        })?;
        let end = offset.saturating_add(length as usize);
        if end > buffer.len() {
            return Err(DataFusionError::Internal(
                "ST_MemSize: view payload exceeds its Arrow buffer".to_string(),
            )
            .into());
        }
        total += length as usize;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_views_only_reference_their_slot() {
        let views = [8_u128];
        assert_eq!(view_buffer_bytes(&views, &[], 1).unwrap(), 16);
    }

    #[test]
    fn external_views_count_the_referenced_payload_range() {
        let payload = Buffer::from(vec![0_u8; 20]);
        // length 20 with buffer index 0 and offset 0.
        let views = [20_u128];
        assert_eq!(view_buffer_bytes(&views, &[payload], 1).unwrap(), 36);
    }
}
