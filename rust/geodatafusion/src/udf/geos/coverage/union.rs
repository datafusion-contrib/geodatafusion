use std::sync::{Arc, OnceLock};

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ListArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Documentation, Signature};
use datafusion::scalar::ScalarValue;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::GeoArrowType;

use super::ffi;
use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{geometry_array_from_arrow, is_null_geometry_input};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CoverageUnion {
    signature: Signature,
}

impl CoverageUnion {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input().clone(),
        }
    }
}

impl Default for CoverageUnion {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl AggregateUDFImpl for CoverageUnion {
    fn name(&self) -> &str {
        "st_coverageunion"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let output_type = super::geometry_type_from_field(&arg_fields[0], self.name())?;
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let input_field = acc_args.exprs[0].return_field(acc_args.schema)?;
        Ok(Box::new(CoverageUnionAccumulator::new(
            input_field,
            acc_args.return_field,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let item = Arc::new(Field::new("item", DataType::Binary, true));
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "geometries"),
            DataType::List(item),
            true,
        ))])
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the union of polygonal geometries that form a coverage. Inputs are expected to be correctly noded and non-overlapping; GEOS may reject inputs that do not meet that precondition. NULL and EMPTY inputs do not contribute. The result is 2D, matching the pinned PostGIS reference; Z and M ordinates are not present. The PostGIS geometry[] scalar overload is not registered yet; it depends on the shared scalar/aggregate SQL dispatch work.",
                "ST_CoverageUnion(geometry)",
            )
            .with_argument("geometry", "polygonal coverage member")
            .build()
        }))
    }
}

#[derive(Debug)]
struct CoverageUnionAccumulator {
    field: FieldRef,
    return_field: FieldRef,
    output_type: GeoArrowType,
    geometries: Vec<Vec<u8>>,
}

impl CoverageUnionAccumulator {
    fn new(field: FieldRef, return_field: FieldRef) -> GeoDataFusionResult<Self> {
        let output_type = GeoArrowType::from_arrow_field(return_field.as_ref())?;
        Ok(Self {
            field,
            return_field,
            output_type,
            geometries: Vec::new(),
        })
    }

    fn state_array(&self) -> ArrayRef {
        let mut builder = BinaryBuilder::new();
        for geometry in &self.geometries {
            builder.append_value(geometry);
        }
        let values = builder.finish();
        let item = Arc::new(Field::new("item", DataType::Binary, true));
        let offsets = OffsetBuffer::<i32>::from_lengths([self.geometries.len()]);
        Arc::new(ListArray::new(item, offsets, Arc::new(values), None))
    }
}

impl Accumulator for CoverageUnionAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let state = self.state_array();
        let list = state.as_list::<i32>();
        Ok(vec![ScalarValue::List(Arc::new(list.clone()))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.geometries.is_empty() {
            return ScalarValue::try_new_null(self.return_field.data_type());
        }

        let inputs = self
            .geometries
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>();
        let Some(result) = ffi::coverage_union(&inputs)? else {
            return ScalarValue::try_new_null(self.return_field.data_type());
        };

        let values = vec![Some(result)].into_iter().collect();
        let array = super::super::adapter::geos_values_to_array(values, self.output_type.clone())?;
        ScalarValue::try_from_array(array.as_ref(), 0)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value = values.first().ok_or_else(|| {
            DataFusionError::Internal("ST_CoverageUnion received no input batch".to_string())
        })?;
        if is_null_geometry_input(self.field.as_ref(), value.as_ref()) {
            return Ok(());
        }

        let geometry = geometry_array_from_arrow(value, &self.field)?;
        let wkb =
            to_wkb::<i32>(geometry.as_ref()).map_err(crate::error::GeoDataFusionError::GeoArrow)?;
        self.geometries
            .extend(wkb.inner().iter().flatten().map(Vec::from));
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let state = states.first().ok_or_else(|| {
            DataFusionError::Internal("ST_CoverageUnion received no state batch".to_string())
        })?;
        let list = state.as_list::<i32>();
        let values = list.values().as_binary::<i32>();
        let offsets = list.value_offsets();

        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            self.geometries
                .extend((start..end).map(|index| values.value(index).to_vec()));
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.geometries.capacity() * std::mem::size_of::<Vec<u8>>()
            + self
                .geometries
                .iter()
                .map(|geometry| geometry.capacity())
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field};
    use datafusion::logical_expr::Accumulator;
    use geoarrow_schema::{GeometryType, Metadata};

    use super::CoverageUnionAccumulator;

    fn accumulator(geometries: Vec<Vec<u8>>) -> CoverageUnionAccumulator {
        let field = Arc::new(Field::new("geom", DataType::Null, true));
        CoverageUnionAccumulator {
            field: Arc::clone(&field),
            return_field: field,
            output_type: geoarrow_schema::GeoArrowType::Geometry(GeometryType::new(Arc::new(
                Metadata::default(),
            ))),
            geometries,
        }
    }

    #[test]
    fn size_tracks_container_and_payload_capacity() {
        let mut geometries = Vec::with_capacity(4);
        for _ in 0..3 {
            let mut payload = Vec::with_capacity(64);
            payload.extend_from_slice(&[0u8; 8]);
            geometries.push(payload);
        }
        let mut accumulator = accumulator(geometries);

        let container_capacity = accumulator.geometries.capacity() * std::mem::size_of::<Vec<u8>>();
        let payload_capacity: usize = accumulator.geometries.iter().map(Vec::capacity).sum();
        assert_eq!(
            accumulator.size(),
            std::mem::size_of_val(&accumulator) + container_capacity + payload_capacity
        );

        // A payload reserve changes reported memory even though neither the
        // vector length nor the container capacity changes.
        let before = accumulator.size();
        accumulator.geometries[0].reserve(1024);
        let after_capacity: usize = accumulator.geometries.iter().map(Vec::capacity).sum();
        assert_eq!(
            accumulator.size() - before,
            after_capacity - payload_capacity
        );

        // A container reserve must be accounted for even when payload
        // capacities do not change.
        let before = accumulator.size();
        let container_before = accumulator.geometries.capacity();
        accumulator.geometries.reserve(16);
        let container_after = accumulator.geometries.capacity();
        assert!(container_after > container_before);
        assert_eq!(
            accumulator.size() - before,
            (container_after - container_before) * std::mem::size_of::<Vec<u8>>()
        );
    }
}
