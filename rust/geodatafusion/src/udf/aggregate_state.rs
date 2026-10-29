//! Shared intermediate-state helpers for geometry aggregates.
//!
//! Every aggregate stores WKB payloads so partial aggregates can be merged
//! without depending on the input array type. Ordering values are carried in a
//! second `List<Struct>` state field, matching DataFusion's ordered `array_agg`
//! representation, so a final accumulator can merge partitions in order.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, ListArray, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion::common::utils::compare_rows;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};

const PAYLOAD_MAGIC: u32 = 0x4753_5045; // "GSPE"

#[derive(Debug)]
pub(crate) struct GeometryAggregateState {
    entries: Vec<Vec<u8>>,
    ordering_values: Vec<Vec<ScalarValue>>,
    ordering_req: Option<LexOrdering>,
    ordering_fields: Vec<FieldRef>,
    grid_size: Option<f64>,
    distinct: bool,
}

impl GeometryAggregateState {
    pub(crate) fn new(
        order_bys: &[PhysicalSortExpr],
        ordering_fields: &[FieldRef],
        distinct: bool,
    ) -> Self {
        Self {
            entries: Vec::new(),
            ordering_values: Vec::new(),
            ordering_req: LexOrdering::new(order_bys.to_vec()),
            ordering_fields: ordering_fields.to_vec(),
            grid_size: None,
            distinct,
        }
    }

    pub(crate) fn has_ordering(&self) -> bool {
        self.ordering_req.is_some()
    }

    pub(crate) fn payload_state_field(name: &str) -> FieldRef {
        Arc::new(Field::new(
            format_state_name(name, "payload"),
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, true))),
            true,
        ))
    }

    pub(crate) fn ordering_state_field(name: &str, ordering_fields: &[FieldRef]) -> FieldRef {
        Arc::new(Field::new(
            format_state_name(name, "orderings"),
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(ordering_fields.to_vec())),
                true,
            ))),
            true,
        ))
    }

    pub(crate) fn push(&mut self, wkb: Vec<u8>, ordering: Vec<ScalarValue>) {
        self.entries.push(wkb);
        self.ordering_values.push(ordering);
    }

    pub(crate) fn set_grid_size(&mut self, grid_size: f64) -> Result<()> {
        match self.grid_size {
            Some(current) if current != grid_size => {
                return Err(DataFusionError::Execution(
                    "ST_Union gridSize must be constant within an aggregate group".to_string(),
                ));
            }
            Some(_) => {}
            None => self.grid_size = Some(grid_size),
        }
        Ok(())
    }

    pub(crate) fn sort(&mut self) -> Result<()> {
        let Some(ordering_req) = &self.ordering_req else {
            return Ok(());
        };
        if self.entries.len() < 2 {
            return Ok(());
        }

        let sort_options = ordering_req
            .iter()
            .map(|sort_expr| sort_expr.options)
            .collect::<Vec<_>>();
        let mut combined = self
            .entries
            .drain(..)
            .zip(self.ordering_values.drain(..))
            .collect::<Vec<_>>();
        let mut sort_error = None;
        combined.sort_by(|(_, left), (_, right)| {
            compare_rows(left, right, &sort_options).unwrap_or_else(|error| {
                sort_error = Some(error);
                Ordering::Equal
            })
        });
        if let Some(error) = sort_error {
            return Err(error);
        }
        (self.entries, self.ordering_values) = combined.into_iter().unzip();
        Ok(())
    }

    pub(crate) fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.sort()?;

        let payload = BinaryArray::from_iter_values(self.entries.iter().map(|entry| {
            encode_payload(
                entry,
                if let Some(grid_size) = self.grid_size {
                    GridState::Present(grid_size)
                } else {
                    GridState::Absent
                },
            )
        }));
        let payload = list_state_scalar(DataType::Binary, Arc::new(payload))?;
        if !self.has_ordering() {
            return Ok(vec![payload]);
        }

        let fields = self.ordering_fields.clone();
        let columns = if self.ordering_values.is_empty() {
            fields
                .iter()
                .map(|field| arrow_array::new_empty_array(field.data_type()))
                .collect::<Vec<_>>()
        } else {
            (0..fields.len())
                .map(|index| {
                    ScalarValue::iter_to_array(
                        self.ordering_values.iter().map(|row| row[index].clone()),
                    )
                })
                .collect::<Result<Vec<_>>>()?
        };
        let ordering_struct = StructArray::try_new(Fields::from(fields), columns, None)?;
        let ordering = list_state_scalar(
            ordering_struct.data_type().clone(),
            Arc::new(ordering_struct),
        )?;
        Ok(vec![payload, ordering])
    }

    pub(crate) fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let expected = if self.has_ordering() { 2 } else { 1 };
        if states.len() != expected {
            return Err(DataFusionError::Internal(format!(
                "geometry aggregate expected {expected} state arrays, got {}",
                states.len()
            )));
        }

        let payloads = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "geometry aggregate payload state must be List<Binary>, got {}",
                    states[0].data_type()
                ))
            })?;
        for row in 0..payloads.len() {
            if payloads.is_null(row) {
                continue;
            }
            let values = payloads.value(row);
            let values = values
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "geometry aggregate payload state must contain Binary values".to_string(),
                    )
                })?;
            for index in 0..values.len() {
                if values.is_null(index) {
                    continue;
                }
                let (wkb, grid_size) = decode_payload(values.value(index))?;
                match grid_size {
                    GridState::Present(grid_size) => self.set_grid_size(grid_size)?,
                    GridState::Absent => {}
                }
                self.entries.push(wkb);
            }
        }

        if self.has_ordering() {
            let orderings = states[1]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "geometry aggregate ordering state must be List<Struct>, got {}",
                        states[1].data_type()
                    ))
                })?;
            for row in 0..orderings.len() {
                if orderings.is_null(row) {
                    continue;
                }
                let values = orderings.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "geometry aggregate ordering state must contain Struct values"
                                .to_string(),
                        )
                    })?;
                for index in 0..values.len() {
                    let row_values = values
                        .columns()
                        .iter()
                        .map(|column| ScalarValue::try_from_array(column, index))
                        .collect::<Result<Vec<_>>>()?;
                    self.ordering_values.push(row_values);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn sorted_entries(&mut self) -> Result<&[Vec<u8>]> {
        self.sort()?;
        if self.distinct {
            // DataFusion deduplicates DISTINCT aggregate input itself unless the
            // call also has ORDER BY, in which case the flag reaches the
            // accumulator. PostGIS keeps the first row of each value in input
            // order, so drop later duplicates after sorting.
            self.entries.dedup();
        }
        Ok(&self.entries)
    }

    #[cfg(feature = "geos-3_11")]
    pub(crate) fn grid_size(&self) -> Option<f64> {
        self.grid_size
    }

    pub(crate) fn size(&self) -> usize {
        self.entries.len() * std::mem::size_of::<Vec<u8>>()
            + self.entries.iter().map(Vec::len).sum::<usize>()
            + self.ordering_values.len() * std::mem::size_of::<Vec<ScalarValue>>()
    }
}

pub(crate) fn ordering_fields_from_sort(
    order_bys: &[PhysicalSortExpr],
    schema: &arrow_schema::Schema,
) -> Result<Vec<FieldRef>> {
    order_bys
        .iter()
        .map(|sort| {
            Ok(Arc::new(Field::new(
                sort.expr.to_string(),
                sort.expr.data_type(schema)?,
                true,
            )))
        })
        .collect()
}

fn list_state_scalar(element_type: DataType, values: ArrayRef) -> Result<ScalarValue> {
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
    let field = Arc::new(Field::new_list_field(element_type, true));
    let list = ListArray::new(field, offsets, values, None);
    ScalarValue::try_from_array(&list, 0)
}

enum GridState {
    Absent,
    Present(f64),
}

fn encode_payload(entry: &[u8], grid_size: GridState) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(entry.len() + 17);
    bytes.extend_from_slice(&PAYLOAD_MAGIC.to_le_bytes());
    let bits = match grid_size {
        GridState::Absent => {
            bytes.push(0);
            0
        }
        GridState::Present(grid_size) => {
            bytes.push(1);
            grid_size.to_bits()
        }
    };
    bytes.extend_from_slice(&bits.to_le_bytes());
    bytes.extend_from_slice(&(entry.len() as u32).to_le_bytes());
    bytes.extend_from_slice(entry);
    bytes
}

fn decode_payload(bytes: &[u8]) -> Result<(Vec<u8>, GridState)> {
    let header = bytes
        .get(..13)
        .ok_or_else(|| DataFusionError::Internal("truncated aggregate payload".to_string()))?;
    if u32::from_le_bytes(header[0..4].try_into().unwrap()) != PAYLOAD_MAGIC {
        return Err(DataFusionError::Internal(
            "invalid aggregate payload header".to_string(),
        ));
    }
    let grid_size = match header[4] {
        0 => GridState::Absent,
        1 => GridState::Present(f64::from_bits(u64::from_le_bytes(
            header[5..13].try_into().unwrap(),
        ))),
        other => {
            return Err(DataFusionError::Internal(format!(
                "invalid aggregate grid state {other}"
            )));
        }
    };
    let length = u32::from_le_bytes(
        bytes
            .get(13..17)
            .ok_or_else(|| DataFusionError::Internal("truncated aggregate payload".to_string()))?
            .try_into()
            .unwrap(),
    ) as usize;
    let entry = bytes
        .get(17..17 + length)
        .ok_or_else(|| DataFusionError::Internal("truncated aggregate payload".to_string()))?;
    if entry.len() != length {
        return Err(DataFusionError::Internal(
            "truncated aggregate payload".to_string(),
        ));
    }
    Ok((entry.to_vec(), grid_size))
}
