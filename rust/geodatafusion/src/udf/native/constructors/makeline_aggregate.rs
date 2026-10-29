use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::common::utils::get_row_at_idx;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, ScalarUDF, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::CoordType;

use super::line::{MakeLine, build_line_array_from_coords};
use super::util::{LineParts, field_dimension, line_parts_from_wkb, output_line_field};
use crate::udf::aggregate_state::{GeometryAggregateState, ordering_fields_from_sort};
use crate::udf::native::geometry::{
    geometry_array_from_arrow, is_null_geometry_input, merge_metadata,
};

/// The PostGIS aggregate overload of `ST_MakeLine`. The plain name belongs to
/// the released scalar function that accepts a geometry pair or a geometry
/// array, so the aggregate carries an explicit name.
pub(super) const MAKELINE_AGGREGATE_NAME: &str = "st_makelineagg";

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeLineAggregate {
    signature: Signature,
    coord_type: CoordType,
}

impl MakeLineAggregate {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for MakeLineAggregate {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl AggregateUDFImpl for MakeLineAggregate {
    fn name(&self) -> &str {
        MAKELINE_AGGREGATE_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [argument] if is_geometry_like(argument) => Ok(arg_types.to_vec()),
            _ => Err(DataFusionError::Plan(
                "ST_MakeLineAgg expects one geometry argument per row; use ST_MakeLine for the PostGIS pair and array overloads"
                    .to_string(),
            )),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let (dimension, metadata, coord_type) = match arg_fields {
            [argument] => (
                field_dimension(argument.as_ref())?,
                merge_metadata([argument.as_ref()], "ST_MakeLineAgg")?,
                coord_type_of(argument.as_ref()),
            ),
            _ => {
                return Err(DataFusionError::Plan(
                    "ST_MakeLineAgg expects one geometry argument".to_string(),
                ));
            }
        };
        Ok(Arc::new(output_line_field(
            dimension,
            metadata,
            coord_type.unwrap_or(self.coord_type),
        )))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let input_field = acc_args.exprs[0].return_field(acc_args.schema)?;
        let ordering_fields = ordering_fields_from_sort(acc_args.order_bys, acc_args.schema)?;
        Ok(Box::new(MakeLineAccumulator {
            input_field,
            return_field: acc_args.return_field,
            state: GeometryAggregateState::new(
                acc_args.order_bys,
                &ordering_fields,
                acc_args.is_distinct,
            ),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let mut fields = vec![GeometryAggregateState::payload_state_field(args.name)];
        if !args.ordering_fields.is_empty() {
            fields.push(GeometryAggregateState::ordering_state_field(
                args.name,
                args.ordering_fields,
            ));
        }
        Ok(fields)
    }
}

#[derive(Debug)]
struct MakeLineAccumulator {
    input_field: FieldRef,
    return_field: FieldRef,
    state: GeometryAggregateState,
}

impl Accumulator for MakeLineAccumulator {
    fn update_batch(&mut self, values: &[arrow_array::ArrayRef]) -> Result<()> {
        let value = values.first().ok_or_else(|| {
            DataFusionError::Internal("ST_MakeLine received no input batch".to_string())
        })?;
        if is_null_geometry_input(&self.input_field, value.as_ref()) {
            return Ok(());
        }

        let geometry = geometry_array_from_arrow(value, &self.input_field)?;
        let wkb =
            to_wkb::<i32>(geometry.as_ref()).map_err(crate::error::GeoDataFusionError::GeoArrow)?;
        for row in 0..wkb.inner().len() {
            if wkb.inner().is_null(row) {
                continue;
            }
            let ordering = if values.len() > 1 {
                get_row_at_idx(&values[1..], row)?
            } else {
                Vec::new()
            };
            self.state.push(wkb.inner().value(row).to_vec(), ordering);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[arrow_array::ArrayRef]) -> Result<()> {
        self.state.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.state.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut merged: Option<LineParts> = None;
        for bytes in self.state.sorted_entries()? {
            if let Some(parts) = line_parts_from_wkb(bytes)? {
                match &mut merged {
                    Some(output) => output.merge(parts),
                    None => merged = Some(parts),
                }
            }
        }

        let rows = match merged {
            Some(parts) => vec![Some(parts.build_coords("ST_MakeLine")?)],
            None => vec![None],
        };
        scalar_from_array(build_line_array_from_coords(rows, &self.return_field)?)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.state.size()
    }
}

fn scalar_from_array(value: ColumnarValue) -> Result<ScalarValue> {
    match value {
        ColumnarValue::Array(array) => ScalarValue::try_from_array(array.as_ref(), 0),
        ColumnarValue::Scalar(value) => Ok(value),
    }
}

fn is_geometry_like(data_type: &DataType) -> bool {
    crate::data_types::any_geometry_type().contains(data_type) || data_type == &DataType::Null
}

fn coord_type_of(field: &arrow_schema::Field) -> Option<CoordType> {
    geoarrow_schema::GeoArrowType::from_arrow_field(field)
        .ok()
        .and_then(|typ| typ.coord_type())
}

pub(super) fn register(session_context: &SessionContext) {
    session_context.register_udf(ScalarUDF::from(MakeLine::default()));
    session_context.register_udaf(MakeLineAggregate::default().into());
}
