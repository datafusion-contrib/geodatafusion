use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::PartitionEvaluator;
use datafusion::logical_expr::function::PartitionEvaluatorArgs;
use geoarrow_schema::GeoArrowType;

use super::ffi::{self, WindowOperation};
use super::{boolean_argument, float64_argument, geometry_wkb_values};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Operation {
    InvalidEdges,
    Simplify,
}

pub(super) struct CoverageWindowEvaluator {
    input_field: FieldRef,
    output_type: GeoArrowType,
    argument_count: usize,
    operation: Operation,
    function: String,
}

impl CoverageWindowEvaluator {
    pub(super) fn from_args(
        args: PartitionEvaluatorArgs,
        operation: Operation,
        function: &str,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let input_field = args.input_fields()[0].clone();
        let function_name = function.to_string();
        let output_type =
            GeoArrowType::Geometry(super::geometry_type_from_field(&input_field, function)?);
        Ok(Box::new(Self {
            input_field,
            output_type,
            argument_count: args.input_fields().len(),
            operation,
            function: function_name,
        }))
    }

    fn operation(&self, values: &[ArrayRef]) -> Result<WindowOperation> {
        match self.operation {
            Operation::InvalidEdges => {
                let tolerance = if self.argument_count >= 2 {
                    float64_argument(values, 1, 0.0, &self.function)?
                } else {
                    0.0
                };
                Ok(WindowOperation::InvalidEdges { tolerance })
            }
            Operation::Simplify => {
                let tolerance = if self.argument_count >= 2 {
                    float64_argument(values, 1, 0.0, &self.function)?
                } else {
                    0.0
                };
                let simplify_boundary = if self.argument_count >= 3 {
                    boolean_argument(values, 2, true, &self.function)?
                } else {
                    true
                };
                Ok(WindowOperation::Simplify {
                    tolerance,
                    simplify_boundary,
                })
            }
        }
    }
}

impl std::fmt::Debug for CoverageWindowEvaluator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoverageWindowEvaluator")
            .field("operation", &self.operation)
            .field("function", &self.function)
            .finish_non_exhaustive()
    }
}

impl PartitionEvaluator for CoverageWindowEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        if values.len() < self.argument_count {
            return Err(DataFusionError::Internal(format!(
                "{} expected at least {} window arguments, got {}",
                self.function,
                self.argument_count,
                values.len()
            )));
        }

        let operation = self.operation(values)?;
        let mut wkb = if self.input_field.data_type() == &DataType::Null {
            vec![None; num_rows]
        } else {
            geometry_wkb_values(&self.input_field, values[0].as_ref())?
        };
        if wkb.len() == 1 && num_rows != 1 {
            wkb = vec![wkb[0].clone(); num_rows];
        }
        if wkb.len() != num_rows {
            return Err(DataFusionError::Internal(format!(
                "{} received {} geometry rows for a {}-row partition",
                self.function,
                wkb.len(),
                num_rows
            )));
        }

        let outputs = ffi::coverage_window(&wkb, operation)?;
        Ok(super::super::adapter::geos_values_to_array(
            outputs.into_iter().collect(),
            self.output_type.clone(),
        )?)
    }
}

pub(super) fn output_field(input_field: &FieldRef, name: &str, function: &str) -> Result<FieldRef> {
    let output_type = super::geometry_type_from_field(input_field, function)?;
    Ok(Arc::new(output_type.to_field(name, true)))
}

pub(super) fn coerce_boolean(
    data_type: &DataType,
    position: usize,
    function: &str,
) -> Result<DataType> {
    match data_type {
        DataType::Null | DataType::Boolean => Ok(DataType::Boolean),
        _ => Err(DataFusionError::Plan(format!(
            "{function} argument {} must be boolean, got {data_type}",
            position + 1
        ))),
    }
}
