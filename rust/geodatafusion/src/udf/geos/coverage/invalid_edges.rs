use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::PartitionEvaluatorArgs;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{Documentation, PartitionEvaluator, Signature, WindowUDFImpl};

use super::window::{self, CoverageWindowEvaluator, Operation};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CoverageInvalidEdges {
    signature: Signature,
}

impl CoverageInvalidEdges {
    pub fn new() -> Self {
        Self {
            signature: super::super::adapter::signature(),
        }
    }
}

impl Default for CoverageInvalidEdges {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl WindowUDFImpl for CoverageInvalidEdges {
    fn name(&self) -> &str {
        "st_coverageinvalidedges"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        super::super::adapter::check_arg_count(arg_types, &[1, 2], self.name())?;
        super::super::adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = vec![arg_types[0].clone()];
        if arg_types.len() == 2 {
            coerced.push(super::super::adapter::coerce_float64(
                &arg_types[1],
                1,
                self.name(),
            )?);
        }
        Ok(coerced)
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        CoverageWindowEvaluator::from_args(args, Operation::InvalidEdges, self.name())
    }

    fn field(
        &self,
        field_args: datafusion::logical_expr::function::WindowUDFFieldArgs,
    ) -> Result<FieldRef> {
        let input = field_args.get_input_field(0).ok_or_else(|| {
            DataFusionError::Internal(
                "ST_CoverageInvalidEdges is missing its geometry input".to_string(),
            )
        })?;
        window::output_field(&input, field_args.name(), self.name())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Finds polygon edges that prevent a window partition from forming a valid polygonal coverage. Rows whose output edge is empty return SQL NULL. NULL, EMPTY, and non-polygonal inputs do not participate and also return SQL NULL. Requires GEOS 3.12 or newer and the geos-3_12 Cargo feature.",
                "ST_CoverageInvalidEdges(geometry, tolerance)",
            )
            .with_argument("geometry", "polygonal coverage member")
            .with_argument("tolerance", "maximum gap width to report")
            .build()
        }))
    }
}
