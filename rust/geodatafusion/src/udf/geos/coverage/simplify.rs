use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::PartitionEvaluatorArgs;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{Documentation, PartitionEvaluator, Signature, WindowUDFImpl};

use super::window::{self, CoverageWindowEvaluator, Operation};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CoverageSimplify {
    signature: Signature,
}

impl CoverageSimplify {
    pub fn new() -> Self {
        Self {
            signature: super::super::adapter::signature(),
        }
    }
}

impl Default for CoverageSimplify {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl WindowUDFImpl for CoverageSimplify {
    fn name(&self) -> &str {
        "st_coveragesimplify"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        super::super::adapter::check_arg_count(arg_types, &[2, 3], self.name())?;
        super::super::adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = vec![
            arg_types[0].clone(),
            super::super::adapter::coerce_float64(&arg_types[1], 1, self.name())?,
        ];
        if arg_types.len() == 3 {
            coerced.push(window::coerce_boolean(&arg_types[2], 2, self.name())?);
        }
        Ok(coerced)
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        CoverageWindowEvaluator::from_args(args, Operation::Simplify, self.name())
    }

    fn field(
        &self,
        field_args: datafusion::logical_expr::function::WindowUDFFieldArgs,
    ) -> Result<FieldRef> {
        let input = field_args.get_input_field(0).ok_or_else(|| {
            DataFusionError::Internal(
                "ST_CoverageSimplify is missing its geometry input".to_string(),
            )
        })?;
        window::output_field(&input, field_args.name(), self.name())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Simplifies polygon edges in a window partition while preserving a valid polygonal coverage. The default simplifies the outer boundary of the coverage; passing false preserves it. NULL, EMPTY, and non-polygonal inputs do not participate and return SQL NULL. Requires GEOS 3.12 or newer and the geos-3_12 Cargo feature.",
                "ST_CoverageSimplify(geometry, tolerance, simplifyBoundary)",
            )
            .with_argument("geometry", "polygonal coverage member")
            .with_argument("tolerance", "Visvalingam-Whyatt linear tolerance")
            .with_argument("simplifyBoundary", "whether to simplify the outer coverage boundary")
            .build()
        }))
    }
}
