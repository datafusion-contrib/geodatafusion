use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::Geom;

use super::super::adapter;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SharedPaths {
    signature: Signature,
}

impl SharedPaths {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for SharedPaths {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SharedPaths {
    fn name(&self) -> &str {
        "st_sharedpaths"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        adapter::check_geometry_type(&arg_types[1], 1, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0, 1], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(shared_paths_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a GeometryCollection whose first member collects paths traversed in the same direction by both inputs and whose second member collects paths traversed in opposite directions. Inputs must be lineal. The result dimension follows GEOS; M ordinates are dropped before GEOS and are not present in the result.",
                "ST_SharedPaths(geomA, geomB)",
            )
            .with_argument("geomA", "linear geometry")
            .with_argument("geomB", "linear geometry")
            .build()
        }))
    }
}

fn shared_paths_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::binary_geos(
        args,
        |_row, _arrays, _left_bytes, left, _right_bytes, right, writer| {
            let result = left.shared_paths(right)?;
            Ok(Some(writer.write(&result)?))
        },
    )
}
