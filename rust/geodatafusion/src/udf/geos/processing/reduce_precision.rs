use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{Geom, Precision};

use super::super::adapter;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ReducePrecision {
    signature: Signature,
}

impl ReducePrecision {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for ReducePrecision {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ReducePrecision {
    fn name(&self) -> &str {
        "st_reduceprecision"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        coerced[1] = adapter::coerce_float64(&arg_types[1], 1, self.name())?;
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(reduce_precision_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a valid geometry with coordinates rounded to the given grid size. Empty input is returned unchanged and may retain M. A grid size of zero leaves coordinates unchanged. GEOS preserves Z while reducing XY precision; computed results drop M. The XY rounding path is the native GEOS one, so decimal grids such as 0.1 can differ by one ULP between GEOS 3.11 and GEOS 3.14 or newer.",
                "ST_ReducePrecision(geometry, gridSize)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("gridSize", "grid size in coordinate units")
            .build()
        }))
    }
}

fn reduce_precision_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |row, arrays, original, geometry, writer| {
        let grid_size = match adapter::float64_value(arrays, 1, row, "ST_ReducePrecision")? {
            Some(grid_size) => grid_size,
            None => return Ok(None),
        };
        if geometry.is_empty()? {
            return Ok(Some(original.to_vec()));
        }

        let result = geometry.set_precision(grid_size, Precision::ValidOutput)?;
        Ok(Some(writer.write(&result)?))
    })
}
