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
pub struct UnaryUnion {
    signature: Signature,
}

impl UnaryUnion {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for UnaryUnion {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for UnaryUnion {
    fn name(&self) -> &str {
        "st_unaryunion"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1, 2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() == 2 {
            coerced[1] = adapter::coerce_float64(&arg_types[1], 1, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(unary_union_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the union of the components of a single geometry. An optional non-negative gridSize performs the operation in GEOS reduced-precision mode. GEOS computes the overlay in XY and may preserve interpolated Z values. M ordinates are dropped before GEOS; empty input is returned unchanged and may retain M.",
                "ST_UnaryUnion(geometry, gridSize)",
            )
            .with_argument("geometry", "geometry or geometry collection")
            .with_argument("gridSize", "optional float; negative values disable precision reduction")
            .build()
        }))
    }
}

fn unary_union_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |row, arrays, original, geometry, writer| {
        let grid_size = if arrays.len() > 1 {
            match adapter::float64_value(arrays, 1, row, "ST_UnaryUnion")? {
                Some(grid_size) => grid_size,
                None => return Ok(None),
            }
        } else {
            -1.0
        };
        if !grid_size.is_finite() {
            return Err(DataFusionError::Execution(
                "ST_UnaryUnion gridSize must be finite".to_string(),
            )
            .into());
        }
        if geometry.is_empty()? {
            return Ok(Some(original.to_vec()));
        }

        let result = if grid_size >= 0.0 {
            geometry.unary_union_prec(grid_size)?
        } else {
            geometry.unary_union()?
        };
        Ok(Some(writer.write(&result)?))
    })
}
