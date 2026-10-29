//! GEOS-backed ST_Normalize.

use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

use super::super::adapter;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Normalize;

impl Normalize {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Normalize {
    fn default() -> Self {
        Self::new()
    }
}

static NORMALIZE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Normalize {
    fn name(&self) -> &str {
        "st_normalize"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(adapter::signature)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1], "ST_Normalize")?;
        adapter::check_geometry_type(&arg_types[0], 0, "ST_Normalize")?;
        Ok(vec![arg_types[0].clone()])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Normalize requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0], "ST_Normalize")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(normalize_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(NORMALIZE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the geometry in its canonical form.",
                "ST_Normalize(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

fn normalize_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(&args, |_row, _arrays, _original, geometry, writer| {
        let mut normalized = geometry.clone();
        normalized.normalize()?;
        Ok(Some(writer.write(&normalized)?))
    })
}
