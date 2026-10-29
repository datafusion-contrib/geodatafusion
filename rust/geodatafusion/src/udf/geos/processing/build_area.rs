use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{CoordDimensions, Geom};

use super::super::adapter;
use crate::error::GeoDataFusionResult;

// Little-endian WKB for POLYGON EMPTY.
const EMPTY_POLYGON_WKB: &[u8] = &[1, 3, 0, 0, 0, 0, 0, 0, 0];
// Little-endian WKB for POLYGON Z EMPTY.
const EMPTY_POLYGON_Z_WKB: &[u8] = &[1, 3, 0, 0, 0x80, 0, 0, 0, 0];

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct BuildArea {
    signature: Signature,
}

impl BuildArea {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for BuildArea {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for BuildArea {
    fn name(&self) -> &str {
        "st_buildarea"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(build_area_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates polygonal geometry from correctly noded linework. Empty input produces POLYGON EMPTY, retaining the input Z flag; non-empty input that does not enclose an area produces SQL NULL. GEOS may preserve Z. M ordinates are dropped before GEOS and are not present in the result.",
                "ST_BuildArea(geometry)",
            )
            .with_argument("geometry", "linework or polygonal geometry")
            .build()
        }))
    }
}

fn build_area_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |_row, _arrays, _original, geometry, writer| {
        if geometry.is_empty()? {
            let empty = if matches!(
                geometry.get_coordinate_dimension()?,
                CoordDimensions::ThreeD
            ) {
                EMPTY_POLYGON_Z_WKB
            } else {
                EMPTY_POLYGON_WKB
            };
            return Ok(Some(empty.to_vec()));
        }

        let result = geometry.build_area()?;
        if result.is_empty()? {
            Ok(None)
        } else {
            Ok(Some(writer.write(&result)?))
        }
    })
}
