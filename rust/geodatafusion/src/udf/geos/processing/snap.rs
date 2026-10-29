//! GEOS-backed ST_Snap.

use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::Geom;

use super::super::{adapter, m_dimension};
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Snap;

impl Snap {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Snap {
    fn default() -> Self {
        Self::new()
    }
}

static SNAP_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Snap {
    fn name(&self) -> &str {
        "st_snap"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(adapter::signature)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[3], "ST_Snap")?;
        adapter::check_geometry_type(&arg_types[0], 0, "ST_Snap")?;
        adapter::check_geometry_type(&arg_types[1], 1, "ST_Snap")?;
        Ok(vec![
            arg_types[0].clone(),
            arg_types[1].clone(),
            adapter::coerce_float64(&arg_types[2], 2, "ST_Snap")?,
        ])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Snap requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0, 1], "ST_Snap")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(snap_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SNAP_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Snaps segments and vertices of the input geometry to vertices of a reference geometry.",
                "ST_Snap(input, reference, tolerance)",
            )
            .with_argument("input", "geometry to snap")
            .with_argument("reference", "geometry supplying snap vertices")
            .with_argument("tolerance", "snap tolerance")
            .build()
        }))
    }
}

fn snap_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::binary_geos(
        &args,
        |row, arrays, left_wkb, left, right_wkb, right, writer| {
            let tolerance =
                adapter::float64_value(arrays, 2, row, "ST_Snap")?.ok_or_else(|| {
                    DataFusionError::Internal(
                        "ST_Snap tolerance was null after adapter filtering".to_string(),
                    )
                })?;
            let snapped = left.snap(right, tolerance)?;
            if snapped.is_empty()? {
                // GEOS 3.11 reports an unstable coordinate dimension for empty
                // results (XY input becomes LINESTRING Z EMPTY). PostGIS derives
                // the empty result from the M-stripped inputs, keeping Z when
                // either operand has it, so write that dimension explicitly and
                // rebuild the empty WKB when the writer does not raise it.
                let dimension = adapter::wkb_coord_dimensions(left_wkb)?
                    .max(adapter::wkb_coord_dimensions(right_wkb)?);
                let bytes = writer.write_with_dimension(&snapped, dimension)?;
                Ok(Some(m_dimension::force_dimension(&bytes, dimension)?))
            } else {
                Ok(Some(writer.write(&snapped)?))
            }
        },
    )
}
