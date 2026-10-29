use std::os::raw::c_uint;
use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{Geom, sys};

use super::super::{adapter, raw};
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SimplifyPolygonHull {
    signature: Signature,
}

impl SimplifyPolygonHull {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for SimplifyPolygonHull {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SimplifyPolygonHull {
    fn name(&self) -> &str {
        "st_simplifypolygonhull"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2, 3], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        coerced[1] = adapter::coerce_float64(&arg_types[1], 1, self.name())?;
        if arg_types.len() == 3 {
            match &arg_types[2] {
                DataType::Null | DataType::Boolean => {}
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "{} argument 3 must be boolean, got {other}",
                        self.name()
                    )));
                }
            }
            coerced[2] = DataType::Boolean;
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
        Ok(simplify_polygon_hull_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Builds an outer or inner polygon hull retaining the given fraction of vertices. GEOS clamps fractions outside [0, 1] and returns a controlled error for non-polygonal input. Z ordinates are preserved; M ordinates are dropped before GEOS.",
                "ST_SimplifyPolygonHull(geom, vertex_fraction, is_outer)",
            )
            .with_argument("geom", "polygonal geometry")
            .with_argument("vertex_fraction", "fraction of vertices to retain")
            .with_argument("is_outer", "true for an outer hull, false for an inner hull")
            .build()
        }))
    }
}

fn simplify_polygon_hull_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |row, arrays, original, geometry, _writer| {
        if geometry.is_empty()?
            && let Some(empty) = adapter::empty_2d_wkb(geometry.geometry_type()?)
        {
            return Ok(Some(empty.to_vec()));
        }

        let Some(vertex_fraction) =
            adapter::float64_value(arrays, 1, row, "ST_SimplifyPolygonHull")?
        else {
            return Err(DataFusionError::Internal(
                "ST_SimplifyPolygonHull lost a non-null vertex fraction".to_string(),
            )
            .into());
        };
        let is_outer = if arrays.len() > 2 {
            adapter::boolean_value(arrays, 2, row, "ST_SimplifyPolygonHull")?.ok_or_else(|| {
                DataFusionError::Internal(
                    "ST_SimplifyPolygonHull lost a non-null is_outer argument".to_string(),
                )
            })?
        } else {
            true
        };

        let prepared = adapter::prepare_geos_wkb(original)?;
        raw::with_context("ST_SimplifyPolygonHull", |handle| unsafe {
            let input = raw::read_wkb(handle, &prepared, "ST_SimplifyPolygonHull")?;
            let output =
                sys::GEOSPolygonHullSimplify_r(handle, input, is_outer as c_uint, vertex_fraction);
            raw::destroy_geometry(handle, input);
            if output.is_null() {
                return Err(DataFusionError::Execution(
                    "ST_SimplifyPolygonHull requires a polygonal input geometry".to_string(),
                )
                .into());
            }

            let wkb = raw::write_wkb(handle, output, "ST_SimplifyPolygonHull")?;
            raw::destroy_geometry(handle, output);
            Ok(wkb)
        })
        .map(Some)
    })
}
