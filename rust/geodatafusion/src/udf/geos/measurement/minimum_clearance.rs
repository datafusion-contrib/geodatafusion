use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::Float64Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{CoordDimensions, Geom};

use crate::error::GeoDataFusionResult;
use crate::udf::geos::adapter;

static SIGNATURE: LazyLock<Signature> = LazyLock::new(adapter::signature);
static CLEARANCE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
static CLEARANCE_LINE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MinimumClearance;

impl MinimumClearance {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MinimumClearance {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MinimumClearance {
    fn name(&self) -> &str {
        "st_minimumclearance"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(minimum_clearance_impl(&args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(CLEARANCE_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the minimum clearance of geometry, a measure of its robustness to \
                 coordinate changes. Returns Infinity when no minimum clearance exists, for \
                 example for a single point or an empty geometry. Z ordinates are ignored, and \
                 M ordinates are dropped before GEOS.",
                "ST_MinimumClearance(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MinimumClearanceLine;

impl MinimumClearanceLine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MinimumClearanceLine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MinimumClearanceLine {
    fn name(&self) -> &str {
        "st_minimumclearanceline"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_MinimumClearanceLine requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(minimum_clearance_line_impl(&args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(CLEARANCE_LINE_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the two-point LineString spanning the minimum clearance of geometry. \
                 Returns a 2D LINESTRING EMPTY when no minimum clearance exists; non-empty \
                 results keep the dimension reported by GEOS. Z ordinates are ignored, and M \
                 ordinates are dropped before GEOS.",
                "ST_MinimumClearanceLine(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

fn minimum_clearance_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let values = adapter::unary_geos_values(args, |_row, _arrays, _original, geometry| {
        Ok(Some(geometry.minimum_clearance()?))
    })?;
    Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
}

fn minimum_clearance_line_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |_row, _arrays, _original, geometry, writer| {
        let line = geometry.minimum_clearance_line()?;
        // GEOS 3.11 reports three dimensions for the empty clearance line even
        // when the input is two-dimensional. PostGIS exposes that empty result
        // as 2D, while non-empty results keep the dimension reported by GEOS.
        let dimension = if line.is_empty()? {
            CoordDimensions::TwoD
        } else {
            line.get_coordinate_dimension()?
        };
        Ok(Some(writer.write_with_dimension(&line, dimension)?))
    })
}
