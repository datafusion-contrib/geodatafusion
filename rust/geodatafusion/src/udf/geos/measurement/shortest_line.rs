use std::sync::{LazyLock, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{Geom, Geometry};

use crate::error::GeoDataFusionResult;
use crate::udf::geos::adapter;

static SIGNATURE: LazyLock<Signature> = LazyLock::new(adapter::signature);
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ShortestLine;

impl ShortestLine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ShortestLine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ShortestLine {
    fn name(&self) -> &str {
        "st_shortestline"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        adapter::check_geometry_type(&arg_types[1], 1, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_ShortestLine requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0, 1], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(shortest_line_impl(&args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D shortest line between geom1 and geom2. The line starts on \
                 geom1 and ends on geom2. Returns NULL when either geometry is empty. When \
                 the geometries intersect the two endpoints coincide at a shared point. Z \
                 ordinates are ignored. When several lines share the minimum length, the \
                 choice among them is not specified. M ordinates are dropped before GEOS.",
                "ST_ShortestLine(geom1, geom2)",
            )
            .with_argument("geom1", "geometry")
            .with_argument("geom2", "geometry")
            .build()
        }))
    }
}

fn shortest_line_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::binary_geos(
        args,
        |_row, _arrays, _left_bytes, left, _right_bytes, right, writer| {
            if left.is_empty()? || right.is_empty()? {
                return Ok(None);
            }

            let nearest = left.nearest_points(right)?;
            let line = Geometry::create_line_string(nearest)?;
            Ok(Some(writer.write(&line)?))
        },
    )
}
