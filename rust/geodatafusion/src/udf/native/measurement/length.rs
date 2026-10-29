//! 3D length measurement UDFs.

use std::sync::{Arc, OnceLock};

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

use super::measure::{Axis, is_null_array, linear_length, null_float64};
use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{geometry_array_from_arrow, is_null_geometry_field};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Length3D;

impl Length3D {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Length3D {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Length3D {
    fn name(&self) -> &str {
        "st_3dlength"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(length_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 3D Cartesian length of a linear geometry. Z is used when present; M is ignored. XY and XYM geometries use their 2D length. Polygon components return 0.",
                "ST_3DLength(geom)",
            )
            .with_argument("geom", "geometry")
            .with_sql_example(
                "SELECT ST_3DLength(ST_GeomFromText('LINESTRING Z (0 0 0, 3 4 12)'));",
            )
            .build()
        }))
    }
}

fn length_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .expect("ST_3DLength has one argument");
    if is_null_array(&array) || is_null_geometry_field(args.arg_fields[0].as_ref()) {
        return Ok(ColumnarValue::Array(Arc::new(null_float64(
            args.number_rows,
        ))));
    }
    let geo_array = geometry_array_from_arrow(&array, &args.arg_fields[0])?;
    let result = linear_length(geo_array.as_ref(), Axis::ThreeDimensional)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}
