//! Polygon perimeter measurement UDFs.

use std::sync::{Arc, LazyLock, OnceLock};

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

use super::measure::{Axis, is_null_array, null_float64, polygonal_perimeter};
use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{geometry_array_from_arrow, is_null_geometry_field};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Perimeter;

impl Perimeter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Perimeter {
    fn default() -> Self {
        Self::new()
    }
}

static ALIASES: LazyLock<Vec<String>> = LazyLock::new(|| vec!["st_perimeter2d".to_string()]);
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Perimeter {
    fn name(&self) -> &str {
        "st_perimeter"
    }

    fn aliases(&self) -> &[String] {
        ALIASES.as_slice()
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(perimeter_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D Cartesian length of the boundary of a polygonal geometry. Z and M are ignored. Non-polygonal components return 0.",
                "ST_Perimeter(geom)",
            )
            .with_argument("geom", "geometry")
            .with_sql_example(
                "SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0, 3 0, 3 4, 0 0))'));",
            )
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Perimeter3D;

impl Perimeter3D {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Perimeter3D {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION_3D: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Perimeter3D {
    fn name(&self) -> &str {
        "st_3dperimeter"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(perimeter_3d_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION_3D.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 3D Cartesian length of the boundary of a polygonal geometry. Z is used when present; M is ignored. XY and XYM geometries use their 2D perimeter. Non-polygonal components return 0.",
                "ST_3DPerimeter(geom)",
            )
            .with_argument("geom", "geometry")
            .with_sql_example(
                "SELECT ST_3DPerimeter(ST_GeomFromText('POLYGON Z ((0 0 0, 3 0 0, 3 4 12, 0 0 0))'));",
            )
            .build()
        }))
    }
}

fn perimeter_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let result = perimeter_array(args, Axis::TwoDimensional)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn perimeter_3d_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let result = perimeter_array(args, Axis::ThreeDimensional)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn perimeter_array(
    args: ScalarFunctionArgs,
    axis: Axis,
) -> GeoDataFusionResult<arrow_array::Float64Array> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .expect("perimeter functions have one argument");
    if is_null_array(&array) || is_null_geometry_field(args.arg_fields[0].as_ref()) {
        return Ok(null_float64(args.number_rows));
    }
    let geo_array = geometry_array_from_arrow(&array, &args.arg_fields[0])?;
    Ok(polygonal_perimeter(geo_array.as_ref(), axis)?)
}
