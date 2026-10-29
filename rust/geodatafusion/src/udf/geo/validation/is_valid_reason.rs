use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringViewBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo::Validation;
use geoarrow_array::GeoArrowArray;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::geo::to_geo::to_geo_iter;
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsValidReason;

impl IsValidReason {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for IsValidReason {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsValidReason {
    fn name(&self) -> &str {
        "st_isvalidreason"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_valid_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns text stating if a geometry is valid, or if invalid a reason why.",
                "ST_IsValidReason(geomA)",
            )
            .with_argument("geomA", "geometry")
            .build()
        }))
    }
}

fn is_valid_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let mut builder = StringViewBuilder::with_capacity(geo_array.len());
    for item in to_geo_iter(&geo_array)? {
        match item {
            Some(geometry) => match geometry?.check_validation() {
                Ok(()) => builder.append_value("Valid Geometry"),
                Err(error) => builder.append_value(error.to_string()),
            },
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}
