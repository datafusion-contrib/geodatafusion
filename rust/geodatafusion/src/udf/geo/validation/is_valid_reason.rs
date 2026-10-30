use std::sync::{Arc, OnceLock};

use arrow_array::StringViewArray;
use arrow_array::builder::StringViewBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::geo::validation::validation_error;

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
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let array = geo_array.as_ref();
    let result = downcast_geoarrow_array!(array, is_valid_reason_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn is_valid_reason_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<StringViewArray> {
    let mut builder = StringViewBuilder::with_capacity(array.len());
    for item in array.iter() {
        match item {
            None => builder.append_null(),
            Some(geometry) => {
                let geometry = geometry?;
                match validation_error(&geometry)? {
                    Some(error) => builder.append_value(error),
                    None => builder.append_value("Valid Geometry"),
                }
            }
        }
    }
    Ok(builder.finish())
}
