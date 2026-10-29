use std::sync::{Arc, OnceLock};

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{Dimensions, GeometryTrait};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HasZ;

impl HasZ {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HasZ {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for HasZ {
    fn name(&self) -> &str {
        "st_hasz"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(has_z_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Checks if a geometry has a Z dimension.",
                "ST_HasZ(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

fn has_z_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let geo_array = from_arrow_array(&array, &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, has_z_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn has_z_array<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for geometry in array.iter() {
        match geometry {
            Some(geometry) => builder.append_value(matches!(
                geometry?.dim(),
                Dimensions::Xyz | Dimensions::Xyzm
            )),
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}
