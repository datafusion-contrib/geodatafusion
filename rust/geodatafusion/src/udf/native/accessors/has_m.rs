use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use geo_traits::{Dimensions, GeometryTrait};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HasM;

impl HasM {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HasM {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![
            TypeSignature::Uniform(1, vec![DataType::Null]),
            any_single_geometry_type_input().type_signature.clone(),
        ],
        Volatility::Immutable,
    )
});

impl ScalarUDFImpl for HasM {
    fn name(&self) -> &str {
        "st_hasm"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(has_m_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Checks if a geometry has an M (measure) dimension.",
                "ST_HasM(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

fn has_m_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    if array.data_type() == &DataType::Null {
        return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
            array.len(),
        ))));
    }
    let geo_array = from_arrow_array(&array, &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, has_m_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn has_m_array<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for geometry in array.iter() {
        match geometry {
            Some(geometry) => builder.append_value(matches!(
                geometry?.dim(),
                Dimensions::Xym | Dimensions::Xyzm
            )),
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}
