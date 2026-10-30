use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::Int16Array;
use arrow_array::builder::Int16Builder;
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
use geoarrow_schema::error::GeoArrowError;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

/// `ST_Zmflag` scalar UDF implementation: returns a code indicating the ZM coordinate dimension of a geometry (`0` = 2D, `1` = 3DM, `2` = 3DZ, `3` = 4D).
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ZmFlag;

impl ZmFlag {
    /// Creates a new `ST_Zmflag` UDF instance.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ZmFlag {
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

impl ScalarUDFImpl for ZmFlag {
    fn name(&self) -> &str {
        "st_zmflag"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int16)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(zm_flag_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a code indicating the ZM coordinate dimension of a geometry.",
                "ST_Zmflag(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

fn zm_flag_value(dimensions: Dimensions) -> GeoDataFusionResult<i16> {
    match dimensions {
        Dimensions::Xy | Dimensions::Unknown(2) => Ok(0),
        Dimensions::Xym => Ok(1),
        Dimensions::Xyz => Ok(2),
        Dimensions::Xyzm | Dimensions::Unknown(4) => Ok(3),
        Dimensions::Unknown(size) => Err(GeoArrowError::InvalidGeoArrow(format!(
            "cannot determine ZM flag for {size} physical dimensions"
        ))
        .into()),
    }
}

fn zm_flag_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    if array.data_type() == &DataType::Null {
        return Ok(ColumnarValue::Array(Arc::new(Int16Array::new_null(
            array.len(),
        ))));
    }
    let geo_array = from_arrow_array(&array, &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, zm_flag_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn zm_flag_array<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoDataFusionResult<Int16Array> {
    let mut builder = Int16Builder::with_capacity(array.len());
    for geometry in array.iter() {
        match geometry {
            Some(geometry) => builder.append_value(zm_flag_value(geometry?.dim())?),
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}
