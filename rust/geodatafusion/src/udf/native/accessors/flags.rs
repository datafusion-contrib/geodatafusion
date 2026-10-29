use std::sync::{Arc, OnceLock};

use arrow_array::builder::{BooleanBuilder, Int16Builder};
use arrow_array::{BooleanArray, Int16Array};
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{Dimensions, GeometryTrait};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Zmflag;

impl Zmflag {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Zmflag {
    fn default() -> Self {
        Self::new()
    }
}

static ZMFLAG_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Zmflag {
    fn name(&self) -> &str {
        "st_zmflag"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int16)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(zmflag_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(ZMFLAG_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return a code indicating the ZM coordinate dimension: 0 for 2D, 1 for 3D-M, \
                 2 for 3D-Z, and 3 for 4D.",
                "ST_Zmflag(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn zmflag_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, zmflag_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn zmflag_array<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoDataFusionResult<Int16Array> {
    let mut builder = Int16Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(zmflag(geom?.dim()));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn zmflag(dim: Dimensions) -> i16 {
    let z = matches!(
        dim,
        Dimensions::Xyz | Dimensions::Xyzm | Dimensions::Unknown(3 | 4)
    );
    let m = matches!(
        dim,
        Dimensions::Xym | Dimensions::Xyzm | Dimensions::Unknown(4)
    );
    i16::from(z) * 2 + i16::from(m)
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HasZ;

impl HasZ {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HasZ {
    fn default() -> Self {
        Self::new()
    }
}

static HAS_Z_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

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
        Ok(has_dimension_impl(args, DimensionAxis::Z)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(HAS_Z_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return TRUE if the geometry has a Z dimension.",
                "ST_HasZ(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HasM;

impl HasM {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HasM {
    fn default() -> Self {
        Self::new()
    }
}

static HAS_M_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for HasM {
    fn name(&self) -> &str {
        "st_hasm"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(has_dimension_impl(args, DimensionAxis::M)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(HAS_M_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return TRUE if the geometry has an M (measure) dimension.",
                "ST_HasM(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

#[derive(Debug, Clone, Copy)]
enum DimensionAxis {
    Z,
    M,
}

fn has_dimension_impl(
    args: ScalarFunctionArgs,
    axis: DimensionAxis,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, has_dimension_array, axis)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn has_dimension_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    axis: DimensionAxis,
) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let dim = geom?.dim();
            let has = match axis {
                DimensionAxis::Z => {
                    matches!(
                        dim,
                        Dimensions::Xyz | Dimensions::Xyzm | Dimensions::Unknown(3 | 4)
                    )
                }
                DimensionAxis::M => matches!(
                    dim,
                    Dimensions::Xym | Dimensions::Xyzm | Dimensions::Unknown(4)
                ),
            };
            builder.append_value(has);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zmflag_values() {
        assert_eq!(zmflag(Dimensions::Xy), 0);
        assert_eq!(zmflag(Dimensions::Xym), 1);
        assert_eq!(zmflag(Dimensions::Xyz), 2);
        assert_eq!(zmflag(Dimensions::Xyzm), 3);
    }
}
