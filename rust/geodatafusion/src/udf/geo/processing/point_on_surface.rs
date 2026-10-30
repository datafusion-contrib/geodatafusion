use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo::InteriorPoint as _;
use geoarrow_array::array::{PointArray, from_arrow_array};
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, Metadata, PointType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::geo::util::{point_or_none, with_owned_geo_geometry};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PointOnSurface {
    coord_type: CoordType,
}

impl PointOnSurface {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for PointOnSurface {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointOnSurface {
    fn name(&self) -> &str {
        "st_pointonsurface"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(interior_point_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a POINT which is guaranteed to lie in the interior of a surface.",
                "ST_PointOnSurface(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = PointType::new(Dimension::XY, metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn interior_point_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let output_type = PointType::new(Dimension::XY, geo_array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let array = geo_array.as_ref();
    let result = downcast_geoarrow_array!(array, interior_point_array, &output_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn interior_point_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &PointType,
) -> GeoArrowResult<PointArray> {
    let mut builder = PointBuilder::with_capacity(output_type.clone(), array.len());
    for item in array.iter() {
        match item {
            None => builder.push_null(),
            Some(geometry) => {
                let geometry = geometry?;
                let point = with_owned_geo_geometry!(&geometry, |owned| {
                    point_or_none(owned.interior_point())
                });
                builder.push_point(point.as_ref());
            }
        }
    }
    Ok(builder.finish())
}
