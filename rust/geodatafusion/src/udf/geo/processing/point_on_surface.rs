use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo::InteriorPoint;
use geoarrow_array::array::PointArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::{CoordType, Dimension, PointType};

use super::util::is_empty_geometry;
use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    field_metadata_or_default, geometry_array_from_arrow, is_null_geometry_input,
};

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
                "Returns a two-dimensional POINT which is guaranteed to lie in the interior of a surface. Lineal Z and M ordinates are dropped.",
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
    let metadata = field_metadata_or_default(args.arg_fields[0].as_ref())?;
    let output_type = PointType::new(Dimension::XY, metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn interior_point_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let input_field = args.arg_fields[0].as_ref();
    let output_type = PointType::new(Dimension::XY, field_metadata_or_default(input_field)?)
        .with_coord_type(coord_type);
    if is_null_geometry_input(input_field, arrays[0].as_ref()) {
        let mut builder =
            geoarrow_array::builder::PointBuilder::with_capacity(output_type, arrays[0].len());
        for _ in 0..arrays[0].len() {
            builder.push_null();
        }
        return Ok(ColumnarValue::Array(
            Arc::new(builder.finish()).to_array_ref(),
        ));
    }

    let geo_array = geometry_array_from_arrow(&arrays[0], input_field)?;
    let geo_array_ref = geo_array.as_ref();
    let output = downcast_geoarrow_array!(geo_array_ref, interior_point_array, &output_type)?;
    Ok(ColumnarValue::Array(Arc::new(output).to_array_ref()))
}

fn interior_point_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &PointType,
) -> GeoDataFusionResult<PointArray> {
    let mut builder =
        geoarrow_array::builder::PointBuilder::with_capacity(output_type.clone(), array.len());
    for item in array.iter() {
        match item {
            None => builder.push_null(),
            Some(geometry) => {
                let geometry = geometry?;
                if is_empty_geometry(&geometry) {
                    builder.push_empty();
                    continue;
                }
                let geometry = geometry_to_geo(&geometry)?;
                match geometry.interior_point() {
                    Some(point) => builder.push_point(Some(&point)),
                    None => builder.push_empty(),
                }
            }
        }
    }
    Ok(builder.finish())
}
