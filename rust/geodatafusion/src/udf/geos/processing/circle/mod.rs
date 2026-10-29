use std::sync::Arc;

use arrow_array::builder::{Float64Builder, NullBufferBuilder};
use arrow_array::{ArrayRef, BinaryArray, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, Metadata, PointType};

use super::super::adapter;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{expand_rect_to_geometry, geometry_array_from_arrow};

mod geos_impl;

pub use geos_impl::{
    LargestEmptyCircle, MaximumInscribedCircle, MinimumBoundingCircle, MinimumBoundingRadius,
};

fn geometry_wkb_column(
    arrays: &[ArrayRef],
    fields: &[FieldRef],
    position: usize,
    function_name: &str,
) -> GeoDataFusionResult<Option<BinaryArray>> {
    let array = arrays.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "{function_name} is missing argument {}",
            position + 1
        ))
    })?;
    let field = fields.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "{function_name} is missing argument {}",
            position + 1
        ))
    })?;
    if matches!(field.data_type(), DataType::Null) {
        return Ok(None);
    }
    let geo_array = geometry_array_from_arrow(array.as_ref(), field.as_ref())?;
    let expanded_rect = expand_rect_to_geometry(geo_array.as_ref())?;
    let geometry = expanded_rect
        .as_ref()
        .map_or(geo_array.as_ref(), |array| array);
    let wkb = to_wkb::<i32>(geometry)?;
    Ok(Some(wkb.inner().clone()))
}

fn record_fields(
    args: &ReturnFieldArgs,
    function_name: &str,
    nearest: bool,
) -> GeoDataFusionResult<(Fields, PointType)> {
    let metadata = metadata_for_input(args, function_name)?;
    let center_type =
        PointType::new(Dimension::XY, Arc::clone(&metadata)).with_coord_type(CoordType::Separated);
    let mut fields = vec![center_type.to_field("center", true)];
    if nearest {
        fields.push(
            PointType::new(Dimension::XY, metadata)
                .with_coord_type(CoordType::Separated)
                .to_field("nearest", true),
        );
    }
    fields.push(Field::new("radius", DataType::Float64, true));
    Ok((Fields::from(fields), center_type))
}

fn metadata_for_input(
    args: &ReturnFieldArgs,
    function_name: &str,
) -> GeoDataFusionResult<Arc<Metadata>> {
    let field = adapter::geometry_return_field(args, &[0], function_name)?;
    Ok(Arc::new(Metadata::try_from(field.as_ref())?))
}

fn record_array(
    fields: Fields,
    center: PointBuilder,
    nearest: Option<PointBuilder>,
    mut radius: Float64Builder,
    mut validity: NullBufferBuilder,
) -> ColumnarValue {
    let mut arrays: Vec<ArrayRef> = vec![center.finish().into_array_ref()];
    if let Some(nearest) = nearest {
        arrays.push(nearest.finish().into_array_ref());
    }
    arrays.push(Arc::new(radius.finish()));
    ColumnarValue::Array(Arc::new(StructArray::new(
        fields,
        arrays,
        validity.finish(),
    )))
}

fn null_record_array(
    fields: Fields,
    point_type: PointType,
    row_count: usize,
    nearest: bool,
) -> ColumnarValue {
    let mut center = PointBuilder::new(point_type.clone());
    let mut nearest_builder = nearest.then(|| PointBuilder::new(point_type));
    let mut radius = Float64Builder::with_capacity(row_count);
    let mut validity = NullBufferBuilder::new(row_count);
    for _ in 0..row_count {
        center.push_null();
        if let Some(builder) = nearest_builder.as_mut() {
            builder.push_null();
        }
        radius.append_null();
        validity.append_null();
    }
    record_array(fields, center, nearest_builder, radius, validity)
}

fn return_record_fields(args: &ScalarFunctionArgs) -> GeoDataFusionResult<(Fields, PointType)> {
    let DataType::Struct(fields) = args.return_field.data_type() else {
        return Err(DataFusionError::Internal(
            "circle function return field is not a Struct".to_string(),
        )
        .into());
    };
    let point_type = match GeoArrowType::from_arrow_field(fields[0].as_ref())? {
        GeoArrowType::Point(point_type) => point_type,
        other => {
            return Err(DataFusionError::Internal(format!(
                "circle function center field is not a Point, got {other:?}"
            ))
            .into());
        }
    };
    Ok((fields.clone(), point_type))
}
