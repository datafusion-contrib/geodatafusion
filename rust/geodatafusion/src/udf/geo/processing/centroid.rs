use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo::Centroid as _;
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
pub struct Centroid {
    coord_type: CoordType,
}

impl Centroid {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Centroid {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Centroid {
    fn name(&self) -> &str {
        "st_centroid"
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
        Ok(centroid_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a point which is the geometric center of mass of a geometry.",
                "ST_Centroid(geometry)",
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

fn centroid_impl(
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
    let output = downcast_geoarrow_array!(geo_array_ref, centroid_array, &output_type)?;
    Ok(ColumnarValue::Array(Arc::new(output).to_array_ref()))
}

fn centroid_array<'a>(
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
                match geometry.centroid() {
                    Some(point) => builder.push_point(Some(&point)),
                    None => builder.push_empty(),
                }
            }
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use approx::relative_eq;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::PointArray;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_centroid() {
        let ctx = SessionContext::new();

        ctx.register_udf(Centroid::default().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql(
                "select ST_Centroid(ST_GeomFromText('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let geo_arr =
            PointArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
        let point = geo_arr.value(0).unwrap();
        assert!(relative_eq!(point.coord().unwrap().x(), 2.3076923076923075));
        assert!(relative_eq!(point.coord().unwrap().y(), 3.3076923076923075));
    }
}
