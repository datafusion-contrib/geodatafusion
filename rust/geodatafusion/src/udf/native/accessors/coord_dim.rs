use std::sync::{Arc, OnceLock};

use arrow_array::UInt8Array;
use arrow_array::builder::UInt8Builder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::GeometryTrait;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CoordDim;

impl CoordDim {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for CoordDim {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for CoordDim {
    fn name(&self) -> &str {
        "st_coorddim"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(coord_dim_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the coordinate dimension of the ST_Geometry value.",
                "ST_CoordDim(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn coord_dim_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_ref, coord_dim_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn coord_dim_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<UInt8Array> {
    let mut builder = UInt8Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geometry) = item.transpose()? {
            builder.append_value(geometry.dim().size() as u8);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NDims;

impl NDims {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NDims {
    fn default() -> Self {
        Self::new()
    }
}

static NDIMS_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for NDims {
    fn name(&self) -> &str {
        "st_ndims"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(coord_dim_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(NDIMS_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the coordinate dimension of the geometry.",
                "ST_NDims(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt8Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_coord_dim() {
        let ctx = SessionContext::new();

        ctx.register_udf(CoordDim::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_CoordDim(ST_GeomFromText('POINT(1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<UInt8Type>().value(0);
        assert_eq!(val, 2);
    }

    #[tokio::test]
    async fn test_ndims() {
        let ctx = SessionContext::new();

        ctx.register_udf(NDims::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_NDims(ST_GeomFromText('POINT(1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<UInt8Type>().value(0);
        assert_eq!(val, 2);
    }
}
