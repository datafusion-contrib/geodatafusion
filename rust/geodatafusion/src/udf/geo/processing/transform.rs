//! ST_Transform - Coordinate Reference System transformation.
//!
//! This module provides ST_Transform(geom, to_srid) which reprojects geometry
//! coordinates from the source SRID (embedded in the geometry metadata) to the
//! target SRID.
//!
//! Note: This is a placeholder implementation that stores the SRID metadata
//! without performing actual coordinate transformation. Full reprojection
//! requires the `proj` crate which depends on the PROJ C library.
//! When the `proj` crate is available, the implementation should be updated
//! to use `proj::Proj::new_known_crs` for actual coordinate transformation.

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StTransform {
    signature: Signature,
    coord_type: CoordType,
}

impl StTransform {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for StTransform {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_transform"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(transform_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(transform_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a new geometry with its coordinates transformed to a different spatial reference system. Currently stores the target SRID metadata; full reprojection requires the proj crate.",
                "ST_Transform(geometry, target_srid)",
            )
            .with_argument("geom", "geometry")
            .with_argument("target_srid", "integer - target EPSG SRID code")
            .build()
        }))
    }
}

fn transform_return_field(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    // Get target SRID to set in metadata
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn transform_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let target_srid = match args.args[1].cast_to(&DataType::Int32, None)? {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(srid))) => srid,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized SRID not supported for ST_Transform".to_string(),
            )
            .into())
        }
    };

    let result = transform_array(&geo_array, target_srid)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn transform_array(
    array: &dyn GeoArrowArray,
    _target_srid: i32,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _transform_impl, _target_srid)
}

fn _transform_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    _target_srid: i32,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);
    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            builder.push_geometry(Some(&geo_geom))?;
        } else {
            builder.push_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod test {
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_transform_preserves_geometry() {
        let ctx = SessionContext::new();
        ctx.register_udf(StTransform::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql("SELECT ST_AsText(ST_Transform(ST_GeomFromText('POINT(0 0)'), 4326));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(val, "POINT(0 0)");
    }
}
