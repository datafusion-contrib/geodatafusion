use std::sync::{Arc, OnceLock};

use arrow_array::Float64Array;
use arrow_array::builder::Float64Builder;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{CoordTrait, GeometryTrait, PointTrait};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

fn coordinate_field() -> arrow_schema::FieldRef {
    // Empty points and missing Z/M ordinates produce SQL NULL even when the input
    // geometry field is declared non-nullable.
    Arc::new(Field::new("", DataType::Float64, true))
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct X {
    signature: Signature,
}

impl X {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input().clone(),
        }
    }
}

impl Default for X {
    fn default() -> Self {
        Self::new()
    }
}

static X_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for X {
    fn name(&self) -> &str {
        "st_x"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<arrow_schema::FieldRef> {
        Ok(coordinate_field())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(axis_impl(args, Axis::X, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(X_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the X coordinate of the point, or NULL if not available. Input must be a point; other geometry types raise an error, matching PostGIS.",
                "ST_X(geometry)",
            )
            .with_argument("a_point", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Y {
    signature: Signature,
}

impl Y {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input().clone(),
        }
    }
}

impl Default for Y {
    fn default() -> Self {
        Self::new()
    }
}

static Y_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Y {
    fn name(&self) -> &str {
        "st_y"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<arrow_schema::FieldRef> {
        Ok(coordinate_field())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(axis_impl(args, Axis::Y, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(Y_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the Y coordinate of the point, or NULL if not available. Input must be a point; other geometry types raise an error, matching PostGIS.",
                "ST_Y(geometry)",
            )
            .with_argument("a_point", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Z {
    signature: Signature,
}

impl Z {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input().clone(),
        }
    }
}

impl Default for Z {
    fn default() -> Self {
        Self::new()
    }
}

static Z_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Z {
    fn name(&self) -> &str {
        "st_z"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<arrow_schema::FieldRef> {
        Ok(coordinate_field())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(axis_impl(args, Axis::Z, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(Z_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the Z coordinate of the point, or NULL if not available. Input must be a point; other geometry types raise an error, matching PostGIS.",
                "ST_Z(geometry)",
            )
            .with_argument("a_point", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct M {
    signature: Signature,
}

impl M {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input().clone(),
        }
    }
}

impl Default for M {
    fn default() -> Self {
        Self::new()
    }
}

static M_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for M {
    fn name(&self) -> &str {
        "st_m"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<arrow_schema::FieldRef> {
        Ok(coordinate_field())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(axis_impl(args, Axis::M, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(M_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the M coordinate of the point, or NULL if not available. Input must be a point; other geometry types raise an error, matching PostGIS.",
                "ST_M(geometry)",
            )
            .with_argument("a_point", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Axis {
    X,
    Y,
    Z,
    M,
}

fn axis_impl(
    args: ScalarFunctionArgs,
    axis: Axis,
    name: &str,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_ref, axis_array, axis, name)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn axis_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    axis: Axis,
    name: &str,
) -> GeoDataFusionResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(array.len());
    for item in array.iter() {
        match item {
            Some(geom) => {
                let geom = geom?;
                match geom.as_type() {
                    geo_traits::GeometryType::Point(point) => builder
                        .append_option(point.coord().and_then(|coord| axis_value(&coord, axis))),
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Argument to {name}() must have type POINT"
                        ))
                        .into());
                    }
                }
            }
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

fn axis_value(coord: &impl CoordTrait<T = f64>, axis: Axis) -> Option<f64> {
    match axis {
        Axis::X => Some(coord.x()),
        Axis::Y => Some(coord.y()),
        Axis::Z => match coord.dim() {
            geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm => coord.nth(2),
            _ => None,
        },
        Axis::M => match coord.dim() {
            geo_traits::Dimensions::Xym => coord.nth(2),
            geo_traits::Dimensions::Xyzm => coord.nth(3),
            _ => None,
        },
    }
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::constructors::{PointM, PointZ, PointZM};
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_accessors() {
        let ctx = SessionContext::new();

        ctx.register_udf(X::new().into());
        ctx.register_udf(Y::new().into());
        ctx.register_udf(Z::new().into());
        ctx.register_udf(M::new().into());
        ctx.register_udf(PointZ::new(Default::default()).into());
        ctx.register_udf(PointM::new(Default::default()).into());
        ctx.register_udf(PointZM::new(Default::default()).into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_X(ST_GeomFromText('POINT(1 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 1.0);

        let df = ctx
            .sql("SELECT ST_Y(ST_GeomFromText('POINT(1 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 2.0);

        let df = ctx.sql("SELECT ST_Z(ST_PointZ(1, 2, 3));").await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 3.0);

        let df = ctx
            .sql("SELECT ST_Z(ST_PointZM(1, 2, 3, 4));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 3.0);

        let df = ctx.sql("SELECT ST_M(ST_PointM(1, 2, 3));").await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 3.0);

        let df = ctx
            .sql("SELECT ST_M(ST_PointZM(1, 2, 3, 4));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 4.0);
    }

    // Coordinate getters can return NULL even when the input geometry is non-nullable.
    #[tokio::test]
    async fn test_return_schema() {
        let ctx = SessionContext::new();

        ctx.register_udf(X::new().into());
        ctx.register_udf(Y::new().into());
        ctx.register_udf(Z::new().into());
        ctx.register_udf(M::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql(
                "SELECT \
                 ST_X(ST_GeomFromText('POINT EMPTY')), \
                 ST_Y(ST_GeomFromText('POINT EMPTY')), \
                 ST_Z(ST_GeomFromText('POINT(1 2)')), \
                 ST_M(ST_GeomFromText('POINT(1 2)'))",
            )
            .await
            .unwrap();
        let df_schema = df.schema().inner().clone();
        for field in df_schema.fields() {
            assert_eq!(field.data_type(), &DataType::Float64);
            assert!(field.is_nullable(), "{} must be nullable", field.name());
        }
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(df_schema, batch.schema());
        for index in 0..4 {
            assert!(batch.column(index).is_null(0));
        }
    }
}
