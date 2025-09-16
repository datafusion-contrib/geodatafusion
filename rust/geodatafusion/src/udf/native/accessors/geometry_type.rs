use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringViewBuilder;
use arrow_array::{ArrayRef, StringViewArray};
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::*;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeometryType {
    signature: Signature,
}

impl GeometryType {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for GeometryType {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeometryType {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geometrytype"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(geometry_type_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the type of the geometry as a string. Eg: 'LINESTRING', 'POLYGON', 'MULTIPOINT', etc.",
                "GeometryType(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
#[allow(non_camel_case_types)]
pub struct ST_GeometryType {
    signature: Signature,
}

impl ST_GeometryType {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for ST_GeometryType {
    fn default() -> Self {
        Self::new()
    }
}

static ST_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ST_GeometryType {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geometrytype"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(geometry_type_impl_st(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(ST_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the type of the geometry as a string. Eg: 'LINESTRING', 'POLYGON', 'MULTIPOINT', etc.",
                "ST_GeometryType(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn geometry_type_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let result = Arc::new(geometry_type_array(&geo_array)?) as ArrayRef;
    Ok(ColumnarValue::Array(result))
}

fn geometry_type_array(array: &dyn GeoArrowArray) -> GeoArrowResult<StringViewArray> {
    downcast_geoarrow_array!(array, _geometry_type_impl)
}

fn _geometry_type_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<StringViewArray> {
    let mut builder = StringViewBuilder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geom = geom?;
            // TODO: should be possible to write to the underlying buffer directly instead of
            // allocating a String?
            let s = format!("{}{}", geometry_type_str(&geom), geometry_suffix_str(&geom));
            builder.append_value(s);
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

#[inline]
fn geometry_type_str(geom: &impl GeometryTrait) -> &'static str {
    use geo_traits::GeometryType::*;

    match geom.as_type() {
        Point(_) => "POINT",
        LineString(_) => "LINESTRING",
        Polygon(_) => "POLYGON",
        MultiPoint(_) => "MULTIPOINT",
        MultiLineString(_) => "MULTILINESTRING",
        MultiPolygon(_) => "MULTIPOLYGON",
        GeometryCollection(_) => "GEOMETRYCOLLECTION",
        Rect(_) => "POLYGON",
        Line(_) => "LINESTRING",
        Triangle(_) => "POLYGON",
    }
}

fn geometry_type_impl_st(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let result = Arc::new(geometry_type_array_st(&geo_array)?) as ArrayRef;
    Ok(ColumnarValue::Array(result))
}

fn geometry_type_array_st(array: &dyn GeoArrowArray) -> GeoArrowResult<StringViewArray> {
    downcast_geoarrow_array!(array, _geometry_type_impl_st)
}

fn _geometry_type_impl_st<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<StringViewArray> {
    let mut builder = StringViewBuilder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(geometry_type_str_st(&geom?));
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

#[inline]
fn geometry_type_str_st(geom: &impl GeometryTrait) -> &'static str {
    use geo_traits::GeometryType::*;

    match geom.as_type() {
        Point(_) => "ST_Point",
        LineString(_) => "ST_LineString",
        Polygon(_) => "ST_Polygon",
        MultiPoint(_) => "ST_MultiPoint",
        MultiLineString(_) => "ST_MultilineString",
        MultiPolygon(_) => "ST_MultiPolygon",
        GeometryCollection(_) => "ST_GeometryCollection",
        Rect(_) => "ST_Polygon",
        Line(_) => "ST_LineString",
        Triangle(_) => "ST_Polygon",
    }
}

#[inline]
fn geometry_suffix_str(geom: &impl GeometryTrait) -> &'static str {
    match geom.dim() {
        geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => "",
        geo_traits::Dimensions::Xyz => "Z",
        geo_traits::Dimensions::Xym => "M",
        geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(4) => "ZM",
        geo_traits::Dimensions::Unknown(_) => "",
    }
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_geometry_type() {
        let ctx = SessionContext::new();

        ctx.register_udf(GeometryType::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_string_view().value(0);
        assert_eq!(val, "LINESTRING");
    }

    #[tokio::test]
    async fn test_st_geometry_type() {
        let ctx = SessionContext::new();

        ctx.register_udf(ST_GeometryType::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_string_view().value(0);
        assert_eq!(val, "ST_LineString");
    }
}
