use std::any::Any;
use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::UInt32Builder;
use arrow_array::{ArrayRef, UInt32Array};
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
pub struct NPoints;

impl NPoints {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NPoints {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
static ALIASES: LazyLock<Vec<String>> = LazyLock::new(|| vec!["st_numpoints".to_string()]);

impl ScalarUDFImpl for NPoints {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_npoints"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn aliases(&self) -> &[String] {
        &ALIASES
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(coord_dim_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the number of points in a geometry. Works for all geometries.",
                "ST_NPoints(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn coord_dim_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = Arc::new(num_points(&geo_array)?) as ArrayRef;
    Ok(ColumnarValue::Array(result))
}

fn num_points(array: &dyn GeoArrowArray) -> GeoArrowResult<UInt32Array> {
    downcast_geoarrow_array!(array, _num_points_impl)
}

fn _num_points_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(num_coords_geometry(&geom?));
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

#[inline]
fn num_coords_geometry(geom: &impl GeometryTrait) -> u32 {
    use geo_traits::GeometryType::*;

    match geom.as_type() {
        Point(geom) => num_coords_point(geom),
        LineString(geom) => num_coords_line_string(geom),
        Polygon(geom) => num_coords_polygon(geom),
        MultiPoint(geom) => num_coords_multi_point(geom),
        MultiLineString(geom) => num_coords_multi_line_string(geom),
        MultiPolygon(geom) => num_coords_multi_polygon(geom),
        GeometryCollection(geom) => num_coords_geometry_collection(geom),
        // Check what postgis says for a Rect
        Rect(_) => 4,
        Line(_) => 2,
        Triangle(_) => 3,
    }
}

#[inline]
fn num_coords_point(geom: &impl PointTrait) -> u32 {
    if geom.coord().is_some() { 1 } else { 0 }
}

#[inline]
fn num_coords_line_string(geom: &impl LineStringTrait) -> u32 {
    geom.num_coords() as u32
}

#[inline]
fn num_coords_polygon(geom: &impl PolygonTrait) -> u32 {
    let exterior_coords = geom
        .exterior()
        .map(|ext| num_coords_line_string(&ext))
        .unwrap_or(0);
    geom.interiors().fold(exterior_coords, |acc, interior| {
        acc + num_coords_line_string(&interior)
    })
}

#[inline]
fn num_coords_multi_point(geom: &impl MultiPointTrait) -> u32 {
    geom.points()
        .fold(0, |acc, point| acc + num_coords_point(&point))
}

#[inline]
fn num_coords_multi_line_string(geom: &impl MultiLineStringTrait) -> u32 {
    geom.line_strings().fold(0, |acc, line_string| {
        acc + num_coords_line_string(&line_string)
    })
}

#[inline]
fn num_coords_multi_polygon(geom: &impl MultiPolygonTrait) -> u32 {
    geom.polygons()
        .fold(0, |acc, polygon| acc + num_coords_polygon(&polygon))
}

#[inline]
fn num_coords_geometry_collection(geom: &impl GeometryCollectionTrait) -> u32 {
    geom.geometries()
        .fold(0, |acc, g| acc + num_coords_geometry(&g))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt32Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();

        ctx.register_udf(NPoints::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql(
                "select ST_NPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<UInt32Type>().value(0);
        assert_eq!(val, 4);
    }
}
