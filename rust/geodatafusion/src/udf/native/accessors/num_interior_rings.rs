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
use geoarrow_array::array::{PolygonArray, from_arrow_array};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowResult;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NumInteriorRings;

impl NumInteriorRings {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NumInteriorRings {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
static ALIASES: LazyLock<Vec<String>> = LazyLock::new(|| vec!["st_numinteriorring".to_string()]);

impl ScalarUDFImpl for NumInteriorRings {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_numinteriorrings"
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
        Ok(num_interior_rings_impl(args)?)
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

fn num_interior_rings_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();

    let out: ArrayRef = match geo_array.data_type() {
        GeoArrowType::Polygon(_) => Arc::new(polygon_impl(geo_array.as_polygon())?),
        _ => Arc::new(downcast_geoarrow_array!(geo_array_ref, geometry_impl)?),
    };

    Ok(out.into())
}

fn polygon_impl(array: &PolygonArray) -> GeoArrowResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(geom?.num_interiors() as u32);
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

fn geometry_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            match geom?.as_type() {
                geo_traits::GeometryType::Polygon(geom) => {
                    builder.append_value(geom.num_interiors() as u32)
                }
                _ => {
                    builder.append_null();
                }
            };
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}
