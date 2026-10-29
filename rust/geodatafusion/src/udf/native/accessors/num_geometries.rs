//! Immediate geometry member count accessor

use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::UInt32Builder;
use arrow_array::{ArrayRef, UInt32Array};
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::is_empty::is_geometry_topologically_empty;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NumGeometries;

impl NumGeometries {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NumGeometries {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    let mut valid_types = any_geometry_type();
    valid_types.push(DataType::Null);
    Signature::uniform(1, valid_types, Volatility::Immutable)
});

impl ScalarUDFImpl for NumGeometries {
    fn name(&self) -> &str {
        "st_numgeometries"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(num_geometries_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the number of immediate geometry members. Topologically empty inputs return zero.",
                "ST_NumGeometries(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))'))",
            )
            .with_argument("g1", "geometry")
            .with_related_udf("st_isempty")
            .build()
        }))
    }
}

fn num_geometries_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        return Ok(ColumnarValue::Array(Arc::new(UInt32Array::from(vec![
            None;
            args.number_rows
        ]))));
    }

    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = Arc::new(_num_geometries_impl(geo_array.as_ref())?) as ArrayRef;
    Ok(ColumnarValue::Array(result))
}

fn _num_geometries_impl(array: &dyn geoarrow_array::GeoArrowArray) -> GeoArrowResult<UInt32Array> {
    downcast_geoarrow_array!(array, impl_num_geometries)
}

fn impl_num_geometries<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geometry) = item {
            let geometry = geometry?;
            let count = if is_geometry_topologically_empty(&geometry) {
                0
            } else {
                immediate_geometry_count(&geometry)
            };
            builder.append_value(count);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn immediate_geometry_count(geometry: &impl GeometryTrait<T = f64>) -> u32 {
    match geometry.as_type() {
        geo_traits::GeometryType::MultiPoint(multi_point) => multi_point.num_points() as u32,
        geo_traits::GeometryType::MultiLineString(multi_line_string) => {
            multi_line_string.num_line_strings() as u32
        }
        geo_traits::GeometryType::MultiPolygon(multi_polygon) => {
            multi_polygon.num_polygons() as u32
        }
        geo_traits::GeometryType::GeometryCollection(collection) => {
            collection.num_geometries() as u32
        }
        _ => 1,
    }
}
