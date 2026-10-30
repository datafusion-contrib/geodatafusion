//! Ring count accessor

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
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiPolygonTrait, PolygonTrait,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

/// `ST_NRings` scalar UDF implementation: returns the total number of rings (exterior and interior) in a polygonal geometry.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NRings;

impl NRings {
    /// Creates a new `ST_NRings` UDF instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NRings {
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

impl ScalarUDFImpl for NRings {
    fn name(&self) -> &str {
        "st_nrings"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(nrings_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the number of rings in a polygonal geometry, including exterior and interior rings.",
                "ST_NRings(ST_GeomFromText('POLYGON((0 0,0 5,5 5,0 0),(1 1,1 2,2 2,1 1))'))",
            )
            .with_argument("g1", "geometry")
            .with_related_udf("st_numinteriorrings")
            .build()
        }))
    }
}

fn nrings_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        return Ok(ColumnarValue::Array(Arc::new(UInt32Array::from(vec![
            None;
            args.number_rows
        ]))));
    }

    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = Arc::new(_nrings_impl(geo_array.as_ref())?) as ArrayRef;
    Ok(ColumnarValue::Array(result))
}

fn _nrings_impl(array: &dyn geoarrow_array::GeoArrowArray) -> GeoArrowResult<UInt32Array> {
    downcast_geoarrow_array!(array, impl_nrings)
}

fn impl_nrings<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geometry) = item {
            builder.append_value(num_rings(&geometry?));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn num_rings(geometry: &impl GeometryTrait<T = f64>) -> u32 {
    match geometry.as_type() {
        geo_traits::GeometryType::Polygon(polygon) => num_polygon_rings(polygon),
        geo_traits::GeometryType::MultiPolygon(multi_polygon) => multi_polygon
            .polygons()
            .map(|polygon| num_polygon_rings(&polygon))
            .sum(),
        geo_traits::GeometryType::GeometryCollection(collection) => collection
            .geometries()
            .map(|geometry| num_rings(&geometry))
            .sum(),
        _ => 0,
    }
}

fn num_polygon_rings(polygon: &impl PolygonTrait<T = f64>) -> u32 {
    let exterior = polygon
        .exterior()
        .is_some_and(|ring| ring.coords().next().is_some()) as u32;
    exterior
        + polygon
            .interiors()
            .filter(|ring| ring.coords().next().is_some())
            .count() as u32
}
