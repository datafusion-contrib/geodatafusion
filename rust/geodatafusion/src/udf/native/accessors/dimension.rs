use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::Int32Builder;
use arrow_array::{ArrayRef, Int32Array};
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::{GeometryCollectionTrait, GeometryTrait};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

/// `ST_Dimension` scalar UDF implementation: returns the inherent topological dimension (`0`, `1`, or `2`) of a geometry.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Dimension;

impl Dimension {
    /// Creates a new `ST_Dimension` UDF instance.
    pub fn new() -> Self {
        Self
    }
}

impl Default for Dimension {
    fn default() -> Self {
        Self::new()
    }
}

static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    let mut valid_types = any_geometry_type();
    valid_types.push(DataType::Null);
    Signature::uniform(1, valid_types, Volatility::Immutable)
});
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Dimension {
    fn name(&self) -> &str {
        "st_dimension"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(dimension_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the topological dimension of a geometry based on its type. Empty geometries retain the dimension of their type; a GeometryCollection returns the maximum dimension of its members, or zero when empty.",
                "ST_Dimension(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

fn dimension_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        let result: ArrayRef = Arc::new(Int32Array::from(vec![None; args.number_rows]));
        return Ok(result.into());
    }

    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = Arc::new(dimension_array(geo_array.as_ref())?) as ArrayRef;
    Ok(result.into())
}

fn dimension_array(array: &dyn GeoArrowArray) -> GeoDataFusionResult<Int32Array> {
    downcast_geoarrow_array!(array, dimension_array_impl)
}

fn dimension_array_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<Int32Array> {
    let mut builder = Int32Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geometry) = item {
            builder.append_value(geometry_dimension(&geometry?));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn geometry_dimension(geometry: &impl GeometryTrait<T = f64>) -> i32 {
    use geo_traits::GeometryType::*;

    match geometry.as_type() {
        Point(_) | MultiPoint(_) => 0,
        LineString(_) | MultiLineString(_) | Line(_) => 1,
        Polygon(_) | MultiPolygon(_) | Rect(_) | Triangle(_) => 2,
        GeometryCollection(collection) => collection
            .geometries()
            .map(|geometry| geometry_dimension(&geometry))
            .max()
            .unwrap_or(0),
    }
}
