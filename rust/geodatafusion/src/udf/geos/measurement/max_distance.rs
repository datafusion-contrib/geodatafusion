use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::Float64Array;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

use super::furthest::farthest_pair;
use crate::error::GeoDataFusionResult;
use crate::udf::geos::adapter;

static SIGNATURE: LazyLock<Signature> = LazyLock::new(adapter::signature);
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MaxDistance;

impl MaxDistance {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MaxDistance {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MaxDistance {
    fn name(&self) -> &str {
        "st_maxdistance"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        adapter::check_geometry_type(&arg_types[1], 1, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(max_distance_impl(&args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the maximum 2D Cartesian distance between any two vertices of \
                 geom1 and geom2, in coordinate-system units. Returns NULL when either \
                 geometry is empty. Z ordinates are ignored, and M ordinates are dropped \
                 before GEOS.",
                "ST_MaxDistance(geom1, geom2)",
            )
            .with_argument("geom1", "geometry")
            .with_argument("geom2", "geometry")
            .build()
        }))
    }
}

fn max_distance_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let values = adapter::binary_geos_values(
        args,
        |_row, _arrays, _left_bytes, left, _right_bytes, right| {
            Ok(farthest_pair(left, right)?.map(|pair| pair.distance))
        },
    )?;
    Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
}
