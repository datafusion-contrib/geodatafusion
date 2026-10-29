use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::Float64Array;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::Geom;

use crate::error::GeoDataFusionResult;
use crate::udf::geos::adapter;

static SIGNATURE: LazyLock<Signature> = LazyLock::new(adapter::signature);
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HausdorffDistance;

impl HausdorffDistance {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HausdorffDistance {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for HausdorffDistance {
    fn name(&self) -> &str {
        "st_hausdorffdistance"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2, 3], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        adapter::check_geometry_type(&arg_types[1], 1, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() == 3 {
            coerced[2] = adapter::coerce_float64(&arg_types[2], 2, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(hausdorff_distance_impl(&args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the discrete Hausdorff distance between geom1 and geom2. The optional \
                 positive densifyFrac splits every segment into equal-length subsegments and \
                 must be in (0, 1]. A non-positive provided value is rejected. Returns NULL when \
                 either geometry is empty or densifyFrac is NULL. Z ordinates are ignored, and \
                 M ordinates are dropped before GEOS.",
                "ST_HausdorffDistance(geom1, geom2[, densifyFrac])",
            )
            .with_argument("geom1", "geometry")
            .with_argument("geom2", "geometry")
            .with_argument("densifyFrac", "optional float in (0, 1]")
            .build()
        }))
    }
}

fn hausdorff_distance_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let values = adapter::binary_geos_values(
        args,
        |row, arrays, _left_bytes, left, _right_bytes, right| {
            if left.is_empty()? || right.is_empty()? {
                return Ok(None);
            }

            let distance = if arrays.len() > 2 {
                let Some(densify_fraction) =
                    adapter::float64_value(arrays, 2, row, "ST_HausdorffDistance")?
                else {
                    return Ok(None);
                };
                left.hausdorff_distance_densify(right, densify_fraction)?
            } else {
                left.hausdorff_distance(right)?
            };
            Ok(Some(distance))
        },
    )?;
    Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
}
