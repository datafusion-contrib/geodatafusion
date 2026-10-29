use std::sync::{LazyLock, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{CoordSeq, CoordType, Geom, Geometry};

use crate::error::GeoDataFusionResult;
use crate::udf::geos::adapter;

static SIGNATURE: LazyLock<Signature> = LazyLock::new(adapter::signature);
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ClosestPoint;

impl ClosestPoint {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClosestPoint {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ClosestPoint {
    fn name(&self) -> &str {
        "st_closestpoint"
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
        Err(DataFusionError::Internal(
            "ST_ClosestPoint requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0, 1], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(closest_point_impl(&args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D point on geom1 closest to geom2. Returns NULL when either \
                 geometry is empty. Z ordinates are ignored. The selected point is a valid \
                 nearest point; when several points share the minimum distance, the choice \
                 among them is not specified. M ordinates are dropped before GEOS.",
                "ST_ClosestPoint(geom1, geom2)",
            )
            .with_argument("geom1", "geometry")
            .with_argument("geom2", "geometry")
            .build()
        }))
    }
}

fn closest_point_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::binary_geos(
        args,
        |_row, _arrays, _left_bytes, left, _right_bytes, right, writer| {
            if left.is_empty()? || right.is_empty()? {
                return Ok(None);
            }

            let nearest = left.nearest_points(right)?;
            let mut point = CoordSeq::new(1, CoordType::XY)?;
            point.set_x(0, nearest.get_x(0)?)?;
            point.set_y(0, nearest.get_y(0)?)?;
            let point = Geometry::create_point(point)?;
            Ok(Some(writer.write(&point)?))
        },
    )
}
