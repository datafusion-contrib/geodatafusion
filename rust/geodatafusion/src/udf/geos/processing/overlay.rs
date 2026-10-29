use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{Geom, Geometry};

use super::super::adapter;
use crate::error::GeoDataFusionResult;

#[derive(Clone, Copy)]
enum EmptyBehavior {
    Intersection,
    Difference,
    SymDifference,
}

macro_rules! overlay_udf {
    (
        $type:ident,
        $name:literal,
        $doc_name:ident,
        $description:literal,
        $example:literal,
        $behavior:ident
    ) => {
        #[derive(Debug, Eq, PartialEq, Hash)]
        pub struct $type {
            signature: Signature,
        }

        impl $type {
            pub fn new() -> Self {
                Self {
                    signature: adapter::signature(),
                }
            }
        }

        impl Default for $type {
            fn default() -> Self {
                Self::new()
            }
        }

        static $doc_name: OnceLock<Documentation> = OnceLock::new();

        impl ScalarUDFImpl for $type {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
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
                Err(DataFusionError::Internal("return_type".to_string()))
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                adapter::geometry_return_field(&args, &[0, 1], self.name())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                Ok(overlay_impl(args, self.name(), EmptyBehavior::$behavior)?)
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some($doc_name.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $description, $example)
                        .with_argument("geomA", "geometry")
                        .with_argument("geomB", "geometry")
                        .with_argument(
                            "gridSize",
                            "optional float; negative values disable precision reduction",
                        )
                        .build()
                }))
            }
        }
    };
}

overlay_udf!(
    Difference,
    "st_difference",
    DIFFERENCE_DOC,
    "Computes the part of geomA that does not intersect geomB. An optional non-negative gridSize performs the operation in GEOS reduced-precision mode. GEOS computes the overlay in XY and may preserve interpolated Z values. M ordinates are dropped before GEOS; an empty operand returned unchanged may retain M.",
    "ST_Difference(geomA, geomB, gridSize)",
    Difference
);
overlay_udf!(
    Intersection,
    "st_intersection",
    INTERSECTION_DOC,
    "Computes the point-set intersection of geomA and geomB. An optional non-negative gridSize performs the operation in GEOS reduced-precision mode. GEOS computes the overlay in XY and may preserve interpolated Z values. M ordinates are dropped before GEOS; an empty operand returned unchanged may retain M.",
    "ST_Intersection(geomA, geomB, gridSize)",
    Intersection
);
overlay_udf!(
    SymDifference,
    "st_symdifference",
    SYM_DIFFERENCE_DOC,
    "Computes the portions of geomA and geomB that do not intersect. An optional non-negative gridSize performs the operation in GEOS reduced-precision mode. GEOS computes the overlay in XY and may preserve interpolated Z values. M ordinates are dropped before GEOS; an empty operand returned unchanged may retain M.",
    "ST_SymDifference(geomA, geomB, gridSize)",
    SymDifference
);

fn overlay_impl(
    args: ScalarFunctionArgs,
    name: &str,
    empty_behavior: EmptyBehavior,
) -> GeoDataFusionResult<ColumnarValue> {
    adapter::binary_geos(
        &args,
        |row, arrays, left_bytes, left, right_bytes, right, writer| {
            let grid_size = if arrays.len() > 2 {
                match adapter::float64_value(arrays, 2, row, name)? {
                    Some(grid_size) => grid_size,
                    None => return Ok(None),
                }
            } else {
                -1.0
            };
            if !grid_size.is_finite() {
                return Err(
                    DataFusionError::Execution(format!("{name} gridSize must be finite")).into(),
                );
            }

            if left.is_empty()? || right.is_empty()? {
                let original = match empty_behavior {
                    EmptyBehavior::Intersection => {
                        // PostGIS checks the right operand first, so when both
                        // operands are empty it returns the right operand.
                        if right.is_empty()? {
                            right_bytes
                        } else {
                            left_bytes
                        }
                    }
                    EmptyBehavior::Difference => left_bytes,
                    EmptyBehavior::SymDifference => {
                        // PostGIS checks the right operand first, so when both
                        // operands are empty it returns the left operand.
                        if right.is_empty()? {
                            left_bytes
                        } else {
                            right_bytes
                        }
                    }
                };
                return Ok(Some(original.to_vec()));
            }

            Ok(Some(overlay_geometry(
                left,
                right,
                grid_size,
                empty_behavior,
                writer,
            )?))
        },
    )
}

fn overlay_geometry(
    left: &Geometry,
    right: &Geometry,
    grid_size: f64,
    empty_behavior: EmptyBehavior,
    writer: &mut adapter::GeosWkbWriter,
) -> GeoDataFusionResult<Vec<u8>> {
    let result = match empty_behavior {
        EmptyBehavior::Intersection => {
            if grid_size >= 0.0 {
                left.intersection_prec(right, grid_size)?
            } else {
                left.intersection(right)?
            }
        }
        EmptyBehavior::Difference => {
            if grid_size >= 0.0 {
                left.difference_prec(right, grid_size)?
            } else {
                left.difference(right)?
            }
        }
        EmptyBehavior::SymDifference => {
            if grid_size >= 0.0 {
                left.sym_difference_prec(right, grid_size)?
            } else {
                left.sym_difference(right)?
            }
        }
    };
    writer.write(&result)
}

pub(super) fn register(context: &datafusion::prelude::SessionContext) {
    context.register_udf(Difference::default().into());
    context.register_udf(Intersection::default().into());
    context.register_udf(SymDifference::default().into());
}
