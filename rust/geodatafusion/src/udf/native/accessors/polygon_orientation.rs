//! `ST_IsPolygonCCW` and `ST_IsPolygonCW`.
//!
//! Orientation is decided with the standard shoelace signed-area contract:
//! exterior rings and holes must have opposite winding, and non-polygonal
//! geometries are accepted. This is an independent mathematical
//! implementation; reviewed PostGIS cases are the behavioral reference.

use std::sync::{Arc, OnceLock};

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiPolygonTrait, PolygonTrait,
};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{geometry_array_from_arrow, is_null_geometry_input};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum Orientation {
    CounterClockwise,
    Clockwise,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsPolygonOrientation {
    orientation: Orientation,
}

impl IsPolygonOrientation {
    pub fn ccw() -> Self {
        Self {
            orientation: Orientation::CounterClockwise,
        }
    }

    pub fn cw() -> Self {
        Self {
            orientation: Orientation::Clockwise,
        }
    }

    fn function_name(&self) -> &'static str {
        match self.orientation {
            Orientation::CounterClockwise => "st_ispolygonccw",
            Orientation::Clockwise => "st_ispolygoncw",
        }
    }
}

static CCW_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
static CW_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsPolygonOrientation {
    fn name(&self) -> &str {
        self.function_name()
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(polygon_orientation_impl(args, self.orientation)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        match self.orientation {
            Orientation::CounterClockwise => {
                Some(CCW_DOCUMENTATION.get_or_init(|| {
                    Documentation::builder(
                        DOC_SECTION_OTHER,
                        "Tests if Polygon exteriors are counter-clockwise and interior rings are clockwise. Non-polygonal inputs return true.",
                        "ST_IsPolygonCCW(geom)",
                    )
                    .with_argument("geom", "geometry")
                    .build()
                }))
            }
            Orientation::Clockwise => Some(CW_DOCUMENTATION.get_or_init(|| {
                Documentation::builder(
                    DOC_SECTION_OTHER,
                    "Tests if Polygon exteriors are clockwise and interior rings are counter-clockwise. Non-polygonal inputs return true.",
                    "ST_IsPolygonCW(geom)",
                )
                .with_argument("geom", "geometry")
                .build()
            })),
        }
    }
}

fn polygon_orientation_impl(
    args: ScalarFunctionArgs,
    orientation: Orientation,
) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .expect("ST_IsPolygonCCW/CW has one argument");
    if is_null_geometry_input(&args.arg_fields[0], array.as_ref()) {
        return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
            array.len(),
        ))));
    }

    let geometry = geometry_array_from_arrow(array.as_ref(), &args.arg_fields[0])?;
    let geometry_ref = geometry.as_ref();
    let values = downcast_geoarrow_array!(geometry_ref, polygon_orientation_array, orientation)?;
    Ok(ColumnarValue::Array(Arc::new(values)))
}

fn polygon_orientation_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    orientation: Orientation,
) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for item in array.iter() {
        match item {
            None => builder.append_null(),
            Some(geometry) => {
                let geometry = geometry?;
                builder.append_value(geometry_matches(&geometry, orientation));
            }
        }
    }
    Ok(builder.finish())
}

fn geometry_matches<G>(geometry: &G, orientation: Orientation) -> bool
where
    G: GeometryTrait<T = f64>,
{
    match geometry.as_type() {
        GeometryType::Polygon(polygon) => polygon_matches(polygon, orientation),
        GeometryType::MultiPolygon(multi_polygon) => {
            (0..multi_polygon.num_polygons()).all(|index| {
                multi_polygon
                    .polygon(index)
                    .is_none_or(|polygon| polygon_matches(&polygon, orientation))
            })
        }
        GeometryType::GeometryCollection(collection) => collection
            .geometries()
            .all(|geometry| geometry_matches(&geometry, orientation)),
        _ => true,
    }
}

fn polygon_matches(polygon: &impl PolygonTrait<T = f64>, orientation: Orientation) -> bool {
    let Some(exterior) = polygon.exterior() else {
        return true;
    };
    if exterior.num_coords() == 0 {
        return true;
    }

    let exterior_area = signed_area(&exterior);
    let shell_matches = match orientation {
        Orientation::CounterClockwise => exterior_area < 0.0,
        Orientation::Clockwise => exterior_area > 0.0,
    };
    if !shell_matches {
        return false;
    }

    (0..polygon.num_interiors()).all(|index| {
        polygon.interior(index).is_none_or(|interior| {
            let area = signed_area(&interior);
            match orientation {
                Orientation::CounterClockwise => area >= 0.0,
                Orientation::Clockwise => area <= 0.0,
            }
        })
    })
}

/// Standard shoelace area with the sign convention used by the polygon
/// orientation contract: counter-clockwise is negative, clockwise is
/// positive, and zero is degenerate.
fn signed_area(ring: &impl LineStringTrait<T = f64>) -> f64 {
    let num_coords = ring.num_coords();
    if num_coords < 3 {
        return 0.0;
    }

    let mut sum = 0.0;
    let mut previous = ring
        .coord(num_coords - 1)
        .expect("a ring with coordinates has a last coordinate");
    for index in 0..num_coords {
        let current = ring
            .coord(index)
            .expect("ring coordinate index is within bounds");
        sum += previous.x() * current.y() - current.x() * previous.y();
        previous = current;
    }
    -sum / 2.0
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(IsPolygonOrientation::ccw().into());
    session_context.register_udf(IsPolygonOrientation::cw().into());
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn context() -> SessionContext {
        let ctx = SessionContext::new();
        register(&ctx);
        ctx.register_udf(crate::udf::native::io::GeomFromText::default().into());
        ctx
    }

    #[tokio::test]
    async fn polygon_orientation_contract() {
        let ctx = context();
        let batch = ctx
            .sql(
                "SELECT
                    ST_IsPolygonCCW(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) AS ccw_shell,
                    ST_IsPolygonCW(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) AS ccw_shell_cw,
                    ST_IsPolygonCW(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))')) AS cw_shell,
                    ST_IsPolygonCCW(ST_GeomFromText('POINT(0 0)')) AS point,
                    ST_IsPolygonCW(ST_GeomFromText('POINT EMPTY')) AS point_empty,
                    ST_IsPolygonCCW(ST_GeomFromText(NULL)) AS null_input",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let values = batch.column(0).as_boolean();
        assert!(values.value(0));
        assert!(!batch.column(1).as_boolean().value(0));
        assert!(batch.column(2).as_boolean().value(0));
        assert!(batch.column(3).as_boolean().value(0));
        assert!(batch.column(4).as_boolean().value(0));
        assert!(batch.column(5).is_null(0));
    }
}
