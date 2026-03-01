use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo::BooleanOps;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::error::GeoDataFusionResult;

/// Enum representing the type of overlay operation.
#[derive(Debug, Clone, Copy)]
enum OverlayOp {
    Union,
    Intersection,
    Difference,
    SymDifference,
}

macro_rules! impl_overlay_udf {
    ($struct_name:ident, $udf_name:expr, $doc_var:ident, $op:expr, $doc_text:expr, $doc_example:expr) => {
        #[derive(Debug, Eq, PartialEq, Hash)]
        pub struct $struct_name {
            signature: Signature,
            coord_type: CoordType,
        }

        impl $struct_name {
            pub fn new(coord_type: CoordType) -> Self {
                Self {
                    signature: Signature::any(2, Volatility::Immutable),
                    coord_type,
                }
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new(Default::default())
            }
        }

        static $doc_var: OnceLock<Documentation> = OnceLock::new();

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $udf_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Err(DataFusionError::Internal("return_type".to_string()))
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                Ok(overlay_return_field(args, self.coord_type)?)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let mut arg_iter = args.args.into_iter();
                let left = arg_iter.next().unwrap();
                let right = arg_iter.next().unwrap();
                Ok(overlay_impl(
                    left,
                    &args.arg_fields[0],
                    right,
                    &args.arg_fields[1],
                    $op,
                    self.coord_type,
                )?)
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some($doc_var.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $doc_text, $doc_example)
                        .with_argument("geomA", "geometry")
                        .with_argument("geomB", "geometry")
                        .build()
                }))
            }
        }
    };
}

impl_overlay_udf!(
    StUnion,
    "st_union",
    UNION_DOC,
    OverlayOp::Union,
    "Returns a geometry representing the point-set union of the input geometries. The result may be a collection geometry.",
    "ST_Union(geomA, geomB)"
);

impl_overlay_udf!(
    StIntersection,
    "st_intersection",
    INTERSECTION_DOC,
    OverlayOp::Intersection,
    "Returns a geometry representing the point-set intersection of two geometries. The result may be a collection of geometries.",
    "ST_Intersection(geomA, geomB)"
);

impl_overlay_udf!(
    StDifference,
    "st_difference",
    DIFFERENCE_DOC,
    OverlayOp::Difference,
    "Returns a geometry representing the point-set difference of two geometries. That is, the part of geometry A that does not intersect with geometry B.",
    "ST_Difference(geomA, geomB)"
);

impl_overlay_udf!(
    StSymDifference,
    "st_symdifference",
    SYMDIFFERENCE_DOC,
    OverlayOp::SymDifference,
    "Returns a geometry representing the symmetric difference (exclusive or) of two geometries. That is, the portions of the two geometries that do not intersect.",
    "ST_SymDifference(geomA, geomB)"
);

fn overlay_return_field(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn overlay_impl(
    left: ColumnarValue,
    left_field: &Field,
    right: ColumnarValue,
    right_field: &Field,
    op: OverlayOp,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    match (left, right) {
        (ColumnarValue::Scalar(left_scalar), ColumnarValue::Scalar(right_scalar)) => {
            let mut arrays =
                ColumnarValue::values_to_arrays(&[left_scalar.into(), right_scalar.into()])?
                    .into_iter();
            let left_array = ColumnarValue::Array(arrays.next().unwrap());
            let right_array = ColumnarValue::Array(arrays.next().unwrap());
            overlay_impl(left_array, left_field, right_array, right_field, op, coord_type)
        }
        (ColumnarValue::Array(left_arr), ColumnarValue::Array(right_arr)) => {
            let left_geo = from_arrow_array(&left_arr, left_field)?;
            let right_geo = from_arrow_array(&right_arr, right_field)?;
            let result = overlay_arrays(&left_geo, &right_geo, op, coord_type)?;
            Ok(ColumnarValue::Array(result.into_array_ref()))
        }
        (ColumnarValue::Scalar(left_scalar), ColumnarValue::Array(right_arr)) => {
            let left_arrays = ColumnarValue::values_to_arrays(&[left_scalar.into()])?;
            let left_geo = from_arrow_array(&left_arrays[0], left_field)?;
            let right_geo = from_arrow_array(&right_arr, right_field)?;
            // Broadcast the single left geometry across right array
            let result = overlay_broadcast_left(&left_geo, &right_geo, op, coord_type)?;
            Ok(ColumnarValue::Array(result.into_array_ref()))
        }
        (ColumnarValue::Array(left_arr), ColumnarValue::Scalar(right_scalar)) => {
            let right_arrays = ColumnarValue::values_to_arrays(&[right_scalar.into()])?;
            let left_geo = from_arrow_array(&left_arr, left_field)?;
            let right_geo = from_arrow_array(&right_arrays[0], right_field)?;
            // Broadcast the single right geometry across left array
            let result = overlay_broadcast_right(&left_geo, &right_geo, op, coord_type)?;
            Ok(ColumnarValue::Array(result.into_array_ref()))
        }
    }
}

fn apply_overlay(left: &geo::Geometry, right: &geo::Geometry, op: OverlayOp) -> geo::Geometry {
    // Convert both to MultiPolygon for BooleanOps
    let left_mp = geometry_to_multi_polygon(left);
    let right_mp = geometry_to_multi_polygon(right);
    let result = match op {
        OverlayOp::Union => left_mp.union(&right_mp),
        OverlayOp::Intersection => left_mp.intersection(&right_mp),
        OverlayOp::Difference => left_mp.difference(&right_mp),
        OverlayOp::SymDifference => left_mp.xor(&right_mp),
    };
    geo::Geometry::MultiPolygon(result)
}

fn geometry_to_multi_polygon(geom: &geo::Geometry) -> geo::MultiPolygon {
    match geom {
        geo::Geometry::Polygon(p) => geo::MultiPolygon(vec![p.clone()]),
        geo::Geometry::MultiPolygon(mp) => mp.clone(),
        // For non-polygonal geometries, return empty
        _ => geo::MultiPolygon(vec![]),
    }
}

fn overlay_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    op: OverlayOp,
    _coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(left, _overlay_arrays_impl, right, op)
}

fn _overlay_arrays_impl<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
    op: OverlayOp,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(right, __overlay_arrays_impl, left, op)
}

fn __overlay_arrays_impl<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
    op: OverlayOp,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(left_geom), Some(right_geom)) => {
                let left_geo = geometry_to_geo(&left_geom?)?;
                let right_geo = geometry_to_geo(&right_geom?)?;
                let result = apply_overlay(&left_geo, &right_geo, op);
                builder.push_geometry(Some(&result))?;
            }
            _ => {
                builder.push_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn overlay_broadcast_left(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    op: OverlayOp,
    _coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(left, _overlay_broadcast_left_impl, right, op)
}

fn _overlay_broadcast_left_impl<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
    op: OverlayOp,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    // Get the single left geometry
    let left_geom = left
        .iter()
        .next()
        .unwrap()
        .map(|g| geometry_to_geo(&g?))
        .transpose()?;

    downcast_geoarrow_array!(right, __overlay_broadcast_left_impl, &left_geom, op)
}

fn __overlay_broadcast_left_impl<'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left_geom: &Option<geo::Geometry>,
    op: OverlayOp,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    if let Some(left_geo) = left_geom {
        for r in right.iter() {
            if let Some(right_geom) = r {
                let right_geo = geometry_to_geo(&right_geom?)?;
                let result = apply_overlay(left_geo, &right_geo, op);
                builder.push_geometry(Some(&result))?;
            } else {
                builder.push_null();
            }
        }
    } else {
        for _ in 0..right.len() {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn overlay_broadcast_right(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    op: OverlayOp,
    _coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(right, _overlay_broadcast_right_impl, left, op)
}

fn _overlay_broadcast_right_impl<'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &dyn GeoArrowArray,
    op: OverlayOp,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let right_geom = right
        .iter()
        .next()
        .unwrap()
        .map(|g| geometry_to_geo(&g?))
        .transpose()?;

    downcast_geoarrow_array!(left, __overlay_broadcast_right_impl, &right_geom, op)
}

fn __overlay_broadcast_right_impl<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right_geom: &Option<geo::Geometry>,
    op: OverlayOp,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    if let Some(right_geo) = right_geom {
        for l in left.iter() {
            if let Some(left_geom) = l {
                let left_geo = geometry_to_geo(&left_geom?)?;
                let result = apply_overlay(&left_geo, right_geo, op);
                builder.push_geometry(Some(&result))?;
            } else {
                builder.push_null();
            }
        }
    } else {
        for _ in 0..left.len() {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::geo::measurement::Area;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_intersection() {
        let ctx = SessionContext::new();

        ctx.register_udf(StIntersection::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Intersection(
                    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'),
                    ST_GeomFromText('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))')
                ));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        // The intersection should be a 5x5 polygon
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_union() {
        let ctx = SessionContext::new();

        ctx.register_udf(StUnion::default().into());
        ctx.register_udf(Area::new().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql(
                "SELECT ST_Area(ST_Union(
                    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'),
                    ST_GeomFromText('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))')
                ));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let area = col.as_primitive::<arrow_array::types::Float64Type>().value(0);
        // Union of two overlapping 10x10 squares = 200 - 25 = 175
        assert!((area - 175.0).abs() < 0.01, "Union area was {}", area);
    }

    #[tokio::test]
    async fn test_difference() {
        let ctx = SessionContext::new();

        ctx.register_udf(StDifference::default().into());
        ctx.register_udf(Area::new().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql(
                "SELECT ST_Area(ST_Difference(
                    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'),
                    ST_GeomFromText('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))')
                ));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let area = col.as_primitive::<arrow_array::types::Float64Type>().value(0);
        // Difference = 100 - 25 = 75
        assert!((area - 75.0).abs() < 0.01, "Difference area was {}", area);
    }

    #[tokio::test]
    async fn test_sym_difference() {
        let ctx = SessionContext::new();

        ctx.register_udf(StSymDifference::default().into());
        ctx.register_udf(Area::new().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql(
                "SELECT ST_Area(ST_SymDifference(
                    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'),
                    ST_GeomFromText('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))')
                ));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let area = col.as_primitive::<arrow_array::types::Float64Type>().value(0);
        // SymDifference = 200 - 2*25 = 150
        assert!(
            (area - 150.0).abs() < 0.01,
            "SymDifference area was {}",
            area
        );
    }
}
