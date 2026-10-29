//! PostGIS-compatible ST_3D* distance, nearest-line, and intersection UDFs.

use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::{BooleanBuilder, Float64Builder};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType, Metadata};
use wkt::Wkt;
use wkt::types::{Coord as WktCoord, LineString as WktLineString, Point as WktPoint};

use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, geometry_array_from_arrow, merge_metadata,
};

mod kernel;

use kernel::{
    GeometryModel, Metric, Point3, flatten, intersects, max_pair, max_with_vertical, min_pair,
    min_with_vertical,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Distance,
    MaxDistance,
    ClosestPoint,
    ShortestLine,
    LongestLine,
    Intersects,
}

#[derive(Debug)]
enum RowValue {
    Float(f64),
    Boolean(bool),
    Point(Point3),
    Line(Point3, Point3),
}

fn output_field(args: &ReturnFieldArgs, name: &str) -> Result<FieldRef> {
    let mut fields = Vec::new();
    for field in args.arg_fields.iter() {
        if matches!(field.data_type(), DataType::Null) {
            continue;
        }
        fields.push(field.as_ref());
    }
    let metadata = merge_metadata(fields, name)?;
    if metadata.edges().is_some() {
        return Err(DataFusionError::Execution(format!(
            "{name} supports only planar Cartesian edge metadata"
        )));
    }
    let metadata = Arc::new(Metadata::new(metadata.crs().clone(), None));
    let output_type = GeometryType::new(metadata);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn flatten_argument(
    args: &ScalarFunctionArgs,
    index: usize,
) -> GeoDataFusionResult<Vec<Option<GeometryModel>>> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if args.arg_fields[index].data_type() == &DataType::Null {
        return Ok(vec![None; args.number_rows]);
    }
    let array = arrays.get(index).ok_or_else(|| {
        DataFusionError::Internal(format!("missing geometry argument {}", index + 1))
    })?;
    let field = &args.arg_fields[index];
    let geometry = geometry_array_from_arrow(array.as_ref(), field)?;
    flatten_geo_array(geometry.as_ref())
}

fn flatten_geo_array(array: &dyn GeoArrowArray) -> GeoDataFusionResult<Vec<Option<GeometryModel>>> {
    downcast_geoarrow_array!(array, flatten_geo_array_impl)
}

fn flatten_geo_array_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<Vec<Option<GeometryModel>>> {
    let mut rows = Vec::with_capacity(array.len());
    for item in array.iter() {
        let Some(geometry) = item else {
            rows.push(None);
            continue;
        };
        rows.push(Some(flatten(&geometry?)?));
    }
    Ok(rows)
}

fn compute(kind: Kind, left: &GeometryModel, right: &GeometryModel) -> Option<RowValue> {
    let full_3d = left.has_z && right.has_z;
    let metric = if full_3d { Metric::Full } else { Metric::Xy };

    if left.is_empty() || right.is_empty() {
        return match kind {
            Kind::Intersects => Some(RowValue::Boolean(false)),
            _ => None,
        };
    }
    if left.non_finite || right.non_finite {
        return match kind {
            Kind::Intersects => Some(RowValue::Boolean(false)),
            _ => None,
        };
    }

    match kind {
        Kind::Distance => {
            min_pair(left, right, metric).map(|candidate| RowValue::Float(candidate.distance))
        }
        Kind::MaxDistance => {
            max_pair(left, right, metric).map(|candidate| RowValue::Float(candidate.distance))
        }
        Kind::Intersects => Some(RowValue::Boolean(intersects(left, right, metric))),
        Kind::ClosestPoint => {
            let candidate = if full_3d || (!left.has_z && !right.has_z) {
                min_pair(left, right, metric)?
            } else {
                let metric_point = min_pair(left, right, Metric::Xy)?;
                if left.has_z {
                    min_with_vertical(
                        left,
                        metric_point.p2.x,
                        metric_point.p2.y,
                        left.z_range()?.0,
                        left.z_range()?.1,
                        true,
                    )?
                } else {
                    min_with_vertical(
                        right,
                        metric_point.p1.x,
                        metric_point.p1.y,
                        right.z_range()?.0,
                        right.z_range()?.1,
                        false,
                    )?
                }
            };
            Some(RowValue::Point(candidate.p1))
        }
        Kind::ShortestLine => {
            let candidate = if full_3d || (!left.has_z && !right.has_z) {
                min_pair(left, right, metric)?
            } else {
                let metric_point = min_pair(left, right, Metric::Xy)?;
                if left.has_z {
                    min_with_vertical(
                        left,
                        metric_point.p2.x,
                        metric_point.p2.y,
                        left.z_range()?.0,
                        left.z_range()?.1,
                        true,
                    )?
                } else {
                    min_with_vertical(
                        right,
                        metric_point.p1.x,
                        metric_point.p1.y,
                        right.z_range()?.0,
                        right.z_range()?.1,
                        false,
                    )?
                }
            };
            Some(RowValue::Line(candidate.p1, candidate.p2))
        }
        Kind::LongestLine => {
            let candidate = if full_3d {
                max_pair(left, right, Metric::Full)?
            } else if !left.has_z && !right.has_z {
                max_pair(left, right, Metric::Xy)?
            } else {
                let metric_point = max_pair(left, right, Metric::Xy)?;
                if left.has_z {
                    max_with_vertical(
                        left,
                        metric_point.p2.x,
                        metric_point.p2.y,
                        left.z_range()?.0,
                        left.z_range()?.1,
                        true,
                    )?
                } else {
                    max_with_vertical(
                        right,
                        metric_point.p1.x,
                        metric_point.p1.y,
                        right.z_range()?.0,
                        right.z_range()?.1,
                        false,
                    )?
                }
            };
            Some(RowValue::Line(candidate.p1, candidate.p2))
        }
    }
}

fn invoke_common(args: ScalarFunctionArgs, kind: Kind) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "{kind:?} expects 2 arguments, got {}",
            arrays.len()
        ))
        .into());
    }
    let number_rows = args.number_rows;
    if arrays.iter().any(|array| array.len() != number_rows) {
        return Err(DataFusionError::Internal(
            "ST_3D* input arrays have mixed lengths".to_string(),
        )
        .into());
    }

    let left_rows = flatten_argument(&args, 0)?;
    let right_rows = flatten_argument(&args, 1)?;

    match kind {
        Kind::Distance | Kind::MaxDistance => {
            let mut builder = Float64Builder::with_capacity(number_rows);
            for index in 0..number_rows {
                let Some(left) = left_rows[index].as_ref() else {
                    builder.append_null();
                    continue;
                };
                let Some(right) = right_rows[index].as_ref() else {
                    builder.append_null();
                    continue;
                };
                match compute(kind, left, right) {
                    Some(RowValue::Float(value)) => builder.append_value(value),
                    _ => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        Kind::Intersects => {
            let mut builder = BooleanBuilder::with_capacity(number_rows);
            for index in 0..number_rows {
                let Some(left) = left_rows[index].as_ref() else {
                    builder.append_null();
                    continue;
                };
                let Some(right) = right_rows[index].as_ref() else {
                    builder.append_null();
                    continue;
                };
                match compute(kind, left, right) {
                    Some(RowValue::Boolean(value)) => builder.append_value(value),
                    _ => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        Kind::ClosestPoint | Kind::ShortestLine | Kind::LongestLine => {
            let mut rows = Vec::with_capacity(number_rows);
            for index in 0..number_rows {
                let Some(left) = left_rows[index].as_ref() else {
                    rows.push(None);
                    continue;
                };
                let Some(right) = right_rows[index].as_ref() else {
                    rows.push(None);
                    continue;
                };
                rows.push(match compute(kind, left, right) {
                    Some(RowValue::Point(point)) => Some(OutputGeometry::Point(point)),
                    Some(RowValue::Line(a, b)) => Some(OutputGeometry::Line(a, b)),
                    _ => None,
                });
            }
            let metadata = metadata_for_args(&args)?;
            build_geometry_array(rows, metadata)
        }
    }
}

fn metadata_for_args(args: &ScalarFunctionArgs) -> GeoDataFusionResult<Arc<Metadata>> {
    let mut fields = Vec::new();
    for field in args.arg_fields.iter() {
        if matches!(field.data_type(), DataType::Null) {
            continue;
        }
        fields.push(field.as_ref());
    }
    let metadata = merge_metadata(fields, "ST_3D geometry output")?;
    if metadata.edges().is_some() {
        return Err(DataFusionError::Execution(
            "ST_3D geometry output supports only planar Cartesian edge metadata".to_string(),
        )
        .into());
    }
    Ok(Arc::new(Metadata::new(metadata.crs().clone(), None)))
}

#[derive(Debug)]
enum OutputGeometry {
    Point(Point3),
    Line(Point3, Point3),
}

fn to_wkt(geometry: &OutputGeometry) -> Wkt<f64> {
    match geometry {
        OutputGeometry::Point(point) => Wkt::Point(WktPoint::from_coord(point_coord(*point))),
        OutputGeometry::Line(a, b) => {
            let line = WktLineString::from_coords([point_coord(*a), point_coord(*b)])
                .expect("line output always has two points");
            Wkt::LineString(line)
        }
    }
}

fn point_coord(point: Point3) -> WktCoord<f64> {
    WktCoord {
        x: point.x,
        y: point.y,
        z: point.has_z.then_some(point.z),
        m: None,
    }
}

fn build_geometry_array(
    rows: Vec<Option<OutputGeometry>>,
    metadata: Arc<Metadata>,
) -> GeoDataFusionResult<ColumnarValue> {
    let output_type = GeometryType::new(metadata).with_coord_type(CoordType::default());
    let mut builder = StrictGeometryBuilder::new(output_type);
    for row in rows {
        let geometry = row.as_ref().map(to_wkt);
        builder.push_geometry(geometry.as_ref())?;
    }
    Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
}

macro_rules! impl_three_d_udf {
    ($struct_name:ident, $signature_name:ident, $doc_name:ident, $udf_name:literal, $kind:expr, $doc:literal, $example:literal) => {
        #[derive(Debug, Eq, PartialEq, Hash)]
        pub struct $struct_name {
            _private: (),
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self { _private: () }
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        static $signature_name: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));

        static $doc_name: OnceLock<Documentation> = OnceLock::new();

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $udf_name
            }

            fn signature(&self) -> &Signature {
                &$signature_name
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                match $kind {
                    Kind::Distance | Kind::MaxDistance => Ok(DataType::Float64),
                    Kind::Intersects => Ok(DataType::Boolean),
                    _ => Err(DataFusionError::Internal(format!(
                        "{} requires return_field_from_args",
                        $udf_name
                    ))),
                }
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                match $kind {
                    Kind::Distance | Kind::MaxDistance => {
                        Ok(Arc::new(Field::new("", DataType::Float64, true)))
                    }
                    Kind::Intersects => Ok(Arc::new(Field::new("", DataType::Boolean, true))),
                    _ => Ok(output_field(&args, $udf_name)?),
                }
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                Ok(invoke_common(args, $kind)?)
            }

            fn is_strict(&self) -> bool {
                true
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some($doc_name.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $doc, $example)
                        .with_argument("geomA", "geometry")
                        .with_argument("geomB", "geometry")
                        .build()
                }))
            }
        }
    };
}

impl_three_d_udf!(
    ThreeDDistance,
    THREE_D_DISTANCE_SIGNATURE,
    THREE_D_DISTANCE_DOC,
    "st_3ddistance",
    Kind::Distance,
    "Returns the 3D Cartesian minimum distance between two geometries in projected units. When either input lacks Z, the result is the 2D Cartesian distance (PostGIS 2.2+ behavior for missing Z). Non-finite coordinates and empty inputs return NULL.",
    "ST_3DDistance(geomA, geomB)"
);
impl_three_d_udf!(
    ThreeDMaxDistance,
    THREE_D_MAX_DISTANCE_SIGNATURE,
    THREE_D_MAX_DISTANCE_DOC,
    "st_3dmaxdistance",
    Kind::MaxDistance,
    "Returns the largest distance between vertex pairs of two geometries in projected units. Polygon outer rings contribute vertices; holes are ignored for the maximum, matching PostGIS. When either input lacks Z, the result is the 2D maximum distance.",
    "ST_3DMaxDistance(geomA, geomB)"
);
impl_three_d_udf!(
    ThreeDClosestPoint,
    THREE_D_CLOSEST_POINT_SIGNATURE,
    THREE_D_CLOSEST_POINT_DOC,
    "st_3dclosestpoint",
    Kind::ClosestPoint,
    "Returns the point on geomA closest to geomB. With two 2D inputs the result is a 2D point; with mixed dimensions the missing Z is treated as free and the result is a 3D point. Empty inputs return NULL.",
    "ST_3DClosestPoint(geomA, geomB)"
);
impl_three_d_udf!(
    ThreeDShortestLine,
    THREE_D_SHORTEST_LINE_SIGNATURE,
    THREE_D_SHORTEST_LINE_DOC,
    "st_3dshortestline",
    Kind::ShortestLine,
    "Returns the 3D line between the closest points of two geometries, starting at geomA. The line has the same dimensionality contract as ST_3DClosestPoint.",
    "ST_3DShortestLine(geomA, geomB)"
);
impl_three_d_udf!(
    ThreeDLongestLine,
    THREE_D_LONGEST_LINE_SIGNATURE,
    THREE_D_LONGEST_LINE_DOC,
    "st_3dlongestline",
    Kind::LongestLine,
    "Returns the first 3D line between the farthest vertex pair of two geometries, starting at geomA. Polygon outer rings contribute vertices; holes are ignored.",
    "ST_3DLongestLine(geomA, geomB)"
);
impl_three_d_udf!(
    ThreeDIntersects,
    THREE_D_INTERSECTS_SIGNATURE,
    THREE_D_INTERSECTS_DOC,
    "st_3dintersects",
    Kind::Intersects,
    "Returns true when two geometries share at least one point in 3D space. The predicate uses exact robust orientation tests for point, segment, and planar surface intersections; no distance tolerance is applied. Mixed 2D/3D input falls back to the 2D intersection. Empty inputs return false.",
    "ST_3DIntersects(geomA, geomB)"
);

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(ThreeDDistance::new().into());
    session_context.register_udf(ThreeDMaxDistance::new().into());
    session_context.register_udf(ThreeDClosestPoint::new().into());
    session_context.register_udf(ThreeDShortestLine::new().into());
    session_context.register_udf(ThreeDLongestLine::new().into());
    session_context.register_udf(ThreeDIntersects::new().into());
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use crate::register;

    async fn scalar_float(ctx: &SessionContext, sql: &str) -> Option<f64> {
        let batch = ctx
            .sql(sql)
            .await
            .expect("query succeeds")
            .collect()
            .await
            .expect("collect succeeds")
            .remove(0);
        let values = batch.column(0).as_primitive::<Float64Type>();
        (!values.is_null(0)).then(|| values.value(0))
    }

    async fn scalar_bool(ctx: &SessionContext, sql: &str) -> Option<bool> {
        let batch = ctx
            .sql(sql)
            .await
            .expect("query succeeds")
            .collect()
            .await
            .expect("collect succeeds")
            .remove(0);
        let values = batch.column(0).as_boolean();
        (!values.is_null(0)).then(|| values.value(0))
    }

    async fn scalar_text(ctx: &SessionContext, sql: &str) -> Option<String> {
        let batch = ctx
            .sql(sql)
            .await
            .expect("query succeeds")
            .collect()
            .await
            .expect("collect succeeds")
            .remove(0);
        let values = batch.column(0).as_string::<i32>();
        (!values.is_null(0)).then(|| values.value(0).to_string())
    }

    #[tokio::test]
    async fn distances_match_reference() {
        let ctx = SessionContext::new();
        register(&ctx);

        assert_eq!(
            scalar_float(
                &ctx,
                "SELECT ST_3DDistance(ST_GeomFromText('POINT Z (0 0 0)'), \
                 ST_GeomFromText('POINT Z (3 4 12)'))"
            )
            .await,
            Some(13.0)
        );
        assert_eq!(
            scalar_float(
                &ctx,
                "SELECT ST_3DDistance(ST_GeomFromText('POINT(2 0)'), \
                 ST_GeomFromText('LINESTRING Z (0 0 0, 10 0 10)'))"
            )
            .await,
            Some(0.0)
        );
        let hole_distance = scalar_float(
            &ctx,
            "SELECT ST_3DDistance(ST_GeomFromText('POINT Z (5 5 1)'), \
                 ST_GeomFromText('POLYGON Z ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0), \
                 (2 2 0, 2 8 0, 8 8 0, 8 2 0, 2 2 0))'))",
        )
        .await
        .expect("hole case has a distance");
        assert!((hole_distance - 10.0_f64.sqrt()).abs() < 1e-9);
        assert_eq!(
            scalar_float(
                &ctx,
                "SELECT ST_3DDistance(ST_GeomFromText(NULL), ST_GeomFromText('POINT(0 0)'))"
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn max_and_lines_match_reference() {
        let ctx = SessionContext::new();
        register(&ctx);

        let max = scalar_float(
            &ctx,
            "SELECT ST_3DMaxDistance(ST_GeomFromText('LINESTRING Z (0 0 0, 10 0 0)'), \
             ST_GeomFromText('LINESTRING Z (5 -5 5, 5 5 5)'))",
        )
        .await
        .expect("max has a value");
        assert!((max - 75.0_f64.sqrt()).abs() < 1e-9);

        assert_eq!(
            scalar_text(
                &ctx,
                "SELECT ST_AsText(ST_3DClosestPoint(ST_GeomFromText('LINESTRING Z (0 0 0, 10 0 0)'), \
                 ST_GeomFromText('POINT Z (2 0 1)')))"
            )
            .await
            .as_deref(),
            Some("POINT Z(2 0 0)")
        );
        assert_eq!(
            scalar_text(
                &ctx,
                "SELECT ST_AsText(ST_3DShortestLine(ST_GeomFromText('LINESTRING Z (0 0 0, 10 0 10)'), \
                 ST_GeomFromText('POINT(2 0)')))"
            )
            .await
            .as_deref(),
            Some("LINESTRING Z(2 0 2,2 0 2)")
        );
        assert_eq!(
            scalar_text(
                &ctx,
                "SELECT ST_AsText(ST_3DLongestLine(ST_GeomFromText('LINESTRING Z (0 0 0, 10 0 10)'), \
                 ST_GeomFromText('POINT(2 0)')))"
            )
            .await
            .as_deref(),
            Some("LINESTRING Z(10 0 10,2 0 0)")
        );
    }

    #[tokio::test]
    async fn intersects_and_null_behavior() {
        let ctx = SessionContext::new();
        register(&ctx);

        assert_eq!(
            scalar_bool(
                &ctx,
                "SELECT ST_3DIntersects(ST_GeomFromText('LINESTRING Z (5 5 -5, 5 5 5)'), \
                 ST_GeomFromText('POLYGON Z ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'))"
            )
            .await,
            Some(true)
        );
        assert_eq!(
            scalar_bool(
                &ctx,
                "SELECT ST_3DIntersects(ST_GeomFromText('LINESTRING Z (0 0 0, 10 0 0)'), \
                 ST_GeomFromText('LINESTRING Z (10 4 3, 0 4 3)'))"
            )
            .await,
            Some(false)
        );
        assert_eq!(
            scalar_bool(
                &ctx,
                "SELECT ST_3DIntersects(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText(NULL))"
            )
            .await,
            None
        );
    }

    #[test]
    fn registration_exposes_all_three_d_targets() {
        let ctx = SessionContext::new();
        register(&ctx);
        let names = ctx
            .state()
            .scalar_functions()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in [
            "st_3ddistance",
            "st_3dmaxdistance",
            "st_3dclosestpoint",
            "st_3dshortestline",
            "st_3dlongestline",
            "st_3dintersects",
        ] {
            assert!(names.iter().any(|candidate| candidate == name), "{name}");
        }
    }
}
