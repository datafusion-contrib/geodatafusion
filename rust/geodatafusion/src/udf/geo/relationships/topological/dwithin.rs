use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, BooleanArray, Float64Array};
use arrow_schema::DataType;
use datafusion::common::types::{NativeType, logical_float64};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use geo::{Distance, Euclidean};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::{
    GeometryBounds, expanded_bounds_intersect, geometry_bounds,
};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DWithin;

impl DWithin {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for DWithin {
    fn default() -> Self {
        Self::new()
    }
}

static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::coercible(
        vec![
            Coercion::new_exact(TypeSignatureClass::Any),
            Coercion::new_exact(TypeSignatureClass::Any),
            Coercion::new_implicit(
                TypeSignatureClass::Native(logical_float64()),
                vec![TypeSignatureClass::Numeric],
                NativeType::Float64,
            ),
        ],
        Volatility::Immutable,
    )
});
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for DWithin {
    fn name(&self) -> &str {
        "st_dwithin"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(dwithin_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true when the minimum 2D Cartesian distance between two \
                 non-empty geometries is less than or equal to a finite, non-negative \
                 radius. Distance and radius use coordinate-system units. The predicate \
                 applies an exact outward-rounded bounds check before using the same \
                 planar distance kernel as ST_Distance, so it can be false where a \
                 rounded ST_Distance result equals the radius at a floating-point \
                 boundary. It never changes ST_Distance semantics. NULL inputs return \
                 NULL; EMPTY geometries return false; non-finite geometry coordinates \
                 or an invalid radius are errors.",
                "ST_DWithin(geomA, geomB, radius)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .with_argument(
                "radius",
                "finite non-negative distance in coordinate-system units",
            )
            .build()
        }))
    }
}

#[derive(Debug)]
enum GeometryValue {
    Null,
    Empty,
    Geometry(geo::Geometry<f64>),
}

fn dwithin_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let output_len = arrays[0].len();

    if args
        .arg_fields
        .iter()
        .take(3)
        .any(|field| field.data_type() == &DataType::Null)
    {
        return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
            output_len,
        ))));
    }

    let left_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_array = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let left = geometry_array_to_values(left_array.as_ref())?;
    let right = geometry_array_to_values(right_array.as_ref())?;

    let radii = arrays[2]
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ST_DWithin expected a Float64 radius array, got {}",
                arrays[2].data_type()
            ))
        })?;

    let mut builder = BooleanBuilder::with_capacity(output_len);
    for ((left, right), radius) in left.into_iter().zip(right).zip(radii.iter()) {
        if matches!(&left, GeometryValue::Null) || matches!(&right, GeometryValue::Null) {
            builder.append_null();
            continue;
        }

        let Some(radius) = radius else {
            builder.append_null();
            continue;
        };
        if !radius.is_finite() || radius < 0.0 {
            return Err(DataFusionError::Execution(format!(
                "ST_DWithin radius must be finite and non-negative, got {radius}"
            ))
            .into());
        }

        match (left, right) {
            (GeometryValue::Null, _) | (_, GeometryValue::Null) => builder.append_null(),
            (GeometryValue::Empty, _) | (_, GeometryValue::Empty) => builder.append_value(false),
            (GeometryValue::Geometry(left), GeometryValue::Geometry(right)) => {
                builder.append_value(geometries_are_within(&left, &right, radius));
            }
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn geometries_are_within(
    left: &geo::Geometry<f64>,
    right: &geo::Geometry<f64>,
    radius: f64,
) -> bool {
    let (GeometryBounds::Finite(left_bounds), GeometryBounds::Finite(right_bounds)) =
        (geometry_bounds(left), geometry_bounds(right))
    else {
        return false;
    };

    expanded_bounds_intersect(
        [
            left_bounds.minx(),
            left_bounds.miny(),
            left_bounds.maxx(),
            left_bounds.maxy(),
        ],
        [
            right_bounds.minx(),
            right_bounds.miny(),
            right_bounds.maxx(),
            right_bounds.maxy(),
        ],
        radius,
    ) && Euclidean.distance(left, right) <= radius
}

fn geometry_array_to_values(array: &dyn GeoArrowArray) -> GeoDataFusionResult<Vec<GeometryValue>> {
    downcast_geoarrow_array!(array, geometry_array_to_values_impl)
}

fn geometry_array_to_values_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<Vec<GeometryValue>> {
    let mut values = Vec::with_capacity(array.len());
    for geometry in array.iter() {
        let value = match geometry {
            None => GeometryValue::Null,
            Some(geometry) => match to_distance_geometry(&geometry?)? {
                Some(geometry) => GeometryValue::Geometry(geometry),
                None => GeometryValue::Empty,
            },
        };
        values.push(value);
    }
    Ok(values)
}

fn to_distance_geometry<G: GeometryTrait<T = f64>>(
    geometry: &G,
) -> GeoDataFusionResult<Option<geo::Geometry<f64>>> {
    match geometry.as_type() {
        GeometryType::Point(point) => Ok(point_to_geo(point)?.map(geo::Geometry::Point)),
        GeometryType::LineString(line_string) => {
            Ok(line_string_to_geo(line_string, "LineString")?.map(geo::Geometry::LineString))
        }
        GeometryType::Polygon(polygon) => Ok(polygon_to_geo(polygon)?.map(geo::Geometry::Polygon)),
        GeometryType::MultiPoint(multi_point) => {
            let mut points = Vec::with_capacity(multi_point.num_points());
            for point in multi_point.points() {
                if let Some(point) = point_to_geo(&point)? {
                    points.push(point);
                }
            }
            if points.is_empty() {
                Ok(None)
            } else {
                Ok(Some(geo::Geometry::MultiPoint(geo::MultiPoint::new(
                    points,
                ))))
            }
        }
        GeometryType::MultiLineString(multi_line_string) => {
            let mut line_strings = Vec::with_capacity(multi_line_string.num_line_strings());
            for line_string in multi_line_string.line_strings() {
                if let Some(line_string) =
                    line_string_to_geo(&line_string, "MultiLineString member")?
                {
                    line_strings.push(line_string);
                }
            }
            if line_strings.is_empty() {
                Ok(None)
            } else {
                Ok(Some(geo::Geometry::MultiLineString(
                    geo::MultiLineString::new(line_strings),
                )))
            }
        }
        GeometryType::MultiPolygon(multi_polygon) => {
            let mut polygons = Vec::with_capacity(multi_polygon.num_polygons());
            for polygon in multi_polygon.polygons() {
                if let Some(polygon) = polygon_to_geo(&polygon)? {
                    polygons.push(polygon);
                }
            }
            if polygons.is_empty() {
                Ok(None)
            } else {
                Ok(Some(geo::Geometry::MultiPolygon(geo::MultiPolygon::new(
                    polygons,
                ))))
            }
        }
        GeometryType::GeometryCollection(geometry_collection) => {
            let mut geometries = Vec::with_capacity(geometry_collection.num_geometries());
            for geometry in geometry_collection.geometries() {
                if let Some(geometry) = to_distance_geometry(&geometry)? {
                    geometries.push(geometry);
                }
            }
            if geometries.is_empty() {
                Ok(None)
            } else {
                Ok(Some(geo::Geometry::GeometryCollection(
                    geo::GeometryCollection::new_from(geometries),
                )))
            }
        }
        GeometryType::Rect(rect) => {
            let min = coord_to_geo(&rect.min(), "rectangle")?;
            let max = coord_to_geo(&rect.max(), "rectangle")?;
            Ok(Some(geo::Geometry::Rect(geo::Rect::new(min, max))))
        }
        GeometryType::Triangle(triangle) => {
            let [first, second, third] = triangle.coords();
            Ok(Some(geo::Geometry::Triangle(geo::Triangle::new(
                coord_to_geo(&first, "triangle")?,
                coord_to_geo(&second, "triangle")?,
                coord_to_geo(&third, "triangle")?,
            ))))
        }
        GeometryType::Line(line) => Ok(Some(geo::Geometry::Line(geo::Line::new(
            coord_to_geo(&line.start(), "line")?,
            coord_to_geo(&line.end(), "line")?,
        )))),
    }
}

fn point_to_geo<P: PointTrait<T = f64>>(point: &P) -> GeoDataFusionResult<Option<geo::Point<f64>>> {
    let Some(coord) = point.coord() else {
        return Ok(None);
    };
    Ok(Some(geo::Point::from(coord_to_geo(&coord, "Point")?)))
}

fn line_string_to_geo<L: LineStringTrait<T = f64>>(
    line_string: &L,
    context: &str,
) -> GeoDataFusionResult<Option<geo::LineString<f64>>> {
    let num_coords = line_string.num_coords();
    if num_coords == 0 {
        return Ok(None);
    }
    if num_coords < 2 {
        return Err(DataFusionError::Execution(format!(
            "ST_DWithin cannot safely measure a {context} with only {num_coords} coordinate"
        ))
        .into());
    }

    let coords = line_string
        .coords()
        .map(|coord| coord_to_geo(&coord, context))
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    Ok(Some(geo::LineString::new(coords)))
}

fn polygon_to_geo<P: PolygonTrait<T = f64>>(
    polygon: &P,
) -> GeoDataFusionResult<Option<geo::Polygon<f64>>> {
    let exterior = match polygon.exterior() {
        Some(exterior) => line_string_to_geo(&exterior, "Polygon exterior ring")?,
        None => None,
    };

    let mut interiors = Vec::with_capacity(polygon.num_interiors());
    for interior in polygon.interiors() {
        if let Some(interior) = line_string_to_geo(&interior, "Polygon interior ring")? {
            interiors.push(interior);
        }
    }

    Ok(exterior.map(|exterior| geo::Polygon::new(exterior, interiors)))
}

fn coord_to_geo<C: CoordTrait<T = f64>>(
    coord: &C,
    context: &str,
) -> GeoDataFusionResult<geo::Coord<f64>> {
    let (x, y) = coord.x_y();
    if !x.is_finite() || !y.is_finite() {
        return Err(DataFusionError::Execution(format!(
            "ST_DWithin requires finite XY coordinates, got ({x}, {y}) in {context}"
        ))
        .into());
    }
    Ok(geo::Coord { x, y })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Float64Array, RecordBatch, StringArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{Field, Schema};
    use datafusion::config::ConfigOptions;
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use geoarrow_array::array::{CoordBuffer, InterleavedCoordBuffer, MultiPolygonArray, WktArray};
    use geoarrow_array::builder::{
        GeometryBuilder, GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder,
        MultiPointBuilder, PointBuilder, PolygonBuilder,
    };
    use geoarrow_schema::{
        Dimension, GeometryCollectionType, GeometryType, MultiLineStringType, MultiPointType,
        PointType, PolygonType,
    };

    use super::*;

    fn register_standard(ctx: &SessionContext) {
        crate::register(ctx);
    }

    async fn query_booleans(ctx: &SessionContext, sql: &str) -> Vec<Option<bool>> {
        let dataframe = ctx.sql(sql).await.unwrap();
        let batches = dataframe.collect().await.unwrap();
        assert_eq!(batches.len(), 1, "{sql}");
        batches[0].column(0).as_boolean().iter().collect()
    }

    async fn query_bool(ctx: &SessionContext, sql: &str) -> Option<bool> {
        let values = query_booleans(ctx, sql).await;
        assert_eq!(values.len(), 1, "{sql}");
        values[0]
    }

    async fn query_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Ok(dataframe) => match dataframe.collect().await {
                Ok(_) => panic!("expected query to fail: {sql}"),
                Err(error) => error.to_string(),
            },
            Err(error) => error.to_string(),
        }
    }

    fn register_points(
        ctx: &SessionContext,
        name: &str,
        points: &[Option<geo::Point<f64>>],
        radii: Option<&[Option<f64>]>,
    ) {
        let point_array = PointBuilder::from_nullable_points(
            points.iter().map(Option::as_ref),
            PointType::new(Dimension::XY, Default::default()),
        )
        .finish();

        let mut fields = vec![Arc::new(point_array.data_type().to_field("geometry", true))];
        let mut columns = vec![point_array.to_array_ref()];
        if let Some(radii) = radii {
            fields.push(Arc::new(Field::new("radius", DataType::Float64, true)));
            columns.push(Arc::new(Float64Array::from(radii.to_vec())));
        }

        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        ctx.register_batch(name, batch).unwrap();
    }

    fn register_geometry_array(
        ctx: &SessionContext,
        name: &str,
        array: impl GeoArrowArray + 'static,
    ) {
        let field = Arc::new(array.data_type().to_field("geometry", true));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![array.to_array_ref()],
        )
        .unwrap();
        ctx.register_batch(name, batch).unwrap();
    }

    fn register_wkt_array(ctx: &SessionContext, name: &str, wkt: &str) {
        let array = WktArray::new(StringArray::from(vec![wkt]), Default::default());
        register_geometry_array(ctx, name, array);
    }

    fn wkt_coord(x: f64, y: f64) -> wkt::types::Coord<f64> {
        wkt::types::Coord {
            x,
            y,
            z: None,
            m: None,
        }
    }

    #[tokio::test]
    async fn test_distance_threshold_and_intersections() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        let cases = [
            (
                "ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'), 5)",
                true,
            ),
            (
                "ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'), 4)",
                false,
            ),
            (
                "ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'), 5.0)",
                true,
            ),
            (
                "ST_DWithin(ST_GeomFromText('POINT Z (0 0 100)'), ST_GeomFromText('POINT Z (3 4 -100)'), 5)",
                true,
            ),
            (
                "ST_DWithin(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(1 1)'), 0)",
                true,
            ),
            (
                "ST_DWithin(ST_GeomFromText('LINESTRING(0 0, 2 2)'), ST_GeomFromText('LINESTRING(0 2, 2 0)'), 0)",
                true,
            ),
            (
                "ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING(2 -1, 2 1)'), 2)",
                true,
            ),
            (
                "ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING(2 -1, 2 1)'), 1.5)",
                false,
            ),
            (
                "ST_DWithin(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'), 0)",
                true,
            ),
            (
                "ST_DWithin(ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_GeomFromText('LINESTRING(0 2, 2 0)'), 0)",
                true,
            ),
        ];

        for (expression, expected) in cases {
            assert_eq!(
                query_bool(&ctx, &format!("SELECT {expression}")).await,
                Some(expected),
                "{expression}"
            );
        }
    }

    #[tokio::test]
    async fn test_scalar_array_combinations_and_numeric_coercion() {
        let ctx = SessionContext::new();
        register_standard(&ctx);
        register_points(
            &ctx,
            "points",
            &[
                Some(geo::Point::new(0.0, 0.0)),
                Some(geo::Point::new(10.0, 0.0)),
            ],
            Some(&[Some(1.0), Some(9.0)]),
        );

        let cases = [
            (
                "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), radius) FROM points",
                vec![Some(true), Some(false)],
            ),
            (
                "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), geometry, radius) FROM points",
                vec![Some(true), Some(false)],
            ),
            (
                "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 5) FROM points",
                vec![Some(true), Some(false)],
            ),
            (
                "SELECT ST_DWithin(p.geometry, s.geometry, 1) FROM points p CROSS JOIN (SELECT ST_GeomFromText('POINT(0 0)') AS geometry) s",
                vec![Some(true), Some(false)],
            ),
        ];

        for (sql, expected) in cases {
            assert_eq!(query_booleans(&ctx, sql).await, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn test_null_arguments() {
        let ctx = SessionContext::new();
        register_standard(&ctx);
        register_points(
            &ctx,
            "points",
            &[
                Some(geo::Point::new(0.0, 0.0)),
                Some(geo::Point::new(10.0, 0.0)),
            ],
            None,
        );

        for sql in [
            "SELECT ST_DWithin(NULL, ST_GeomFromText('POINT(0 0)'), 1)",
            "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), NULL, 1)",
            "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 0)'), NULL)",
        ] {
            assert_eq!(query_bool(&ctx, sql).await, None, "{sql}");
        }

        for sql in [
            "SELECT ST_DWithin(NULL, geometry, 1) FROM points",
            "SELECT ST_DWithin(geometry, NULL, 1) FROM points",
            "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), NULL) FROM points",
        ] {
            assert_eq!(query_booleans(&ctx, sql).await, vec![None, None], "{sql}");
        }

        register_points(
            &ctx,
            "null_radii",
            &[
                Some(geo::Point::new(0.0, 0.0)),
                Some(geo::Point::new(10.0, 0.0)),
            ],
            Some(&[None, Some(0.0)]),
        );
        assert_eq!(
            query_booleans(
                &ctx,
                "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), radius) FROM null_radii",
            )
            .await,
            vec![None, Some(false)]
        );

        for (table, invalid_radius) in [
            ("negative_radius_with_null_geometry", -1.0),
            ("nan_radius_with_null_geometry", f64::NAN),
        ] {
            register_points(
                &ctx,
                table,
                &[None, Some(geo::Point::new(0.0, 0.0))],
                Some(&[Some(invalid_radius), Some(0.0)]),
            );
            assert_eq!(
                query_booleans(
                    &ctx,
                    &format!(
                        "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), radius) FROM {table}"
                    ),
                )
                .await,
                vec![None, Some(true)],
                "{table}"
            );
        }
    }

    #[tokio::test]
    async fn test_logical_null_geometry() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        let mut builder = GeometryBuilder::new(GeometryType::new(Default::default()));
        builder.push_null();
        builder
            .push_geometry(Some(&geo::Geometry::Point(geo::Point::new(0.0, 0.0))))
            .unwrap();
        let geometry_array = builder.finish();
        let field = Arc::new(geometry_array.data_type().to_field("geometry", true));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![geometry_array.to_array_ref()],
        )
        .unwrap();
        ctx.register_batch("logical_nulls", batch).unwrap();

        assert_eq!(
            query_booleans(
                &ctx,
                "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 0) FROM logical_nulls",
            )
            .await,
            vec![None, Some(true)]
        );
    }

    #[tokio::test]
    async fn test_empty_geometries_are_false() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        for empty in [
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "MULTIPOINT EMPTY",
            "MULTILINESTRING EMPTY",
            "MULTIPOLYGON EMPTY",
            "GEOMETRYCOLLECTION EMPTY",
        ] {
            let sql = format!(
                "SELECT ST_DWithin(ST_GeomFromText('{empty}'), ST_GeomFromText('POINT(0 0)'), 10)"
            );
            assert_eq!(query_bool(&ctx, &sql).await, Some(false), "{sql}");
        }
    }

    #[tokio::test]
    async fn test_mixed_empty_collection_members() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        let multi_point = wkt::types::MultiPoint::new(
            vec![
                wkt::types::Point::empty(wkt::types::Dimension::XY),
                wkt::types::Point::from_coord(wkt_coord(10.0, 0.0)),
            ],
            wkt::types::Dimension::XY,
        );
        let multi_point_array = MultiPointBuilder::from_multi_points(
            &[multi_point],
            MultiPointType::new(Dimension::XY, Default::default()),
        )
        .finish();
        register_geometry_array(&ctx, "mixed_multipoint", multi_point_array);

        let multi_line_string = wkt::types::MultiLineString::new(
            vec![
                wkt::types::LineString::empty(wkt::types::Dimension::XY),
                wkt::types::LineString::new(
                    vec![wkt_coord(10.0, 0.0), wkt_coord(11.0, 0.0)],
                    wkt::types::Dimension::XY,
                ),
            ],
            wkt::types::Dimension::XY,
        );
        let multi_line_string_array = MultiLineStringBuilder::from_multi_line_strings(
            &[multi_line_string],
            MultiLineStringType::new(Dimension::XY, Default::default()),
        )
        .finish();
        register_geometry_array(&ctx, "mixed_multilinestring", multi_line_string_array);

        let polygon_coords = [
            geo::Coord { x: 20.0, y: 0.0 },
            geo::Coord { x: 22.0, y: 0.0 },
            geo::Coord { x: 22.0, y: 2.0 },
            geo::Coord { x: 20.0, y: 0.0 },
        ];
        let multi_polygon_array = MultiPolygonArray::new(
            CoordBuffer::Interleaved(
                InterleavedCoordBuffer::from_coords(polygon_coords.iter(), Dimension::XY).unwrap(),
            ),
            OffsetBuffer::new(vec![0, 2].into()),
            OffsetBuffer::new(vec![0, 0, 1].into()),
            OffsetBuffer::new(vec![0, 4].into()),
            None,
            Default::default(),
        );
        register_geometry_array(&ctx, "mixed_multipolygon", multi_polygon_array);

        let geometry_collection = wkt::types::GeometryCollection::new(
            vec![
                wkt::types::Point::empty(wkt::types::Dimension::XY).into(),
                wkt::types::LineString::new(
                    vec![wkt_coord(10.0, 0.0), wkt_coord(11.0, 0.0)],
                    wkt::types::Dimension::XY,
                )
                .into(),
            ],
            wkt::types::Dimension::XY,
        );
        let geometry_collection_array = GeometryCollectionBuilder::from_geometry_collections(
            &[geometry_collection],
            GeometryCollectionType::new(Dimension::XY, Default::default()),
        )
        .unwrap()
        .finish();
        register_geometry_array(&ctx, "mixed_geometrycollection", geometry_collection_array);

        for (table, radius) in [
            ("mixed_multipoint", 10.0),
            ("mixed_multilinestring", 10.0),
            ("mixed_multipolygon", 20.0),
            ("mixed_geometrycollection", 10.0),
        ] {
            for (candidate_radius, expected) in [(radius, true), (radius - 0.1, false)] {
                for sql in [
                    format!(
                        "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), {candidate_radius}) FROM {table}"
                    ),
                    format!(
                        "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), geometry, {candidate_radius}) FROM {table}"
                    ),
                ] {
                    assert_eq!(query_bool(&ctx, &sql).await, Some(expected), "{sql}");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_mixed_geometry_collection_dispatch() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        let collection = "GEOMETRYCOLLECTION (POINT(10 0), LINESTRING(20 0, 21 0), MULTIPOINT((30 0), (31 0)), POLYGON((40 0, 42 0, 42 2, 40 0)))";
        for (radius, expected) in [(10.0, true), (9.9, false)] {
            for sql in [
                format!(
                    "SELECT ST_DWithin(ST_GeomFromText('{collection}'), ST_GeomFromText('POINT(0 0)'), {radius})"
                ),
                format!(
                    "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('{collection}'), {radius})"
                ),
            ] {
                assert_eq!(query_bool(&ctx, &sql).await, Some(expected), "{sql}");
            }
        }
    }

    #[tokio::test]
    async fn test_nested_geometry_collection() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        register_wkt_array(
            &ctx,
            "nested",
            "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT EMPTY), GEOMETRYCOLLECTION (LINESTRING (10 0, 11 0)))",
        );
        assert_eq!(
            query_bool(
                &ctx,
                "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 10) FROM nested",
            )
            .await,
            Some(true)
        );
    }

    #[tokio::test]
    async fn test_invalid_radii() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        for radius in [
            "-1",
            "CAST('NaN' AS DOUBLE)",
            "CAST('Infinity' AS DOUBLE)",
            "CAST('-Infinity' AS DOUBLE)",
        ] {
            let sql = format!(
                "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 0)'), {radius})"
            );
            let error = query_error(&ctx, &sql).await;
            assert!(
                error.contains("radius must be finite and non-negative"),
                "{sql}: {error}"
            );
        }

        assert_eq!(
            query_bool(
                &ctx,
                "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 0)'), -0.0)",
            )
            .await,
            Some(true)
        );

        register_points(
            &ctx,
            "invalid_radii",
            &[
                Some(geo::Point::new(0.0, 0.0)),
                Some(geo::Point::new(10.0, 0.0)),
            ],
            Some(&[Some(1.0), Some(f64::NAN)]),
        );
        let error = query_error(
            &ctx,
            "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), radius) FROM invalid_radii",
        )
        .await;
        assert!(
            error.contains("radius must be finite and non-negative"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_nonfinite_xy_is_rejected() {
        let ctx = SessionContext::new();
        register_standard(&ctx);
        register_points(
            &ctx,
            "nonfinite",
            &[
                Some(geo::Point::new(f64::NAN, 0.0)),
                Some(geo::Point::new(0.0, f64::INFINITY)),
            ],
            None,
        );

        let error = query_error(
            &ctx,
            "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) FROM nonfinite",
        )
        .await;
        assert!(error.contains("requires finite XY coordinates"), "{error}");
    }

    #[tokio::test]
    async fn test_malformed_line_strings_are_rejected() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        let line_string = geo::LineString::new(vec![geo::Coord { x: 0.0, y: 0.0 }]);
        let line_array = LineStringBuilder::from_line_strings(
            &[line_string],
            geoarrow_schema::LineStringType::new(Dimension::XY, Default::default()),
        )
        .finish();
        let field = Arc::new(line_array.data_type().to_field("geometry", true));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![line_array.to_array_ref()],
        )
        .unwrap();
        ctx.register_batch("malformed_line", batch).unwrap();

        let error = query_error(
            &ctx,
            "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) FROM malformed_line",
        )
        .await;
        assert!(error.contains("only 1 coordinate"), "{error}");

        let malformed_polygon = wkt::wkt! { POLYGON((0.0 0.0)) };
        let polygon_array = PolygonBuilder::from_polygons(
            &[malformed_polygon],
            PolygonType::new(Dimension::XY, Default::default()),
        )
        .finish();
        let field = Arc::new(polygon_array.data_type().to_field("geometry", true));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![polygon_array.to_array_ref()],
        )
        .unwrap();
        ctx.register_batch("malformed_ring", batch).unwrap();

        let error = query_error(
            &ctx,
            "SELECT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) FROM malformed_ring",
        )
        .await;
        assert!(error.contains("only 1 coordinate"), "{error}");
    }

    #[tokio::test]
    async fn test_malformed_member_is_not_dropped() {
        let ctx = SessionContext::new();
        register_standard(&ctx);

        for collection in [
            "MULTILINESTRING (EMPTY, (0 0))",
            "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (0 0))",
        ] {
            let sql = format!(
                "SELECT ST_DWithin(ST_GeomFromText('POINT EMPTY'), ST_GeomFromText('{collection}'), 1)"
            );
            let error = query_error(&ctx, &sql).await;
            assert!(error.contains("only 1 coordinate"), "{sql}: {error}");
        }
    }

    #[tokio::test]
    async fn test_mixed_length_arrays_are_rejected() {
        let left = PointBuilder::from_points(
            [geo::Point::new(0.0, 0.0), geo::Point::new(1.0, 1.0)].iter(),
            PointType::new(Dimension::XY, Default::default()),
        )
        .finish();
        let right = PointBuilder::from_points(
            [
                geo::Point::new(0.0, 0.0),
                geo::Point::new(1.0, 1.0),
                geo::Point::new(2.0, 2.0),
            ]
            .iter(),
            PointType::new(Dimension::XY, Default::default()),
        )
        .finish();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(left.to_array_ref()),
                ColumnarValue::Array(right.to_array_ref()),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
            ],
            arg_fields: vec![
                Arc::new(left.data_type().to_field("left", true)),
                Arc::new(right.data_type().to_field("right", true)),
                Arc::new(Field::new("radius", DataType::Float64, true)),
            ],
            number_rows: 2,
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::new()),
        };

        let error = DWithin::new()
            .invoke_with_args(args)
            .unwrap_err()
            .to_string();
        assert!(error.contains("mixed length"), "{error}");
    }
}
