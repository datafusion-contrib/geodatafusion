use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::BooleanBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array};
use arrow_schema::{DataType, Field};
use datafusion::arrow::compute::cast as cast_array;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::types::{NativeType, logical_float64};
use datafusion::common::{Column, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use geo::{Distance, Euclidean};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, WrapArray, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;

use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::{GeometryBounds, geometry_bounds};

/// A conservative planar XY envelope for a spatial index candidate query.
///
/// `envelope` is `[min_x, min_y, max_x, max_y]`. The bounds are expanded
/// outward to cover the distance condition before the caller queries an index.
#[derive(Debug, Clone, PartialEq)]
pub struct DWithinIndexFilter {
    /// The indexed geometry column, including its optional qualifier.
    pub column: Column,
    /// A conservative planar XY envelope in coordinate-system units.
    pub envelope: [f64; 4],
}

/// Extract the indexable shape of a root `ST_DWithin` expression.
///
/// The helper accepts exactly one bare geometry column and one folded geometry
/// literal with a finite, non-negative constant radius. It does not recurse
/// through `AND`, `OR`, or `NOT`; callers should pass individual pushdown
/// conjuncts.
///
/// A returned envelope is the directed-rounded mathematical bounds condition
/// enforced by [`DWithin`]. `ST_DWithin` checks the same condition before it
/// invokes the distance kernel, so every `ST_DWithin` true row is a candidate
/// without assuming an error bound for that kernel. Providers must declare an
/// inexact pushdown and retain the original predicate for exact rechecking.
pub fn extract_dwithin_index_filter(expr: &Expr) -> Result<Option<DWithinIndexFilter>> {
    Ok(extract_dwithin_index_filter_impl(expr)?)
}

fn extract_dwithin_index_filter_impl(
    expr: &Expr,
) -> GeoDataFusionResult<Option<DWithinIndexFilter>> {
    let Expr::ScalarFunction(function) = expr else {
        return Ok(None);
    };

    if !function.func.inner().is::<DWithin>() || function.args.len() != 3 {
        return Ok(None);
    }

    let (column, geometry, metadata) = match (&function.args[0], &function.args[1]) {
        (Expr::Column(column), Expr::Literal(geometry, metadata)) => (column, geometry, metadata),
        (Expr::Literal(geometry, metadata), Expr::Column(column)) => (column, geometry, metadata),
        _ => return Ok(None),
    };

    let Expr::Literal(radius, _) = &function.args[2] else {
        return Ok(None);
    };

    let Some(bounds) = literal_geometry_bounds(geometry, metadata.as_ref())? else {
        return Ok(None);
    };
    let Some(radius) = literal_radius(radius)? else {
        return Ok(None);
    };

    let envelope = expand_bounds(bounds, radius);
    if !envelope.iter().all(|value| value.is_finite()) {
        return Ok(None);
    }

    Ok(Some(DWithinIndexFilter {
        column: column.clone(),
        envelope,
    }))
}

fn literal_geometry_bounds(
    value: &ScalarValue,
    metadata: Option<&FieldMetadata>,
) -> GeoDataFusionResult<Option<[f64; 4]>> {
    if value.is_null() {
        return Ok(None);
    }

    let array = value.to_array()?;
    let mut field = Field::new("geometry", value.data_type(), true);
    if let Some(metadata) = metadata {
        field = metadata.add_to_field(field);
    }

    let Ok(geo_type) = GeoArrowType::from_arrow_field(&field) else {
        return Ok(None);
    };
    let geo_array = geo_type.wrap_array(array.as_ref())?;
    let Some(Some(bounds)) = geo_array_bounds(geo_array.as_ref())?.into_iter().next() else {
        return Ok(None);
    };

    Ok(match bounds {
        GeometryBounds::Finite(bounds) => {
            Some([bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()])
        }
        GeometryBounds::Empty | GeometryBounds::NonFinite => None,
    })
}

fn literal_radius(value: &ScalarValue) -> GeoDataFusionResult<Option<f64>> {
    if value.is_null() || !is_supported_radius_type(&value.data_type()) {
        return Ok(None);
    }

    let array = value.to_array()?;
    let radius = radius_values(array.as_ref())?.into_iter().next().flatten();

    Ok(radius.filter(|radius| radius.is_finite() && *radius >= 0.0))
}

fn geo_array_bounds(array: &dyn GeoArrowArray) -> GeoDataFusionResult<Vec<Option<GeometryBounds>>> {
    downcast_geoarrow_array!(array, geo_array_bounds_impl)
}

fn geo_array_bounds_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<Vec<Option<GeometryBounds>>> {
    let mut bounds = Vec::with_capacity(array.len());

    for item in array.iter() {
        let Some(geometry) = item else {
            bounds.push(None);
            continue;
        };

        let geometry = geometry?;
        bounds.push(Some(geometry_bounds(&geometry)));
    }

    Ok(bounds)
}

fn expand_bounds(bounds: [f64; 4], radius: f64) -> [f64; 4] {
    // This directed rounding covers the exact mathematical condition
    // `distance <= radius`. It deliberately makes no claim about the error of
    // the distance kernel. ST_DWithin enforces this same envelope before it
    // calls that kernel, which is what makes the extracted candidate safe.
    [
        (bounds[0] - radius).next_down(),
        (bounds[1] - radius).next_down(),
        (bounds[2] + radius).next_up(),
        (bounds[3] + radius).next_up(),
    ]
}

fn expanded_bounds_intersect(left: [f64; 4], right: [f64; 4], radius: f64) -> bool {
    let left_expanded = expand_bounds(left, radius);
    let right_expanded = expand_bounds(right, radius);

    bounds_intersect(left_expanded, right) && bounds_intersect(right_expanded, left)
}

fn bounds_intersect(left: [f64; 4], right: [f64; 4]) -> bool {
    left[0] <= right[2] && right[0] <= left[2] && left[1] <= right[3] && right[1] <= left[3]
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DWithin;

impl DWithin {
    pub fn new() -> Self {
        Self
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
        Ok(dwithin_impl(args)?)
    }

    fn is_strict(&self) -> bool {
        true
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

fn dwithin_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != 3 {
        return Err(DataFusionError::Internal(format!(
            "ST_DWithin expects 3 arguments, got {}",
            arrays.len()
        ))
        .into());
    }

    let number_rows = args.number_rows;
    if number_rows == 0 {
        return Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(Vec::<
            bool,
        >::new(
        )))));
    }
    if arrays.iter().any(|array| array.len() != number_rows) {
        return Err(DataFusionError::Internal(
            "ST_DWithin input arrays have mixed lengths".to_string(),
        )
        .into());
    }

    if arrays.iter().any(|array| array_is_all_null(array.as_ref())) {
        return Ok(ColumnarValue::Array(null_result(number_rows)));
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
    let mut builder = BooleanBuilder::with_capacity(number_rows);
    for index in 0..number_rows {
        let left = &left[index];
        let right = &right[index];
        if matches!(left, GeometryValue::Null) || matches!(right, GeometryValue::Null) {
            builder.append_null();
            continue;
        }

        let Some(radius) = (!radii.is_null(index)).then(|| radii.value(index)) else {
            builder.append_null();
            continue;
        };
        if !radius.is_finite() || radius < 0.0 {
            return Err(DataFusionError::Execution(
                "ST_DWithin radius must be finite and non-negative".to_string(),
            )
            .into());
        }

        match (left, right) {
            (GeometryValue::Null, _) | (_, GeometryValue::Null) => builder.append_null(),
            (GeometryValue::Empty, _) | (_, GeometryValue::Empty) => builder.append_value(false),
            (
                GeometryValue::Geometry {
                    geometry: left_geometry,
                    bounds: left_bounds,
                },
                GeometryValue::Geometry {
                    geometry: right_geometry,
                    bounds: right_bounds,
                },
            ) => {
                let candidate = expanded_bounds_intersect(*left_bounds, *right_bounds, radius);
                builder.append_value(
                    candidate && Euclidean.distance(left_geometry, right_geometry) <= radius,
                );
            }
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

#[derive(Debug)]
enum GeometryValue {
    Null,
    Empty,
    Geometry {
        geometry: geo::Geometry<f64>,
        bounds: [f64; 4],
    },
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
                Some(geometry) => {
                    let GeometryBounds::Finite(bounds) = geometry_bounds(&geometry) else {
                        return Err(DataFusionError::Internal(
                            "ST_DWithin converted a non-empty geometry without finite bounds"
                                .to_string(),
                        )
                        .into());
                    };
                    GeometryValue::Geometry {
                        geometry,
                        bounds: [bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()],
                    }
                }
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
            let min = coord_to_geo(&rect.min(), "Rect")?;
            let max = coord_to_geo(&rect.max(), "Rect")?;
            Ok(Some(geo::Geometry::Rect(geo::Rect::new(min, max))))
        }
        GeometryType::Triangle(triangle) => {
            let [first, second, third] = triangle.coords();
            Ok(Some(geo::Geometry::Triangle(geo::Triangle::new(
                coord_to_geo(&first, "Triangle")?,
                coord_to_geo(&second, "Triangle")?,
                coord_to_geo(&third, "Triangle")?,
            ))))
        }
        GeometryType::Line(line) => Ok(Some(geo::Geometry::Line(geo::Line::new(
            coord_to_geo(&line.start(), "Line")?,
            coord_to_geo(&line.end(), "Line")?,
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

fn radius_values(array: &dyn Array) -> GeoDataFusionResult<Vec<Option<f64>>> {
    if array_is_all_null(array) {
        return Ok(vec![None; array.len()]);
    }

    if !is_supported_radius_type(array.data_type()) {
        return Err(DataFusionError::Execution(format!(
            "ST_DWithin radius must be numeric, got {}",
            array.data_type()
        ))
        .into());
    }

    let array = cast_array(array, &DataType::Float64)?;
    let array = array.as_primitive::<Float64Type>();
    Ok((0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index)))
        .collect())
}

fn is_supported_radius_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn null_result(number_rows: usize) -> ArrayRef {
    Arc::new(BooleanArray::from(vec![None; number_rows]))
}

fn array_is_all_null(array: &dyn Array) -> bool {
    matches!(array.data_type(), DataType::Null)
        || (!array.is_empty() && array.null_count() == array.len())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use arrow_array::builder::BooleanBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{Schema, SchemaRef};
    use datafusion::arrow::compute::filter_record_batch;
    use datafusion::catalog::Session;
    use datafusion::common::TableReference;
    use datafusion::datasource::memory::MemTable;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::logical_expr::{ScalarUDF, TableProviderFilterPushDown};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use geo::Distance;
    use geoarrow_array::GeoArrowArray;
    use geoarrow_array::array::{
        CoordBuffer, InterleavedCoordBuffer, MultiPolygonArray, WktArray, from_arrow_array,
    };
    use geoarrow_array::builder::{
        GeometryBuilder, GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder,
        MultiPointBuilder, PointBuilder, PolygonBuilder,
    };
    use geoarrow_schema::{
        Dimension, GeometryCollectionType, GeometryType as GeoArrowGeometryType, Metadata,
        MultiLineStringType, MultiPointType, PointType, PolygonType,
    };
    use wkt::wkt;

    use super::*;
    use crate::udf::native::bounding_box::util::total_bounds;

    async fn execute_bools(ctx: &SessionContext, sql: &str) -> Vec<Option<bool>> {
        let dataframe = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("failed to plan {sql}: {error}"));
        let batches = dataframe
            .collect()
            .await
            .unwrap_or_else(|error| panic!("failed to execute {sql}: {error}"));
        let column = batches[0].column(0).as_boolean();
        (0..column.len())
            .map(|index| (!column.is_null(index)).then(|| column.value(index)))
            .collect()
    }

    async fn assert_query_error(ctx: &SessionContext, sql: &str, expected: &str) {
        let error = match ctx.sql(sql).await {
            Ok(dataframe) => dataframe.collect().await.unwrap_err(),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(expected),
            "expected error containing {expected:?}, got {error}"
        );
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

    #[derive(Debug, Eq, PartialEq, Hash)]
    struct FakeDWithin;

    impl ScalarUDFImpl for FakeDWithin {
        fn name(&self) -> &str {
            "st_dwithin"
        }

        fn signature(&self) -> &Signature {
            &SIGNATURE
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Boolean)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Err(DataFusionError::Internal(
                "fake ST_DWithin must not be invoked".to_string(),
            ))
        }
    }

    fn folded_point_literal() -> (ScalarValue, FieldMetadata) {
        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!(POINT(0.0 0.0))));
        let geometry = builder.finish();
        let field = geometry.data_type().to_field("geometry", true);
        let value = ScalarValue::try_from_array(&geometry.to_array_ref(), 0).unwrap();
        (value, FieldMetadata::from(field.metadata()))
    }

    #[test]
    fn test_extract_dwithin_identity_qualifier_and_unsupported_shapes() {
        let dwithin = ScalarUDF::from(DWithin::new());
        let column = Column::new(Some(TableReference::bare("shapes")), "geometry");
        let (geometry, metadata) = folded_point_literal();
        let radius = Expr::Literal(ScalarValue::Float64(Some(1.0)), None);

        let expression = dwithin.call(vec![
            Expr::Column(column.clone()),
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            radius.clone(),
        ]);
        let extracted = extract_dwithin_index_filter(&expression)
            .unwrap()
            .expect("qualified DWithin should be extractable");
        assert_eq!(extracted.column, column);

        let reversed = dwithin.call(vec![
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            Expr::Column(column.clone()),
            radius.clone(),
        ]);
        let extracted = extract_dwithin_index_filter(&reversed)
            .unwrap()
            .expect("reversed DWithin should be extractable");
        assert_eq!(extracted.column, column);

        let fake = ScalarUDF::from(FakeDWithin).call(vec![
            Expr::Column(column.clone()),
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            radius.clone(),
        ]);
        assert!(
            extract_dwithin_index_filter(&fake).unwrap().is_none(),
            "a different UDF named ST_DWithin must not match"
        );

        let both_columns = dwithin.call(vec![
            Expr::Column(column.clone()),
            Expr::Column(Column::new_unqualified("other_geometry")),
            radius.clone(),
        ]);
        assert!(
            extract_dwithin_index_filter(&both_columns)
                .unwrap()
                .is_none()
        );

        let dynamic_radius = dwithin.call(vec![
            Expr::Column(column.clone()),
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            Expr::Column(Column::new_unqualified("radius")),
        ]);
        assert!(
            extract_dwithin_index_filter(&dynamic_radius)
                .unwrap()
                .is_none()
        );

        let non_finite_radius = dwithin.call(vec![
            Expr::Column(column.clone()),
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            Expr::Literal(ScalarValue::Float64(Some(f64::NAN)), None),
        ]);
        assert!(
            extract_dwithin_index_filter(&non_finite_radius)
                .unwrap()
                .is_none()
        );

        let disjunction = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(expression.clone()),
            datafusion::logical_expr::Operator::Or,
            Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
        ));
        assert!(
            extract_dwithin_index_filter(&disjunction)
                .unwrap()
                .is_none()
        );

        let negation = Expr::Not(Box::new(expression));
        assert!(extract_dwithin_index_filter(&negation).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_dwithin_scalar_semantics() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(3, 4), 5)").await;
        assert_eq!(values, vec![Some(true)]);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(3, 4), 4)").await;
        assert_eq!(values, vec![Some(false)]);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(3, 4), ST_Point(0, 0), 5)").await;
        assert_eq!(values, vec![Some(true)]);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(0, 0), 0)").await;
        assert_eq!(values, vec![Some(true)]);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), 0)").await;
        assert_eq!(values, vec![Some(false)]);
    }

    #[tokio::test]
    async fn test_dwithin_non_point_geometries() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(\
                ST_Point(1, 1), \
                ST_GeomFromText('LINESTRING(0 0, 2 0)'), \
                1\
            )",
        )
        .await;
        assert_eq!(values, vec![Some(true)]);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(\
                ST_Point(1, 1), \
                ST_GeomFromText('LINESTRING(0 0, 2 0)'), \
                0.9999999999999999\
            )",
        )
        .await;
        assert_eq!(values, vec![Some(false)]);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(\
                ST_Point(1, 1), \
                ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), \
                0\
            )",
        )
        .await;
        assert_eq!(values, vec![Some(true)]);
    }

    #[tokio::test]
    async fn test_dwithin_null_precedence_and_logical_nulls() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        for sql in [
            "SELECT ST_DWithin(NULL, ST_Point(0, 0), 1)",
            "SELECT ST_DWithin(ST_Point(0, 0), NULL, 1)",
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(0, 0), NULL)",
        ] {
            assert_eq!(execute_bools(&ctx, sql).await, vec![None], "{sql}");
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
            execute_bools(
                &ctx,
                "SELECT ST_DWithin(geometry, ST_Point(0, 0), radius) FROM null_radii",
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
                execute_bools(
                    &ctx,
                    &format!("SELECT ST_DWithin(geometry, ST_Point(0, 0), radius) FROM {table}"),
                )
                .await,
                vec![None, Some(true)],
                "{table}"
            );
        }

        let mut builder = GeometryBuilder::new(GeoArrowGeometryType::new(Default::default()));
        builder.push_null();
        builder
            .push_geometry(Some(&geo::Geometry::Point(geo::Point::new(0.0, 0.0))))
            .unwrap();
        register_geometry_array(&ctx, "logical_nulls", builder.finish());

        assert_eq!(
            execute_bools(
                &ctx,
                "SELECT ST_DWithin(geometry, ST_Point(0, 0), 0) FROM logical_nulls",
            )
            .await,
            vec![None, Some(true)]
        );
    }

    #[tokio::test]
    async fn test_dwithin_empty_collections_and_nested_geometry_collection() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        for empty in [
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "MULTIPOINT EMPTY",
            "MULTILINESTRING EMPTY",
            "MULTIPOLYGON EMPTY",
            "GEOMETRYCOLLECTION EMPTY",
        ] {
            let sql = format!("SELECT ST_DWithin(ST_GeomFromText('{empty}'), ST_Point(0, 0), 10)");
            assert_eq!(execute_bools(&ctx, &sql).await, vec![Some(false)], "{sql}");
        }
        assert_eq!(
            execute_bools(
                &ctx,
                "SELECT ST_DWithin(\
                    ST_GeomFromText('POLYGON EMPTY'), \
                    ST_GeomFromText('POLYGON EMPTY'), \
                    0\
                )",
            )
            .await,
            vec![Some(false)]
        );

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
                        "SELECT ST_DWithin(geometry, ST_Point(0, 0), {candidate_radius}) FROM {table}"
                    ),
                    format!(
                        "SELECT ST_DWithin(ST_Point(0, 0), geometry, {candidate_radius}) FROM {table}"
                    ),
                ] {
                    assert_eq!(
                        execute_bools(&ctx, &sql).await,
                        vec![Some(expected)],
                        "{sql}"
                    );
                }
            }
        }

        register_wkt_array(
            &ctx,
            "nested",
            "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT EMPTY), GEOMETRYCOLLECTION (LINESTRING (10 0, 11 0)))",
        );
        assert_eq!(
            execute_bools(
                &ctx,
                "SELECT ST_DWithin(geometry, ST_Point(0, 0), 10) FROM nested",
            )
            .await,
            vec![Some(true)]
        );
    }

    #[tokio::test]
    async fn test_dwithin_array_scalar_combinations() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!(POINT(0.0 0.0))));
        builder.push_point(Some(&wkt!(POINT(3.0 4.0))));
        builder.push_point(Some(&wkt!(POINT(6.0 8.0))));
        builder.push_null();
        let geometry = builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
            Arc::new(Field::new("radius", DataType::Float64, true)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                geometry.into_array_ref(),
                Arc::new(Float64Array::from(vec![
                    Some(5.0),
                    Some(4.0),
                    Some(4.0),
                    None,
                ])),
            ],
        )
        .unwrap();
        ctx.register_batch("points", batch).unwrap();

        let batches = ctx
            .sql(
                "SELECT \
                    ST_DWithin(p.geometry, ST_GeomFromText('POINT(0 0)'), p.radius), \
                    ST_DWithin(ST_GeomFromText('POINT(0 0)'), p.geometry, p.radius), \
                    ST_DWithin(p.geometry, ST_GeomFromText('POINT(0 0)'), 5), \
                    ST_DWithin(p.geometry, s.geometry, 1) \
                 FROM points p \
                 CROSS JOIN (SELECT ST_GeomFromText('POINT(0 0)') AS geometry) s \
                 ORDER BY p.id",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];

        for column_index in 0..2 {
            let column = batch.column(column_index).as_boolean();
            assert!(column.value(0));
            assert!(!column.value(1));
            assert!(!column.value(2));
            assert!(column.is_null(3));
        }

        let integer_radius = batch.column(2).as_boolean();
        assert!(integer_radius.value(0));
        assert!(integer_radius.value(1));
        assert!(!integer_radius.value(2));
        assert!(integer_radius.is_null(3));

        let cross_join = batch.column(3).as_boolean();
        assert!(cross_join.value(0));
        assert!(!cross_join.value(1));
        assert!(!cross_join.value(2));
        assert!(cross_join.is_null(3));
    }

    #[tokio::test]
    async fn test_dwithin_invalid_radius_and_malformed_input() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), -1)",
            "finite and non-negative",
        )
        .await;
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), CAST('NaN' AS DOUBLE))",
            "finite and non-negative",
        )
        .await;
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), CAST('Infinity' AS DOUBLE))",
            "finite and non-negative",
        )
        .await;
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), '1')",
            "requires Float64",
        )
        .await;
        assert_eq!(
            execute_bools(
                &ctx,
                "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(0, 0), -0.0)",
            )
            .await,
            vec![Some(true)]
        );
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(\
                ST_Point(CAST('NaN' AS DOUBLE), 0), \
                ST_Point(0, 0), \
                1\
            )",
            "requires finite XY coordinates",
        )
        .await;

        register_points(
            &ctx,
            "invalid_radii",
            &[
                Some(geo::Point::new(0.0, 0.0)),
                Some(geo::Point::new(10.0, 0.0)),
            ],
            Some(&[Some(1.0), Some(f64::NAN)]),
        );
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(geometry, ST_Point(0, 0), radius) FROM invalid_radii",
            "finite and non-negative",
        )
        .await;
    }

    #[tokio::test]
    async fn test_dwithin_rejects_malformed_geometries() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        let line_string = geo::LineString::new(vec![geo::Coord { x: 0.0, y: 0.0 }]);
        let line_array = LineStringBuilder::from_line_strings(
            &[line_string],
            geoarrow_schema::LineStringType::new(Dimension::XY, Default::default()),
        )
        .finish();
        register_geometry_array(&ctx, "malformed_line", line_array);
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(geometry, ST_Point(0, 0), 1) FROM malformed_line",
            "only 1 coordinate",
        )
        .await;

        let malformed_polygon = wkt::wkt! { POLYGON((0.0 0.0)) };
        let polygon_array = PolygonBuilder::from_polygons(
            &[malformed_polygon],
            PolygonType::new(Dimension::XY, Default::default()),
        )
        .finish();
        register_geometry_array(&ctx, "malformed_ring", polygon_array);
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(geometry, ST_Point(0, 0), 1) FROM malformed_ring",
            "only 1 coordinate",
        )
        .await;

        for collection in [
            "MULTILINESTRING (EMPTY, (0 0))",
            "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (0 0))",
        ] {
            let sql = format!(
                "SELECT ST_DWithin(ST_GeomFromText('POINT EMPTY'), ST_GeomFromText('{collection}'), 1)"
            );
            assert_query_error(&ctx, &sql, "only 1 coordinate").await;
        }
    }

    #[tokio::test]
    async fn test_distance_boundary_does_not_drive_candidate_extraction() {
        let query_x = -1.0;
        let row_x = 2f64.powi(-54);
        let distance =
            geo::Euclidean.distance(geo::Point::new(query_x, 0.0), geo::Point::new(row_x, 0.0));

        assert_eq!(distance, 1.0);
        assert!(row_x > 0.0f64.next_up());

        let ctx = SessionContext::new();
        crate::register(&ctx);
        let batches = ctx
            .sql(
                "SELECT \
                    ST_Distance(\
                        ST_GeomFromText('POINT(-1 0)'), \
                        ST_GeomFromText('POINT(0.000000000000000055511151231257827021181583404541015625 0)')\
                    ), \
                    ST_DWithin(\
                        ST_GeomFromText('POINT(-1 0)'), \
                        ST_GeomFromText('POINT(0.000000000000000055511151231257827021181583404541015625 0)'), \
                        1\
                    )",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batch = &batches[0];
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 1.0);
        assert!(!batch.column(1).as_boolean().value(0));
    }

    #[derive(Debug)]
    struct OptimizedDWithinTable {
        schema: SchemaRef,
        batch: RecordBatch,
        scanned_ids: Arc<Mutex<Vec<i32>>>,
        extracted_filters: Arc<Mutex<Vec<DWithinIndexFilter>>>,
    }

    impl OptimizedDWithinTable {
        fn new(
            batch: RecordBatch,
            scanned_ids: Arc<Mutex<Vec<i32>>>,
            extracted_filters: Arc<Mutex<Vec<DWithinIndexFilter>>>,
        ) -> Self {
            Self {
                schema: batch.schema(),
                batch,
                scanned_ids,
                extracted_filters,
            }
        }

        fn candidate_mask(&self, filter: &DWithinIndexFilter) -> Result<BooleanArray> {
            let geometry_field = self.schema.field_with_name("geometry")?;
            let geometry = from_arrow_array(
                self.batch
                    .column_by_name("geometry")
                    .expect("test batch has geometry"),
                geometry_field,
            )
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let mut mask = BooleanBuilder::with_capacity(geometry.len());
            let [min_x, min_y, max_x, max_y] = filter.envelope;

            for index in 0..geometry.len() {
                if geometry.is_null(index) {
                    mask.append_value(false);
                    continue;
                }

                let bounds = total_bounds(geometry.slice(index, 1).as_ref())
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                let intersects = [bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()]
                    .iter()
                    .all(|value| value.is_finite())
                    && bounds.minx() <= max_x
                    && min_x <= bounds.maxx()
                    && bounds.miny() <= max_y
                    && min_y <= bounds.maxy();
                mask.append_value(intersects);
            }

            Ok(mask.finish())
        }
    }

    impl TableProvider for OptimizedDWithinTable {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            let mut support = Vec::with_capacity(filters.len());
            for filter in filters {
                support.push(if extract_dwithin_index_filter(filter)?.is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                });
            }
            Ok(support)
        }

        fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            state: &'life1 dyn Session,
            projection: Option<&'life2 Vec<usize>>,
            filters: &'life3 [Expr],
            limit: Option<usize>,
        ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let mut batch = self.batch.clone();

                for filter in filters {
                    let Some(filter) = extract_dwithin_index_filter(filter)? else {
                        continue;
                    };
                    self.extracted_filters.lock().unwrap().push(filter.clone());
                    let mask = self.candidate_mask(&filter)?;
                    batch = filter_record_batch(&batch, &mask)?;
                }

                let ids = batch
                    .column_by_name("id")
                    .expect("test batch has id")
                    .as_primitive::<arrow_array::types::Int32Type>();
                self.scanned_ids
                    .lock()
                    .unwrap()
                    .extend((0..ids.len()).map(|index| ids.value(index)));

                let table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
                table.scan(state, projection, &[], limit).await
            })
        }
    }

    fn optimized_test_batch() -> RecordBatch {
        let geometry = WktArray::new(
            StringArray::from(vec![
                Some("POINT(0 0)"),
                Some("POINT(1 0)"),
                Some("POINT(0.9 0.9)"),
                Some("POINT(2 0)"),
                Some("LINESTRING(0.5 -0.5, 0.5 0.5)"),
                Some("LINESTRING(1.2 -0.5, 1.2 0.5)"),
                None,
                Some("POINT EMPTY"),
                Some("POINT(0.000000000000000055511151231257827021181583404541015625 0)"),
            ]),
            Arc::new(Metadata::default()),
        );
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from((1..=9).collect::<Vec<_>>())),
                geometry.into_array_ref(),
            ],
        )
        .unwrap()
    }

    fn collect_ids(batches: &[RecordBatch]) -> Vec<i32> {
        let mut ids = Vec::new();
        for batch in batches {
            let column = batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            ids.extend((0..column.len()).map(|index| column.value(index)));
        }
        ids
    }

    async fn run_provider_query(sql: &str) -> (Vec<i32>, Vec<i32>, Vec<DWithinIndexFilter>) {
        let scanned_ids = Arc::new(Mutex::new(Vec::new()));
        let extracted_filters = Arc::new(Mutex::new(Vec::new()));
        let table = Arc::new(OptimizedDWithinTable::new(
            optimized_test_batch(),
            scanned_ids.clone(),
            extracted_filters.clone(),
        ));
        let ctx = SessionContext::new();
        crate::register(&ctx);
        ctx.register_table("shapes", table).unwrap();

        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut scanned_ids = scanned_ids.lock().unwrap().clone();
        scanned_ids.sort_unstable();
        let extracted_filters = extracted_filters.lock().unwrap().clone();
        (collect_ids(&batches), scanned_ids, extracted_filters)
    }

    async fn run_full_scan_query(sql: &str) -> Vec<i32> {
        let ctx = SessionContext::new();
        crate::register(&ctx);
        ctx.register_batch("shapes", optimized_test_batch())
            .unwrap();
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        collect_ids(&batches)
    }

    #[tokio::test]
    async fn test_optimized_provider_matches_full_scan() {
        let sql = "SELECT id FROM shapes \
                   WHERE ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) \
                   ORDER BY id";
        let full_scan = run_full_scan_query(sql).await;
        let (indexed, scanned, filters) = run_provider_query(sql).await;

        assert_eq!(full_scan, vec![1, 2, 5, 9]);
        assert_eq!(indexed, full_scan);
        // Row 3 is an in-bounds false positive removed by the exact recheck.
        assert_eq!(scanned, vec![1, 2, 3, 5, 9]);
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].column.name, "geometry");
    }

    #[tokio::test]
    async fn test_optimized_provider_boundary_counterexample() {
        for sql in [
            "SELECT id FROM shapes \
             WHERE ST_DWithin(geometry, ST_GeomFromText('POINT(-1 0)'), 1) \
             ORDER BY id",
            "SELECT id FROM shapes \
             WHERE ST_DWithin(ST_GeomFromText('POINT(-1 0)'), geometry, 1) \
             ORDER BY id",
        ] {
            let full_scan = run_full_scan_query(sql).await;
            let (indexed, scanned, _) = run_provider_query(sql).await;

            assert_eq!(full_scan, vec![1], "{sql}");
            assert_eq!(indexed, full_scan, "{sql}");
            assert_eq!(scanned, vec![1], "{sql}");
        }
    }

    #[tokio::test]
    async fn test_optimized_provider_root_only_pushdown() {
        let all_ids = (1..=9).collect::<Vec<_>>();

        let sql_or = "SELECT id FROM shapes \
                      WHERE ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) OR id = 6 \
                      ORDER BY id";
        let full_scan_or = run_full_scan_query(sql_or).await;
        let (indexed_or, scanned_or, _) = run_provider_query(sql_or).await;
        assert_eq!(indexed_or, full_scan_or);
        assert_eq!(scanned_or, all_ids);

        let sql_not = "SELECT id FROM shapes \
                       WHERE NOT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) \
                       ORDER BY id";
        let full_scan_not = run_full_scan_query(sql_not).await;
        let (indexed_not, scanned_not, _) = run_provider_query(sql_not).await;
        assert_eq!(indexed_not, full_scan_not);
        assert_eq!(scanned_not, all_ids);

        let sql_and = "SELECT id FROM shapes \
                       WHERE ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) \
                         AND id >= 2 \
                       ORDER BY id";
        let full_scan_and = run_full_scan_query(sql_and).await;
        let (indexed_and, scanned_and, _) = run_provider_query(sql_and).await;
        assert_eq!(full_scan_and, vec![2, 5, 9]);
        assert_eq!(indexed_and, full_scan_and);
        assert_eq!(scanned_and, vec![1, 2, 3, 5, 9]);
    }
}
