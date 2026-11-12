use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::Float64Array;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo::{Euclidean, Length, Polygon};
use geo_traits::to_geo::{ToGeoMultiPolygon, ToGeoPolygon, ToGeoTriangle};
use geo_traits::{CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, RectTrait};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

/// `ST_Perimeter` (`ST_Perimeter2D`) scalar UDF implementation: returns the 2D Cartesian perimeter of polygonal geometries.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Perimeter;

impl Perimeter {
    /// Creates a new `ST_Perimeter` UDF instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Perimeter {
    fn default() -> Self {
        Self::new()
    }
}

static ALIASES: LazyLock<Vec<String>> = LazyLock::new(|| vec!["st_perimeter2d".to_string()]);
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Perimeter {
    fn name(&self) -> &str {
        "st_perimeter"
    }

    fn aliases(&self) -> &[String] {
        ALIASES.as_slice()
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let Some(result) = crate::data_types::null_input_result(&args)? {
            return Ok(result);
        }
        Ok(perimeter_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D Cartesian perimeter of polygonal geometries. For a GeometryCollection, the perimeters of its polygonal members are summed. For other geometries 0 is returned.",
                "ST_Perimeter(geom)",
            )
            .with_argument("geom", "geometry")
            .with_sql_example(
                "SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0, 3 0, 3 4, 0 4, 0 0))'));",
            )
            .build()
        }))
    }
}

fn perimeter_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let field = &args.arg_fields[0];
    let geo_array = from_arrow_array(&array, field)?;
    let result = euclidean_perimeter(geo_array.as_ref())?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn euclidean_perimeter(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, euclidean_perimeter_impl)
}

fn euclidean_perimeter_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    let mut result = Float64Array::builder(array.len());
    for item in array.iter() {
        if let Some(geometry) = item {
            result.append_value(geometry_perimeter(&geometry?));
        } else {
            result.append_null();
        }
    }
    Ok(result.finish())
}

/// Perimeter of a polygonal geometry, summed over the members of a GeometryCollection.
fn geometry_perimeter(geometry: &impl GeometryTrait<T = f64>) -> f64 {
    let perimeter = match geometry.as_type() {
        GeometryType::Polygon(polygon) => polygon_perimeter(&polygon.to_polygon()),
        GeometryType::Rect(rect) => rect_perimeter(rect),
        GeometryType::Triangle(triangle) => polygon_perimeter(&triangle.to_triangle().to_polygon()),
        GeometryType::MultiPolygon(multi_polygon) => multi_polygon
            .to_multi_polygon()
            .0
            .iter()
            .fold(0.0_f64, |sum, polygon| sum + polygon_perimeter(polygon)),
        GeometryType::GeometryCollection(collection) => collection
            .geometries()
            .fold(0.0_f64, |sum, geometry| sum + geometry_perimeter(&geometry)),
        _ => 0.0,
    };

    // PostGIS returns positive zero for empty inputs, and the PostGIS oracle
    // compares float64 bit patterns exactly.
    if perimeter == 0.0 { 0.0 } else { perimeter }
}

/// Perimeter of a rectangle, matching PostGIS's conversion of `box2d` to geometry.
///
/// A non-degenerate box becomes a polygon. If either side has zero length, the
/// box becomes a LineString or Point, whose perimeter is zero.
fn rect_perimeter(rect: &impl RectTrait<T = f64>) -> f64 {
    let min = rect.min();
    let max = rect.max();
    let x_min = min.x().min(max.x());
    let x_max = min.x().max(max.x());
    let y_min = min.y().min(max.y());
    let y_max = min.y().max(max.y());

    if x_min == x_max || y_min == y_max {
        return 0.0;
    }

    polygon_perimeter(&Polygon::new(
        geo::LineString::from(vec![
            (x_min, y_min),
            (x_min, y_max),
            (x_max, y_max),
            (x_max, y_min),
            (x_min, y_min),
        ]),
        Vec::new(),
    ))
}

fn polygon_perimeter(polygon: &Polygon) -> f64 {
    Euclidean.length(polygon.exterior())
        + polygon
            .interiors()
            .iter()
            .fold(0.0_f64, |sum, ring| sum + Euclidean.length(ring))
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::bounding_box::MakeBox2D;
    use crate::udf::native::constructors::Point;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_perimeter_for_multiple_rows_with_null() {
        let ctx = SessionContext::new();

        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql(
                "WITH t(id, wkt) AS (VALUES \
                    (1, 'POLYGON((0 0, 4 0, 4 3, 0 3, 0 0))'), \
                    (2, 'POINT(1 2)'), \
                    (3, 'POLYGON EMPTY'), \
                    (4, CAST(NULL AS VARCHAR)), \
                    (5, 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) \
                 SELECT ST_Perimeter(ST_GeomFromText(wkt)) FROM t ORDER BY id;",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0].column(0).as_primitive::<Float64Type>();

        assert_eq!(col.len(), 5);
        assert_eq!(col.value(0), 14.0);
        assert_eq!(col.value(1), 0.0);
        assert_eq!(col.value(2), 0.0);
        assert!(col.is_null(3));
        assert_eq!(col.value(4), 4.0);
    }

    #[tokio::test]
    async fn test_polygon_hole_perimeter() {
        let ctx = SessionContext::new();

        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(2 2, 4 2, 4 4, 2 4, 2 2))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 48.0);
    }

    #[tokio::test]
    async fn test_multipolygon_perimeter() {
        let ctx = SessionContext::new();

        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Perimeter(ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)),((10 10, 12 10, 12 12, 10 12, 10 10)))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 12.0);
    }

    #[tokio::test]
    async fn test_nested_geometry_collection_perimeter() {
        let ctx = SessionContext::new();

        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Perimeter(ST_GeomFromText('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((0 0, 4 0, 4 3, 0 3, 0 0)),POINT EMPTY,LINESTRING EMPTY),POLYGON EMPTY,POINT(1 2))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 14.0);
    }

    #[tokio::test]
    async fn test_box2d_perimeter_matches_postgis() {
        let ctx = SessionContext::new();

        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(MakeBox2D::new().into());
        ctx.register_udf(Point::default().into());

        let df = ctx
            .sql(
                "SELECT \
                    ST_Perimeter(ST_MakeBox2D(ST_Point(0, 0), ST_Point(3, 2))), \
                    ST_Perimeter(ST_MakeBox2D(ST_Point(3, 2), ST_Point(0, 0))), \
                    ST_Perimeter(ST_MakeBox2D(ST_Point(-2, -1), ST_Point(3, 4))), \
                    ST_Perimeter(ST_MakeBox2D(ST_Point(0, 0), ST_Point(3, 0))), \
                    ST_Perimeter(ST_MakeBox2D(ST_Point(0, 0), ST_Point(0, 4))), \
                    ST_Perimeter(ST_MakeBox2D(ST_Point(1, 1), ST_Point(1, 1))), \
                    ST_Perimeter2D(ST_MakeBox2D(ST_Point(0, 0), ST_Point(3, 2)));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let values = (0..batch.num_columns())
            .map(|index| batch.column(index).as_primitive::<Float64Type>().value(0))
            .collect::<Vec<_>>();

        assert_eq!(values, vec![10.0, 10.0, 20.0, 0.0, 0.0, 0.0, 10.0]);
    }

    #[test]
    fn test_triangle_perimeter() {
        // GeoArrow 0.9 normalizes Triangle values to Polygon while building
        // arrays, so exercise the Triangle geometry branch directly.
        let triangle = geo::Triangle::new(
            geo::coord! { x: 0.0, y: 0.0 },
            geo::coord! { x: 3.0, y: 0.0 },
            geo::coord! { x: 0.0, y: 4.0 },
        );
        assert_eq!(geometry_perimeter(&triangle), 12.0);
    }

    #[tokio::test]
    async fn test_zero_perimeters_have_positive_zero_bits() {
        let ctx = SessionContext::new();

        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql(
                "SELECT \
                    ST_Perimeter(ST_GeomFromText('POLYGON EMPTY')), \
                    ST_Perimeter(ST_GeomFromText('MULTIPOLYGON EMPTY')), \
                    ST_Perimeter(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')), \
                    ST_Perimeter(ST_GeomFromText('POINT(1 2)'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();

        assert_eq!(batch.num_columns(), 4);
        for index in 0..batch.num_columns() {
            let value = batch.column(index).as_primitive::<Float64Type>().value(0);
            assert_eq!(
                value.to_bits(),
                0.0_f64.to_bits(),
                "column {index} did not return positive zero"
            );
        }
    }
}
