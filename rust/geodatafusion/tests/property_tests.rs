use std::env;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF};
use datafusion::prelude::SessionContext;
use geo::{LineString, Point, Polygon};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{PointBuilder, PolygonBuilder};
use geoarrow_schema::{CoordType, Dimension, PointType, PolygonType};
use proptest::prelude::*;
use proptest::test_runner::{FileFailurePersistence, RngSeed, TestRunner};

const DEFAULT_CASES: u32 = 32;
const COORD_BOUND: f64 = 10_000.0;
const EPSILON: f64 = 1e-12;

struct Udfs {
    distance: ScalarUDF,
    within: ScalarUDF,
    contains: ScalarUDF,
    #[cfg(feature = "geos-3_11")]
    area: ScalarUDF,
    #[cfg(feature = "geos-3_11")]
    intersection: ScalarUDF,
    #[cfg(feature = "geos-3_11")]
    difference: ScalarUDF,
}

impl Udfs {
    fn new() -> Self {
        let context = SessionContext::new();
        geodatafusion::register(&context);
        let state = context.state();
        let functions = state.scalar_functions();
        let function = |name: &str| {
            functions
                .get(name)
                .unwrap_or_else(|| panic!("missing UDF: {name}"))
                .as_ref()
                .clone()
        };

        Self {
            distance: function("st_distance"),
            within: function("st_within"),
            contains: function("st_contains"),
            #[cfg(feature = "geos-3_11")]
            area: function("st_area"),
            #[cfg(feature = "geos-3_11")]
            intersection: function("st_intersection"),
            #[cfg(feature = "geos-3_11")]
            difference: function("st_difference"),
        }
    }
}

#[test]
fn point_distance_obeys_independent_metric_identities() {
    // ST_DWithin has its own outward-rounded bounds precheck and is not part of
    // this equality property.
    let udfs = Udfs::new();
    run_property(
        0x6a11_0001,
        concat!(
            module_path!(),
            "::point_distance_obeys_independent_metric_identities"
        ),
        (point_strategy(), point_strategy(), point_strategy()),
        |(a, b, c)| {
            for layout in layouts() {
                let sentinel = [0.0, 0.0];
                let left = point_array(&[sentinel, a, b, a, b, sentinel], layout);
                let right = point_array(&[sentinel, b, a, c, c, sentinel], layout);
                let (left, left_field) = slice_geometry(&left, 1, 4);
                let (right, right_field) = slice_geometry(&right, 1, 4);
                let distances = float64_values(
                    invoke_binary(&udfs.distance, &left, &left_field, &right, &right_field, 4).0,
                    4,
                );

                let [ab, ba, ac, bc] = distances.as_slice() else {
                    unreachable!("distance result length was checked");
                };
                assert_close(*ab, hypot(a, b), "distance(a, b)");
                assert_close(*ba, hypot(a, b), "distance(b, a)");
                assert_close(*ac, hypot(a, c), "distance(a, c)");
                assert_close(*bc, hypot(b, c), "distance(b, c)");
                assert_close(*ab, *ba, "distance symmetry");

                assert_triangle(*ac, *ab, *bc);
                assert_triangle(*ab, *ac, *bc);
                assert_triangle(*bc, *ab, *ac);
            }
            Ok(())
        },
    );
}

#[test]
fn rectangle_within_and_contains_match_independent_bounds() {
    let udfs = Udfs::new();
    run_property(
        0x6a11_0002,
        concat!(
            module_path!(),
            "::rectangle_within_and_contains_match_independent_bounds"
        ),
        rectangle_pair(),
        |(a, b)| {
            for layout in layouts() {
                let sentinel = Rect::new(0.0, 0.0, 1.0, 1.0);
                let left = polygon_array(&[sentinel, a, b, sentinel], layout);
                let right = polygon_array(&[sentinel, b, a, sentinel], layout);
                let (left, left_field) = slice_geometry(&left, 1, 2);
                let (right, right_field) = slice_geometry(&right, 1, 2);

                let within = boolean_values(
                    invoke_binary(&udfs.within, &left, &left_field, &right, &right_field, 2).0,
                    2,
                );
                let contains = boolean_values(
                    invoke_binary(&udfs.contains, &left, &left_field, &right, &right_field, 2).0,
                    2,
                );

                let expected_ab = contains_rect(a, b);
                let expected_ba = contains_rect(b, a);
                assert_eq!(within, [expected_ba, expected_ab]);
                assert_eq!(contains, [expected_ab, expected_ba]);
                assert_eq!(within[0], contains[1]);
                assert_eq!(within[1], contains[0]);
            }
            Ok(())
        },
    );
}

#[cfg(feature = "geos-3_11")]
#[test]
fn rectangle_overlay_preserves_area_identity() {
    let udfs = Udfs::new();
    run_property(
        0x6a11_0003,
        concat!(
            module_path!(),
            "::rectangle_overlay_preserves_area_identity"
        ),
        rectangle_pair(),
        |(a, b)| {
            for layout in layouts() {
                let sentinel = Rect::new(0.0, 0.0, 1.0, 1.0);
                let left = polygon_array(&[sentinel, a, b, sentinel], layout);
                let right = polygon_array(&[sentinel, b, a, sentinel], layout);
                let (left, left_field) = slice_geometry(&left, 1, 2);
                let (right, right_field) = slice_geometry(&right, 1, 2);

                let (intersection, intersection_field) = invoke_binary(
                    &udfs.intersection,
                    &left,
                    &left_field,
                    &right,
                    &right_field,
                    2,
                );
                let (difference, difference_field) = invoke_binary(
                    &udfs.difference,
                    &left,
                    &left_field,
                    &right,
                    &right_field,
                    2,
                );
                let intersection_areas = float64_values(
                    invoke_unary(&udfs.area, intersection, &intersection_field, 2),
                    2,
                );
                let difference_areas = float64_values(
                    invoke_unary(&udfs.area, difference, &difference_field, 2),
                    2,
                );
                let input_areas = float64_values(
                    invoke_unary(
                        &udfs.area,
                        ColumnarValue::Array(Arc::clone(&left)),
                        &left_field,
                        2,
                    ),
                    2,
                );

                let expected_overlap = overlap_area(a, b);
                let expected_areas = [a.area(), b.area()];
                let expected_differences = [
                    expected_areas[0] - expected_overlap,
                    expected_areas[1] - expected_overlap,
                ];
                for row in 0..2 {
                    assert_close(input_areas[row], expected_areas[row], "input area");
                    assert_close(
                        intersection_areas[row],
                        expected_overlap,
                        "intersection area",
                    );
                    assert_close(
                        difference_areas[row],
                        expected_differences[row],
                        "difference area",
                    );
                    assert_close(
                        intersection_areas[row] + difference_areas[row],
                        input_areas[row],
                        "intersection plus difference area",
                    );
                }
                assert_close(
                    intersection_areas[0],
                    intersection_areas[1],
                    "intersection area symmetry",
                );
            }
            Ok(())
        },
    );
}

#[derive(Debug, Clone, Copy)]
struct Rect {
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
}

impl Rect {
    fn new(x0: f64, y0: f64, x1: f64, y1: f64) -> Self {
        assert!(x0 < x1 && y0 < y1, "rectangle must have positive area");
        Self { x0, y0, x1, y1 }
    }

    #[cfg(feature = "geos-3_11")]
    fn area(self) -> f64 {
        (self.x1 - self.x0) * (self.y1 - self.y0)
    }

    fn polygon(self) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![
                (self.x0, self.y0),
                (self.x1, self.y0),
                (self.x1, self.y1),
                (self.x0, self.y1),
                (self.x0, self.y0),
            ]),
            Vec::new(),
        )
    }
}

fn point_strategy() -> impl Strategy<Value = [f64; 2]> {
    (-COORD_BOUND..COORD_BOUND, -COORD_BOUND..COORD_BOUND).prop_map(|(x, y)| [x, y])
}

fn rectangle_pair() -> impl Strategy<Value = (Rect, Rect)> {
    (
        -10i16..=10,
        -10i16..=10,
        6i16..=10,
        6i16..=10,
        -2i16..=2,
        -2i16..=2,
        -2i16..=2,
        -2i16..=2,
    )
        .prop_map(|(x, y, width, height, left, right, bottom, top)| {
            let a = Rect::new(x as f64, y as f64, (x + width) as f64, (y + height) as f64);
            let b = Rect::new(
                a.x0 + left as f64,
                a.y0 + bottom as f64,
                a.x1 - right as f64,
                a.y1 - top as f64,
            );
            (a, b)
        })
}

fn point_array(points: &[[f64; 2]], layout: CoordType) -> geoarrow_array::array::PointArray {
    let points = points
        .iter()
        .map(|[x, y]| Point::new(*x, *y))
        .collect::<Vec<_>>();
    let point_type = PointType::new(Dimension::XY, Default::default()).with_coord_type(layout);
    PointBuilder::from_points(points.iter(), point_type).finish()
}

fn polygon_array(rectangles: &[Rect], layout: CoordType) -> geoarrow_array::array::PolygonArray {
    let polygons = rectangles
        .iter()
        .map(|rectangle| rectangle.polygon())
        .collect::<Vec<_>>();
    let polygon_type = PolygonType::new(Dimension::XY, Default::default()).with_coord_type(layout);
    PolygonBuilder::from_polygons(&polygons, polygon_type).finish()
}

fn slice_geometry<A: GeoArrowArray>(
    array: &A,
    offset: usize,
    length: usize,
) -> (ArrayRef, FieldRef) {
    let sliced = array.slice(offset, length);
    let field = Arc::new(sliced.data_type().to_field("geometry", false));
    (sliced.to_array_ref(), field)
}

fn invoke_binary(
    udf: &ScalarUDF,
    left: &ArrayRef,
    left_field: &FieldRef,
    right: &ArrayRef,
    right_field: &FieldRef,
    number_rows: usize,
) -> (ColumnarValue, FieldRef) {
    invoke_udf(
        udf,
        vec![
            ColumnarValue::Array(Arc::clone(left)),
            ColumnarValue::Array(Arc::clone(right)),
        ],
        vec![Arc::clone(left_field), Arc::clone(right_field)],
        number_rows,
    )
}

#[cfg(feature = "geos-3_11")]
fn invoke_unary(
    udf: &ScalarUDF,
    argument: ColumnarValue,
    argument_field: &FieldRef,
    number_rows: usize,
) -> ColumnarValue {
    invoke_udf(
        udf,
        vec![argument],
        vec![Arc::clone(argument_field)],
        number_rows,
    )
    .0
}

fn invoke_udf(
    udf: &ScalarUDF,
    args: Vec<ColumnarValue>,
    arg_fields: Vec<FieldRef>,
    number_rows: usize,
) -> (ColumnarValue, FieldRef) {
    let scalar_arguments = vec![None::<&datafusion::scalar::ScalarValue>; arg_fields.len()];
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })
        .unwrap_or_else(|error| panic!("{} return field failed: {error}", udf.name()));
    let result = udf
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: return_field.clone(),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap_or_else(|error| panic!("{} failed: {error}", udf.name()));
    (result, return_field)
}

fn columnar_array(value: ColumnarValue) -> ArrayRef {
    ColumnarValue::values_to_arrays(&[value])
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

fn float64_values(value: ColumnarValue, expected_len: usize) -> Vec<f64> {
    let array = columnar_array(value);
    let values = array.as_primitive::<Float64Type>();
    assert_eq!(values.len(), expected_len);
    assert_eq!(values.null_count(), 0);
    values.values().to_vec()
}

fn boolean_values(value: ColumnarValue, expected_len: usize) -> Vec<bool> {
    let array = columnar_array(value);
    let values = array.as_boolean();
    assert_eq!(values.len(), expected_len);
    assert_eq!(values.null_count(), 0);
    (0..expected_len).map(|index| values.value(index)).collect()
}

fn layouts() -> [CoordType; 2] {
    [CoordType::Separated, CoordType::Interleaved]
}

fn hypot(a: [f64; 2], b: [f64; 2]) -> f64 {
    (a[0] - b[0]).hypot(a[1] - b[1])
}

fn contains_rect(outer: Rect, inner: Rect) -> bool {
    inner.x0 >= outer.x0 && inner.x1 <= outer.x1 && inner.y0 >= outer.y0 && inner.y1 <= outer.y1
}

#[cfg(feature = "geos-3_11")]
fn overlap_area(a: Rect, b: Rect) -> f64 {
    let width = (a.x1.min(b.x1) - a.x0.max(b.x0)).max(0.0);
    let height = (a.y1.min(b.y1) - a.y0.max(b.y0)).max(0.0);
    width * height
}

fn assert_close(actual: f64, expected: f64, label: &str) {
    let scale = 1.0 + actual.abs().max(expected.abs());
    assert!(
        (actual - expected).abs() <= EPSILON * scale,
        "{label}: expected {expected}, got {actual}"
    );
}

fn assert_triangle(sum: f64, first: f64, second: f64) {
    let slack = EPSILON * (1.0 + first.abs().max(second.abs()));
    assert!(
        sum <= first + second + slack,
        "triangle inequality: {sum} > {first} + {second}"
    );
}

fn run_property<S, F>(seed: u64, test_name: &'static str, strategy: S, property: F)
where
    S: Strategy,
    S::Value: std::fmt::Debug,
    F: Fn(S::Value) -> Result<(), TestCaseError>,
{
    let config = proptest_config(seed, test_name);
    let cases = config.cases;
    let mut runner = TestRunner::new(config);
    runner.run(&strategy, property).unwrap();
    eprintln!("{test_name}: cases={cases}, layouts=2");
}

fn proptest_config(seed: u64, test_name: &'static str) -> ProptestConfig {
    let mut config = ProptestConfig::default();
    if let Ok(value) = env::var("GDF_PROP_CASES") {
        config.cases = value
            .parse()
            .unwrap_or_else(|error| panic!("GDF_PROP_CASES must be a u32: {error}"));
    } else if env::var_os("PROPTEST_CASES").is_none() {
        config.cases = DEFAULT_CASES;
    }
    if env::var_os("PROPTEST_RNG_SEED").is_none() {
        config.rng_seed = RngSeed::Fixed(seed);
    }
    config.failure_persistence = Some(Box::new(FileFailurePersistence::WithSource(
        "proptest-regressions",
    )));
    config.source_file = Some(file!());
    config.test_name = Some(test_name);
    config
}
