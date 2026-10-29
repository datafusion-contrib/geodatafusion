use std::env;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF};
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, PointTrait};
use geoarrow_array::array::{LineStringArray, PointArray};
use geoarrow_array::builder::LineStringBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Dimension, LineStringType};
use proptest::prelude::*;
use proptest::test_runner::{FileFailurePersistence, RngSeed, TestRunner};
use wkt::Wkt;
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString};

const DEFAULT_CASES: u32 = 32;
const EPSILON: f64 = 1e-9;

#[derive(Debug, Clone)]
struct LineInput {
    coords: Vec<[f64; 2]>,
}

#[derive(Debug, Clone)]
struct SliceCase {
    lines: Vec<LineInput>,
    offset: usize,
    length: usize,
}

struct Udfs {
    interpolate: ScalarUDF,
    locate: ScalarUDF,
    substring: ScalarUDF,
    as_text: ScalarUDF,
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
            interpolate: function("st_lineinterpolatepoint"),
            locate: function("st_linelocatepoint"),
            substring: function("st_linesubstring"),
            as_text: function("st_astext"),
        }
    }
}

#[test]
fn line_interpolation_endpoints_are_declared_vertices() {
    let udfs = Udfs::new();
    run_property(
        0x11a7_0001,
        concat!(
            module_path!(),
            "::line_interpolation_endpoints_are_declared_vertices"
        ),
        monotonic_line_strategy(),
        |line| {
            for layout in layouts() {
                let array = line_array(&[line.clone(), line.clone()], layout);
                let (geometry, geometry_field) = slice_geometry(&array, 0, 2);
                let fractions = float64_array(vec![0.0, 1.0]);
                let fraction_field = Arc::new(Field::new("fraction", DataType::Float64, false));
                let (points, point_field) = invoke_binary(
                    &udfs.interpolate,
                    &geometry,
                    &geometry_field,
                    &fractions,
                    &fraction_field,
                    2,
                );
                let actual = point_coords(points, &point_field, 2);
                let expected = [line.coords.first(), line.coords.last()];
                for (index, expected) in expected.into_iter().enumerate() {
                    let expected = expected.expect("generated lines are non-empty");
                    assert_close(actual[index][0], expected[0], "endpoint x");
                    assert_close(actual[index][1], expected[1], "endpoint y");
                }
            }
            Ok(())
        },
    );
}

#[test]
fn monotonic_line_locate_and_substring_identities() {
    let udfs = Udfs::new();
    run_property(
        0x11a7_0002,
        concat!(
            module_path!(),
            "::monotonic_line_locate_and_substring_identities"
        ),
        (monotonic_line_strategy(), ordered_fractions()),
        |(line, (a, b, c))| {
            for layout in layouts() {
                let array = line_array(std::slice::from_ref(&line), layout);
                let (geometry, geometry_field) = slice_geometry(&array, 0, 1);
                let fraction_field = Arc::new(Field::new("fraction", DataType::Float64, false));

                let point = |fraction: f64| {
                    let fractions = float64_array(vec![fraction]);
                    invoke_binary(
                        &udfs.interpolate,
                        &geometry,
                        &geometry_field,
                        &fractions,
                        &fraction_field,
                        1,
                    )
                };
                let substring = |start: f64, end: f64| {
                    let starts = float64_array(vec![start]);
                    let ends = float64_array(vec![end]);
                    invoke_udf(
                        &udfs.substring,
                        vec![
                            ColumnarValue::Array(Arc::clone(&geometry)),
                            ColumnarValue::Array(starts),
                            ColumnarValue::Array(ends),
                        ],
                        vec![
                            Arc::clone(&geometry_field),
                            Arc::clone(&fraction_field),
                            Arc::clone(&fraction_field),
                        ],
                        1,
                    )
                };
                let substring_wkt = |start: f64, end: f64| {
                    let (value, field) = substring(start, end);
                    let (text, _) = invoke_unary(&udfs.as_text, value, &field, 1);
                    string_values(text, 1).remove(0)
                };

                let (point_a, field_a) = point(a);
                let (point_b, field_b) = point(b);
                let (point_c, field_c) = point(c);

                let locate = |point: &ColumnarValue, field: &FieldRef| {
                    let (fraction, _) = invoke_binary(
                        &udfs.locate,
                        &geometry,
                        &geometry_field,
                        &columnar_array(point.clone()),
                        field,
                        1,
                    );
                    float64_values(fraction, 1)[0]
                };
                assert_close(locate(&point_a, &field_a), a, "locate(a)");
                assert_close(locate(&point_b, &field_b), b, "locate(b)");
                assert_close(locate(&point_c, &field_c), c, "locate(c)");

                let ab = substring_wkt(a, b);
                let bc = substring_wkt(b, c);
                let ac = substring_wkt(a, c);
                let point_a = point_coords(point_a, &field_a, 1)[0];
                let point_b = point_coords(point_b, &field_b, 1)[0];
                let point_c = point_coords(point_c, &field_c, 1)[0];

                let (ab_start, ab_end) = line_endpoints(&ab);
                let (bc_start, bc_end) = line_endpoints(&bc);
                let (ac_start, ac_end) = line_endpoints(&ac);
                assert_points_close(ab_start, point_a, "substring(a,b) start");
                assert_points_close(ab_end, point_b, "substring(a,b) end");
                assert_points_close(bc_start, point_b, "substring(b,c) start");
                assert_points_close(bc_end, point_c, "substring(b,c) end");
                assert_points_close(ac_start, point_a, "substring(a,c) start");
                assert_points_close(ac_end, point_c, "substring(a,c) end");

                let length_ab = wkt_length(&ab);
                let length_bc = wkt_length(&bc);
                let length_ac = wkt_length(&ac);
                let full_length = polyline_length(&line.coords);
                assert_close(length_ab + length_bc, length_ac, "substring partition");
                assert!(
                    length_ac <= full_length + EPSILON * (1.0 + full_length),
                    "substring(a,c) exceeds the source line length"
                );
            }
            Ok(())
        },
    );
}

#[test]
fn sliced_line_rows_match_direct_rows() {
    let udfs = Udfs::new();
    run_property(
        0x11a7_0003,
        concat!(module_path!(), "::sliced_line_rows_match_direct_rows"),
        slice_case_strategy(),
        |case| {
            for layout in layouts() {
                let sentinel = LineInput {
                    coords: vec![[-10.0, -10.0], [10.0, 10.0]],
                };
                let mut full = Vec::with_capacity(case.lines.len() + 2);
                full.push(sentinel.clone());
                full.extend(case.lines.iter().cloned());
                full.push(sentinel);

                let full_array = line_array(&full, layout);
                let (sliced, sliced_field) =
                    slice_geometry(&full_array, 1 + case.offset, case.length);
                let direct_array =
                    line_array(&case.lines[case.offset..case.offset + case.length], layout);
                let (direct, direct_field) = slice_geometry(&direct_array, 0, case.length);

                let fractions = float64_array(
                    (0..case.length)
                        .map(|index| 0.25 + index as f64 * 0.25)
                        .collect(),
                );
                let fraction_field = Arc::new(Field::new("fraction", DataType::Float64, false));
                let (sliced_points, sliced_point_field) = invoke_binary(
                    &udfs.interpolate,
                    &sliced,
                    &sliced_field,
                    &fractions,
                    &fraction_field,
                    case.length,
                );
                let (direct_points, direct_point_field) = invoke_binary(
                    &udfs.interpolate,
                    &direct,
                    &direct_field,
                    &fractions,
                    &fraction_field,
                    case.length,
                );
                assert_eq!(
                    point_coords(sliced_points, &sliced_point_field, case.length),
                    point_coords(direct_points, &direct_point_field, case.length),
                    "interpolation differs after slicing"
                );

                let starts = float64_array(vec![0.2; case.length]);
                let ends = float64_array(vec![0.8; case.length]);
                let (sliced_substring, sliced_substring_field) = invoke_udf(
                    &udfs.substring,
                    vec![
                        ColumnarValue::Array(Arc::clone(&sliced)),
                        ColumnarValue::Array(starts.clone()),
                        ColumnarValue::Array(ends.clone()),
                    ],
                    vec![
                        Arc::clone(&sliced_field),
                        Arc::clone(&fraction_field),
                        Arc::clone(&fraction_field),
                    ],
                    case.length,
                );
                let (direct_substring, direct_substring_field) = invoke_udf(
                    &udfs.substring,
                    vec![
                        ColumnarValue::Array(Arc::clone(&direct)),
                        ColumnarValue::Array(starts),
                        ColumnarValue::Array(ends),
                    ],
                    vec![
                        Arc::clone(&direct_field),
                        Arc::clone(&fraction_field),
                        Arc::clone(&fraction_field),
                    ],
                    case.length,
                );
                let (sliced_text, _) = invoke_unary(
                    &udfs.as_text,
                    sliced_substring,
                    &sliced_substring_field,
                    case.length,
                );
                let (direct_text, _) = invoke_unary(
                    &udfs.as_text,
                    direct_substring,
                    &direct_substring_field,
                    case.length,
                );
                assert_eq!(
                    string_values(sliced_text, case.length),
                    string_values(direct_text, case.length),
                    "substring differs after slicing"
                );
            }
            Ok(())
        },
    );
}

fn monotonic_line_strategy() -> impl Strategy<Value = LineInput> {
    (3usize..=8).prop_flat_map(|count| {
        prop::collection::vec((0.5f64..2.0, -10.0f64..10.0), count).prop_map(|segments| {
            let mut coords = Vec::with_capacity(segments.len());
            let mut x = 0.0;
            for (dx, y) in segments {
                x += dx;
                coords.push([x, y]);
            }
            LineInput { coords }
        })
    })
}

fn ordered_fractions() -> impl Strategy<Value = (f64, f64, f64)> {
    (0u16..=100).prop_flat_map(|a| {
        ((a + 1)..=101).prop_flat_map(move |b| {
            ((b + 1)..=102).prop_map(move |c| {
                (
                    f64::from(a) / 102.0,
                    f64::from(b) / 102.0,
                    f64::from(c) / 102.0,
                )
            })
        })
    })
}

fn slice_case_strategy() -> impl Strategy<Value = SliceCase> {
    prop::collection::vec(monotonic_line_strategy(), 2..=4).prop_flat_map(|lines| {
        let total = lines.len();
        (0usize..total, 1usize..=total).prop_map(move |(offset, raw_length)| {
            let remaining = total - offset;
            let length = ((raw_length - 1) % remaining) + 1;
            SliceCase {
                lines: lines.clone(),
                offset,
                length,
            }
        })
    })
}

fn line_array(lines: &[LineInput], layout: CoordType) -> LineStringArray {
    let typ = LineStringType::new(Dimension::XY, Default::default()).with_coord_type(layout);
    let mut builder = LineStringBuilder::new(typ);
    for line in lines {
        let wkt_line = WktLineString::new(
            line.coords
                .iter()
                .map(|[x, y]| WktCoord {
                    x: *x,
                    y: *y,
                    z: None,
                    m: None,
                })
                .collect(),
            WktDimension::XY,
        );
        builder.push_geometry(Some(&wkt_line)).unwrap();
    }
    builder.finish()
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

fn invoke_unary(
    udf: &ScalarUDF,
    argument: ColumnarValue,
    argument_field: &FieldRef,
    number_rows: usize,
) -> (ColumnarValue, FieldRef) {
    invoke_udf(
        udf,
        vec![argument],
        vec![Arc::clone(argument_field)],
        number_rows,
    )
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

fn float64_array(values: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

fn float64_values(value: ColumnarValue, expected_len: usize) -> Vec<f64> {
    let array = columnar_array(value);
    let values = array.as_primitive::<Float64Type>();
    assert_eq!(values.len(), expected_len);
    assert_eq!(values.null_count(), 0);
    values.values().to_vec()
}

fn point_coords(value: ColumnarValue, field: &FieldRef, expected_len: usize) -> Vec<[f64; 2]> {
    let array = columnar_array(value);
    let points = PointArray::try_from((array.as_ref(), field.as_ref())).unwrap();
    assert_eq!(points.len(), expected_len);
    (0..expected_len)
        .map(|index| {
            let point = points.value(index).expect("point should not be null");
            let coord = point.coord().expect("point should not be empty");
            [coord.x(), coord.y()]
        })
        .collect()
}

fn string_values(value: ColumnarValue, expected_len: usize) -> Vec<String> {
    let array = columnar_array(value);
    let values = match array.data_type() {
        DataType::Utf8 => array
            .as_string::<i32>()
            .iter()
            .map(|value| value.unwrap().to_string())
            .collect::<Vec<_>>(),
        DataType::LargeUtf8 => array
            .as_string::<i64>()
            .iter()
            .map(|value| value.unwrap().to_string())
            .collect::<Vec<_>>(),
        DataType::Utf8View => array
            .as_string_view()
            .iter()
            .map(|value| value.unwrap().to_string())
            .collect::<Vec<_>>(),
        other => panic!("unexpected ST_AsText output type: {other}"),
    };
    assert_eq!(values.len(), expected_len);
    values
}

fn layouts() -> [CoordType; 2] {
    [CoordType::Separated, CoordType::Interleaved]
}

fn polyline_length(coords: &[[f64; 2]]) -> f64 {
    coords
        .windows(2)
        .map(|pair| {
            let dx = pair[1][0] - pair[0][0];
            let dy = pair[1][1] - pair[0][1];
            dx.hypot(dy)
        })
        .sum()
}

fn parse_line(wkt: &str) -> WktLineString<f64> {
    match Wkt::from_str(wkt).unwrap() {
        Wkt::LineString(line) => line,
        other => panic!("expected LINESTRING, got {other:?}"),
    }
}

fn line_endpoints(wkt: &str) -> ([f64; 2], [f64; 2]) {
    let line = parse_line(wkt);
    let first = line.coords().first().expect("substring is non-empty");
    let last = line.coords().last().expect("substring is non-empty");
    ([first.x, first.y], [last.x, last.y])
}

fn wkt_length(wkt: &str) -> f64 {
    let line = parse_line(wkt);
    let coords = line
        .coords()
        .iter()
        .map(|coord| [coord.x, coord.y])
        .collect::<Vec<_>>();
    polyline_length(&coords)
}

fn assert_close(actual: f64, expected: f64, label: &str) {
    let scale = 1.0 + actual.abs().max(expected.abs());
    assert!(
        (actual - expected).abs() <= EPSILON * scale,
        "{label}: expected {expected}, got {actual}"
    );
}

fn assert_points_close(actual: [f64; 2], expected: [f64; 2], label: &str) {
    assert_close(actual[0], expected[0], &format!("{label} x"));
    assert_close(actual[1], expected[1], &format!("{label} y"));
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
