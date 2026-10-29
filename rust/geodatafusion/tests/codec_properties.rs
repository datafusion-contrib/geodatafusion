use std::env;
use std::f64::consts::PI;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, StringArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType as NativeGeometryType,
    LineStringTrait, MultiPolygonTrait, PointTrait, PolygonTrait, UnimplementedGeometry,
};
use geoarrow_array::array::{
    CoordBuffer, GeometryArray, InterleavedCoordBuffer, PointArray, SeparatedCoordBuffer,
};
use geoarrow_array::builder::{GeometryCollectionBuilder, LineStringBuilder, PolygonBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata,
    PolygonType, WkbType, WktType,
};
use proptest::prelude::*;
use proptest::test_runner::{FileFailurePersistence, RngSeed, TestRunner};
use wkt::wkt;

const DEFAULT_CASES: u32 = 32;
const COORD_BOUND: f64 = 10_000.0;
const EPSILON: f64 = 1e-9;

#[derive(Debug, Clone, Copy)]
enum IoFormat {
    Wkt,
    Wkb,
}

#[derive(Debug, Clone, Copy)]
enum StructureKind {
    LineString,
    Polygon,
    GeometryCollection,
}

#[derive(Debug, Clone)]
struct PointInput {
    dimension: Dimension,
    coord_type: CoordType,
    crs: Option<&'static str>,
    coords: [f64; 4],
}

#[derive(Debug, Clone, Copy)]
struct AffineParams {
    a: f64,
    b: f64,
    d: f64,
    e: f64,
    z_scale: f64,
    x_off: f64,
    y_off: f64,
    z_off: f64,
}

impl AffineParams {
    fn inverse(self) -> Self {
        let determinant = self.a * self.e - self.b * self.d;
        assert!(determinant.abs() > 0.05);
        Self {
            a: self.e / determinant,
            b: -self.b / determinant,
            d: -self.d / determinant,
            e: self.a / determinant,
            z_scale: 1.0 / self.z_scale,
            x_off: -(self.e * self.x_off - self.b * self.y_off) / determinant,
            y_off: (self.d * self.x_off - self.a * self.y_off) / determinant,
            z_off: -self.z_off / self.z_scale,
        }
    }

    fn coefficients(self) -> [f64; 12] {
        [
            self.a,
            self.b,
            0.0,
            self.d,
            self.e,
            0.0,
            0.0,
            0.0,
            self.z_scale,
            self.x_off,
            self.y_off,
            self.z_off,
        ]
    }
}

#[derive(Debug, Clone, Copy)]
struct NamedAffineParams {
    translate: [f64; 3],
    angle: f64,
    scale: [f64; 3],
}

#[derive(Debug, Clone, PartialEq)]
enum CanonicalGeometry {
    Point(Option<Vec<f64>>),
    LineString(Vec<Vec<f64>>),
    Polygon(Vec<Vec<Vec<f64>>>),
    MultiPolygon(Vec<Vec<Vec<Vec<f64>>>>),
    GeometryCollection(Vec<CanonicalGeometry>),
}

#[derive(Debug, Clone, PartialEq)]
enum CanonicalRow {
    Null,
    Value(CanonicalGeometry),
}

struct Udfs {
    affine: ScalarUDF,
    translate: ScalarUDF,
    rotate_z: ScalarUDF,
    scale: ScalarUDF,
    is_empty: ScalarUDF,
    as_text: ScalarUDF,
    from_text: ScalarUDF,
    multipolygon_from_text: ScalarUDF,
    as_binary: ScalarUDF,
    from_binary: ScalarUDF,
    multipolygon_from_binary: ScalarUDF,
    point_from_binary: ScalarUDF,
}

impl Udfs {
    fn new() -> Self {
        let ctx = SessionContext::new();
        geodatafusion::register(&ctx);
        let state = ctx.state();
        let function = |name: &str| {
            state
                .scalar_functions()
                .get(name)
                .unwrap_or_else(|| panic!("missing UDF: {name}"))
                .as_ref()
                .clone()
        };

        Self {
            affine: function("st_affine"),
            translate: function("st_translate"),
            rotate_z: function("st_rotatez"),
            scale: function("st_scale"),
            is_empty: function("st_isempty"),
            as_text: function("st_astext"),
            from_text: function("st_geomfromtext"),
            multipolygon_from_text: function("st_mpolyfromtext"),
            as_binary: function("st_asbinary"),
            from_binary: function("st_geomfromwkb"),
            multipolygon_from_binary: function("st_mpolyfromwkb"),
            point_from_binary: function("st_pointfromwkb"),
        }
    }
}

#[test]
fn affine_matrix_composition_round_trip() {
    let udfs = Udfs::new();
    let strategy = (affine_params(), affine_params(), point_input());
    let mut runner = TestRunner::new(proptest_config(
        0xaff1_0001,
        concat!(module_path!(), "::affine_matrix_composition_round_trip"),
    ));

    runner
        .run(&strategy, |(first, second, input)| {
            let metadata = metadata(input.crs);
            let source = point_fixture(&input, metadata.clone());
            let source_field = Arc::new(source.data_type().to_field("geometry", false));
            let original = point_input_coordinates(&input);
            assert_coords_near(&point_values(&source), &original);
            assert_array_contract(
                &source,
                source_field.as_ref(),
                input.dimension,
                input.coord_type,
                &metadata,
            );

            let expected_first = transform_coords(first, &original, input.dimension);
            let (first_output, first_field) =
                invoke_affine(&udfs.affine, &source, source_field, &first.coefficients());
            assert_array_contract(
                &first_output,
                first_field.as_ref(),
                input.dimension,
                input.coord_type,
                &metadata,
            );
            assert_coords_near(&point_values(&first_output), &expected_first);

            let expected_second = transform_coords(second, &expected_first, input.dimension);
            let (second_output, second_field) = invoke_affine(
                &udfs.affine,
                &first_output,
                first_field,
                &second.coefficients(),
            );
            assert_array_contract(
                &second_output,
                second_field.as_ref(),
                input.dimension,
                input.coord_type,
                &metadata,
            );
            assert_coords_near(&point_values(&second_output), &expected_second);

            let (inverse_second, inverse_second_field) = invoke_affine(
                &udfs.affine,
                &second_output,
                second_field,
                &second.inverse().coefficients(),
            );
            assert_array_contract(
                &inverse_second,
                inverse_second_field.as_ref(),
                input.dimension,
                input.coord_type,
                &metadata,
            );
            let (restored, restored_field) = invoke_affine(
                &udfs.affine,
                &inverse_second,
                inverse_second_field,
                &first.inverse().coefficients(),
            );
            assert_array_contract(
                &restored,
                restored_field.as_ref(),
                input.dimension,
                input.coord_type,
                &metadata,
            );
            assert_coords_near(&point_values(&restored), &original);
            Ok(())
        })
        .unwrap();
}

#[test]
fn affine_named_composition_round_trip() {
    let udfs = Udfs::new();
    let strategy = (named_affine_params(), point_input());
    let mut runner = TestRunner::new(proptest_config(
        0xaff1_0002,
        concat!(module_path!(), "::affine_named_composition_round_trip"),
    ));

    runner
        .run(&strategy, |(params, input)| {
            let metadata = metadata(input.crs);
            let source = point_fixture(&input, metadata.clone());
            let source_field = Arc::new(source.data_type().to_field("geometry", false));
            let original = point_input_coordinates(&input);
            assert_coords_near(&point_values(&source), &original);

            let (translated, translated_field) =
                invoke_affine(&udfs.translate, &source, source_field, &params.translate);
            let expected_translated = translate_named(&original, input.dimension, params.translate);
            assert_coords_near(&point_values(&translated), &expected_translated);

            let (rotated, rotated_field) = invoke_affine(
                &udfs.rotate_z,
                &translated,
                translated_field,
                &[params.angle],
            );
            let expected_rotated = rotate_z_named(&expected_translated, params.angle);
            assert_coords_near(&point_values(&rotated), &expected_rotated);

            let (scaled, scaled_field) =
                invoke_affine(&udfs.scale, &rotated, rotated_field, &params.scale);
            let expected_scaled = scale_named(&expected_rotated, input.dimension, params.scale);
            assert_coords_near(&point_values(&scaled), &expected_scaled);

            let (unscaled, unscaled_field) = invoke_affine(
                &udfs.scale,
                &scaled,
                scaled_field,
                &params.scale.map(|value| 1.0 / value),
            );
            let (unrotated, unrotated_field) =
                invoke_affine(&udfs.rotate_z, &unscaled, unscaled_field, &[-params.angle]);
            let (restored, restored_field) = invoke_affine(
                &udfs.translate,
                &unrotated,
                unrotated_field,
                &params.translate.map(|value| -value),
            );

            assert_array_contract(
                &restored,
                restored_field.as_ref(),
                input.dimension,
                input.coord_type,
                &metadata,
            );
            assert_coords_near(&point_values(&restored), &original);
            Ok(())
        })
        .unwrap();
}

#[test]
fn wkt_wkb_point_round_trip() {
    let udfs = Udfs::new();

    for dimension in dimensions() {
        for coord_type in coord_types() {
            for format in [IoFormat::Wkt, IoFormat::Wkb] {
                let input = PointInput {
                    dimension,
                    coord_type,
                    crs: Some("EPSG:4979"),
                    coords: [1.25, -2.5, 3.75, 4.5],
                };
                run_point_round_trip(&udfs, &input, format);
            }
        }
    }

    let strategy = (point_input(), io_format());
    let mut runner = TestRunner::new(proptest_config(
        0xaff1_0003,
        concat!(module_path!(), "::wkt_wkb_point_round_trip"),
    ));
    runner
        .run(&strategy, |(input, format)| {
            run_point_round_trip(&udfs, &input, format);
            Ok(())
        })
        .unwrap();
}

#[test]
fn wkt_wkb_structure_round_trip() {
    let udfs = Udfs::new();
    let base_metadata = metadata(Some("EPSG:4979"));

    for kind in [
        StructureKind::LineString,
        StructureKind::Polygon,
        StructureKind::GeometryCollection,
    ] {
        for dimension in dimensions() {
            for coord_type in coord_types() {
                for format in [IoFormat::Wkt, IoFormat::Wkb] {
                    run_structure_round_trip(
                        &udfs,
                        kind,
                        dimension,
                        coord_type,
                        format,
                        base_metadata.clone(),
                    );
                }
            }
        }
    }

    let strategy = (
        structure_kind(),
        dimension_strategy(),
        coord_type_strategy(),
        io_format(),
        crs_strategy(),
    );
    let mut runner = TestRunner::new(proptest_config(
        0xaff1_0004,
        concat!(module_path!(), "::wkt_wkb_structure_round_trip"),
    ));
    runner
        .run(&strategy, |(kind, dimension, coord_type, format, crs)| {
            run_structure_round_trip(&udfs, kind, dimension, coord_type, format, metadata(crs));
            Ok(())
        })
        .unwrap();
}

#[test]
fn malformed_unknown_wkb_types_are_controlled_errors() {
    // The fixed PostGIS 18/3.6 oracle reports XX000 "Unknown WKB type (...)" for these payloads.
    // DataFusion errors remain its own typed errors, but the malformed value must not become NULL.
    const MALFORMED_WKBS: &[&[u8]] = &[
        &[
            0x01, 0xbf, 0x0b, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xb9, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x40, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0xe5, 0xe5, 0xe5, 0xe5,
            0xe5, 0xe5, 0xe5, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40,
        ],
        &[
            0x01, 0x46, 0xf6, 0x00, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0xe5, 0xe5, 0xe5, 0x80, 0x08, 0x40, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x10, 0x40,
        ],
    ];

    for malformed in MALFORMED_WKBS {
        let udfs = Udfs::new();
        let (argument, field) = binary_argument(malformed);
        let error =
            invoke_udf_result(&udfs.from_binary, vec![argument], vec![field], 1).unwrap_err();
        assert_controlled_wkb_error(error, "generic constructor");

        let (argument, field) = binary_argument(malformed);
        let error =
            invoke_udf_result(&udfs.point_from_binary, vec![argument], vec![field], 1).unwrap_err();
        assert_controlled_wkb_error(error, "typed constructor");

        let (argument, argument_field) = binary_argument(malformed);
        let numeric_fields = (0..2)
            .map(|_| Arc::new(Field::new("argument", DataType::Float64, false)) as FieldRef)
            .collect::<Vec<_>>();
        let mut args = vec![argument];
        args.extend(
            [1.0, 2.0].map(|value| ColumnarValue::Scalar(ScalarValue::Float64(Some(value)))),
        );
        let mut arg_fields = vec![argument_field];
        arg_fields.extend(numeric_fields);
        let error = invoke_udf_result(&udfs.translate, args, arg_fields, 1).unwrap_err();
        assert_controlled_wkb_error(error, "affine");
    }
}

fn assert_controlled_wkb_error(error: datafusion::common::DataFusionError, stage: &str) {
    let message = error.to_string();
    assert!(
        message.contains("dimension")
            || message.contains("empty polygon")
            || message.contains("unknown WKB type code"),
        "{stage} unexpectedly panicked or accepted malformed WKB: {message}"
    );
}

#[test]
fn multipolygon_empty_members_round_trip_all_dimensions() {
    let udfs = Udfs::new();

    for dimension in dimensions() {
        let rows = vec![
            None,
            Some(empty_member_multipolygon_wkt(dimension).to_string()),
        ];
        let expected = vec![
            CanonicalRow::Null,
            CanonicalRow::Value(expected_empty_member_multipolygon(dimension)),
        ];
        let expected_translated = vec![
            CanonicalRow::Null,
            CanonicalRow::Value(expected_translated_empty_member_multipolygon(
                dimension,
                [1.0, 2.0, 0.0],
            )),
        ];

        let (source, _) = parse_text_rows(&udfs.from_text, rows.clone());
        assert_eq!(canonical_rows(&source), expected);
        assert_eq!(is_empty_values(&udfs, &source), vec![None, Some(false)]);

        for format in [IoFormat::Wkt, IoFormat::Wkb] {
            let (round_tripped, round_tripped_field) = round_trip(&udfs, format, &source);
            assert_eq!(
                canonical_rows(&round_tripped),
                expected,
                "generic {format:?} round trip lost an EMPTY MultiPolygon member in {dimension:?}"
            );
            let (translated, _) = invoke_affine(
                &udfs.translate,
                &round_tripped,
                round_tripped_field,
                &[1.0, 2.0],
            );
            assert_eq!(
                canonical_rows(&translated),
                expected_translated,
                "affine changed a legal EMPTY MultiPolygon member from generic {format:?} in {dimension:?}"
            );
            assert_eq!(
                is_empty_values(&udfs, &translated),
                vec![None, Some(false)],
                "ST_IsEmpty changed after affine on {format:?} in {dimension:?}"
            );
        }

        let (typed_text, _) = parse_text_rows(&udfs.multipolygon_from_text, rows);
        assert_eq!(
            canonical_rows(&typed_text),
            expected,
            "typed WKT lost an EMPTY MultiPolygon member in {dimension:?}"
        );

        let source_field = Arc::new(source.data_type().to_field("geometry", false));
        let (serialized, serialized_field) = invoke_single_argument(
            &udfs.as_binary,
            ColumnarValue::Array(source.to_array_ref()),
            source_field,
            source.len(),
        );
        let (typed_binary, typed_binary_field) = invoke_single_argument(
            &udfs.multipolygon_from_binary,
            serialized,
            serialized_field,
            source.len(),
        );
        let typed_binary = geometry_array_from_columnar(typed_binary, &typed_binary_field);
        assert_eq!(
            canonical_rows(&typed_binary),
            expected,
            "typed WKB lost an EMPTY MultiPolygon member in {dimension:?}"
        );

        let text_values = as_text_values(&udfs, &source);
        assert!(
            text_values[1]
                .as_deref()
                .is_some_and(|value| value.contains("MULTIPOLYGON") && value.contains("EMPTY")),
            "serialized WKT did not preserve the EMPTY member: {text_values:?}"
        );
    }
}

#[test]
fn geometry_collection_empty_multipolygon_members_round_trip_all_dimensions() {
    let udfs = Udfs::new();

    for dimension in dimensions() {
        let rows = vec![
            None,
            Some(empty_multipolygon_geometry_collection_wkt(dimension).to_string()),
        ];
        let expected = vec![
            CanonicalRow::Null,
            CanonicalRow::Value(CanonicalGeometry::GeometryCollection(vec![
                expected_empty_member_multipolygon(dimension),
            ])),
        ];
        let expected_translated = vec![
            CanonicalRow::Null,
            CanonicalRow::Value(CanonicalGeometry::GeometryCollection(vec![
                expected_translated_empty_member_multipolygon(dimension, [1.0, 2.0, 0.0]),
            ])),
        ];

        let (source, _) = parse_text_rows(&udfs.from_text, rows);
        assert_eq!(canonical_rows(&source), expected);
        assert_eq!(is_empty_values(&udfs, &source), vec![None, Some(false)]);

        for format in [IoFormat::Wkt, IoFormat::Wkb] {
            let (round_tripped, round_tripped_field) = round_trip(&udfs, format, &source);
            assert_eq!(
                canonical_rows(&round_tripped),
                expected,
                "generic {format:?} round trip changed a nested MultiPolygon EMPTY member in {dimension:?}"
            );
            let (translated, _) = invoke_affine(
                &udfs.translate,
                &round_tripped,
                round_tripped_field,
                &[1.0, 2.0],
            );
            assert_eq!(
                canonical_rows(&translated),
                expected_translated,
                "affine changed a nested EMPTY MultiPolygon member from generic {format:?} in {dimension:?}"
            );
        }
    }
}

#[test]
fn postgis_multipolygon_empty_member_wkb_is_preserved() {
    // ST_AsBinary(ST_GeomFromText('MULTIPOLYGON(EMPTY,((0 0,1 0,1 1,0 1,0 0)))'))
    // from PostgreSQL 18.6 / PostGIS 3.6.4.
    let wkb = hex::decode(
        "010600000002000000010300000000000000010300000001000000050000000000000000000000\
         0000000000000000000000000000f03f0000000000000000000000000000f03f000000000000\
         f03f0000000000000000000000000000f03f00000000000000000000000000000000",
    )
    .unwrap();
    let expected = vec![CanonicalRow::Value(expected_empty_member_multipolygon(
        Dimension::XY,
    ))];
    let udfs = Udfs::new();

    let (argument, field) = binary_argument(&wkb);
    let (value, output_field) =
        invoke_udf_result(&udfs.from_binary, vec![argument], vec![field], 1).unwrap();
    let generic = geometry_array_from_columnar(value, &output_field);
    assert_eq!(canonical_rows(&generic), expected);
    assert_eq!(is_empty_values(&udfs, &generic), vec![Some(false)]);

    let (argument, field) = binary_argument(&wkb);
    let (value, output_field) = invoke_udf_result(
        &udfs.multipolygon_from_binary,
        vec![argument],
        vec![field],
        1,
    )
    .unwrap();
    let typed = geometry_array_from_columnar(value, &output_field);
    assert_eq!(canonical_rows(&typed), expected);
}

#[test]
fn multipoint_empty_members_preserve_dimension_through_wkb() {
    let udfs = Udfs::new();

    let cases = [
        (
            "MULTIPOINT(EMPTY,(1 2))",
            "0104000000020000000101000000000000000000F87F000000000000F87F0101000000000000000000F03F0000000000000040",
        ),
        (
            "MULTIPOINT Z (EMPTY,(1 2 3))",
            "01EC0300000200000001E9030000000000000000F87F000000000000F87F000000000000F87F01E9030000000000000000F03F00000000000000400000000000000840",
        ),
        (
            "MULTIPOINT M (EMPTY,(1 2 3))",
            "01D40700000200000001D1070000000000000000F87F000000000000F87F000000000000F87F01D1070000000000000000F03F00000000000000400000000000000840",
        ),
        (
            "MULTIPOINT ZM (EMPTY,(1 2 3 4))",
            "01BC0B00000200000001B90B0000000000000000F87F000000000000F87F000000000000F87F000000000000F87F01B90B0000000000000000F03F000000000000004000000000000008400000000000001040",
        ),
        (
            "GEOMETRYCOLLECTION Z (MULTIPOINT Z (EMPTY,(1 2 5)))",
            "01EF0300000100000001EC0300000200000001E9030000000000000000F87F000000000000F87F000000000000F87F01E9030000000000000000F03F00000000000000400000000000001440",
        ),
    ];

    for (wkt, expected_wkb) in cases {
        let (source, _) = parse_text_rows(&udfs.from_text, vec![None, Some(wkt.to_string())]);
        assert_eq!(
            is_empty_values(&udfs, &source),
            vec![None, Some(false)],
            "{wkt}"
        );

        let encoded = as_binary_values(&udfs, &source);
        assert_eq!(
            hex::encode_upper(encoded[1].as_ref().unwrap()),
            expected_wkb,
            "{wkt}"
        );

        let (argument, field) = binary_argument(encoded[1].as_ref().unwrap());
        let (decoded, decoded_field) =
            invoke_udf_result(&udfs.from_binary, vec![argument], vec![field], 1).unwrap();
        let decoded = geometry_array_from_columnar(decoded, &decoded_field);
        let reencoded = as_binary_values(&udfs, &decoded);
        assert_eq!(
            hex::encode_upper(reencoded[0].as_ref().unwrap()),
            expected_wkb,
            "{wkt}"
        );
    }
}

fn empty_member_multipolygon_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "MULTIPOLYGON(EMPTY,((0 0,1 0,1 1,0 1,0 0)))",
        Dimension::XYZ => "MULTIPOLYGON Z (EMPTY, ((0 0 10,1 0 20,1 1 30,0 1 40,0 0 10)))",
        Dimension::XYM => "MULTIPOLYGON M (EMPTY, ((0 0 100,1 0 200,1 1 300,0 1 400,0 0 100)))",
        Dimension::XYZM => {
            "MULTIPOLYGON ZM (EMPTY, ((0 0 10 100,1 0 20 200,1 1 30 300,0 1 40 400,0 0 10 100)))"
        }
    }
}

fn empty_multipolygon_geometry_collection_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "GEOMETRYCOLLECTION(MULTIPOLYGON(EMPTY,((0 0,1 0,1 1,0 1,0 0))))",
        Dimension::XYZ => {
            "GEOMETRYCOLLECTION Z (MULTIPOLYGON Z (EMPTY, ((0 0 10,1 0 20,1 1 30,0 1 40,0 0 10))))"
        }
        Dimension::XYM => {
            "GEOMETRYCOLLECTION M (MULTIPOLYGON M (EMPTY, ((0 0 100,1 0 200,1 1 300,0 1 400,0 0 100))))"
        }
        Dimension::XYZM => {
            "GEOMETRYCOLLECTION ZM (MULTIPOLYGON ZM (EMPTY, ((0 0 10 100,1 0 20 200,1 1 30 300,0 1 40 400,0 0 10 100))))"
        }
    }
}

fn expected_empty_member_multipolygon(dimension: Dimension) -> CanonicalGeometry {
    expected_translated_empty_member_multipolygon(dimension, [0.0, 0.0, 0.0])
}

fn expected_translated_empty_member_multipolygon(
    dimension: Dimension,
    offsets: [f64; 3],
) -> CanonicalGeometry {
    CanonicalGeometry::MultiPolygon(vec![
        Vec::new(),
        vec![vec![
            translate_named(
                &coordinate_values(dimension, [0.0, 0.0], 10.0, 100.0),
                dimension,
                offsets,
            ),
            translate_named(
                &coordinate_values(dimension, [1.0, 0.0], 20.0, 200.0),
                dimension,
                offsets,
            ),
            translate_named(
                &coordinate_values(dimension, [1.0, 1.0], 30.0, 300.0),
                dimension,
                offsets,
            ),
            translate_named(
                &coordinate_values(dimension, [0.0, 1.0], 40.0, 400.0),
                dimension,
                offsets,
            ),
            translate_named(
                &coordinate_values(dimension, [0.0, 0.0], 10.0, 100.0),
                dimension,
                offsets,
            ),
        ]],
    ])
}

fn parse_text_rows(udf: &ScalarUDF, rows: Vec<Option<String>>) -> (GeometryArray, FieldRef) {
    let number_rows = rows.len();
    let array = StringArray::from(rows);
    let field: FieldRef = Arc::new(
        Field::new("input", DataType::Utf8, true)
            .with_extension_type(WktType::new(Arc::new(Metadata::default()))),
    );
    let (value, output_field) = invoke_single_argument(
        udf,
        ColumnarValue::Array(Arc::new(array)),
        field,
        number_rows,
    );
    (
        geometry_array_from_columnar(value, &output_field),
        output_field,
    )
}

fn geometry_array_from_columnar(value: ColumnarValue, field: &FieldRef) -> GeometryArray {
    let array = columnar_array(value);
    GeometryArray::try_from((array.as_ref(), field.as_ref())).unwrap()
}

fn as_text_values(udfs: &Udfs, source: &GeometryArray) -> Vec<Option<String>> {
    let source_field = Arc::new(source.data_type().to_field("geometry", false));
    let (value, _) = invoke_single_argument(
        &udfs.as_text,
        ColumnarValue::Array(source.to_array_ref()),
        source_field,
        source.len(),
    );
    let array = columnar_array(value);
    let values = array.as_any().downcast_ref::<StringArray>().unwrap();
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index).to_string()))
        .collect()
}

fn as_binary_values(udfs: &Udfs, source: &GeometryArray) -> Vec<Option<Vec<u8>>> {
    let source_field = Arc::new(source.data_type().to_field("geometry", false));
    let (value, _) = invoke_single_argument(
        &udfs.as_binary,
        ColumnarValue::Array(source.to_array_ref()),
        source_field,
        source.len(),
    );
    let array = columnar_array(value);
    let values = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index).to_vec()))
        .collect()
}

fn is_empty_values(udfs: &Udfs, source: &GeometryArray) -> Vec<Option<bool>> {
    let source_field = Arc::new(source.data_type().to_field("geometry", false));
    let (value, _) = invoke_single_argument(
        &udfs.is_empty,
        ColumnarValue::Array(source.to_array_ref()),
        source_field,
        source.len(),
    );
    let array = columnar_array(value);
    let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index)))
        .collect()
}

fn run_point_round_trip(udfs: &Udfs, input: &PointInput, format: IoFormat) {
    let metadata = metadata(input.crs);
    let source = point_fixture(input, metadata.clone());
    let expected = vec![
        CanonicalRow::Value(CanonicalGeometry::Point(Some(point_input_coordinates(
            input,
        )))),
        CanonicalRow::Null,
        CanonicalRow::Value(CanonicalGeometry::Point(None)),
    ];

    assert_eq!(canonical_rows(&source), expected);
    assert_array_contract(
        &source,
        source.data_type().to_field("geometry", false).as_ref(),
        input.dimension,
        input.coord_type,
        &metadata,
    );

    let (decoded, decoded_field) = round_trip(udfs, format, &source);
    assert_eq!(canonical_rows(&decoded), expected);
    assert_array_contract(
        &decoded,
        decoded_field.as_ref(),
        input.dimension,
        CoordType::Separated,
        &metadata,
    );
    assert_separated_geometry_output(decoded_field.as_ref(), &metadata);
}

fn run_structure_round_trip(
    udfs: &Udfs,
    kind: StructureKind,
    dimension: Dimension,
    coord_type: CoordType,
    format: IoFormat,
    metadata: Arc<Metadata>,
) {
    let source = structure_fixture(kind, dimension, coord_type, metadata.clone());
    let expected = expected_structure_rows(kind, dimension);
    assert_eq!(canonical_rows(&source), expected);
    assert_array_contract(
        &source,
        source.data_type().to_field("geometry", false).as_ref(),
        dimension,
        coord_type,
        &metadata,
    );

    let (decoded, decoded_field) = round_trip(udfs, format, &source);
    assert_eq!(
        canonical_rows(&decoded),
        expected,
        "{kind:?} {dimension:?} {coord_type:?} {format:?}"
    );
    assert_array_contract(
        &decoded,
        decoded_field.as_ref(),
        dimension,
        CoordType::Separated,
        &metadata,
    );
    assert_separated_geometry_output(decoded_field.as_ref(), &metadata);
}

fn round_trip(udfs: &Udfs, format: IoFormat, source: &GeometryArray) -> (GeometryArray, FieldRef) {
    let source_field = Arc::new(source.data_type().to_field("geometry", false));
    let (serialized, serialized_field) = match format {
        IoFormat::Wkt => invoke_single_argument(
            &udfs.as_text,
            ColumnarValue::Array(source.to_array_ref()),
            source_field,
            source.len(),
        ),
        IoFormat::Wkb => invoke_single_argument(
            &udfs.as_binary,
            ColumnarValue::Array(source.to_array_ref()),
            source_field,
            source.len(),
        ),
    };
    let (parsed, parsed_field) = match format {
        IoFormat::Wkt => {
            invoke_single_argument(&udfs.from_text, serialized, serialized_field, source.len())
        }
        IoFormat::Wkb => invoke_single_argument(
            &udfs.from_binary,
            serialized,
            serialized_field,
            source.len(),
        ),
    };
    let parsed_array = columnar_array(parsed);
    let parsed_geometry = GeometryArray::try_from((parsed_array.as_ref(), parsed_field.as_ref()))
        .unwrap_or_else(|error| {
            panic!("failed to parse {format:?} output back into GeometryArray: {error}")
        });
    (parsed_geometry, parsed_field)
}

fn invoke_affine(
    udf: &ScalarUDF,
    source: &GeometryArray,
    source_field: FieldRef,
    numeric_arguments: &[f64],
) -> (GeometryArray, FieldRef) {
    let mut args = Vec::with_capacity(numeric_arguments.len() + 1);
    args.push(ColumnarValue::Array(source.to_array_ref()));
    args.extend(
        numeric_arguments
            .iter()
            .map(|value| ColumnarValue::Scalar(ScalarValue::Float64(Some(*value)))),
    );

    let mut arg_fields = Vec::with_capacity(numeric_arguments.len() + 1);
    arg_fields.push(source_field);
    arg_fields.extend(
        numeric_arguments
            .iter()
            .map(|_| Arc::new(Field::new("argument", DataType::Float64, false)) as FieldRef),
    );

    let (result, return_field) = invoke_udf(udf, args, arg_fields, source.len());
    let output = columnar_array(result);
    let geometry = GeometryArray::try_from((output.as_ref(), return_field.as_ref())).unwrap();
    (geometry, return_field)
}

fn invoke_single_argument(
    udf: &ScalarUDF,
    argument: ColumnarValue,
    argument_field: FieldRef,
    number_rows: usize,
) -> (ColumnarValue, FieldRef) {
    invoke_udf(udf, vec![argument], vec![argument_field], number_rows)
}

fn binary_argument(data: &[u8]) -> (ColumnarValue, FieldRef) {
    let array = BinaryArray::from(vec![Some(data)]);
    let field: FieldRef = Arc::new(
        Field::new("input", DataType::Binary, true)
            .with_extension_type(WkbType::new(Arc::new(Metadata::default()))),
    );
    (ColumnarValue::Array(Arc::new(array)), field)
}

fn invoke_udf(
    udf: &ScalarUDF,
    args: Vec<ColumnarValue>,
    arg_fields: Vec<FieldRef>,
    number_rows: usize,
) -> (ColumnarValue, FieldRef) {
    invoke_udf_result(udf, args, arg_fields, number_rows)
        .unwrap_or_else(|error| panic!("{} failed: {error}", udf.name()))
}

fn invoke_udf_result(
    udf: &ScalarUDF,
    args: Vec<ColumnarValue>,
    arg_fields: Vec<FieldRef>,
    number_rows: usize,
) -> datafusion::error::Result<(ColumnarValue, FieldRef)> {
    let scalar_arguments = vec![None::<&ScalarValue>; arg_fields.len()];
    let return_field = udf.return_field_from_args(ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &scalar_arguments,
    })?;
    let result = udf.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field: return_field.clone(),
        config_options: Arc::new(ConfigOptions::default()),
    })?;
    Ok((result, return_field))
}

fn columnar_array(value: ColumnarValue) -> ArrayRef {
    ColumnarValue::values_to_arrays(&[value])
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

fn point_fixture(input: &PointInput, metadata: Arc<Metadata>) -> GeometryArray {
    point_array(
        &[
            (point_input_storage(input), true),
            ([f64::NAN; 4], false),
            ([f64::NAN; 4], true),
        ],
        input.dimension,
        input.coord_type,
        metadata,
    )
}

fn point_array(
    rows: &[([f64; 4], bool)],
    dimension: Dimension,
    coord_type: CoordType,
    metadata: Arc<Metadata>,
) -> GeometryArray {
    let coord_buffer = match coord_type {
        CoordType::Separated => {
            let buffers: [Vec<f64>; 4] = std::array::from_fn(|dimension_index| {
                rows.iter()
                    .map(|(values, valid)| {
                        if *valid {
                            values[dimension_index]
                        } else {
                            f64::NAN
                        }
                    })
                    .collect()
            });
            let buffers: [ScalarBuffer<f64>; 4] = buffers.map(ScalarBuffer::from);
            CoordBuffer::Separated(SeparatedCoordBuffer::from_array(buffers, dimension).unwrap())
        }
        CoordType::Interleaved => {
            let values = rows
                .iter()
                .flat_map(|(values, valid)| {
                    (0..dimension.size()).map(move |dimension_index| {
                        if *valid {
                            values[dimension_index]
                        } else {
                            f64::NAN
                        }
                    })
                })
                .collect::<Vec<_>>();
            CoordBuffer::Interleaved(InterleavedCoordBuffer::new(
                ScalarBuffer::from(values),
                dimension,
            ))
        }
    };
    let validity = NullBuffer::from(rows.iter().map(|(_, valid)| *valid).collect::<Vec<_>>());
    GeometryArray::from(PointArray::new(coord_buffer, Some(validity), metadata))
}

fn structure_fixture(
    kind: StructureKind,
    dimension: Dimension,
    coord_type: CoordType,
    metadata: Arc<Metadata>,
) -> GeometryArray {
    match kind {
        StructureKind::LineString => {
            let mut builder = LineStringBuilder::new(
                LineStringType::new(dimension, Default::default()).with_coord_type(coord_type),
            );
            match dimension {
                Dimension::XY => {
                    builder
                        .push_geometry(Some(&wkt!(LINESTRING(1.0 2.0, 3.0 4.0))))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(LINESTRING EMPTY)))
                        .unwrap();
                }
                Dimension::XYZ => {
                    builder
                        .push_geometry(Some(&wkt!(LINESTRING Z (1.0 2.0 10.0, 3.0 4.0 30.0))))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(LINESTRING Z EMPTY)))
                        .unwrap();
                }
                Dimension::XYM => {
                    builder
                        .push_geometry(Some(&wkt!(LINESTRING M (1.0 2.0 100.0, 3.0 4.0 300.0))))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(LINESTRING M EMPTY)))
                        .unwrap();
                }
                Dimension::XYZM => {
                    builder
                        .push_geometry(Some(&wkt!(
                            LINESTRING ZM (1.0 2.0 10.0 100.0, 3.0 4.0 30.0 300.0)
                        )))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(LINESTRING ZM EMPTY)))
                        .unwrap();
                }
            }
            GeometryArray::from(builder.finish().with_metadata(metadata))
        }
        StructureKind::Polygon => {
            let mut builder = PolygonBuilder::new(
                PolygonType::new(dimension, Default::default()).with_coord_type(coord_type),
            );
            match dimension {
                Dimension::XY => {
                    builder
                        .push_geometry(Some(&wkt!(
                            POLYGON((1.0 2.0, 3.0 4.0, 5.0 2.0, 1.0 2.0))
                        )))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder.push_geometry(Some(&wkt!(POLYGON EMPTY))).unwrap();
                }
                Dimension::XYZ => {
                    builder
                        .push_geometry(Some(&wkt!(
                            POLYGON Z (
                                (1.0 2.0 10.0, 3.0 4.0 30.0, 5.0 2.0 50.0, 1.0 2.0 10.0)
                            )
                        )))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder.push_geometry(Some(&wkt!(POLYGON Z EMPTY))).unwrap();
                }
                Dimension::XYM => {
                    builder
                        .push_geometry(Some(&wkt!(
                            POLYGON M (
                                (1.0 2.0 100.0, 3.0 4.0 300.0, 5.0 2.0 500.0, 1.0 2.0 100.0)
                            )
                        )))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder.push_geometry(Some(&wkt!(POLYGON M EMPTY))).unwrap();
                }
                Dimension::XYZM => {
                    builder
                        .push_geometry(Some(&wkt!(
                            POLYGON ZM (
                                (
                                    1.0 2.0 10.0 100.0,
                                    3.0 4.0 30.0 300.0,
                                    5.0 2.0 50.0 500.0,
                                    1.0 2.0 10.0 100.0
                                )
                            )
                        )))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(POLYGON ZM EMPTY)))
                        .unwrap();
                }
            }
            GeometryArray::from(builder.finish().with_metadata(metadata))
        }
        StructureKind::GeometryCollection => {
            let mut builder = GeometryCollectionBuilder::new(
                GeometryCollectionType::new(dimension, Default::default())
                    .with_coord_type(coord_type),
            );
            match dimension {
                Dimension::XY => {
                    builder
                        .push_geometry(Some(&wkt!(GEOMETRYCOLLECTION(POINT(5.0 6.0)))))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(GEOMETRYCOLLECTION EMPTY)))
                        .unwrap();
                }
                Dimension::XYZ => {
                    builder
                        .push_geometry(Some(&wkt!(
                            GEOMETRYCOLLECTION Z (POINT Z (5.0 6.0 7.0))
                        )))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(GEOMETRYCOLLECTION Z EMPTY)))
                        .unwrap();
                }
                Dimension::XYM => {
                    builder
                        .push_geometry(Some(&wkt!(
                            GEOMETRYCOLLECTION M (POINT M (5.0 6.0 70.0))
                        )))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(GEOMETRYCOLLECTION M EMPTY)))
                        .unwrap();
                }
                Dimension::XYZM => {
                    builder
                        .push_geometry(Some(&wkt!(
                            GEOMETRYCOLLECTION ZM (POINT ZM (5.0 6.0 7.0 70.0))
                        )))
                        .unwrap();
                    builder
                        .push_geometry(None::<&UnimplementedGeometry<f64>>)
                        .unwrap();
                    builder
                        .push_geometry(Some(&wkt!(GEOMETRYCOLLECTION ZM EMPTY)))
                        .unwrap();
                }
            }
            GeometryArray::from(builder.finish().with_metadata(metadata))
        }
    }
}

fn expected_structure_rows(kind: StructureKind, dimension: Dimension) -> Vec<CanonicalRow> {
    vec![
        CanonicalRow::Value(expected_structure(kind, dimension)),
        CanonicalRow::Null,
        CanonicalRow::Value(match kind {
            StructureKind::LineString => CanonicalGeometry::LineString(Vec::new()),
            StructureKind::Polygon => CanonicalGeometry::Polygon(Vec::new()),
            StructureKind::GeometryCollection => CanonicalGeometry::GeometryCollection(Vec::new()),
        }),
    ]
}

fn expected_structure(kind: StructureKind, dimension: Dimension) -> CanonicalGeometry {
    match kind {
        StructureKind::LineString => CanonicalGeometry::LineString(vec![
            coordinate_values(dimension, [1.0, 2.0], 10.0, 100.0),
            coordinate_values(dimension, [3.0, 4.0], 30.0, 300.0),
        ]),
        StructureKind::Polygon => CanonicalGeometry::Polygon(vec![vec![
            coordinate_values(dimension, [1.0, 2.0], 10.0, 100.0),
            coordinate_values(dimension, [3.0, 4.0], 30.0, 300.0),
            coordinate_values(dimension, [5.0, 2.0], 50.0, 500.0),
            coordinate_values(dimension, [1.0, 2.0], 10.0, 100.0),
        ]]),
        StructureKind::GeometryCollection => {
            CanonicalGeometry::GeometryCollection(vec![CanonicalGeometry::Point(Some(
                coordinate_values(dimension, [5.0, 6.0], 7.0, 70.0),
            ))])
        }
    }
}

fn canonical_rows(array: &GeometryArray) -> Vec<CanonicalRow> {
    (0..array.len())
        .map(|index| {
            if array.is_null(index) {
                CanonicalRow::Null
            } else {
                CanonicalRow::Value(canonical_geometry(&array.value(index).unwrap()))
            }
        })
        .collect()
}

fn canonical_geometry(geometry: &impl GeometryTrait<T = f64>) -> CanonicalGeometry {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => CanonicalGeometry::Point(
            point
                .coord()
                .map(|coord| coordinate_values_from_coord(&coord)),
        ),
        NativeGeometryType::LineString(line) => CanonicalGeometry::LineString(
            line.coords()
                .map(|coord| coordinate_values_from_coord(&coord))
                .collect(),
        ),
        NativeGeometryType::Polygon(polygon) => {
            CanonicalGeometry::Polygon(canonical_polygon(polygon))
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => CanonicalGeometry::MultiPolygon(
            multi_polygon
                .polygons()
                .map(|polygon| canonical_polygon(&polygon))
                .collect(),
        ),
        NativeGeometryType::GeometryCollection(collection) => {
            CanonicalGeometry::GeometryCollection(
                (0..collection.num_geometries())
                    .map(|index| canonical_geometry(&collection.geometry(index).unwrap()))
                    .collect(),
            )
        }
        _ => panic!("unexpected geometry type in fixture"),
    }
}

fn canonical_polygon(polygon: &impl PolygonTrait<T = f64>) -> Vec<Vec<Vec<f64>>> {
    let mut rings = Vec::with_capacity(1 + polygon.num_interiors());
    if let Some(exterior) = polygon.exterior() {
        rings.push(
            exterior
                .coords()
                .map(|coord| coordinate_values_from_coord(&coord))
                .collect(),
        );
    }
    rings.extend(polygon.interiors().map(|interior| {
        interior
            .coords()
            .map(|coord| coordinate_values_from_coord(&coord))
            .collect()
    }));
    rings
}

fn coordinate_values_from_coord(coord: &impl CoordTrait<T = f64>) -> Vec<f64> {
    (0..coord.dim().size())
        .map(|index| coord.nth_or_panic(index))
        .collect()
}

fn coordinate_values(dimension: Dimension, xy: [f64; 2], z: f64, m: f64) -> Vec<f64> {
    match dimension {
        Dimension::XY => vec![xy[0], xy[1]],
        Dimension::XYZ => vec![xy[0], xy[1], z],
        Dimension::XYM => vec![xy[0], xy[1], m],
        Dimension::XYZM => vec![xy[0], xy[1], z, m],
    }
}

fn point_values(array: &GeometryArray) -> Vec<f64> {
    let rows = canonical_rows(array);
    match &rows[0] {
        CanonicalRow::Value(CanonicalGeometry::Point(Some(coords))) => coords.clone(),
        other => panic!("expected one point geometry, got {other:?}"),
    }
}

fn point_input_coordinates(input: &PointInput) -> Vec<f64> {
    coordinate_values(
        input.dimension,
        [input.coords[0], input.coords[1]],
        input.coords[2],
        input.coords[3],
    )
}

fn point_input_storage(input: &PointInput) -> [f64; 4] {
    let coordinates = point_input_coordinates(input);
    let mut storage = [f64::NAN; 4];
    storage[..coordinates.len()].copy_from_slice(&coordinates);
    storage
}

fn assert_array_contract(
    array: &GeometryArray,
    field: &Field,
    dimension: Dimension,
    coord_type: CoordType,
    metadata: &Metadata,
) {
    let geometry_type = field.try_extension_type::<GeometryType>().unwrap();
    assert_eq!(geometry_type.coord_type(), coord_type);
    assert_eq!(geometry_type.metadata().crs(), metadata.crs());
    let dimensions = (0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index).unwrap().dim()))
        .collect::<Vec<_>>();
    let expected_dimension = geometry_dimension(dimension);
    for actual in dimensions.into_iter().flatten() {
        assert_eq!(actual, expected_dimension);
    }
}

fn assert_separated_geometry_output(field: &Field, metadata: &Metadata) {
    let geometry_type = field.try_extension_type::<GeometryType>().unwrap();
    assert_eq!(geometry_type.coord_type(), CoordType::Separated);
    assert_eq!(geometry_type.metadata().crs(), metadata.crs());
}

fn geometry_dimension(dimension: Dimension) -> geo_traits::Dimensions {
    match dimension {
        Dimension::XY => geo_traits::Dimensions::Xy,
        Dimension::XYZ => geo_traits::Dimensions::Xyz,
        Dimension::XYM => geo_traits::Dimensions::Xym,
        Dimension::XYZM => geo_traits::Dimensions::Xyzm,
    }
}

fn transform_coords(params: AffineParams, coords: &[f64], dimension: Dimension) -> Vec<f64> {
    match dimension {
        Dimension::XY => vec![
            params.a * coords[0] + params.b * coords[1] + params.x_off,
            params.d * coords[0] + params.e * coords[1] + params.y_off,
        ],
        Dimension::XYZ => vec![
            params.a * coords[0] + params.b * coords[1] + params.x_off,
            params.d * coords[0] + params.e * coords[1] + params.y_off,
            params.z_scale * coords[2] + params.z_off,
        ],
        Dimension::XYM => vec![
            params.a * coords[0] + params.b * coords[1] + params.x_off,
            params.d * coords[0] + params.e * coords[1] + params.y_off,
            coords[2],
        ],
        Dimension::XYZM => vec![
            params.a * coords[0] + params.b * coords[1] + params.x_off,
            params.d * coords[0] + params.e * coords[1] + params.y_off,
            params.z_scale * coords[2] + params.z_off,
            coords[3],
        ],
    }
}

fn translate_named(coords: &[f64], dimension: Dimension, offsets: [f64; 3]) -> Vec<f64> {
    match dimension {
        Dimension::XY | Dimension::XYM => {
            let mut output = coords.to_vec();
            output[0] += offsets[0];
            output[1] += offsets[1];
            output
        }
        Dimension::XYZ | Dimension::XYZM => {
            let mut output = coords.to_vec();
            output[0] += offsets[0];
            output[1] += offsets[1];
            output[2] += offsets[2];
            output
        }
    }
}

fn rotate_z_named(coords: &[f64], angle: f64) -> Vec<f64> {
    let (sin, cos) = angle.sin_cos();
    let mut output = coords.to_vec();
    output[0] = cos * coords[0] - sin * coords[1];
    output[1] = sin * coords[0] + cos * coords[1];
    output
}

fn scale_named(coords: &[f64], dimension: Dimension, factors: [f64; 3]) -> Vec<f64> {
    match dimension {
        Dimension::XY | Dimension::XYM => {
            let mut output = coords.to_vec();
            output[0] *= factors[0];
            output[1] *= factors[1];
            output
        }
        Dimension::XYZ | Dimension::XYZM => {
            let mut output = coords.to_vec();
            output[0] *= factors[0];
            output[1] *= factors[1];
            output[2] *= factors[2];
            output
        }
    }
}

fn assert_coords_near(actual: &[f64], expected: &[f64]) {
    assert_eq!(actual.len(), expected.len());
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        let scale = 1.0 + actual.abs().max(expected.abs());
        assert!(
            (actual - expected).abs() <= EPSILON * scale,
            "coordinate {index}: expected {expected}, got {actual}"
        );
    }
}

fn metadata(crs: Option<&'static str>) -> Arc<Metadata> {
    let crs = crs
        .map(|authority| Crs::from_authority_code(authority.to_string()))
        .unwrap_or_default();
    Arc::new(Metadata::new(crs, None))
}

fn dimensions() -> [Dimension; 4] {
    [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ]
}

fn coord_types() -> [CoordType; 2] {
    [CoordType::Separated, CoordType::Interleaved]
}

fn dimension_strategy() -> impl Strategy<Value = Dimension> {
    prop_oneof![
        Just(Dimension::XY),
        Just(Dimension::XYZ),
        Just(Dimension::XYM),
        Just(Dimension::XYZM),
    ]
}

fn coord_type_strategy() -> impl Strategy<Value = CoordType> {
    prop_oneof![Just(CoordType::Separated), Just(CoordType::Interleaved)]
}

fn crs_strategy() -> impl Strategy<Value = Option<&'static str>> {
    prop_oneof![Just(None), Just(Some("EPSG:4326")), Just(Some("EPSG:4979")),]
}

fn io_format() -> impl Strategy<Value = IoFormat> {
    prop_oneof![Just(IoFormat::Wkt), Just(IoFormat::Wkb)]
}

fn structure_kind() -> impl Strategy<Value = StructureKind> {
    prop_oneof![
        Just(StructureKind::LineString),
        Just(StructureKind::Polygon),
        Just(StructureKind::GeometryCollection),
    ]
}

fn point_input() -> impl Strategy<Value = PointInput> {
    (
        dimension_strategy(),
        coord_type_strategy(),
        crs_strategy(),
        (
            -COORD_BOUND..COORD_BOUND,
            -COORD_BOUND..COORD_BOUND,
            -COORD_BOUND..COORD_BOUND,
            -COORD_BOUND..COORD_BOUND,
        ),
    )
        .prop_map(|(dimension, coord_type, crs, (x, y, z, m))| PointInput {
            dimension,
            coord_type,
            crs,
            coords: [x, y, z, m],
        })
}

fn affine_params() -> impl Strategy<Value = AffineParams> {
    (
        0.25..4.0,
        0.25..4.0,
        -PI..PI,
        -1_000.0..1_000.0,
        -1_000.0..1_000.0,
        0.25..4.0,
        -1_000.0..1_000.0,
    )
        .prop_map(
            |(scale_x, scale_y, angle, x_off, y_off, z_scale, z_off): (
                f64,
                f64,
                f64,
                f64,
                f64,
                f64,
                f64,
            )| {
                let (sin, cos) = angle.sin_cos();
                AffineParams {
                    a: scale_x * cos,
                    b: -scale_y * sin,
                    d: scale_x * sin,
                    e: scale_y * cos,
                    z_scale,
                    x_off,
                    y_off,
                    z_off,
                }
            },
        )
}

fn named_affine_params() -> impl Strategy<Value = NamedAffineParams> {
    (
        (-1_000.0..1_000.0, -1_000.0..1_000.0, -1_000.0..1_000.0),
        -PI..PI,
        (0.25..4.0, 0.25..4.0, 0.25..4.0),
    )
        .prop_map(|(translate, angle, scale)| NamedAffineParams {
            translate: [translate.0, translate.1, translate.2],
            angle,
            scale: [scale.0, scale.1, scale.2],
        })
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
