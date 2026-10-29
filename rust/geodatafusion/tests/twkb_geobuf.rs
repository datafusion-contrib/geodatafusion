//! Native Arrow contracts for the TWKB and Geobuf codecs.

mod support;

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray,
    StructArray,
};
use datafusion::arrow::datatypes::{Field, Fields, Int16Type, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{MultiPointBuilder, PointBuilder};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeometryType, Metadata, MultiPointType, PointType, WkbType,
};
use support::context::context;
use support::geoarrow::collect;
use wkt::types::Coord;

async fn scalar_text(sql: &str) -> String {
    let batches = collect(&context(), sql).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    batches[0].column(0).as_string::<i32>().value(0).to_string()
}

async fn scalar_bool(sql: &str) -> bool {
    let batches = collect(&context(), sql).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    batches[0].column(0).as_boolean().value(0)
}

fn decode_geobuf(bytes: &[u8]) -> serde_json::Value {
    geobuf_codec::decode(bytes).unwrap()
}

#[tokio::test]
async fn as_twkb_matches_reviewed_postgis_bytes() {
    for (sql, expected) in [
        (
            "SELECT encode(ST_AsTWKB(ST_GeomFromText('POINT(1.23456789 2.3456789)')), 'hex')",
            "01000204",
        ),
        (
            "SELECT encode(ST_AsTWKB(ST_GeomFromText('POINT(1.23456789 2.3456789)'), 3), 'hex')",
            "6100a613d424",
        ),
        (
            "SELECT encode(ST_AsTWKB(ST_GeomFromText('POINT Z (1 2 3)')), 'hex')",
            "010801020406",
        ),
        (
            "SELECT encode(ST_AsTWKB(ST_GeomFromText('POINT M (1 2 3)')), 'hex')",
            "010802020406",
        ),
        (
            "SELECT encode(ST_AsTWKB(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')), 'hex')",
            "0710",
        ),
        (
            "SELECT encode(ST_AsTWKB(ST_GeomFromText('POINT ZM (1.25 2.5 3.75 4.125)'), \
             2, 2, 1, true, true), 'hex')",
            "410b2b12fa0100f40300ee05005200fa01f403ee0552",
        ),
    ] {
        assert_eq!(scalar_text(sql).await, expected, "{sql}");
    }
}

#[tokio::test]
async fn geomfromtwkb_decodes_reviewed_postgis_bytes() {
    for (hex, expected_ewkt) in [("01000204", "POINT(1 2)"), ("0110", "POINT EMPTY")] {
        let sql = format!("SELECT ST_AsEWKT(ST_GeomFromTWKB(decode('{hex}', 'hex')))");
        assert_eq!(scalar_text(&sql).await, expected_ewkt, "{hex}");
    }

    for (hex, expected_flags) in [("010801020406", "2"), ("010802020406", "1")] {
        let sql =
            format!("SELECT ST_Zmflag(ST_GeomFromTWKB(decode('{hex}', 'hex'))) = {expected_flags}");
        assert!(scalar_bool(&sql).await, "{hex}");
    }
}

#[tokio::test]
async fn geomfromtwkb_rejects_malformed_wire_data() {
    for (hex, expected) in [
        ("0400ffffffffffffffffff01", "exceeds remaining"),
        ("0300ffffffffffffffffff01", "exceeds remaining"),
        ("0200ffffffffffffffffff02", "varint overflow"),
    ] {
        let sql = format!("SELECT ST_GeomFromTWKB(decode('{hex}', 'hex'))");
        let error = collect(&context(), &sql)
            .await
            .expect_err("malformed TWKB unexpectedly decoded")
            .to_string();
        assert!(error.contains(expected), "{hex}: {error}");
    }

    let mut nested = String::new();
    for _ in 0..=64 {
        nested.push_str("070001");
    }
    nested.push_str("01000204");
    let sql = format!("SELECT ST_GeomFromTWKB(decode('{nested}', 'hex'))");
    let error = collect(&context(), &sql)
        .await
        .expect_err("over-deep TWKB unexpectedly decoded")
        .to_string();
    assert!(error.contains("maximum depth"), "{error}");
}

#[tokio::test]
async fn twkb_roundtrip_preserves_empty_and_collection_members() {
    for wkt in [
        "LINESTRING(0 0,1 1,2 2)",
        "POLYGON((0 0,0 3,3 3,3 0,0 0))",
        "MULTIPOINT((0 0),(1 1))",
        "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))",
        "GEOMETRYCOLLECTION EMPTY",
    ] {
        let sql = format!(
            "SELECT ST_AsEWKT(ST_GeomFromTWKB(ST_AsTWKB(ST_GeomFromText('{wkt}'), 6, 6, 6))) \
             = ST_AsEWKT(ST_GeomFromText('{wkt}'))"
        );
        assert!(scalar_bool(&sql).await, "{wkt}");
    }
}

#[tokio::test]
async fn astwkb_array_overload_matches_reviewed_postgis_idlist_bytes() {
    for (sql, expected) in [
        (
            "SELECT encode(ST_AsTWKB(\
                ARRAY[ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)')],\
                ARRAY[10, 20]\
            ), 'hex')",
            "040402142802040404",
        ),
        (
            "SELECT encode(ST_AsTWKB(\
                ARRAY[ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)')],\
                ARRAY[10, 20], 6, 6, 6, true, true\
            ), 'hex')",
            "c4072180897a8092f4018092f4018092f40102142880897a8092f4018092f4018092f401",
        ),
        (
            "SELECT encode(ST_AsTWKB(\
                ARRAY[ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('LINESTRING(0 0,1 1)')],\
                ARRAY[7, 8]\
            ), 'hex')",
            "0704020e100100020402000200000202",
        ),
    ] {
        assert_eq!(scalar_text(sql).await, expected, "{sql}");
    }
}

#[tokio::test]
async fn astwkb_rejects_invalid_options_and_mismatched_ids() {
    let error = collect(
        &context(),
        "SELECT ST_AsTWKB(ST_GeomFromText('POINT Z (1 2 3)'), 0, -1)",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("Z precision"), "{error}");

    let error = collect(
        &context(),
        "SELECT ST_AsTWKB(\
            ARRAY[ST_GeomFromText('POINT(1 2)')], ARRAY[1, 2]\
        )",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("must match"), "{error}");
}

#[tokio::test]
async fn twkb_and_geobuf_preserve_crs_dimensions_and_sliced_arrays() {
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let point_type =
        PointType::new(Dimension::XYZM, metadata).with_coord_type(CoordType::Separated);
    let mut builder = PointBuilder::with_capacity(point_type.clone(), 4);
    builder.push_coord(Some(&Coord {
        x: 1.0,
        y: 2.0,
        z: Some(3.0),
        m: Some(4.0),
    }));
    builder.push_empty();
    builder.push_coord(Some(&Coord {
        x: 5.0,
        y: 6.0,
        z: Some(7.0),
        m: Some(8.0),
    }));
    builder.push_coord(Some(&Coord {
        x: 9.0,
        y: 10.0,
        z: Some(11.0),
        m: Some(12.0),
    }));
    let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish().slice(1, 3));
    let field = point_type.to_field("geom", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![array.to_array_ref()],
    )
    .unwrap();
    let ctx = context();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_Zmflag(ST_GeomFromTWKB(ST_AsTWKB(geom, 6, 6, 6))) AS flags FROM t",
    )
    .await
    .unwrap();
    let flags = batches[0]
        .column(0)
        .as_primitive::<Int16Type>()
        .values()
        .to_vec();
    assert_eq!(flags, vec![3, 3, 3]);

    let batches = collect(
        &ctx,
        "SELECT ST_GeomFromTWKB(ST_AsTWKB(geom, 6, 6, 6)) AS geom FROM t",
    )
    .await
    .unwrap();
    let output_field = batches[0].schema().field(0).clone();
    let geometry_type = output_field.extension_type::<GeometryType>();
    assert_eq!(geometry_type.coord_type(), CoordType::Separated);
    // TWKB carries no SRID, matching PostGIS. The source CRS is intentionally
    // not invented during a byte-format round trip.
    assert_eq!(geometry_type.metadata().crs(), &Crs::default());

    let batches = collect(
        &ctx,
        "SELECT ST_AsGeobuf(named_struct('geom', geom, 'name', 'x')) FROM t",
    )
    .await
    .unwrap();
    let bytes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    let decoded = decode_geobuf(bytes);
    assert_eq!(decoded["features"].as_array().unwrap().len(), 3);
    assert_eq!(
        decoded["features"][0]["geometry"]["coordinates"],
        serde_json::json!([])
    );
}

#[tokio::test]
async fn astwkb_uses_reviewed_default_precision_for_crs() {
    let cases = [
        (Some("EPSG:4326"), "a10090a10fa0c21e"),
        (Some("EPSG:4269"), "a10090a10fa0c21e"),
        (Some("EPSG:4979"), "a10090a10fa0c21e"),
        (Some("EPSG:4978"), "01000206"),
        (Some("EPSG:3857"), "01000206"),
        (None, "01000206"),
    ];
    for (authority, expected) in cases {
        for coord_type in [CoordType::Separated, CoordType::Interleaved] {
            let metadata = Arc::new(match authority {
                Some(authority) => {
                    Metadata::new(Crs::from_authority_code(authority.to_string()), None)
                }
                None => Metadata::default(),
            });
            let point_type = PointType::new(Dimension::XY, metadata).with_coord_type(coord_type);
            let mut builder = PointBuilder::with_capacity(point_type.clone(), 1);
            builder.push_coord(Some(&Coord {
                x: 1.25,
                y: 2.5,
                z: None,
                m: None,
            }));
            let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
            let field = point_type.to_field("geom", false);
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![array.to_array_ref()],
            )
            .unwrap();
            let ctx = context();
            ctx.register_batch("t", batch).unwrap();

            let batches = collect(&ctx, "SELECT ST_AsTWKB(geom) FROM t")
                .await
                .unwrap();
            assert!(!batches[0].schema().field(0).is_nullable());
            assert_eq!(
                hex::encode(batches[0].column(0).as_binary::<i32>().value(0)),
                expected,
                "{authority:?} {coord_type:?}"
            );
        }
    }
}

#[tokio::test]
async fn astwkb_reads_per_row_options_and_null_defaults() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("wkt", datafusion::arrow::datatypes::DataType::Utf8, false),
        Field::new("prec", datafusion::arrow::datatypes::DataType::Int64, true),
        Field::new(
            "sizes",
            datafusion::arrow::datatypes::DataType::Boolean,
            true,
        ),
        Field::new(
            "boxes",
            datafusion::arrow::datatypes::DataType::Boolean,
            true,
        ),
        Field::new("ord", datafusion::arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "POINT(1.23456789 2.3456789)",
                "POINT(1.23456789 2.3456789)",
                "POINT(1 2)",
                "POINT(3 4)",
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, Some(3), Some(3), None])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                None,
                Some(false),
                Some(true),
                Some(true),
            ])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                None,
                Some(false),
                Some(true),
                Some(true),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);

    let batches = collect(
        &ctx,
        "SELECT encode(ST_AsTWKB(ST_GeomFromText(wkt), prec), 'hex') AS twkb, ord \
         FROM t ORDER BY ord",
    )
    .await
    .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["01000204", "6100a613d424", "6100d00fa01f", "01000608"]
    );

    let batches = collect(
        &ctx,
        "SELECT encode(ST_AsTWKB(ST_GeomFromText('POINT(1.25 2.5)'), 0, 0, 0, sizes, boxes), 'hex') AS twkb, \
         ord FROM t ORDER BY ord",
    )
    .await
    .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec![
            "01000206",
            "01000206",
            "010306020006000206",
            "010306020006000206"
        ]
    );

    let batches = collect(
        &ctx,
        "SELECT encode(ST_AsTWKB(ARRAY[ST_GeomFromText(wkt)], ARRAY[ord], prec), 'hex') AS twkb, ord \
         FROM t ORDER BY ord",
    )
    .await
    .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec![
            "040401000204",
            "64040102a613d424",
            "64040104d00fa01f",
            "040401060608"
        ]
    );

    let batches = collect(
        &ctx,
        "SELECT encode(ST_AsTWKB(ST_GeomFromText(wkt), 3), 'hex') AS twkb, ord \
         FROM t ORDER BY ord",
    )
    .await
    .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec![
            "6100a613d424",
            "6100a613d424",
            "6100d00fa01f",
            "6100f02ec03e"
        ]
    );
}

#[tokio::test]
async fn astwkb_classifies_typed_projjson_crs() {
    for (crs, expected) in [
        (
            Crs::from_projjson(serde_json::json!({
                "type": "GeographicCRS",
                "name": "WGS 84"
            })),
            "a10090a10fa0c21e",
        ),
        (
            Crs::from_projjson(serde_json::json!({
                "type": "ProjectedCRS",
                "name": "Web Mercator"
            })),
            "01000206",
        ),
    ] {
        let metadata = Arc::new(Metadata::new(crs, None));
        let point_type =
            PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
        let mut builder = PointBuilder::with_capacity(point_type.clone(), 1);
        builder.push_coord(Some(&Coord {
            x: 1.25,
            y: 2.5,
            z: None,
            m: None,
        }));
        let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
        let field = point_type.to_field("geom", false);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![array.to_array_ref()],
        )
        .unwrap();
        let ctx = context();
        ctx.register_batch("t", batch).unwrap();
        let batches = collect(&ctx, "SELECT encode(ST_AsTWKB(geom), 'hex') FROM t")
            .await
            .unwrap();
        assert_eq!(string_values(&batches, 0), vec![expected]);
    }
}

#[tokio::test]
async fn astwkb_errors_on_unclassifiable_crs_without_explicit_precision() {
    let metadata = Arc::new(Metadata::new(
        Crs::from_unknown_crs_type("urn:example:unknown-crs".to_string()),
        None,
    ));
    let point_type = PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let mut builder = PointBuilder::with_capacity(point_type.clone(), 1);
    builder.push_coord(Some(&Coord {
        x: 1.25,
        y: 2.5,
        z: None,
        m: None,
    }));
    let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    let field = point_type.to_field("geom", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![array.to_array_ref()],
    )
    .unwrap();
    let ctx = context();
    ctx.register_batch("t", batch).unwrap();

    let error = collect(&ctx, "SELECT ST_AsTWKB(geom) FROM t")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("pass an explicit X/Y precision"), "{error}");

    let batches = collect(&ctx, "SELECT encode(ST_AsTWKB(geom, 0), 'hex') FROM t")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["01000206"]);
}

fn string_values(batches: &[RecordBatch], column: usize) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        let array = batch.column(column).as_string::<i32>();
        for row in 0..array.len() {
            values.push(array.value(row).to_string());
        }
    }
    values
}

#[tokio::test]
async fn geomfromtwkb_preserves_input_field_crs_metadata() {
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let field = Field::new("twkb", datafusion::arrow::datatypes::DataType::Binary, true)
        .with_extension_type(WkbType::new(metadata));
    let bytes = hex::decode("01000204").unwrap();
    let array = BinaryArray::from(vec![Some(bytes.as_slice())]);
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(array)]).unwrap();
    let ctx = context();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(&ctx, "SELECT ST_GeomFromTWKB(twkb) AS geom FROM t")
        .await
        .unwrap();
    let output_field = batches[0].schema().field(0).clone();
    let geometry_type = output_field.extension_type::<GeometryType>();
    assert_eq!(geometry_type.coord_type(), CoordType::Separated);
    assert_eq!(
        geometry_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
}

#[tokio::test]
async fn asgeobuf_accepts_interleaved_geometry_records() {
    let point_type = PointType::new(Dimension::XY, Arc::new(Metadata::default()))
        .with_coord_type(CoordType::Interleaved);
    let mut builder = PointBuilder::with_capacity(point_type.clone(), 1);
    builder.push_coord(Some(&Coord {
        x: 2.5,
        y: 3.5,
        z: None,
        m: None,
    }));
    let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    let field = point_type.to_field("geom", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![array.to_array_ref()],
    )
    .unwrap();
    let ctx = context();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_AsGeobuf(named_struct('geom', geom, 'name', 'interleaved')) FROM t",
    )
    .await
    .unwrap();
    let bytes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    let decoded = decode_geobuf(bytes);
    assert_eq!(
        decoded["features"][0]["geometry"]["coordinates"],
        serde_json::json!([2.5, 3.5])
    );
}

#[tokio::test]
async fn asgeobuf_rejects_unsupported_property_types_and_empty_members() {
    let error = collect(
        &context(),
        "SELECT ST_AsGeobuf(named_struct(\
            'geom', ST_GeomFromText('POINT(1 2)'),\
            'ts', CAST('2020-01-01 00:00:00' AS TIMESTAMP)\
        ))",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("does not support property type"), "{error}");
    assert!(error.contains("Timestamp"), "{error}");

    let multi_type = MultiPointType::new(Dimension::XY, Arc::new(Metadata::default()))
        .with_coord_type(CoordType::Separated);
    let mut builder = MultiPointBuilder::new(multi_type.clone());
    let multi_point = wkt::types::MultiPoint::new(
        vec![
            wkt::types::Point::empty(wkt::types::Dimension::XY),
            wkt::types::Point::from_coord(Coord {
                x: 1.0,
                y: 1.0,
                z: None,
                m: None,
            }),
        ],
        wkt::types::Dimension::XY,
    );
    builder.push_multi_point(Some(&multi_point)).unwrap();
    let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    let geometry_field = Arc::new(multi_type.to_field("geom", true));
    let fields = Fields::from(vec![
        geometry_field,
        Arc::new(Field::new(
            "name",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        )),
    ]);
    let records = StructArray::try_new(
        fields.clone(),
        vec![
            array.to_array_ref(),
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
        ],
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "records",
            datafusion::arrow::datatypes::DataType::Struct(fields),
            true,
        )])),
        vec![Arc::new(records)],
    )
    .unwrap();
    let ctx = context();
    ctx.register_batch("t", batch).unwrap();
    let error = collect(&ctx, "SELECT ST_AsGeobuf(records) FROM t")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("does not support empty MULTIPOINT members"),
        "{error}"
    );
}

#[tokio::test]
async fn asgeobuf_encodes_record_properties_and_skips_null_properties() {
    let sql = "SELECT ST_AsGeobuf(named_struct(\
        'geom', ST_GeomFromText('POINT(1.5 2.5)'),\
        'name', 'alpha',\
        'n', 7,\
        'nullable', CAST(NULL AS VARCHAR)\
    ))";
    let bytes = scalar_binary(sql).await.unwrap();
    assert_eq!(
        hex::encode(&bytes),
        "0a046e616d650a016e0a086e756c6c61626c651801221d0a1b0a0608001a021e326a070a05616c7068616a021807720400000101"
    );
    let decoded = decode_geobuf(&bytes);
    assert_eq!(decoded["type"], "FeatureCollection");
    assert_eq!(decoded["features"].as_array().unwrap().len(), 1);
    let feature = &decoded["features"][0];
    assert_eq!(feature["geometry"]["type"], "Point");
    assert_eq!(
        feature["geometry"]["coordinates"],
        serde_json::json!([1.5, 2.5])
    );
    assert_eq!(feature["properties"]["name"], "alpha");
    assert_eq!(feature["properties"]["n"], 7);
    assert!(feature["properties"].get("nullable").is_none());

    let bytes = scalar_binary(
        "SELECT ST_AsGeobuf(named_struct(\
            'geom', ST_GeomFromText('POINT(1 2)'), 'name', 'alpha'\
        ), 'geom')",
    )
    .await
    .unwrap();
    assert_eq!(
        hex::encode(&bytes),
        "0a046e616d65180022170a150a0608001a0202046a070a05616c70686172020000"
    );
    let decoded = decode_geobuf(&bytes);
    assert_eq!(decoded["features"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn asgeobuf_distinguishes_empty_input_from_all_null_geometry() {
    let empty = scalar_binary(
        "SELECT ST_AsGeobuf(named_struct(\
            'geom', ST_GeomFromText('POINT(1 2)'), 'name', 'alpha'\
        )) FROM (SELECT 1 WHERE false)",
    )
    .await;
    assert!(empty.is_none());

    let all_null = scalar_binary(
        "SELECT ST_AsGeobuf(named_struct(\
            'geom', ST_GeomFromText(NULL), 'name', 'alpha'\
        ))",
    )
    .await
    .unwrap();
    assert_eq!(hex::encode(&all_null), "0a046e616d6518002200");
    let decoded = decode_geobuf(&all_null);
    assert_eq!(decoded["type"], "FeatureCollection");
    assert!(decoded["features"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn asgeobuf_merges_partial_states_across_partitions() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("wkt", datafusion::arrow::datatypes::DataType::Utf8, false),
        Field::new("name", datafusion::arrow::datatypes::DataType::Utf8, false),
    ]));
    let make_batch = |wkts: Vec<&str>, names: Vec<&str>| {
        let wkts = Arc::new(StringArray::from(wkts)) as ArrayRef;
        let names = Arc::new(StringArray::from(names)) as ArrayRef;
        RecordBatch::try_new(Arc::clone(&schema), vec![wkts, names]).unwrap()
    };
    let table = MemTable::try_new(
        Arc::clone(&schema),
        vec![
            vec![make_batch(vec!["POINT(1 2)", "POINT(3 4)"], vec!["a", "b"])],
            vec![make_batch(vec!["POINT(5 6)", "POINT(7 8)"], vec!["c", "d"])],
        ],
    )
    .unwrap();

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table("points", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);
    let dataframe = ctx
        .sql(
            "SELECT ST_AsGeobuf(named_struct(\
                'geom', ST_GeomFromText(wkt), 'name', name\
            )) FROM points",
        )
        .await
        .unwrap();
    let plan = dataframe.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref() as &dyn ExecutionPlan)
        .indent(true)
        .to_string();
    assert!(plan_text.contains("mode=Partial"), "{plan_text}");
    assert!(plan_text.contains("mode=Final"), "{plan_text}");

    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    let bytes = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            (0..array.len())
                .filter(|index| !array.is_null(*index))
                .map(|index| array.value(index).to_vec())
                .collect::<Vec<_>>()
        })
        .next()
        .expect("ST_AsGeobuf must produce one merged payload");
    let decoded = decode_geobuf(&bytes);
    let mut names = decoded["features"]
        .as_array()
        .unwrap()
        .iter()
        .map(|feature| feature["properties"]["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["a", "b", "c", "d"]);
}

async fn scalar_binary(sql: &str) -> Option<Vec<u8>> {
    let batches = collect(&context(), sql).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    if array.is_null(0) {
        None
    } else {
        Some(array.value(0).to_vec())
    }
}
