use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, UInt8Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Int64Array, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType as NativeGeometryType, LineStringTrait, PointTrait,
};
use geoarrow_array::array::GeometryArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{Crs, GeometryType, Metadata, WkbType, WktType};

const POINT_ZM_WKB: &[u8] = &[
    0x01, 0xb9, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x10, 0x40,
];

const LINESTRING_ZM_WKB: &[u8] = &[
    0x01, 0xba, 0x0b, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0,
    0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
    0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14,
    0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40,
];

const GEOMETRYCOLLECTION_POINT_ZM_WKB: &[u8] = &[
    0x01, 0xbf, 0x0b, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xb9, 0x0b, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40,
];

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

async fn collect_one(ctx: &SessionContext, sql: &str) -> Result<RecordBatch> {
    let batches = ctx.sql(sql).await?.collect().await?;
    assert_eq!(batches.len(), 1);
    Ok(batches.into_iter().next().unwrap())
}

fn geometry_array(batch: &RecordBatch, index: usize) -> GeometryArray {
    GeometryArray::try_from((batch.column(index).as_ref(), batch.schema().field(index))).unwrap()
}

fn crs(field: &Field) -> Crs {
    field
        .try_extension_type::<GeometryType>()
        .unwrap()
        .metadata()
        .crs()
        .clone()
}

fn crs_value(field: &Field) -> Option<String> {
    field
        .try_extension_type::<GeometryType>()
        .unwrap()
        .metadata()
        .crs()
        .crs_value()
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn geometry_name(geometry: &impl GeometryTrait<T = f64>) -> &'static str {
    match geometry.as_type() {
        NativeGeometryType::Point(_) => "Point",
        NativeGeometryType::LineString(_) => "LineString",
        NativeGeometryType::Polygon(_) => "Polygon",
        NativeGeometryType::MultiPoint(_) => "MultiPoint",
        NativeGeometryType::MultiLineString(_) => "MultiLineString",
        NativeGeometryType::MultiPolygon(_) => "MultiPolygon",
        NativeGeometryType::GeometryCollection(_) => "GeometryCollection",
        NativeGeometryType::Rect(_) => "Rect",
        NativeGeometryType::Line(_) => "Line",
        NativeGeometryType::Triangle(_) => "Triangle",
    }
}

fn assert_xyzm_point(batch: &RecordBatch, index: usize, expected: [f64; 4]) {
    let array = geometry_array(batch, index);
    let geometry = array.value(0).unwrap();
    assert_eq!(geometry.dim(), geo_traits::Dimensions::Xyzm);

    let NativeGeometryType::Point(point) = geometry.as_type() else {
        panic!("expected point geometry");
    };
    let coord = point.coord().unwrap();
    assert_eq!(coord.x(), expected[0]);
    assert_eq!(coord.y(), expected[1]);
    assert_eq!(coord.nth(2), Some(expected[2]));
    assert_eq!(coord.nth(3), Some(expected[3]));
}

#[tokio::test]
async fn typed_wkt_constructors_register_and_preserve_srid() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))', 3857) AS gc,
            ST_LineFromText('LINESTRING Z (0 0 1,3 4 5)', 3857) AS line,
            ST_MLineFromText('MULTILINESTRING((0 0,1 1))', 3857) AS mline,
            ST_MPointFromText('MULTIPOINT((1 2),(3 4))', 3857) AS mpoint,
            ST_MPolyFromText('MULTIPOLYGON(((0 0,0 1,1 1,0 0)))', 3857) AS mpoly,
            ST_PointFromText('POINT ZM (1 2 3 4)', 3857) AS point,
            ST_PolygonFromText('POLYGON((0 0,0 1,1 1,0 0))', 3857) AS polygon
        "#,
    )
    .await?;

    let expected_crs = Crs::from_authority_code("EPSG:3857".to_string());
    for index in 0..7 {
        assert_eq!(crs(batch.schema().field(index)), expected_crs);
    }

    assert_eq!(
        geometry_name(&geometry_array(&batch, 0).value(0).unwrap()),
        "GeometryCollection"
    );
    assert_eq!(
        geometry_array(&batch, 1).value(0).unwrap().dim(),
        geo_traits::Dimensions::Xyz
    );
    assert_eq!(
        geometry_name(&geometry_array(&batch, 2).value(0).unwrap()),
        "MultiLineString"
    );
    assert_eq!(
        geometry_name(&geometry_array(&batch, 3).value(0).unwrap()),
        "MultiPoint"
    );
    assert_eq!(
        geometry_name(&geometry_array(&batch, 4).value(0).unwrap()),
        "MultiPolygon"
    );
    assert_xyzm_point(&batch, 5, [1.0, 2.0, 3.0, 4.0]);
    assert_eq!(
        geometry_name(&geometry_array(&batch, 6).value(0).unwrap()),
        "Polygon"
    );

    Ok(())
}

#[tokio::test]
async fn typed_wkt_returns_null_for_wrong_type_and_preserves_empty_and_null() -> Result<()> {
    let ctx = context();
    let wkt = StringArray::from(vec![
        Some("POINT ZM (1 2 3 4)"),
        Some("LINESTRING(0 0,1 1)"),
        None,
        Some("POINT EMPTY"),
    ]);
    let schema = Schema::new([Arc::new(Field::new("wkt", DataType::Utf8, true))]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(wkt)]).unwrap();
    ctx.register_batch("wkt_input", batch).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_PointFromText(wkt, 4326) AS geom FROM wkt_input",
    )
    .await?;
    assert_eq!(
        crs(batch.schema().field(0)),
        Crs::from_authority_code("EPSG:4326".to_string())
    );

    let geometries = geometry_array(&batch, 0);
    assert!(!geometries.is_null(0));
    assert!(geometries.is_null(1));
    assert!(geometries.is_null(2));
    assert!(!geometries.is_null(3));

    let geometry = geometries.value(0).unwrap();
    let NativeGeometryType::Point(point) = geometry.as_type() else {
        panic!("expected point geometry");
    };
    let coord = point.coord().unwrap();
    assert_eq!(
        [
            coord.x(),
            coord.y(),
            coord.nth(2).unwrap(),
            coord.nth(3).unwrap()
        ],
        [1.0, 2.0, 3.0, 4.0]
    );

    let geometry = geometries.value(3).unwrap();
    let NativeGeometryType::Point(empty) = geometry.as_type() else {
        panic!("expected empty point geometry");
    };
    assert!(empty.coord().is_none());

    Ok(())
}

#[tokio::test]
async fn typed_wkb_constructors_register_alias_and_preserve_raw_metadata() -> Result<()> {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:3857".to_string()),
        None,
    ));
    let field = Field::new("wkb", DataType::Binary, true)
        .with_extension_type(WkbType::new(metadata.clone()));
    let wkb = BinaryArray::from(vec![Some(POINT_ZM_WKB), Some(LINESTRING_ZM_WKB), None]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(wkb)],
    )
    .unwrap();
    ctx.register_batch("wkb_input", batch).unwrap();

    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_PointFromWKB(wkb) AS point,
            ST_LineFromWKB(wkb) AS line,
            ST_LinestringFromWKB(wkb) AS line_alias
        FROM wkb_input
        "#,
    )
    .await?;

    for index in 0..3 {
        let schema = batch.schema();
        let field = schema.field(index);
        let geometry_type = field.try_extension_type::<GeometryType>().unwrap();
        assert_eq!(geometry_type.metadata(), &metadata);
    }

    let points = geometry_array(&batch, 0);
    assert!(!points.is_null(0));
    assert!(points.is_null(1));
    assert!(points.is_null(2));
    let point = points.value(0).unwrap();
    let NativeGeometryType::Point(point) = point.as_type() else {
        panic!("expected point geometry");
    };
    let coord = point.coord().unwrap();
    assert_eq!(
        [
            coord.x(),
            coord.y(),
            coord.nth(2).unwrap(),
            coord.nth(3).unwrap()
        ],
        [1.0, 2.0, 3.0, 4.0]
    );

    for index in [1, 2] {
        let lines = geometry_array(&batch, index);
        assert!(lines.is_null(0));
        assert!(!lines.is_null(1));
        assert!(lines.is_null(2));
        let line = lines.value(1).unwrap();
        assert_eq!(line.dim(), geo_traits::Dimensions::Xyzm);
        let NativeGeometryType::LineString(line) = line.as_type() else {
            panic!("expected line string geometry");
        };
        assert_eq!(line.num_coords(), 2);
        let last = line.coord(line.num_coords() - 1).unwrap();
        assert_eq!(
            [
                last.x(),
                last.y(),
                last.nth(2).unwrap(),
                last.nth(3).unwrap()
            ],
            [3.0, 4.0, 5.0, 6.0]
        );
    }

    let point_wkb = POINT_ZM_WKB
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    let batch = collect_one(
        &ctx,
        &format!("SELECT ST_PointFromWKB(X'{point_wkb}', 4326) AS geom"),
    )
    .await?;
    assert_eq!(
        crs(batch.schema().field(0)),
        Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert_xyzm_point(&batch, 0, [1.0, 2.0, 3.0, 4.0]);

    Ok(())
}

#[tokio::test]
async fn typed_wkt_preserves_raw_metadata_when_srid_is_omitted() -> Result<()> {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:3857".to_string()),
        None,
    ));
    let field =
        Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata.clone()));
    let wkt = StringArray::from(vec![Some("POINT M (1 2 9)")]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(wkt)],
    )
    .unwrap();
    ctx.register_batch("wkt_metadata", batch).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_PointFromText(wkt) AS geom FROM wkt_metadata",
    )
    .await?;
    let geometry_type = batch
        .schema()
        .field(0)
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(geometry_type.metadata(), &metadata);

    let geometries = geometry_array(&batch, 0);
    let geometry = geometries.value(0).unwrap();
    assert_eq!(geometry.dim(), geo_traits::Dimensions::Xym);
    let NativeGeometryType::Point(point) = geometry.as_type() else {
        panic!("expected point geometry");
    };
    let coord = point.coord().unwrap();
    assert_eq!(coord.nth(2), Some(9.0));

    let batch = collect_one(
        &ctx,
        "SELECT ST_PointFromText(wkt, 4326) AS geom FROM wkt_metadata",
    )
    .await?;
    assert_eq!(
        crs(batch.schema().field(0)),
        Crs::from_authority_code("EPSG:4326".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn typed_constructors_keep_non_epsg_crs_without_numeric_srid() -> Result<()> {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("OGC:CRS84".to_string()),
        None,
    ));

    let wkt_field =
        Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata.clone()));
    let wkt = StringArray::from(vec![Some("POINT(1 2)")]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(wkt_field)])),
        vec![Arc::new(wkt)],
    )
    .unwrap();
    ctx.register_batch("wkt_ogc84", batch).unwrap();

    let wkb_field = Field::new("wkb", DataType::Binary, true)
        .with_extension_type(WkbType::new(metadata.clone()));
    let wkb = BinaryArray::from(vec![Some(POINT_ZM_WKB)]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(wkb_field)])),
        vec![Arc::new(wkb)],
    )
    .unwrap();
    ctx.register_batch("wkb_ogc84", batch).unwrap();

    let batch = collect_one(
        &ctx,
        r#"
        SELECT ST_PointFromText(wkt) AS from_wkt, ST_PointFromWKB(wkb) AS from_wkb
        FROM wkt_ogc84, wkb_ogc84
        "#,
    )
    .await?;
    for index in 0..2 {
        let geometry_type = batch
            .schema()
            .field(index)
            .try_extension_type::<GeometryType>()
            .unwrap();
        assert_eq!(geometry_type.metadata(), &metadata);
        assert!(!geometry_array(&batch, index).is_null(0));
    }

    // A NULL SRID must not require the declared CRS to be numeric either.
    let batch = collect_one(
        &ctx,
        "SELECT ST_PointFromText(wkt, NULL) AS geom FROM wkt_ogc84",
    )
    .await?;
    assert!(geometry_array(&batch, 0).is_null(0));

    Ok(())
}

#[tokio::test]
async fn optional_srid_is_a_literal_and_null_geometry_is_preserved() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_PointFromText('POINT(1 2)', 4326) AS literal_srid,
            ST_PointFromText(NULL, 4326) AS null_geometry
        "#,
    )
    .await?;

    let expected_crs = Crs::from_authority_code("EPSG:4326".to_string());
    assert_eq!(crs(batch.schema().field(0)), expected_crs);
    assert_eq!(crs(batch.schema().field(1)), expected_crs);
    assert!(!geometry_array(&batch, 0).is_null(0));
    assert!(geometry_array(&batch, 1).is_null(0));

    let srids = Int64Array::from(vec![4326]);
    let schema = Schema::new([Arc::new(Field::new("srid", DataType::Int64, true))]);
    ctx.register_batch(
        "srid_input",
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(srids)]).unwrap(),
    )
    .unwrap();
    let err = match ctx
        .sql("SELECT ST_PointFromText('POINT(1 2)', srid) FROM srid_input")
        .await
    {
        Ok(dataframe) => dataframe.collect().await.unwrap_err(),
        Err(err) => err,
    };
    assert!(err.to_string().contains("literal"));

    Ok(())
}

#[tokio::test]
async fn typed_wkt_and_wkb_srid_metadata_follows_postgis_normalization() -> Result<()> {
    let ctx = context();
    let point_wkb = POINT_ZM_WKB
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    let batch = collect_one(
        &ctx,
        &format!(
            r#"
            SELECT
                ST_PointFromText('POINT(1 2)', 0) AS wkt_zero,
                ST_PointFromText('POINT(1 2)', -1) AS wkt_negative,
                ST_PointFromText('POINT(1 2)', 999999) AS wkt_feature_max,
                ST_PointFromText('POINT(1 2)', 1000000) AS wkt_above_feature_max,
                ST_PointFromText('POINT(1 2)', 2147483647) AS wkt_i32_max,
                ST_PointFromText('POINT(1 2)', -2147483648) AS wkt_i32_min,
                ST_PointFromWKB(X'{point_wkb}', 0) AS wkb_zero,
                ST_PointFromWKB(X'{point_wkb}', -1) AS wkb_negative,
                ST_PointFromWKB(X'{point_wkb}', 999999) AS wkb_feature_max,
                ST_PointFromWKB(X'{point_wkb}', 1000000) AS wkb_above_feature_max,
                ST_PointFromWKB(X'{point_wkb}', 2147483647) AS wkb_i32_max,
                ST_PointFromWKB(X'{point_wkb}', -2147483648) AS wkb_i32_min
            "#
        ),
    )
    .await?;

    let expected = [
        None,
        None,
        Some("EPSG:999999"),
        Some("EPSG:999001"),
        Some("EPSG:999280"),
        None,
        None,
        None,
        Some("EPSG:999999"),
        Some("EPSG:999001"),
        Some("EPSG:999280"),
        None,
    ];
    for (index, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            crs_value(batch.schema().field(index)).as_deref(),
            expected,
            "SRID metadata at column {index}"
        );
    }

    for index in 0..12 {
        assert!(
            !geometry_array(&batch, index).is_null(0),
            "valid geometry at column {index}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn typed_wkt_and_wkb_null_srid_is_preserved() -> Result<()> {
    let ctx = context();
    let point_wkb = POINT_ZM_WKB
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    let batch = collect_one(
        &ctx,
        &format!(
            r#"
            SELECT
                ST_PointFromText('POINT(1 2)', NULL) AS wkt_null_srid,
                ST_PointFromWKB(X'{point_wkb}', NULL) AS wkb_null_srid
            "#
        ),
    )
    .await?;

    for index in 0..2 {
        assert_eq!(crs_value(batch.schema().field(index)), None);
        assert!(geometry_array(&batch, index).is_null(0));
    }

    Ok(())
}

#[tokio::test]
async fn typed_wkt_and_wkb_srid_outside_i32_is_rejected() -> Result<()> {
    let ctx = context();
    let point_wkb = POINT_ZM_WKB
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();

    for sql in [
        "SELECT ST_PointFromText('POINT(1 2)', 2147483648)".to_string(),
        format!("SELECT ST_PointFromWKB(X'{point_wkb}', 2147483648)"),
    ] {
        let err = match ctx.sql(&sql).await {
            Ok(dataframe) => dataframe.collect().await.unwrap_err(),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("outside the integer range"),
            "{sql}: {err}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn malformed_wkt_and_wkb_errors_are_preserved() -> Result<()> {
    let ctx = context();

    let wkt_err = ctx
        .sql("SELECT ST_PointFromText('POINT(1 2', 4326)")
        .await?
        .collect()
        .await
        .unwrap_err();
    assert!(wkt_err.to_string().to_lowercase().contains("wkt"));

    let wkb_err = ctx
        .sql("SELECT ST_PointFromWKB(X'00', 4326)")
        .await?
        .collect()
        .await
        .unwrap_err();
    assert!(
        wkb_err.to_string().to_lowercase().contains("buffer"),
        "{wkb_err}"
    );

    Ok(())
}

#[tokio::test]
async fn type_specific_wrong_type_inputs_return_null() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_GeomCollFromText('POINT(1 2)') IS NULL AS geomcoll,
            ST_LineFromText('POINT(1 2)') IS NULL AS line,
            ST_MLineFromText('LINESTRING(0 0,1 1)') IS NULL AS mline,
            ST_MPointFromText('POINT(1 2)') IS NULL AS mpoint,
            ST_MPolyFromText('POINT(1 2)') IS NULL AS mpoly,
            ST_PointFromText('LINESTRING(0 0,1 1)') IS NULL AS point,
            ST_PolygonFromText('POINT(1 2)') IS NULL AS polygon,
            ST_LineFromWKB(ST_AsBinary(ST_GeomFromText('POINT(1 2)'))) IS NULL AS line_wkb,
            ST_LinestringFromWKB(ST_AsBinary(ST_GeomFromText('POINT(1 2)'))) IS NULL AS linestring_wkb,
            ST_PointFromWKB(ST_AsBinary(ST_GeomFromText('LINESTRING(0 0,1 1)'))) IS NULL AS point_wkb
        "#,
    )
    .await?;

    // PostGIS 3.5 returns NULL for ST_MPolyFromText even though its page says "throws".
    for index in 0..10 {
        assert!(
            batch.column(index).as_boolean().value(0),
            "wrong-type constructor at index {index} did not return NULL"
        );
    }

    Ok(())
}

#[tokio::test]
async fn single_member_collection_identity_is_preserved_through_public_io() -> Result<()> {
    let ctx = context();
    let wkb_hex = GEOMETRYCOLLECTION_POINT_ZM_WKB
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    let batch = collect_one(
        &ctx,
        &format!(
            r#"
            SELECT
                GeometryType(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))')) AS generic_wkt,
                ST_GeometryType(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))')) AS generic_wkt_st,
                GeometryType(ST_GeomFromWKB(X'{wkb_hex}')) AS generic_wkb,
                GeometryType(ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(1 2))')) AS typed_wkt,
                ST_AsText(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))')) AS generic_wkt_text,
                ST_GeomCollFromText('POINT(1 2)') IS NULL AS wrong_type_is_null,
                ST_CoordDim(ST_GeomFromWKB(X'{wkb_hex}')) AS generic_wkb_dim
            "#
        ),
    )
    .await?;

    for (index, expected) in [
        (0, "GEOMETRYCOLLECTION"),
        (1, "ST_GeometryCollection"),
        // PostGIS `GeometryType()` never names the Z or ZM dimension.
        (2, "GEOMETRYCOLLECTION"),
        (3, "GEOMETRYCOLLECTION"),
    ] {
        assert_eq!(batch.column(index).as_string_view().value(0), expected);
    }
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "GEOMETRYCOLLECTION(POINT(1 2))"
    );
    assert!(batch.column(5).as_boolean().value(0));
    assert_eq!(batch.column(6).as_primitive::<UInt8Type>().value(0), 4);

    let batch = collect_one(
        &ctx,
        r#"
        SELECT ST_Dump(
            ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))')
        ) AS dumped
        "#,
    )
    .await?;
    let dumps = batch.column(0).as_list::<i32>();
    let dumped_rows = dumps.value(0);
    let dumped_rows = dumped_rows.as_struct();
    let paths = dumped_rows.column(0).as_list::<i32>();
    assert_eq!(paths.len(), 1);
    let path_values = paths.value(0);
    let path = path_values.as_primitive::<Int32Type>();
    assert_eq!(path.values().as_ref(), &[1]);

    let batch = collect_one(
        &ctx,
        "SELECT ST_AsText(ST_GeomCollFromText('GEOMETRYCOLLECTION EMPTY', 4326)) AS empty",
    )
    .await?;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "GEOMETRYCOLLECTION EMPTY"
    );

    Ok(())
}

#[tokio::test]
async fn generic_wkt_preserves_mixed_geometry_collection_row_order() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT ST_AsText(ST_GeomFromText(wkt)) AS text
        FROM (
            VALUES
                ('POINT(3 4)'),
                ('GEOMETRYCOLLECTION(POINT(1 2))'),
                ('POINT(7 8)'),
                ('GEOMETRYCOLLECTION(POINT(5 6))')
        ) AS t(wkt)
        "#,
    )
    .await?;

    let texts = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..texts.len())
            .map(|index| texts.value(index))
            .collect::<Vec<_>>(),
        [
            "POINT(3 4)",
            "GEOMETRYCOLLECTION(POINT(1 2))",
            "POINT(7 8)",
            "GEOMETRYCOLLECTION(POINT(5 6))",
        ]
    );

    Ok(())
}

#[tokio::test]
async fn generic_wkb_preserves_mixed_geometry_collection_row_order() -> Result<()> {
    let ctx = context();
    let wkb = BinaryArray::from(vec![
        Some(POINT_ZM_WKB),
        Some(GEOMETRYCOLLECTION_POINT_ZM_WKB),
        Some(POINT_ZM_WKB),
        Some(GEOMETRYCOLLECTION_POINT_ZM_WKB),
        None,
    ]);
    let schema = Schema::new([Arc::new(Field::new("wkb", DataType::Binary, true))]);
    ctx.register_batch(
        "mixed_wkb",
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(wkb)]).unwrap(),
    )
    .unwrap();

    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            GeometryType(ST_GeomFromWKB(wkb)) AS geometry_type,
            ST_PointFromWKB(wkb) IS NULL AS point_is_null,
            ST_CoordDim(ST_GeomFromWKB(wkb)) AS coord_dim
        FROM mixed_wkb
        "#,
    )
    .await?;

    let types = batch.column(0).as_string_view();
    assert_eq!(
        (0..types.len())
            .map(|index| types.value(index))
            .collect::<Vec<_>>(),
        [
            "POINT",
            "GEOMETRYCOLLECTION",
            "POINT",
            "GEOMETRYCOLLECTION",
            "",
        ]
    );
    assert!(types.is_null(4));

    let point_is_null = batch.column(1).as_boolean();
    assert_eq!(
        (0..point_is_null.len())
            .map(|index| point_is_null.value(index))
            .collect::<Vec<_>>(),
        [false, true, false, true, true]
    );

    // The ZM ordinates survive even though PostGIS names these types without
    // the dimension suffix.
    let coord_dims = batch.column(2).as_primitive::<UInt8Type>();
    for index in 0..4 {
        assert_eq!(coord_dims.value(index), 4, "coord dim row {index}");
    }
    assert!(coord_dims.is_null(4));

    Ok(())
}

#[tokio::test]
async fn mixed_dimensions_empty_null_and_typed_mismatch_in_one_wkt_batch() -> Result<()> {
    let ctx = context();
    let wkt = StringArray::from(vec![
        Some("POINT(1 2)"),
        Some("POINT Z (3 4 5)"),
        Some("POINT M (6 7 8)"),
        Some("POINT ZM (9 10 11 12)"),
        None,
        Some("POINT EMPTY"),
        Some("LINESTRING(0 0,1 1)"),
        Some("GEOMETRYCOLLECTION(POINT(13 14))"),
    ]);
    let schema = Schema::new([Arc::new(Field::new("wkt", DataType::Utf8, true))]);
    ctx.register_batch(
        "mixed_wkt",
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(wkt)]).unwrap(),
    )
    .unwrap();

    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            GeometryType(ST_GeomFromText(wkt)) AS geometry_type,
            ST_AsText(ST_GeomFromText(wkt)) AS text,
            ST_PointFromText(wkt) IS NULL AS point_is_null,
            ST_AsText(ST_PointFromText(wkt)) AS point_text
        FROM mixed_wkt
        "#,
    )
    .await?;

    let types = batch.column(0).as_string_view();
    // PostGIS `GeometryType()` names measure-only geometries but never names
    // the Z or ZM dimension; the text expectations below still pin that the
    // Z and ZM ordinates survive the round trip.
    let expected_types = [
        Some("POINT"),
        Some("POINT"),
        Some("POINTM"),
        Some("POINT"),
        None,
        Some("POINT"),
        Some("LINESTRING"),
        Some("GEOMETRYCOLLECTION"),
    ];
    for (index, expected) in expected_types.into_iter().enumerate() {
        assert_eq!(types.is_null(index), expected.is_none(), "type row {index}");
        if let Some(expected) = expected {
            assert_eq!(types.value(index), expected, "type row {index}");
        }
    }

    let texts = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let expected_texts = [
        Some("POINT(1 2)"),
        Some("POINT Z(3 4 5)"),
        Some("POINT M(6 7 8)"),
        Some("POINT ZM(9 10 11 12)"),
        None,
        Some("POINT EMPTY"),
        Some("LINESTRING(0 0,1 1)"),
        Some("GEOMETRYCOLLECTION(POINT(13 14))"),
    ];
    for (index, expected) in expected_texts.into_iter().enumerate() {
        assert_eq!(texts.is_null(index), expected.is_none(), "text row {index}");
        if let Some(expected) = expected {
            assert_eq!(texts.value(index), expected, "text row {index}");
        }
    }

    let point_is_null = batch.column(2).as_boolean();
    assert_eq!(
        (0..point_is_null.len())
            .map(|index| point_is_null.value(index))
            .collect::<Vec<_>>(),
        [false, false, false, false, true, false, true, true]
    );

    let point_texts = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let expected_point_texts = [
        Some("POINT(1 2)"),
        Some("POINT Z(3 4 5)"),
        Some("POINT M(6 7 8)"),
        Some("POINT ZM(9 10 11 12)"),
        None,
        Some("POINT EMPTY"),
        None,
        None,
    ];
    for (index, expected) in expected_point_texts.into_iter().enumerate() {
        assert_eq!(
            point_texts.is_null(index),
            expected.is_none(),
            "typed text row {index}"
        );
        if let Some(expected) = expected {
            assert_eq!(point_texts.value(index), expected, "typed text row {index}");
        }
    }

    Ok(())
}

#[tokio::test]
async fn generic_io_behavior_is_preserved() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT ZM (1 2 3 4)')))) AS roundtrip,
            ST_AsText(ST_GeometryFromText('LINESTRING Z (0 0 1,3 4 5)')) AS alias_result,
            ST_AsText(ST_WKTToSQL('POLYGON EMPTY')) AS sql_mm_alias
        "#,
    )
    .await?;

    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "POINT ZM(1 2 3 4)");

    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "LINESTRING Z(0 0 1,3 4 5)");

    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "POLYGON EMPTY");

    Ok(())
}

const EWKB_POINT_4326_NDR: &str = "0101000020E6100000000000000000F03F0000000000000040";
const EWKB_POINT_4326_XDR: &str = "0020000001000010E63FF00000000000004000000000000000";
const EWKB_POINT_3857_NDR: &str = "0101000020110F0000000000000000F03F0000000000000040";
const EWKB_POINT_EMPTY_NDR: &str = "0101000000000000000000F87F000000000000F87F";
const EWKB_POINT_XY_NDR: &str = "0101000000000000000000F03F0000000000000040";
const EWKB_MULTIPOINT_EMPTY_MEMBER: &str =
    "0104000000010000000101000000000000000000F87F000000000000F87F";
const EWKB_MULTILINESTRING_EMPTY_MEMBER: &str = "010500000001000000010200000000000000";
const EWKB_MULTIPOLYGON_EMPTY_MEMBER: &str = "010600000001000000010300000000000000";

fn ewkb_srid(bytes: &[u8]) -> Option<i32> {
    let little_endian = match bytes[0] {
        0 => false,
        1 => true,
        value => panic!("unexpected EWKB byte order {value}"),
    };
    let mut type_bytes = [0u8; 4];
    type_bytes.copy_from_slice(&bytes[1..5]);
    let type_code = if little_endian {
        u32::from_le_bytes(type_bytes)
    } else {
        u32::from_be_bytes(type_bytes)
    };
    if type_code & 0x2000_0000 == 0 {
        return None;
    }
    let mut srid_bytes = [0u8; 4];
    srid_bytes.copy_from_slice(&bytes[5..9]);
    Some(if little_endian {
        i32::from_le_bytes(srid_bytes)
    } else {
        i32::from_be_bytes(srid_bytes)
    })
}

/// Decode with the independent `wkb` reader instead of the writer's own logic.
fn assert_ewkb_point(
    hex_value: &str,
    srid: Option<i32>,
    dim: geo_traits::Dimensions,
    expected: &[f64],
) {
    let bytes = hex::decode(hex_value).unwrap();
    assert_eq!(ewkb_srid(&bytes), srid);
    let parsed = wkb::reader::Wkb::try_new(&bytes).unwrap();
    assert_eq!(parsed.buf().len(), bytes.len());
    assert_eq!(parsed.dim(), dim);
    let NativeGeometryType::Point(point) = parsed.as_type() else {
        panic!("expected point");
    };
    let coord = point.coord().expect("expected a non-empty point");
    for (index, expected) in expected.iter().enumerate() {
        assert_eq!(coord.nth_or_panic(index), *expected);
    }
}

async fn assert_sql_error(ctx: &SessionContext, sql: &str, expected: &str) {
    let error = match ctx.sql(sql).await {
        Ok(dataframe) => dataframe
            .collect()
            .await
            .expect_err("query was expected to fail")
            .to_string(),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains(expected),
        "error {error:?} does not contain {expected:?}"
    );
}

#[tokio::test]
async fn ewkt_output_matches_postgis_reference() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        &format!(
            r#"
            SELECT
                ST_AsEWKT(ST_GeomFromEWKT('POINT(1 2)')) AS zero,
                ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;POINT(1 2)')) AS srid,
                ST_AsEWKT(ST_GeomFromEWKT('srid=4326 ;point(1 2)')) AS lower_and_space,
                ST_AsEWKT(ST_GeomFromEWKT('SRID=-1;POINT(1 2)')) AS negative,
                ST_AsEWKT(ST_GeomFromEWKT('SRID=1000000;POINT(1 2)')) AS folded,
                ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;POINT M (1 2 3)')) AS xym,
                ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;POINT Z (1 2 3)')) AS xyz,
                ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;POINT ZM (1 2 3 4)')) AS xyzm,
                ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;MULTIPOINT((1 2),(3 4))')) AS multipoint,
                ST_AsEWKT(ST_GeomFromEWKT('MULTIPOINT EMPTY')) AS multipoint_empty,
                ST_AsEWKT(ST_GeomFromEWKT('POINT Z EMPTY')) AS empty_z,
                ST_AsEWKT(ST_GeomFromEWKT('POINTM EMPTY')) AS empty_m,
                ST_AsEWKT(ST_GeomFromEWKT(
                    'SRID=4326;GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY)'
                )) AS collection,
                ST_AsEWKT(ST_GeomFromEWKT(
                    'SRID=4326;GEOMETRYCOLLECTION(POINT(1 2))'
                )) AS single_member_collection,
                ST_AsEWKT(ST_GeomFromEWKT(
                    'SRID=4326;GEOMETRYCOLLECTIONM(POINTM(1 2 3))'
                )) AS collection_m,
                ST_AsEWKT(ST_GeomFromEWKB(X'{EWKB_MULTIPOINT_EMPTY_MEMBER}')) AS empty_point_member,
                ST_AsEWKT(ST_GeomFromEWKB(X'{EWKB_MULTILINESTRING_EMPTY_MEMBER}')) AS empty_line_member
            "#
        ),
    )
    .await?;
    let expected = [
        "POINT(1 2)",
        "SRID=4326;POINT(1 2)",
        "SRID=4326;POINT(1 2)",
        "POINT(1 2)",
        "SRID=999001;POINT(1 2)",
        "SRID=4326;POINTM(1 2 3)",
        "SRID=4326;POINT(1 2 3)",
        "SRID=4326;POINT(1 2 3 4)",
        "SRID=4326;MULTIPOINT(1 2,3 4)",
        "MULTIPOINT EMPTY",
        "POINT EMPTY",
        "POINTM EMPTY",
        "SRID=4326;GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY)",
        "SRID=4326;GEOMETRYCOLLECTION(POINT(1 2))",
        "SRID=4326;GEOMETRYCOLLECTIONM(POINTM(1 2 3))",
        "MULTIPOINT(EMPTY)",
        "MULTILINESTRING(EMPTY)",
    ];
    for (index, expected) in expected.into_iter().enumerate() {
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), expected, "column {index}");
    }

    Ok(())
}

#[tokio::test]
async fn ewkt_number_formatting_matches_postgis() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_AsEWKT(ST_GeomFromEWKT('POINT(0.33333333333333333 1.23456789012345678)')) AS default_precision,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(1.123456789 2.987654321 3.555555555)'), 2) AS two_digits,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(1.5 2.5)'), 0) AS half_even,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(0.15 0.35)'), 1) AS shortest_digits,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(1.5 2.5)'), -3) AS negative_precision,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(1.23e-9 1.23e+16)')) AS scientific,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(9.6e+16 9.6e-9)'), 0) AS overflow,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(1.2345678901234567e-7 0)'), 100) AS full_shortest,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(0.1 0)'), 100) AS no_padding,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(1 2)'), CAST(NULL AS INT)) AS null_precision,
            ST_AsEWKT(ST_GeomFromEWKT(NULL)) AS null_geometry
        "#,
    )
    .await?;
    let expected = [
        Some("POINT(0.333333333333333 1.234567890123457)"),
        Some("POINT(1.12 2.99 3.56)"),
        Some("POINT(2 2)"),
        Some("POINT(0.2 0.4)"),
        Some("POINT(2 2)"),
        Some("POINT(1.23e-9 1.23e+16)"),
        Some("POINT(10e+16 10e-9)"),
        Some("POINT(0.00000012345678901234566 0)"),
        Some("POINT(0.1 0)"),
        None,
        None,
    ];
    for (index, expected) in expected.into_iter().enumerate() {
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.is_null(0), expected.is_none(), "column {index}");
        if let Some(expected) = expected {
            assert_eq!(values.value(0), expected, "column {index}");
        }
    }

    Ok(())
}

#[tokio::test]
async fn geom_from_ewkt_infers_postgis_dimensions() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_AsEWKT(ST_GeomFromEWKT('POINT(1 2 3)')) AS implicit_xyz,
            ST_AsEWKT(ST_GeomFromEWKT('POINT(1 2 3 4)')) AS implicit_xyzm,
            ST_AsEWKT(ST_GeomFromEWKT('MULTIPOINT((1 2 3),(4 5 6))')) AS implicit_multi,
            ST_AsEWKT(ST_GeomFromEWKT('GEOMETRYCOLLECTION(POINT(1 2 3))')) AS implicit_collection,
            ST_AsEWKT(ST_GeomFromEWKT('POINTM(1 2 3)')) AS explicit_measure,
            ST_AsEWKT(ST_GeomFromEWKT('GEOMETRYCOLLECTION(POINT M (1 2 3))')) AS measure_collection,
            ST_AsEWKT(ST_GeomFromEWKT(
                ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;POINT ZM (1 2 3 4)'))
            )) AS round_trip
        "#,
    )
    .await?;
    let expected = [
        "POINT(1 2 3)",
        "POINT(1 2 3 4)",
        "MULTIPOINT(1 2 3,4 5 6)",
        "GEOMETRYCOLLECTION(POINT(1 2 3))",
        "POINTM(1 2 3)",
        "GEOMETRYCOLLECTIONM(POINTM(1 2 3))",
        "SRID=4326;POINT(1 2 3 4)",
    ];
    for (index, expected) in expected.into_iter().enumerate() {
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), expected, "column {index}");
    }

    assert_sql_error(
        &ctx,
        "SELECT ST_GeomFromEWKT('LINESTRING(1 2,3 4 5)')",
        "mixes coordinate dimensions",
    )
    .await;
    assert_sql_error(
        &ctx,
        "SELECT ST_GeomFromEWKT('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(1 2 3,4 5 6))')",
        "mixes coordinate dimensions",
    )
    .await;

    Ok(())
}

const GEOMETRY_FAMILY_CASES: [(&str, &str, &str); 28] = [
    (
        "POINT(1 2)",
        "POINT(1 2)",
        "0101000020E6100000000000000000F03F0000000000000040",
    ),
    (
        "POINT Z(1 2 3)",
        "POINT(1 2 3)",
        "01010000A0E6100000000000000000F03F00000000000000400000000000000840",
    ),
    (
        "POINT M(1 2 3)",
        "POINTM(1 2 3)",
        "0101000060E6100000000000000000F03F00000000000000400000000000000840",
    ),
    (
        "POINT ZM(1 2 3 4)",
        "POINT(1 2 3 4)",
        "01010000E0E6100000000000000000F03F000000000000004000000000000008400000000000001040",
    ),
    (
        "LINESTRING(1 2,3 4)",
        "LINESTRING(1 2,3 4)",
        "0102000020E610000002000000000000000000F03F000000000000004000000000000008400000000000001040",
    ),
    (
        "LINESTRING Z(1 2 3,4 5 6)",
        "LINESTRING(1 2 3,4 5 6)",
        "01020000A0E610000002000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840",
    ),
    (
        "LINESTRING M(1 2 3,4 5 6)",
        "LINESTRINGM(1 2 3,4 5 6)",
        "0102000060E610000002000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840",
    ),
    (
        "LINESTRING ZM(1 2 3 4,5 6 7 8)",
        "LINESTRING(1 2 3 4,5 6 7 8)",
        "01020000E0E610000002000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C400000000000002040",
    ),
    (
        "POLYGON((1 2,3 4,5 6,1 2))",
        "POLYGON((1 2,3 4,5 6,1 2))",
        "0103000020E61000000100000004000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840000000000000F03F0000000000000040",
    ),
    (
        "POLYGON Z((1 2 3,4 5 6,7 8 9,1 2 3))",
        "POLYGON((1 2 3,4 5 6,7 8 9,1 2 3))",
        "01030000A0E61000000100000004000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C4000000000000020400000000000002240000000000000F03F00000000000000400000000000000840",
    ),
    (
        "POLYGON M((1 2 3,4 5 6,7 8 9,1 2 3))",
        "POLYGONM((1 2 3,4 5 6,7 8 9,1 2 3))",
        "0103000060E61000000100000004000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C4000000000000020400000000000002240000000000000F03F00000000000000400000000000000840",
    ),
    (
        "POLYGON ZM((1 2 3 4,5 6 7 8,9 10 11 12,1 2 3 4))",
        "POLYGON((1 2 3 4,5 6 7 8,9 10 11 12,1 2 3 4))",
        "01030000E0E61000000100000004000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C4000000000000020400000000000002240000000000000244000000000000026400000000000002840000000000000F03F000000000000004000000000000008400000000000001040",
    ),
    (
        "MULTIPOINT((1 2),(3 4))",
        "MULTIPOINT(1 2,3 4)",
        "0104000020E6100000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040",
    ),
    (
        "MULTIPOINT Z((1 2 3),(4 5 6))",
        "MULTIPOINT(1 2 3,4 5 6)",
        "01040000A0E6100000020000000101000080000000000000F03F000000000000004000000000000008400101000080000000000000104000000000000014400000000000001840",
    ),
    (
        "MULTIPOINT M((1 2 3),(4 5 6))",
        "MULTIPOINTM(1 2 3,4 5 6)",
        "0104000060E6100000020000000101000040000000000000F03F000000000000004000000000000008400101000040000000000000104000000000000014400000000000001840",
    ),
    (
        "MULTIPOINT ZM((1 2 3 4),(5 6 7 8))",
        "MULTIPOINT(1 2 3 4,5 6 7 8)",
        "01040000E0E61000000200000001010000C0000000000000F03F00000000000000400000000000000840000000000000104001010000C0000000000000144000000000000018400000000000001C400000000000002040",
    ),
    (
        "MULTILINESTRING((1 2,3 4))",
        "MULTILINESTRING((1 2,3 4))",
        "0105000020E610000001000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040",
    ),
    (
        "MULTILINESTRING Z((1 2 3,4 5 6))",
        "MULTILINESTRING((1 2 3,4 5 6))",
        "01050000A0E610000001000000010200008002000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840",
    ),
    (
        "MULTILINESTRING M((1 2 3,4 5 6))",
        "MULTILINESTRINGM((1 2 3,4 5 6))",
        "0105000060E610000001000000010200004002000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840",
    ),
    (
        "MULTILINESTRING ZM((1 2 3 4,5 6 7 8))",
        "MULTILINESTRING((1 2 3 4,5 6 7 8))",
        "01050000E0E61000000100000001020000C002000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C400000000000002040",
    ),
    (
        "MULTIPOLYGON(((1 2,3 4,5 6,1 2)))",
        "MULTIPOLYGON(((1 2,3 4,5 6,1 2)))",
        "0106000020E61000000100000001030000000100000004000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000001840000000000000F03F0000000000000040",
    ),
    (
        "MULTIPOLYGON Z(((1 2 3,4 5 6,7 8 9,1 2 3)))",
        "MULTIPOLYGON(((1 2 3,4 5 6,7 8 9,1 2 3)))",
        "01060000A0E61000000100000001030000800100000004000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C4000000000000020400000000000002240000000000000F03F00000000000000400000000000000840",
    ),
    (
        "MULTIPOLYGON M(((1 2 3,4 5 6,7 8 9,1 2 3)))",
        "MULTIPOLYGONM(((1 2 3,4 5 6,7 8 9,1 2 3)))",
        "0106000060E61000000100000001030000400100000004000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C4000000000000020400000000000002240000000000000F03F00000000000000400000000000000840",
    ),
    (
        "MULTIPOLYGON ZM(((1 2 3 4,5 6 7 8,9 10 11 12,1 2 3 4)))",
        "MULTIPOLYGON(((1 2 3 4,5 6 7 8,9 10 11 12,1 2 3 4)))",
        "01060000E0E61000000100000001030000C00100000004000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C4000000000000020400000000000002240000000000000244000000000000026400000000000002840000000000000F03F000000000000004000000000000008400000000000001040",
    ),
    (
        "GEOMETRYCOLLECTION(POINT(1 2))",
        "GEOMETRYCOLLECTION(POINT(1 2))",
        "0107000020E6100000010000000101000000000000000000F03F0000000000000040",
    ),
    (
        "GEOMETRYCOLLECTION Z(POINT Z(1 2 3))",
        "GEOMETRYCOLLECTION(POINT(1 2 3))",
        "01070000A0E6100000010000000101000080000000000000F03F00000000000000400000000000000840",
    ),
    (
        "GEOMETRYCOLLECTION M(POINT M(1 2 3))",
        "GEOMETRYCOLLECTIONM(POINTM(1 2 3))",
        "0107000060E6100000010000000101000040000000000000F03F00000000000000400000000000000840",
    ),
    (
        "GEOMETRYCOLLECTION ZM(POINT ZM(1 2 3 4))",
        "GEOMETRYCOLLECTION(POINT(1 2 3 4))",
        "01070000E0E61000000100000001010000C0000000000000F03F000000000000004000000000000008400000000000001040",
    ),
];

#[tokio::test]
async fn ewkt_output_covers_every_geometry_family_and_dimension() -> Result<()> {
    let ctx = context();
    let projections = GEOMETRY_FAMILY_CASES
        .iter()
        .enumerate()
        .map(|(index, (input, _, _))| format!("ST_AsEWKT(ST_GeomFromEWKT('{input}')) AS c{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let batch = collect_one(&ctx, &format!("SELECT {projections}")).await?;
    for (index, (input, expected, _)) in GEOMETRY_FAMILY_CASES.into_iter().enumerate() {
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), expected, "{input}");
    }

    Ok(())
}

#[tokio::test]
async fn ewkb_output_covers_every_geometry_family_and_dimension() -> Result<()> {
    let ctx = context();
    let projections = GEOMETRY_FAMILY_CASES
        .iter()
        .enumerate()
        .map(|(index, (input, _, _))| {
            format!("ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;{input}')) AS c{index}")
        })
        .collect::<Vec<_>>()
        .join(",");
    let batch = collect_one(&ctx, &format!("SELECT {projections}")).await?;
    for (index, (input, _, expected)) in GEOMETRY_FAMILY_CASES.into_iter().enumerate() {
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), expected, "{input}");
    }

    Ok(())
}

#[tokio::test]
async fn ewkb_and_hexewkb_output_matches_postgis_reference() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_AsHEXEWKB(ST_GeomFromEWKT('POINT(1 2)')) AS point_zero,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT(1 2)')) AS point_srid,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT(1 2)'), 'XDR') AS point_xdr,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT Z (1 2 3)')) AS point_z,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT M (1 2 3)')) AS point_m,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT ZM (1 2 3 4)')) AS point_zm,
            ST_AsHEXEWKB(ST_GeomFromEWKT(
                'SRID=4326;MULTIPOINT((1 2),(3 4))'
            )) AS multipoint,
            ST_AsHEXEWKB(ST_GeomFromEWKT(
                'SRID=4326;GEOMETRYCOLLECTION(POINT(1 2))'
            )) AS collection,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT EMPTY')) AS point_empty,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT Z EMPTY')) AS point_empty_z,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT M EMPTY')) AS point_empty_m,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT ZM EMPTY')) AS point_empty_zm,
            ST_AsHEXEWKB(ST_GeomFromEWKT(
                'SRID=4326;GEOMETRYCOLLECTION EMPTY'
            )) AS collection_empty,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT(1 2)'), 'xdr') AS xdr_lower,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT(1 2)'), 'XDRx') AS xdr_prefix,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT(1 2)'), 'Xdr') AS xdr_mixed,
            ST_AsHEXEWKB(ST_GeomFromEWKT('SRID=4326;POINT(1 2)'), 'junk') AS ndr_junk,
            ST_AsHEXEWKB(
                ST_GeomFromEWKT('SRID=4326;POINT(1 2)'),
                CAST(NULL AS VARCHAR)
            ) AS null_endian,
            ST_AsEWKB(ST_GeomFromEWKT('SRID=4326;POINT(1 2)')) AS binary
        "#,
    )
    .await?;
    let expected = [
        EWKB_POINT_XY_NDR,
        EWKB_POINT_4326_NDR,
        EWKB_POINT_4326_XDR,
        "01010000A0E6100000000000000000F03F00000000000000400000000000000840",
        "0101000060E6100000000000000000F03F00000000000000400000000000000840",
        "01010000E0E6100000000000000000F03F000000000000004000000000000008400000000000001040",
        "0104000020E6100000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040",
        "0107000020E6100000010000000101000000000000000000F03F0000000000000040",
        "0101000020E6100000000000000000F87F000000000000F87F",
        "01010000A0E6100000000000000000F87F000000000000F87F000000000000F87F",
        "0101000060E6100000000000000000F87F000000000000F87F000000000000F87F",
        "01010000E0E6100000000000000000F87F000000000000F87F000000000000F87F000000000000F87F",
        "0107000020E610000000000000",
        EWKB_POINT_4326_XDR,
        EWKB_POINT_4326_XDR,
        EWKB_POINT_4326_NDR,
        EWKB_POINT_4326_NDR,
    ];
    for (index, expected) in expected.into_iter().enumerate() {
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), expected, "column {index}");
    }
    let null_endian = batch
        .column(17)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(null_endian.is_null(0));

    let binary = batch
        .column(18)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(hex::encode_upper(binary.value(0)), EWKB_POINT_4326_NDR);

    assert_ewkb_point(
        &hex::encode_upper(binary.value(0)),
        Some(4326),
        geo_traits::Dimensions::Xy,
        &[1.0, 2.0],
    );
    assert_ewkb_point(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        Some(4326),
        geo_traits::Dimensions::Xyzm,
        &[1.0, 2.0, 3.0, 4.0],
    );
    assert_ewkb_point(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        Some(4326),
        geo_traits::Dimensions::Xy,
        &[1.0, 2.0],
    );

    Ok(())
}

#[tokio::test]
async fn geom_from_ewkt_reads_all_string_layouts() -> Result<()> {
    let ctx = context();
    let values = vec![
        Some("POINT(1 2)"),
        Some("SRID=-1;POINT(3 4)"),
        Some("srid=0;point(5 6)"),
        None,
        Some("POINT EMPTY"),
    ];
    let tables: [(&str, ArrayRef); 3] = [
        ("ewkt_utf8", Arc::new(StringArray::from(values.clone()))),
        (
            "ewkt_large",
            Arc::new(LargeStringArray::from(values.clone())),
        ),
        ("ewkt_view", Arc::new(StringViewArray::from(values.clone()))),
    ];
    for (name, array) in tables {
        let field = Field::new("ewkt", array.data_type().clone(), true);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new([Arc::new(field)])), vec![array]).unwrap();
        ctx.register_batch(name, batch).unwrap();
    }

    let expected = [
        Some("POINT(1 2)"),
        Some("POINT(3 4)"),
        Some("POINT(5 6)"),
        None,
        Some("POINT EMPTY"),
    ];
    for table in ["ewkt_utf8", "ewkt_large", "ewkt_view"] {
        let batch = collect_one(
            &ctx,
            &format!("SELECT ST_AsEWKT(ST_GeomFromEWKT(ewkt)) AS value FROM {table}"),
        )
        .await?;
        let texts = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (index, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                texts.is_null(index),
                expected.is_none(),
                "{table} row {index}"
            );
            if let Some(expected) = expected {
                assert_eq!(texts.value(index), expected, "{table} row {index}");
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn geom_from_ewkt_requires_matching_field_crs() -> Result<()> {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let field = Field::new("ewkt", DataType::Utf8, true)
        .with_extension_type(WktType::new(metadata.clone()));
    let ewkt = StringArray::from(vec![Some("SRID=4326;POINT(1 2)"), None]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(ewkt)],
    )
    .unwrap();
    ctx.register_batch("annotated_ewkt", batch).unwrap();

    let geometry_batch = collect_one(
        &ctx,
        "SELECT ST_GeomFromEWKT(ewkt) AS geom FROM annotated_ewkt",
    )
    .await?;
    assert_eq!(
        crs(geometry_batch.schema().field(0)),
        Crs::from_authority_code("EPSG:4326".to_string())
    );

    let batch = collect_one(
        &ctx,
        "SELECT ST_AsEWKT(ST_GeomFromEWKT(ewkt)) AS value FROM annotated_ewkt",
    )
    .await?;
    let texts = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(texts.value(0), "SRID=4326;POINT(1 2)");
    assert!(texts.is_null(1));

    let mismatched = StringArray::from(vec![Some("SRID=3857;POINT(3 4)")]);
    let field =
        Field::new("ewkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(mismatched)],
    )
    .unwrap();
    ctx.register_batch("mismatched_ewkt", batch).unwrap();
    assert_sql_error(
        &ctx,
        "SELECT ST_GeomFromEWKT(ewkt) FROM mismatched_ewkt",
        "does not match declared CRS SRID 4326",
    )
    .await;

    // A plain text column has no field-level CRS, so a non-zero EWKT SRID is rejected
    // instead of being silently dropped.
    let plain = StringArray::from(vec![Some("SRID=4326;POINT(1 2)")]);
    let field = Field::new("ewkt", DataType::Utf8, true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(plain)],
    )
    .unwrap();
    ctx.register_batch("plain_ewkt", batch).unwrap();
    assert_sql_error(
        &ctx,
        "SELECT ST_GeomFromEWKT(ewkt) FROM plain_ewkt",
        "does not match declared CRS SRID 0",
    )
    .await;

    assert_sql_error(
        &ctx,
        "SELECT ST_GeomFromEWKT('SRID=abc;POINT(1 2)')",
        "invalid EWKT SRID",
    )
    .await;
    assert_sql_error(
        &ctx,
        "SELECT ST_GeomFromEWKT('SRID=4326')",
        "missing a semicolon",
    )
    .await;
    assert_sql_error(
        &ctx,
        "SELECT ST_GeomFromEWKT('SRID=+4326;POINT(1 2)')",
        "invalid EWKT SRID",
    )
    .await;

    Ok(())
}

#[tokio::test]
async fn geom_from_ewkb_reads_all_binary_layouts() -> Result<()> {
    let ctx = context();
    let values: Vec<Option<Vec<u8>>> = vec![
        Some(hex::decode(EWKB_POINT_XY_NDR).unwrap()),
        Some(hex::decode(EWKB_POINT_EMPTY_NDR).unwrap()),
        None,
        // XDR with an explicit zero SRID flag.
        Some(hex::decode("002000000100000000401C0000000000004020000000000000").unwrap()),
    ];
    let refs = values
        .iter()
        .map(|value| value.as_deref())
        .collect::<Vec<_>>();
    let tables: [(&str, ArrayRef); 3] = [
        (
            "ewkb_binary",
            Arc::new(BinaryArray::from_iter(refs.iter().copied())),
        ),
        (
            "ewkb_large",
            Arc::new(LargeBinaryArray::from_iter(refs.iter().copied())),
        ),
        (
            "ewkb_view",
            Arc::new(BinaryViewArray::from_iter(refs.iter().copied())),
        ),
    ];
    for (name, array) in tables {
        let field = Field::new("ewkb", array.data_type().clone(), true);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new([Arc::new(field)])), vec![array]).unwrap();
        ctx.register_batch(name, batch).unwrap();
    }

    let expected = [
        Some("POINT(1 2)"),
        Some("POINT EMPTY"),
        None,
        Some("POINT(7 8)"),
    ];
    for table in ["ewkb_binary", "ewkb_large", "ewkb_view"] {
        let batch = collect_one(
            &ctx,
            &format!("SELECT ST_AsEWKT(ST_GeomFromEWKB(ewkb)) AS value FROM {table}"),
        )
        .await?;
        let texts = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (index, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                texts.is_null(index),
                expected.is_none(),
                "{table} row {index}"
            );
            if let Some(expected) = expected {
                assert_eq!(texts.value(index), expected, "{table} row {index}");
            }
        }
    }

    let batch = collect_one(
        &ctx,
        &format!(
            r#"
            SELECT
                ST_AsEWKT(ST_GeomFromEWKB(X'{EWKB_POINT_4326_NDR}')) AS ndr,
                ST_AsEWKT(ST_GeomFromEWKB(X'{EWKB_POINT_4326_XDR}')) AS xdr
            "#
        ),
    )
    .await?;
    let texts = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(texts.value(0), "SRID=4326;POINT(1 2)");
    let xdr = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(xdr.value(0), "SRID=4326;POINT(1 2)");

    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let field =
        Field::new("ewkb", DataType::Binary, true).with_extension_type(WkbType::new(metadata));
    let ewkb = BinaryArray::from_iter([Some(hex::decode(EWKB_POINT_4326_NDR).unwrap()), None]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(ewkb)],
    )
    .unwrap();
    ctx.register_batch("annotated_ewkb", batch).unwrap();
    let batch = collect_one(
        &ctx,
        "SELECT ST_AsEWKT(ST_GeomFromEWKB(ewkb)) AS value FROM annotated_ewkb",
    )
    .await?;
    let texts = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(texts.value(0), "SRID=4326;POINT(1 2)");
    assert!(texts.is_null(1));

    let batch = collect_one(
        &ctx,
        &format!(
            r#"
            SELECT
                ST_AsEWKT(ST_GeomFromEWKB(X'{EWKB_MULTIPOLYGON_EMPTY_MEMBER}')) AS ewkt,
                ST_AsEWKB(ST_GeomFromEWKB(X'{EWKB_MULTIPOLYGON_EMPTY_MEMBER}')) AS ewkb
            "#
        ),
    )
    .await?;
    let ewkt = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ewkt.value(0), "MULTIPOLYGON(EMPTY)");
    let ewkb = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(hex::encode(ewkb.value(0)), EWKB_MULTIPOLYGON_EMPTY_MEMBER);

    Ok(())
}

#[tokio::test]
async fn extended_io_rejects_mismatched_and_non_epsg_crs() -> Result<()> {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let field =
        Field::new("ewkb", DataType::Binary, true).with_extension_type(WkbType::new(metadata));
    let ewkb = BinaryArray::from_iter([Some(hex::decode(EWKB_POINT_3857_NDR).unwrap())]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(ewkb)],
    )
    .unwrap();
    ctx.register_batch("mismatched_ewkb", batch).unwrap();
    for sql in [
        "SELECT ST_GeomFromEWKB(ewkb) FROM mismatched_ewkb",
        "SELECT ST_AsEWKB(ewkb) FROM mismatched_ewkb",
        "SELECT ST_AsEWKT(ewkb) FROM mismatched_ewkb",
    ] {
        assert_sql_error(&ctx, sql, "does not match declared CRS SRID 4326").await;
    }

    let geometry = collect_one(
        &ctx,
        "SELECT ST_GeomFromEWKT('SRID=4326;POINT(1 2)') AS geom",
    )
    .await?;
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("OGC:CRS84".to_string()),
        None,
    ));
    let field = GeometryType::new(metadata).to_field("geom", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        geometry.columns().to_vec(),
    )
    .unwrap();
    ctx.register_batch("non_epsg_geometry", batch).unwrap();
    for sql in [
        "SELECT ST_AsEWKT(geom) FROM non_epsg_geometry",
        "SELECT ST_AsEWKB(geom) FROM non_epsg_geometry",
        "SELECT ST_AsHEXEWKB(geom) FROM non_epsg_geometry",
    ] {
        assert_sql_error(&ctx, sql, "requires an EPSG authority code").await;
    }

    Ok(())
}

#[tokio::test]
async fn ewkb_round_trip_preserves_geometry_identity() -> Result<()> {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            GeometryType(ST_GeomFromEWKB(ST_AsEWKB(ST_GeomFromEWKT(
                'SRID=4326;GEOMETRYCOLLECTION(POINT(1 2))'
            )))) AS collection_type,
            ST_AsEWKT(ST_GeomFromEWKB(ST_AsEWKB(ST_GeomFromEWKT(
                'SRID=4326;MULTIPOINT((1 2),(3 4))'
            )))) AS multipoint,
            ST_AsEWKT(ST_GeomFromEWKB(ST_AsEWKB(ST_GeomFromEWKT(
                'SRID=4326;MULTILINESTRING((0 0,1 1),(2 2,3 3))'
            )))) AS multiline,
            ST_AsEWKT(ST_GeomFromEWKB(ST_AsEWKB(ST_GeomFromEWKT(
                'SRID=4326;GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY)'
            )))) AS collection
        "#,
    )
    .await?;
    let values = batch.column(0).as_string_view();
    assert_eq!(values.value(0), "GEOMETRYCOLLECTION");
    let multipoint = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(multipoint.value(0), "SRID=4326;MULTIPOINT(1 2,3 4)");
    let multiline = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        multiline.value(0),
        "SRID=4326;MULTILINESTRING((0 0,1 1),(2 2,3 3))"
    );
    let collection = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        collection.value(0),
        "SRID=4326;GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY)"
    );

    Ok(())
}
