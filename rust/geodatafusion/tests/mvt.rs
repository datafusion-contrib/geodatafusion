//! MVT SQL and raw Arrow contracts.

mod support;

#[path = "support/mvt.rs"]
mod mvt_decode;

use std::sync::Arc;

#[cfg(feature = "geos-3_11")]
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
#[cfg(feature = "geos-3_11")]
use geoarrow_array::GeoArrowArray;
#[cfg(feature = "geos-3_11")]
use geoarrow_array::builder::PointBuilder;
#[cfg(feature = "geos-3_11")]
use geoarrow_schema::crs::Crs;
#[cfg(feature = "geos-3_11")]
use geoarrow_schema::{CoordType, Dimension, GeometryType, Metadata, PointType};
use mvt_decode::{DecodedGeometry, DecodedValue, decode_hex, decode_tile};
use support::context::context;
use support::geoarrow::collect;

#[cfg(feature = "geos-3_11")]
async fn text(ctx: &SessionContext, sql: &str) -> Option<String> {
    let batches = collect(ctx, sql).await.unwrap();
    let batch = batches.first().expect("query returned no batch");
    if batch.column(0).is_null(0) {
        None
    } else {
        Some(batch.column(0).as_string::<i32>().value(0).to_string())
    }
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn as_mvt_geom_transforms_points_lines_polygons_and_dimensions() {
    let ctx = context();
    let bounds = "ST_MakeBox2D(ST_Point(0, 0), ST_Point(4, 4))";

    assert_eq!(
        text(
            &ctx,
            &format!("SELECT ST_AsText(ST_AsMVTGeom(ST_GeomFromText('POINT(1 2)'), {bounds}))"),
        )
        .await
        .as_deref(),
        Some("POINT(1024 2048)")
    );
    assert_eq!(
        text(
            &ctx,
            &format!(
                "SELECT ST_AsText(ST_AsMVTGeom(ST_GeomFromText('LINESTRING(1 2,5 6)'), {bounds}))"
            ),
        )
        .await
        .as_deref(),
        Some("LINESTRING(1024 2048,3328 -256)")
    );
    assert!(
        bool_sql(
            &ctx,
            &format!(
                "SELECT ST_Area(ST_AsMVTGeom(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), {bounds})) = 16777216 \
                 AND ST_NPoints(ST_AsMVTGeom(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), {bounds})) = 5 \
                 AND ST_XMin(ST_AsMVTGeom(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), {bounds})) = 0 \
                 AND ST_YMin(ST_AsMVTGeom(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), {bounds})) = 0 \
                 AND ST_XMax(ST_AsMVTGeom(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), {bounds})) = 4096 \
                 AND ST_YMax(ST_AsMVTGeom(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), {bounds})) = 4096"
            ),
        )
        .await
    );
    assert_eq!(
        text(
            &ctx,
            &format!(
                "SELECT ST_AsText(ST_AsMVTGeom(ST_GeomFromText('POINT ZM(1 2 3 4)'), {bounds}))"
            ),
        )
        .await
        .as_deref(),
        Some("POINT ZM(1024 2048 3 4)")
    );
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn as_mvt_geom_clips_and_handles_nulls() {
    let ctx = context();
    let bounds = "ST_MakeBox2D(ST_Point(0, 0), ST_Point(4, 4))";

    assert_eq!(
        text(
            &ctx,
            &format!("SELECT ST_AsText(ST_AsMVTGeom(ST_GeomFromText('POINT(-0.1 2)'), {bounds}))"),
        )
        .await
        .as_deref(),
        Some("POINT(-102 2048)")
    );
    assert_eq!(
        text(
            &ctx,
            &format!("SELECT ST_AsText(ST_AsMVTGeom(ST_GeomFromText('POINT(-1 2)'), {bounds}))"),
        )
        .await,
        None
    );
    assert_eq!(
        text(
            &ctx,
            &format!(
                "SELECT ST_AsText(ST_AsMVTGeom(ST_GeomFromText('POINT(-1 2)'), {bounds}, 4096, 256, false))"
            ),
        )
        .await
        .as_deref(),
        Some("POINT(-1024 2048)")
    );
    assert!(
        bool_sql(
            &ctx,
            &format!("SELECT ST_AsMVTGeom(NULL, {bounds}) IS NULL"),
        )
        .await
    );
    for empty in [
        "POINT EMPTY",
        "LINESTRING EMPTY",
        "POLYGON EMPTY",
        "MULTIPOINT EMPTY",
        "GEOMETRYCOLLECTION EMPTY",
    ] {
        assert!(
            bool_sql(
                &ctx,
                &format!("SELECT ST_AsMVTGeom(ST_GeomFromText('{empty}'), {bounds}) IS NULL"),
            )
            .await,
            "{empty} must produce NULL"
        );
    }
}

#[cfg(feature = "geos-3_11")]
async fn bool_sql(ctx: &SessionContext, sql: &str) -> bool {
    let batches = collect(ctx, sql).await.unwrap();
    batches.first().unwrap().column(0).as_boolean().value(0)
}

#[tokio::test]
async fn as_mvt_encodes_features_semantically() {
    let ctx = context();
    let sql = "SELECT ST_AsMVT(row, 'layer', 4096, 'geom', 'id') \
               FROM (SELECT named_struct('id', 1, 'name', 'a', 'geom', ST_GeomFromText('POINT(1 2)')) AS row \
                     UNION ALL \
                     SELECT named_struct('id', 2, 'name', 'b', 'geom', ST_GeomFromText('LINESTRING(0 0,1 1)')) AS row) t";
    let batches = collect(&ctx, sql).await.unwrap();
    let batch = batches.first().unwrap();
    let tile = decode_tile(batch.column(0).as_binary::<i32>().value(0)).unwrap();
    let layer = tile.only_layer().expect("tile must contain one layer");
    assert_eq!(layer.name, "layer");
    assert_eq!(layer.extent, 4096);
    assert_eq!(layer.features.len(), 2);
    assert_eq!(layer.features[0].id, Some(1));
    assert_eq!(
        layer.features[0].properties.get("name"),
        Some(&DecodedValue::String("a".to_string()))
    );
    assert_eq!(
        layer.features[0].geometry,
        Some(DecodedGeometry::Points(vec![(1, 2)]))
    );
    assert_eq!(
        layer.features[1].geometry,
        Some(DecodedGeometry::Lines(vec![vec![(0, 0), (1, 1)]]))
    );
}

#[tokio::test]
async fn as_mvt_returns_empty_tile_for_no_rows() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_AsMVT(row, 'layer') FROM (SELECT named_struct('geom', ST_GeomFromText('POINT(1 2)')) AS row LIMIT 0) t",
    )
    .await
    .unwrap();
    let batch = batches.first().unwrap();
    if batch.num_rows() == 1 {
        assert_eq!(batch.column(0).as_binary::<i32>().value(0), b"");
    }
}

#[test]
fn decoder_rejects_truncated_tiles() {
    assert!(decode_hex("1a03ff").is_err());
}
#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn as_mvt_geom_preserves_nullable_false_and_optional_null_contracts() {
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let point_type = PointType::new(
        Dimension::XY,
        Arc::new(Metadata::new(crs.clone(), Default::default())),
    )
    .with_coord_type(CoordType::Separated);
    let mut builder = PointBuilder::with_capacity(point_type.clone(), 3);
    builder.push_point(Some(&geo::Point::new(1.0, 2.0)));
    builder.push_empty();
    builder.push_point(Some(&geo::Point::new(3.0, 4.0)));
    let points = builder.finish().slice(1, 2);

    let field = Arc::new(point_type.to_field("geom", false));
    let batch: RecordBatch = arrow_array::StructArray::new(
        arrow_schema::Fields::from(vec![Arc::clone(&field)]),
        vec![points.to_array_ref()],
        None,
    )
    .into();
    let ctx = context();
    ctx.register_batch("points", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_AsMVTGeom(geom, ST_MakeBox2D(ST_Point(0, 0), ST_Point(4, 4))) AS geom, \
                ST_X(ST_AsMVTGeom(geom, ST_MakeBox2D(ST_Point(0, 0), ST_Point(4, 4)))) AS x \
         FROM points",
    )
    .await
    .unwrap();
    let output_field = batches[0].schema().field(0).clone();
    assert_eq!(
        output_field
            .extension_type::<GeometryType>()
            .metadata()
            .crs(),
        &crs
    );
    assert_eq!(
        output_field.extension_type::<GeometryType>().coord_type(),
        CoordType::Separated
    );
    let x = batches[0]
        .column(1)
        .as_primitive::<arrow_array::types::Float64Type>();
    assert!(x.is_null(0));
    assert_eq!(x.value(1), 3072.0);

    let batches = collect(
        &ctx,
        "SELECT ST_AsMVTGeom( \
             geom, \
             ST_MakeBox2D(ST_Point(0, 0), ST_Point(4, 4)), \
             CAST(NULL AS BIGINT), \
             CAST(NULL AS BIGINT), \
             CAST(NULL AS BOOLEAN) \
         ) IS NULL AS is_null \
         FROM points",
    )
    .await
    .unwrap();
    let is_null = batches[0].column(0).as_boolean();
    assert!(is_null.value(0));
    assert!(!is_null.value(1));
}

#[tokio::test]
async fn as_mvt_merges_partial_states_and_omits_null_geometry_rows() {
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int64, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("wkt", DataType::Utf8, true)),
    ]));
    let batch = |rows: Vec<(i64, &str, Option<&str>)>| {
        let ids = arrow_array::Int64Array::from(rows.iter().map(|row| row.0).collect::<Vec<_>>());
        let names =
            arrow_array::StringArray::from(rows.iter().map(|row| row.1).collect::<Vec<_>>());
        let wkts = arrow_array::StringArray::from(
            rows.iter()
                .map(|row| row.2.map(str::to_string))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids), Arc::new(names), Arc::new(wkts)],
        )
        .unwrap()
    };
    let table = MemTable::try_new(
        Arc::clone(&schema),
        vec![
            vec![batch(vec![
                (1, "a", Some("POINT(1 2)")),
                (2, "b", Some("POINT(3 4)")),
            ])],
            vec![batch(vec![
                (3, "c", Some("LINESTRING(0 0,1 1)")),
                (4, "d", None),
            ])],
        ],
    )
    .unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table("points", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);

    let sql = "SELECT ST_AsMVT( \
                   named_struct('id', id, 'name', name, 'geom', ST_GeomFromText(wkt)), \
                   'layer', 4096, 'geom', 'id') \
               FROM points";
    let dataframe = ctx.sql(sql).await.unwrap();
    let plan = dataframe.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(plan_text.contains("mode=Partial"), "{plan_text}");
    assert!(plan_text.contains("mode=Final"), "{plan_text}");
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    let tile = decode_tile(batches[0].column(0).as_binary::<i32>().value(0)).unwrap();
    let layer = tile.only_layer().expect("tile must contain one layer");
    assert_eq!(layer.features.len(), 3);
    assert_eq!(
        layer
            .features
            .iter()
            .map(|feature| feature.id)
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(3)]
    );
    assert_eq!(
        layer
            .features
            .iter()
            .map(|feature| {
                feature
                    .properties
                    .get("name")
                    .cloned()
                    .expect("name property")
            })
            .collect::<Vec<_>>(),
        vec![
            DecodedValue::String("a".to_string()),
            DecodedValue::String("b".to_string()),
            DecodedValue::String("c".to_string()),
        ]
    );
}

#[tokio::test]
async fn as_mvt_reports_stable_argument_errors() {
    let ctx = context();
    for (sql, expected) in [
        (
            "SELECT ST_AsMVT(named_struct('id', 1, 'geom2', ST_Point(1, 2)), 'layer', 4096, 'geom')"
                .to_string(),
            "no geometry column found",
        ),
        (
            "SELECT ST_AsMVT(named_struct('id', 1.5, 'geom', ST_Point(1, 2)), 'layer', 4096, 'geom', 'id')"
                .to_string(),
            "feature id column",
        ),
        (
            "SELECT ST_AsMVT(named_struct('geom', ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))')), 'layer')"
                .to_string(),
            "GeometryCollection",
        ),
        (
            "SELECT ST_AsMVT(named_struct('attrs', named_struct('a', 1), 'geom', ST_Point(1, 2)), 'layer')"
                .to_string(),
            "unsupported attribute type",
        ),
    ] {
        let error = sql_error(&ctx, &sql).await;
        assert!(error.contains(expected), "{sql}\n{error}");
    }
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn as_mvt_geom_reports_stable_argument_errors() {
    let ctx = context();
    let bounds = "ST_MakeBox2D(ST_Point(0, 0), ST_Point(4, 4))";
    for (sql, expected) in [
        (
            format!("SELECT ST_AsMVTGeom(ST_Point(1, 2), {bounds}, 0, 256, true)"),
            "Extent must be greater than 0",
        ),
        (
            "SELECT ST_AsMVTGeom(ST_Point(1, 2), ST_MakeBox2D(ST_Point(1, 0), ST_Point(1, 4)))"
                .to_string(),
            "Geometric bounds are too small",
        ),
    ] {
        let error = sql_error(&ctx, &sql).await;
        assert!(error.contains(expected), "{sql}\n{error}");
    }
}

async fn sql_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(dataframe) => dataframe
            .collect()
            .await
            .expect_err("query unexpectedly succeeded")
            .to_string(),
        Err(error) => error.to_string(),
    }
}
