//! Clustering contracts through the public registration entry point.
//!
//! Expected values in this file come from the pinned PostGIS reference described in
//! `tests/postgis/README.md`; see `tests/postgis/clustering` for the reviewed baselines.

mod support;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{PointBuilder, WkbBuilder};
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{CoordType, Dimension, Metadata, PointType, WkbType};
use support::context::context;
use support::geoarrow::collect;

async fn ints(ctx: &SessionContext, sql: &str) -> Vec<Option<i32>> {
    let batches = collect(ctx, sql).await.unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column(batch.num_columns() - 1)
            .as_primitive::<Int32Type>();
        for row in 0..column.len() {
            values.push(if column.is_null(row) {
                None
            } else {
                Some(column.value(row))
            });
        }
    }
    values
}

/// Decode the `List<Struct{geom: geoarrow.wkb}>` aggregate result into WKT strings.
fn cluster_wkts(batch: &RecordBatch, column: usize) -> Vec<String> {
    let list = batch.column(column).as_list::<i32>();
    let schema = batch.schema();
    let DataType::List(item) = schema.field(column).data_type() else {
        panic!("clustering aggregate must return a list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("clustering aggregate list items must be structs");
    };
    let geom_field = fields
        .iter()
        .find(|field| field.name() == "geom")
        .expect("cluster struct must have a geom field");
    let geom_array = list.values().as_struct().column(0);
    let wrapped =
        geoarrow_array::array::from_arrow_array(geom_array.as_ref(), geom_field.as_ref()).unwrap();
    let wkt = geoarrow_array::cast::to_wkt::<i32>(wrapped.as_ref()).unwrap();
    let wkt = wkt.inner();
    (0..wkt.len())
        .map(|row| wkt.value(row).to_string())
        .collect()
}

const DBSCAN_INPUTS: &str = "WITH t(id, geom) AS (VALUES \
    (1, ST_GeomFromText('POINT (0 0)')), \
    (2, ST_GeomFromText('POINT (0 1)')), \
    (3, ST_GeomFromText('POINT (-0.5 0.5)')), \
    (4, ST_GeomFromText('POINT (1 0)')), \
    (5, ST_GeomFromText('POINT (1 1)')), \
    (6, ST_GeomFromText('POINT (1.0 0.5)')))";

#[tokio::test]
async fn registers_clustering_functions_with_real_kinds() {
    let ctx = context();
    let state = ctx.state();
    for name in ["st_clusterintersecting", "st_clusterwithin"] {
        assert!(
            state.aggregate_functions().contains_key(name),
            "{name} must be registered as an aggregate"
        );
    }
    for name in [
        "st_clusterdbscan",
        "st_clusterintersectingwin",
        "st_clusterkmeans",
        "st_clusterwithinwin",
    ] {
        assert!(
            state.window_functions().contains_key(name),
            "{name} must be registered as a window function"
        );
    }
}

#[tokio::test]
async fn dbscan_matches_reference_partition_membership() {
    let ctx = context();
    let values = ints(
        &ctx,
        &format!(
            "{DBSCAN_INPUTS} SELECT id, ST_ClusterDBSCAN(geom, 0.8, 1) OVER () \
             AS cid FROM t ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        values,
        [Some(0), Some(0), Some(0), Some(1), Some(1), Some(1)]
    );

    let values = ints(
        &ctx,
        &format!(
            "{DBSCAN_INPUTS} SELECT id, ST_ClusterDBSCAN(geom, 0.8, 4) OVER () \
             AS cid FROM t ORDER BY id"
        ),
    )
    .await;
    assert_eq!(values, [None, None, None, None, None, None]);

    let values = ints(
        &ctx,
        &format!(
            "{DBSCAN_INPUTS} SELECT id, ST_ClusterDBSCAN(geom, 0.6, 3) OVER () \
             AS cid FROM t ORDER BY id"
        ),
    )
    .await;
    assert_eq!(values, [None, None, None, Some(0), Some(0), Some(0)]);
}

#[tokio::test]
async fn dbscan_null_and_empty_follow_reference_ids() {
    let ctx = context();
    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText('POINT(0 0)')), \
         (2, ST_GeomFromText(NULL)), \
         (3, ST_GeomFromText('POINT EMPTY')), \
         (4, ST_GeomFromText('POINT(10 10)'))) \
         SELECT id, ST_ClusterDBSCAN(geom, 100, 1) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [Some(0), None, Some(2), Some(0)]);

    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText('POINT(0 0)')), \
         (2, ST_GeomFromText(NULL)), \
         (3, ST_GeomFromText('POINT EMPTY')), \
         (4, ST_GeomFromText('POINT(10 10)'))) \
         SELECT id, ST_ClusterDBSCAN(geom, 100, 2) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [Some(0), None, None, Some(0)]);
}

#[tokio::test]
async fn within_and_intersecting_windows_match_reference_membership() {
    let ctx = context();
    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText('LINESTRING (0 0, 1 1)')), \
         (2, ST_GeomFromText('LINESTRING (5 5, 4 4)')), \
         (3, ST_GeomFromText(NULL)), \
         (4, ST_GeomFromText('LINESTRING (0 0, -1 -1)')), \
         (5, ST_GeomFromText('LINESTRING (6 6, 7 7)')), \
         (6, ST_GeomFromText('POLYGON EMPTY')), \
         (7, ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))'))) \
         SELECT id, ST_ClusterWithinWin(geom, 1.4) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        values,
        [Some(0), Some(0), None, Some(0), Some(2), Some(3), Some(0)]
    );

    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText('LINESTRING (0 0, 1 1)')), \
         (2, ST_GeomFromText('LINESTRING (5 5, 4 4)')), \
         (3, ST_GeomFromText(NULL)), \
         (4, ST_GeomFromText('LINESTRING (0 0, -1 -1)')), \
         (5, ST_GeomFromText('LINESTRING (6 6, 7 7)')), \
         (6, ST_GeomFromText('POLYGON EMPTY')), \
         (7, ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))'))) \
         SELECT id, ST_ClusterIntersectingWin(geom) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        values,
        [Some(0), Some(0), None, Some(0), Some(2), Some(3), Some(0)]
    );
}

#[tokio::test]
async fn kmeans_matches_reference_ids_and_sentinels() {
    let ctx = context();
    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText(NULL)), \
         (2, ST_GeomFromText('POINT(1 1)')), \
         (3, ST_GeomFromText('POINT EMPTY')), \
         (4, ST_GeomFromText('POINT(0 0)')), \
         (5, ST_GeomFromText('POINT(4 4)'))) \
         SELECT id, ST_ClusterKMeans(geom, 1) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [Some(-1), Some(0), Some(-1), Some(0), Some(0)]);

    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText('POINT(0 0)')), \
         (2, ST_GeomFromText('POINT(1 0)')), \
         (3, ST_GeomFromText('POINT(10 0)'))) \
         SELECT id, ST_ClusterKMeans(geom, 2) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [Some(1), Some(1), Some(0)]);

    let values = ints(
        &ctx,
        "SELECT ST_ClusterKMeans(ST_GeomFromText('POINT(0 0)'), 0) OVER ()",
    )
    .await;
    assert_eq!(values, [None]);

    let values = ints(
        &ctx,
        "SELECT ST_ClusterKMeans(ST_GeomFromText('POINT(0 0)'), NULL) OVER ()",
    )
    .await;
    assert_eq!(values, [None]);
}

#[tokio::test]
async fn kmeans_uses_m_weights_and_max_radius() {
    let ctx = context();
    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText('POINT M (0 0 1)')), \
         (2, ST_GeomFromText('POINT M (1 0 1)')), \
         (3, ST_GeomFromText('POINT M (2 0 10000)'))) \
         SELECT id, ST_ClusterKMeans(geom, 1, 1) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [Some(1), Some(0), Some(0)]);

    let error = collect(
        &ctx,
        "SELECT ST_ClusterKMeans(ST_GeomFromText('POINT M (0 0 0)'), 1) OVER ()",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("weight in M less or equal to 0"), "{error}");
}

#[tokio::test]
async fn clustering_window_parameters_use_the_first_partition_row() {
    let ctx = context();
    let values = ints(
        &ctx,
        "WITH t(id, geom, distance) AS (VALUES \
         (1, ST_GeomFromText('POINT(0 0)'), 0.5), \
         (2, ST_GeomFromText('POINT(1 0)'), 0.5), \
         (3, ST_GeomFromText('POINT(10 0)'), 1000.0)) \
         SELECT id, ST_ClusterWithinWin(geom, distance) OVER () AS cid \
         FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [Some(0), Some(1), Some(2)]);

    let values = ints(
        &ctx,
        "WITH t(id, geom, eps, minpoints) AS (VALUES \
         (1, ST_GeomFromText('POINT(0 0)'), 0.5, 2), \
         (2, ST_GeomFromText('POINT(1 0)'), 1000.0, 2)) \
         SELECT id, ST_ClusterDBSCAN(geom, eps, minpoints) OVER () AS cid \
         FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [None, None]);
}

#[tokio::test]
async fn cluster_aggregates_return_geometry_collection_lists() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "WITH t(geom) AS (VALUES \
         (ST_GeomFromText('LINESTRING (0 0, 1 1)')), \
         (ST_GeomFromText('LINESTRING (5 5, 4 4)')), \
         (ST_GeomFromText(NULL)), \
         (ST_GeomFromText('LINESTRING (0 0, -1 -1)')), \
         (ST_GeomFromText('LINESTRING (6 6, 7 7)')), \
         (ST_GeomFromText('POLYGON EMPTY')), \
         (ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))'))) \
         SELECT ST_ClusterIntersecting(geom) AS clusters FROM t",
    )
    .await
    .unwrap();
    let list = batches[0].column(0).as_list::<i32>();
    assert_eq!(list.len(), 1);
    assert_eq!(list.value_length(0), 3);
    let field = batches[0].schema().field(0).clone();
    let DataType::List(item) = field.data_type() else {
        panic!("ST_ClusterIntersecting must return a list");
    };
    let DataType::Struct(item_fields) = item.data_type() else {
        panic!("clustering aggregate list items must be structs");
    };
    let geom_field = item_fields
        .iter()
        .find(|field| field.name() == "geom")
        .expect("cluster struct must have a geom field");
    assert_eq!(geom_field.extension_type_name(), Some("geoarrow.wkb"));
    let mut texts = cluster_wkts(&batches[0], 0);
    texts.sort();
    assert_eq!(
        texts,
        [
            "GEOMETRYCOLLECTION(LINESTRING(0 0,1 1),LINESTRING(5 5,4 4),LINESTRING(0 0,-1 -1),POLYGON((0 0,4 0,4 4,0 4,0 0)))".to_string(),
            "GEOMETRYCOLLECTION(LINESTRING(6 6,7 7))".to_string(),
            "GEOMETRYCOLLECTION(POLYGON EMPTY)".to_string(),
        ]
    );

    let batches = collect(
        &ctx,
        "WITH t(geom) AS (VALUES (ST_GeomFromText('POINT(0 0)'))) \
         SELECT ST_ClusterWithin(geom, 1.0) AS clusters FROM t",
    )
    .await
    .unwrap();
    let field = batches[0].schema().field(0).clone();
    let DataType::List(item) = field.data_type() else {
        panic!("ST_ClusterWithin must return a list");
    };
    let DataType::Struct(item_fields) = item.data_type() else {
        panic!("clustering aggregate list items must be structs");
    };
    let geom_field = item_fields
        .iter()
        .find(|field| field.name() == "geom")
        .expect("cluster struct must have a geom field");
    assert_eq!(
        geom_field.extension_type_name(),
        Some("geoarrow.wkb"),
        "cluster geom fields must keep the WKB extension"
    );
}

#[tokio::test]
async fn unnest_exposes_cluster_geom_to_geometry_consumers() {
    // The aggregate returns `List<Struct{geom: geoarrow.wkb}>`, mirroring ST_Dump,
    // so DataFusion's struct expansion keeps the geom metadata without an optimizer
    // rule and geometry-producing UDFs compose on `cluster.geom`.
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let points = point_array(&[Some((0.0, 0.0)), Some((10.0, 0.0))], Some(crs.clone()));
    let field = points.data_type().to_field("geometry", false);
    ctx.register_batch(
        "t",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![points.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_AsText(cluster.geom) AS wkt, ST_AsEWKT(cluster.geom) AS ewkt, \
                ST_AsText(ST_Force2D(cluster.geom)) AS force2d \
         FROM (SELECT unnest(ST_ClusterWithin(geometry, 1.0)) AS cluster FROM t) \
         ORDER BY ewkt",
    )
    .await
    .unwrap();
    let wkt = batches[0].column(0).as_string::<i32>();
    let ewkt = batches[0].column(1).as_string::<i32>();
    let force2d = batches[0].column(2).as_string::<i32>();
    assert_eq!(
        (0..wkt.len()).map(|row| wkt.value(row)).collect::<Vec<_>>(),
        [
            "GEOMETRYCOLLECTION(POINT(0 0))",
            "GEOMETRYCOLLECTION(POINT(10 0))"
        ]
    );
    assert_eq!(
        (0..ewkt.len())
            .map(|row| ewkt.value(row))
            .collect::<Vec<_>>(),
        [
            "SRID=4326;GEOMETRYCOLLECTION(POINT(0 0))",
            "SRID=4326;GEOMETRYCOLLECTION(POINT(10 0))"
        ]
    );
    assert_eq!(
        (0..force2d.len())
            .map(|row| force2d.value(row))
            .collect::<Vec<_>>(),
        [
            "GEOMETRYCOLLECTION(POINT(0 0))",
            "GEOMETRYCOLLECTION(POINT(10 0))"
        ]
    );

    #[cfg(feature = "geos-3_11")]
    {
        let buffers = collect(
            &ctx,
            "SELECT ST_AsText(ST_Buffer(cluster.geom, 1.0)) AS buffered \
             FROM (SELECT unnest(ST_ClusterWithin(geometry, 1.0)) AS cluster FROM t) \
             ORDER BY buffered",
        )
        .await
        .unwrap();
        let buffered = buffers[0].column(0).as_string::<i32>();
        assert_eq!(buffered.len(), 2);
        assert!(
            buffered.value(0).starts_with("POLYGON"),
            "{}",
            buffered.value(0)
        );
    }

    let batches = collect(
        &ctx,
        "SELECT ST_ClusterWithin(geometry, 1.0) AS clusters FROM t",
    )
    .await
    .unwrap();
    let field = batches[0].schema().field(0).clone();
    let DataType::List(item) = field.data_type() else {
        panic!("ST_ClusterWithin must return a list");
    };
    let DataType::Struct(item_fields) = item.data_type() else {
        panic!("clustering aggregate list items must be structs");
    };
    let geom_field = item_fields
        .iter()
        .find(|field| field.name() == "geom")
        .expect("cluster struct must have a geom field");
    let geometry_type = geom_field.extension_type::<WkbType>();
    assert_eq!(geometry_type.metadata().crs(), &crs);
}

#[tokio::test]
async fn unnest_preserves_crs_across_partitions_and_nulls() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let points = point_array(
        &[
            Some((0.0, 0.0)),
            Some((10.0, 0.0)),
            None,
            Some((0.0, 1.0)),
            None,
            None,
        ],
        Some(crs),
    );
    let categories = StringArray::from(vec![
        Some("a"),
        Some("a"),
        Some("a"),
        Some("b"),
        Some("b"),
        Some("c"),
    ]);
    ctx.register_batch(
        "t",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("category", DataType::Utf8, false),
                points.data_type().to_field("geometry", true),
            ])),
            vec![Arc::new(categories), points.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();

    let batches = collect(
        &ctx,
        "WITH clustered AS ( \
             SELECT category, ST_ClusterWithin(geometry, 1.0) AS clusters FROM t GROUP BY category \
         ) \
         SELECT category, ST_AsEWKT(cluster.geom) AS ewkt \
         FROM (SELECT category, unnest(clusters) AS cluster FROM clustered) \
         ORDER BY category, ewkt",
    )
    .await
    .unwrap();
    let categories = batches[0].column(0).as_string::<i32>();
    let ewkt = batches[0].column(1).as_string::<i32>();
    assert_eq!(
        (0..categories.len())
            .map(|row| categories.value(row))
            .collect::<Vec<_>>(),
        ["a", "a", "b"]
    );
    assert_eq!(
        (0..ewkt.len())
            .map(|row| ewkt.value(row))
            .collect::<Vec<_>>(),
        [
            "SRID=4326;GEOMETRYCOLLECTION(POINT(0 0))",
            "SRID=4326;GEOMETRYCOLLECTION(POINT(10 0))",
            "SRID=4326;GEOMETRYCOLLECTION(POINT(0 1))"
        ]
    );
}

#[tokio::test]
async fn planning_time_geometry_consumers_use_cluster_geom() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "WITH t(geom) AS (VALUES (ST_GeomFromText('POINT(0 0)'))) \
         SELECT ST_AsText(ST_Force2D(cluster.geom)) AS force2d FROM \
         (SELECT unnest(ST_ClusterIntersecting(geom)) AS cluster FROM t)",
    )
    .await
    .unwrap();
    let force2d = batches[0].column(0).as_string::<i32>();
    assert_eq!(force2d.value(0), "GEOMETRYCOLLECTION(POINT(0 0))");

    #[cfg(feature = "geos-3_11")]
    {
        let batches = collect(
            &ctx,
            "WITH t(geom) AS (VALUES (ST_GeomFromText('POINT(0 0)'))) \
             SELECT ST_AsText(ST_Buffer(cluster.geom, 1.0)) AS buffered FROM \
             (SELECT unnest(ST_ClusterIntersecting(geom)) AS cluster FROM t)",
        )
        .await
        .unwrap();
        let buffered = batches[0].column(0).as_string::<i32>();
        assert!(
            buffered.value(0).starts_with("POLYGON"),
            "{}",
            buffered.value(0)
        );
    }
}

#[tokio::test]
async fn cluster_aggregate_tolerance_errors_match_reference() {
    let ctx = context();
    let error = collect(
        &ctx,
        "SELECT ST_ClusterWithin(ST_GeomFromText('POINT(0 0)'), 0.0)",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("Tolerance not defined"), "{error}");

    let error = collect(
        &ctx,
        "SELECT ST_ClusterWithin(ST_GeomFromText('POINT(0 0)'), NULL)",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("Tolerance not defined"), "{error}");
}

#[tokio::test]
async fn dbscan_border_assignment_follows_window_order() {
    let ctx = context();
    let sql = "WITH t(id, geom) AS (VALUES \
        (1, ST_GeomFromText('POINT(0 0)')), \
        (2, ST_GeomFromText('POINT(-0.9 0)')), \
        (3, ST_GeomFromText('POINT(-0.89 0)')), \
        (4, ST_GeomFromText('POINT(-0.88 0)')), \
        (5, ST_GeomFromText('POINT(1.9 0)')), \
        (6, ST_GeomFromText('POINT(2.8 0)')), \
        (7, ST_GeomFromText('POINT(2.79 0)')), \
        (8, ST_GeomFromText('POINT(2.78 0)')), \
        (9, ST_GeomFromText('POINT(0.95 0)'))) \
        SELECT id, ST_ClusterDBSCAN(geom, 1.0, 4) OVER ({order}) AS cid \
        FROM t ORDER BY id";

    let ascending = ints(&ctx, &sql.replace("{order}", "ORDER BY id")).await;
    assert_eq!(ascending[8], ascending[0], "{ascending:?}");
    assert_ne!(ascending[8], ascending[4], "{ascending:?}");

    let descending = ints(&ctx, &sql.replace("{order}", "ORDER BY id DESC")).await;
    assert_eq!(descending[8], descending[4], "{descending:?}");
    assert_ne!(descending[8], descending[0], "{descending:?}");
}

#[tokio::test]
async fn kmeans_3d_and_centroid_membership_matches_reference() {
    let ctx = context();
    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText('POINT Z (0 0 1)')), \
         (2, ST_GeomFromText('POINT Z (0 0 5)')), \
         (3, ST_GeomFromText('POINT Z (0 0 7)'))) \
         SELECT id, ST_ClusterKMeans(geom, 2) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [Some(1), Some(0), Some(0)]);

    let values = ints(
        &ctx,
        "WITH t(id, geom) AS (VALUES \
         (1, ST_GeomFromText('LINESTRING (0 0, 10 0)')), \
         (2, ST_GeomFromText('LINESTRING (100 0, 110 0)')), \
         (3, ST_GeomFromText('POLYGON ((200 0, 210 0, 210 10, 200 10, 200 0))'))) \
         SELECT id, ST_ClusterKMeans(geom, 2) OVER () AS cid FROM t ORDER BY id",
    )
    .await;
    assert_eq!(values, [Some(1), Some(1), Some(0)]);
}

#[tokio::test]
async fn kmeans_respects_window_partitions() {
    let ctx = context();
    let values = ints(
        &ctx,
        "WITH t(part, id, geom) AS (VALUES \
         (1, 1, ST_GeomFromText('POINT(0 0)')), \
         (1, 2, ST_GeomFromText('POINT(1 0)')), \
         (1, 3, ST_GeomFromText('POINT(10 0)')), \
         (2, 1, ST_GeomFromText('POINT(50 0)')), \
         (2, 2, ST_GeomFromText('POINT(51 0)')), \
         (2, 3, ST_GeomFromText('POINT(100 0)'))) \
         SELECT id, ST_ClusterKMeans(geom, 2) OVER (PARTITION BY part) AS cid \
         FROM t ORDER BY part, id",
    )
    .await;
    assert_eq!(
        values,
        [Some(1), Some(1), Some(0), Some(1), Some(1), Some(0)]
    );
}

#[tokio::test]
async fn clustering_errors_are_explicit() {
    let ctx = context();
    for (sql, expected) in [
        (
            "SELECT ST_ClusterDBSCAN(ST_GeomFromText('POINT(0 0)'), -1, 1) OVER ()",
            "Tolerance must be a positive number, got -1",
        ),
        (
            "SELECT ST_ClusterDBSCAN(ST_GeomFromText('POINT(0 0)'), 1, -1) OVER ()",
            "Minpoints must be a positive number, got -1",
        ),
        (
            "SELECT ST_ClusterWithinWin(ST_GeomFromText('POINT(0 0)'), NULL) OVER ()",
            "Tolerance must be a positive number, got 0",
        ),
        (
            "SELECT ST_ClusterKMeans(geom, 3) OVER () FROM \
             (VALUES (ST_GeomFromText('POINT(0 0)')), (ST_GeomFromText('POINT(1 1)'))) t(geom)",
            "K (3) must be smaller than the number of rows in the group (2)",
        ),
    ] {
        let error = collect(&ctx, sql).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{sql}: {error}");
    }
}

#[tokio::test]
async fn aggregate_collections_require_one_dimension_per_cluster() {
    let ctx = context();
    let error = collect(
        &ctx,
        "SELECT ST_ClusterWithin(geom, 100.0) FROM \
         (VALUES (ST_GeomFromText('POINT M (0 0 5)')), \
                 (ST_GeomFromText('POINT Z (0 0 5)'))) t(geom)",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("lwcollection_construct: mixed dimension geometries"),
        "{error}"
    );

    let batches = collect(
        &ctx,
        "SELECT ST_ClusterWithin(geom, 0.1) FROM \
         (VALUES (ST_GeomFromText('POINT M (0 0 5)')), \
                 (ST_GeomFromText('POINT Z (10 10 5)'))) t(geom)",
    )
    .await
    .unwrap();
    assert_eq!(batches[0].column(0).as_list::<i32>().value_length(0), 2);
}

async fn session_with_partitions(partitions: usize) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);
    geodatafusion::register(&ctx);
    ctx
}

async fn two_partition_table(ctx: &SessionContext, sql: &str, split: usize) -> Arc<MemTable> {
    let batches = collect(ctx, sql).await.unwrap();
    let batch = &batches[0];
    let schema = batch.schema();
    let first = batch.slice(0, split);
    let second = batch.slice(split, batch.num_rows() - split);
    Arc::new(MemTable::try_new(schema, vec![vec![first], vec![second]]).unwrap())
}

#[tokio::test]
async fn aggregate_merge_across_partitions_matches_single_partition() {
    let source = context();
    let table = two_partition_table(
        &source,
        "SELECT geom, grp FROM (VALUES \
         (ST_GeomFromText('POINT(0 0)'), 1), \
         (ST_GeomFromText('POINT(0.1 0)'), 1), \
         (ST_GeomFromText('POINT(10 0)'), 1), \
         (ST_GeomFromText('POINT(10.1 0)'), 1), \
         (ST_GeomFromText('POINT(100 0)'), 2), \
         (ST_GeomFromText('POINT(100.1 0)'), 2)) t(geom, grp)",
        3,
    )
    .await;

    let partition_count = |partitioned: bool| {
        let table = Arc::clone(&table);
        async move {
            let ctx = session_with_partitions(if partitioned { 4 } else { 1 }).await;
            ctx.register_table("t", table).unwrap();
            let batches = collect(
                &ctx,
                "SELECT grp, ST_ClusterWithin(geom, 0.5) AS clusters \
                 FROM t GROUP BY grp ORDER BY grp",
            )
            .await
            .unwrap();
            let mut result = Vec::new();
            for batch in batches {
                let list = batch.column(1).as_list::<i32>();
                result.push(
                    (0..list.len())
                        .map(|row| list.value_length(row))
                        .collect::<Vec<_>>(),
                );
            }
            result
        }
    };

    let single = partition_count(false).await;
    let partitioned = partition_count(true).await;
    assert_eq!(single, partitioned);
    assert_eq!(single.concat(), [2, 1]);

    // Prove the partitioned plan really uses the partial -> merge path.
    let ctx = session_with_partitions(4).await;
    let provider: Arc<dyn datafusion::catalog::TableProvider> = table.clone();
    ctx.register_table("t", provider).unwrap();
    let explain = collect(
        &ctx,
        "EXPLAIN VERBOSE SELECT grp, ST_ClusterWithin(geom, 0.5) AS clusters \
         FROM t GROUP BY grp",
    )
    .await
    .unwrap();
    let plan_column = explain[0].column(1).as_string::<i32>();
    let plan = (0..plan_column.len())
        .map(|row| plan_column.value(row))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("Partial"), "{plan}");
}

#[tokio::test]
async fn window_functions_span_partitions_and_batches() {
    let source = context();
    let table = two_partition_table(
        &source,
        "SELECT grp, id, geom FROM (VALUES \
         (1, 1, ST_GeomFromText('POINT(0 0)')), \
         (1, 2, ST_GeomFromText('POINT(0.2 0)')), \
         (1, 3, ST_GeomFromText('POINT(10 0)')), \
         (2, 1, ST_GeomFromText('POINT(50 0)')), \
         (2, 2, ST_GeomFromText('POINT(50.2 0)')), \
         (2, 3, ST_GeomFromText('POINT(60 0)'))) t(grp, id, geom)",
        4,
    )
    .await;

    let compare = |partitioned: bool| {
        let table = Arc::clone(&table);
        async move {
            let ctx = session_with_partitions(if partitioned { 4 } else { 1 }).await;
            ctx.register_table("t", table).unwrap();
            let batches = collect(
                &ctx,
                "SELECT grp, id, ST_ClusterDBSCAN(geom, 0.5, 1) OVER (PARTITION BY grp) AS cid \
                 FROM t ORDER BY grp, id",
            )
            .await
            .unwrap();
            let mut result = Vec::new();
            for batch in batches {
                let values = batch.column(2).as_primitive::<Int32Type>();
                result.extend((0..values.len()).map(|row| {
                    if values.is_null(row) {
                        None
                    } else {
                        Some(values.value(row))
                    }
                }));
            }
            result
        }
    };

    let single = compare(false).await;
    let partitioned = compare(true).await;
    assert_eq!(single, partitioned);
    assert_eq!(
        single,
        [Some(0), Some(0), Some(1), Some(0), Some(0), Some(1)]
    );
}

fn point_array(
    points: &[Option<(f64, f64)>],
    crs: Option<Crs>,
) -> geoarrow_array::array::PointArray {
    let metadata = Arc::new(match crs {
        Some(crs) => Metadata::new(crs, None),
        None => Metadata::default(),
    });
    let typ = PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let points = points
        .iter()
        .map(|point| point.map(|(x, y)| geo::Point::new(x, y)))
        .collect::<Vec<_>>();
    PointBuilder::from_nullable_points(points.iter().map(Option::as_ref), typ).finish()
}

#[tokio::test]
async fn window_nullability_does_not_depend_on_input_nullability() {
    let ctx = context();
    let points = point_array(&[Some((0.0, 0.0)), Some((10.0, 0.0))], None);
    // The geometry input is declared non-nullable, but DBSCAN can still produce NULL
    // for noise rows; the output field must stay nullable.
    let geometry_field = points.data_type().to_field("geometry", false);
    let eps = Float64Array::from(vec![0.5, 0.5]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            geometry_field,
            Field::new("eps", DataType::Float64, true),
        ])),
        vec![points.to_array_ref(), Arc::new(eps)],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_ClusterDBSCAN(geometry, eps, 2) OVER () AS cid, \
                ST_ClusterDBSCAN(geometry, eps, 2) OVER () IS NULL AS noise \
         FROM t",
    )
    .await
    .unwrap();
    let field = batches[0].schema().field(0).clone();
    assert!(field.is_nullable(), "DBSCAN output must be nullable");
    assert_eq!(batches[0].column(0).null_count(), 2);
    let noise = batches[0].column(1).as_boolean();
    assert!(noise.value(0) && noise.value(1));
}

#[tokio::test]
async fn aggregate_output_list_keeps_crs_metadata() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let points = point_array(&[Some((0.0, 0.0)), Some((0.1, 0.0))], Some(crs.clone()));
    let field = points.data_type().to_field("geometry", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![points.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_ClusterIntersecting(geometry) AS clusters FROM t",
    )
    .await
    .unwrap();
    let field = batches[0].schema().field(0).clone();
    let DataType::List(item) = field.data_type() else {
        panic!("ST_ClusterIntersecting must return a list");
    };
    let DataType::Struct(item_fields) = item.data_type() else {
        panic!("clustering aggregate list items must be structs");
    };
    let geom_field = item_fields
        .iter()
        .find(|field| field.name() == "geom")
        .expect("cluster struct must have a geom field");
    let geometry_type = geom_field.extension_type::<WkbType>();
    assert_eq!(geometry_type.metadata().crs(), &crs);
}

const ALL_GEOMETRY_TYPES: &str = "WITH t(id, geom) AS (VALUES \
    (1, ST_GeomFromText('POINT (0 0)')), \
    (2, ST_GeomFromText('LINESTRING (10 0, 11 0)')), \
    (3, ST_GeomFromText('POLYGON ((20 0, 21 0, 21 1, 20 0))')), \
    (4, ST_GeomFromText('MULTIPOINT ((30 0), (30.1 0))')), \
    (5, ST_GeomFromText('MULTILINESTRING ((40 0, 41 0), (42 0, 43 0))')), \
    (6, ST_GeomFromText('MULTIPOLYGON (((50 0, 51 0, 51 1, 50 0)))')), \
    (7, ST_GeomFromText('GEOMETRYCOLLECTION (POINT (60 0))')))";

const NON_COLLECTION_GEOMETRY_TYPES: &str = "WITH t(id, geom) AS (VALUES \
    (1, ST_GeomFromText('POINT (0 0)')), \
    (2, ST_GeomFromText('LINESTRING (10 0, 11 0)')), \
    (3, ST_GeomFromText('POLYGON ((20 0, 21 0, 21 1, 20 0))')), \
    (4, ST_GeomFromText('MULTIPOINT ((30 0), (30.1 0))')), \
    (5, ST_GeomFromText('MULTILINESTRING ((40 0, 41 0), (42 0, 43 0))')), \
    (6, ST_GeomFromText('MULTIPOLYGON (((50 0, 51 0, 51 1, 50 0)))')))";

#[tokio::test]
async fn all_gearrow_geometry_types_are_clustered() {
    let ctx = context();
    let values = ints(
        &ctx,
        &format!(
            "{ALL_GEOMETRY_TYPES} \
             SELECT id, ST_ClusterIntersectingWin(geom) OVER (ORDER BY id) \
             FROM t ORDER BY id"
        ),
    )
    .await;
    assert_eq!(
        values,
        [
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6)
        ]
    );

    let batches = collect(
        &ctx,
        &format!(
            "{NON_COLLECTION_GEOMETRY_TYPES} \
             SELECT ST_ClusterIntersecting(geom ORDER BY id) FROM t"
        ),
    )
    .await
    .unwrap();
    let list = batches[0].column(0).as_list::<i32>();
    assert_eq!(list.value_length(0), 6);
    let mut texts = cluster_wkts(&batches[0], 0);
    texts.sort();
    assert!(
        texts.iter().any(|text| text.contains("MULTIPOLYGON")),
        "{texts:?}"
    );
}

/// WKB for `GEOMETRYCOLLECTION(POINT(0 0), GEOMETRYCOLLECTION(POINT(1 1)))`.
fn nested_collection_wkb() -> Vec<u8> {
    fn push_header(output: &mut Vec<u8>, geometry_type: u32, count: Option<u32>) {
        output.push(1);
        output.extend_from_slice(&geometry_type.to_le_bytes());
        if let Some(count) = count {
            output.extend_from_slice(&count.to_le_bytes());
        }
    }

    fn push_point(output: &mut Vec<u8>, x: f64, y: f64) {
        push_header(output, 1, None);
        output.extend_from_slice(&x.to_le_bytes());
        output.extend_from_slice(&y.to_le_bytes());
    }

    let mut wkb = Vec::new();
    push_header(&mut wkb, 7, Some(2));
    push_point(&mut wkb, 0.0, 0.0);
    push_header(&mut wkb, 7, Some(1));
    push_point(&mut wkb, 1.0, 1.0);
    wkb
}

#[tokio::test]
async fn nested_geometry_collections_are_preserved_from_typed_wkb() {
    // Native constructors reject nested collections, so feed the aggregate a
    // real typed WKB batch and keep the CRS on the geom field.
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let mut builder = WkbBuilder::<i32>::new(WkbType::new(Arc::new(Metadata::new(crs, None))));
    let nested = nested_collection_wkb();
    builder.push_wkb(Some(nested.as_slice())).unwrap();
    let array = builder.finish();
    let field = array.data_type().to_field("geometry", false);
    ctx.register_batch(
        "t",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![array.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_AsText(cluster.geom) AS wkt, ST_AsEWKT(cluster.geom) AS ewkt FROM \
         (SELECT unnest(ST_ClusterIntersecting(geometry)) AS cluster FROM t)",
    )
    .await
    .unwrap();
    let wkt = batches[0].column(0).as_string::<i32>();
    let ewkt = batches[0].column(1).as_string::<i32>();
    assert_eq!(
        wkt.value(0),
        "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0),GEOMETRYCOLLECTION(POINT(1 1))))"
    );
    assert_eq!(
        ewkt.value(0),
        "SRID=4326;GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0),GEOMETRYCOLLECTION(POINT(1 1))))"
    );
}

#[tokio::test]
async fn non_finite_coordinates_are_rejected_explicitly() {
    let ctx = context();
    let points = point_array(&[Some((f64::NAN, 0.0))], None);
    let field = points.data_type().to_field("geometry", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![points.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    let error = collect(
        &ctx,
        "SELECT ST_ClusterWithinWin(geometry, 0.5) OVER () FROM t",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("non-finite coordinates"),
        "expected an explicit non-finite error, got {error}"
    );
}

#[tokio::test]
async fn aggregate_tolerance_uses_the_first_input_row() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "WITH t(id, geom, distance) AS (VALUES \
         (1, ST_GeomFromText('POINT (0 0)'), 1000.0), \
         (2, ST_GeomFromText('POINT (1 0)'), 0.5), \
         (3, ST_GeomFromText('POINT (10 0)'), 0.5)) \
         SELECT ST_ClusterWithin(geom, distance ORDER BY id) AS clusters FROM t",
    )
    .await
    .unwrap();
    assert_eq!(batches[0].column(0).as_list::<i32>().value_length(0), 1);

    let batches = collect(
        &ctx,
        "WITH t(id, geom, distance) AS (VALUES \
         (1, ST_GeomFromText('POINT (0 0)'), 0.5), \
         (2, ST_GeomFromText('POINT (1 0)'), 1000.0), \
         (3, ST_GeomFromText('POINT (10 0)'), 0.5)) \
         SELECT ST_ClusterWithin(geom, distance ORDER BY id) AS clusters FROM t",
    )
    .await
    .unwrap();
    assert_eq!(batches[0].column(0).as_list::<i32>().value_length(0), 3);

    let error = collect(
        &ctx,
        "WITH t(id, geom, distance) AS (VALUES \
         (1, ST_GeomFromText('POINT (0 0)'), CAST(NULL AS DOUBLE)), \
         (2, ST_GeomFromText('POINT (1 0)'), 1000.0)) \
         SELECT ST_ClusterWithin(geom, distance ORDER BY id) FROM t",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("Tolerance not defined"), "{error}");
}

#[tokio::test]
async fn windows_and_aggregates_span_batches_in_one_partition() {
    let source = context();
    let batches = collect(
        &source,
        "SELECT id, geom FROM (VALUES \
         (1, ST_GeomFromText('POINT (0 0)')), \
         (2, ST_GeomFromText('POINT (0.2 0)')), \
         (3, ST_GeomFromText('POINT (10 0)')), \
         (4, ST_GeomFromText('POINT (10.2 0)'))) t(id, geom)",
    )
    .await
    .unwrap();
    let batch = &batches[0];
    let schema = batch.schema();
    let table = Arc::new(
        MemTable::try_new(
            schema,
            vec![vec![
                batch.slice(0, 1),
                batch.slice(1, 2),
                batch.slice(3, 1),
            ]],
        )
        .unwrap(),
    );
    let ctx = session_with_partitions(1).await;
    ctx.register_table("t", table).unwrap();

    let batches = collect(
        &ctx,
        "SELECT id, ST_ClusterWithinWin(geom, 0.5) OVER (ORDER BY id) AS cid \
         FROM t ORDER BY id",
    )
    .await
    .unwrap();
    let values = batches[0].column(1).as_primitive::<Int32Type>();
    assert_eq!(
        (0..values.len())
            .map(|row| Some(values.value(row)))
            .collect::<Vec<_>>(),
        [Some(0), Some(0), Some(1), Some(1)]
    );

    let batches = collect(
        &ctx,
        "SELECT ST_ClusterWithin(geom, 0.5 ORDER BY id) AS clusters FROM t",
    )
    .await
    .unwrap();
    assert_eq!(batches[0].column(0).as_list::<i32>().value_length(0), 2);
}

#[tokio::test]
async fn mixed_crs_columns_cannot_be_detected_at_runtime() {
    let ctx = context();
    let first = point_array(
        &[Some((0.0, 0.0))],
        Some(Crs::from_authority_code("EPSG:4326".to_string())),
    );
    let second = point_array(
        &[Some((1.0, 0.0))],
        Some(Crs::from_authority_code("EPSG:3857".to_string())),
    );
    ctx.register_batch(
        "a",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                first.data_type().to_field("geometry", true),
            ])),
            vec![first.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();
    ctx.register_batch(
        "b",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                second.data_type().to_field("geometry", true),
            ])),
            vec![second.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();
    let batches = collect(
        &ctx,
        "SELECT ST_ClusterIntersecting(geometry) AS clusters FROM \
         (SELECT geometry FROM a UNION ALL SELECT geometry FROM b)",
    )
    .await
    .unwrap();
    let field = batches[0].schema().field(0).clone();
    let DataType::List(item) = field.data_type() else {
        panic!("ST_ClusterIntersecting must return a list");
    };
    let DataType::Struct(item_fields) = item.data_type() else {
        panic!("clustering aggregate list items must be structs");
    };
    let geom_field = item_fields
        .iter()
        .find(|field| field.name() == "geom")
        .expect("cluster struct must have a geom field");
    let crs = geom_field
        .extension_type::<WkbType>()
        .metadata()
        .crs()
        .clone();
    // DataFusion's type union keeps a single field for the combined column, so the
    // function cannot observe that the two inputs had different CRS values. This test
    // pins that limitation instead of claiming per-row CRS preservation.
    assert!(
        crs == Crs::from_authority_code("EPSG:4326".to_string())
            || crs == Crs::from_authority_code("EPSG:3857".to_string()),
        "unexpected coerced CRS: {crs:?}"
    );
}
