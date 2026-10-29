#![cfg(feature = "geos-3_11")]

mod support;

use std::sync::Arc;

#[cfg(feature = "geos-3_12")]
use arrow_array::Int32Array;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use geo_traits::{
    CoordTrait, Dimensions, GeometryTrait, GeometryType, LineStringTrait, MultiPolygonTrait,
    PolygonTrait,
};
#[cfg(feature = "geos-3_12")]
use geoarrow_array::IntoArrow;
use geoarrow_array::array::GeometryArray;
#[cfg(feature = "geos-3_12")]
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
#[cfg(feature = "geos-3_12")]
use geoarrow_schema::{CoordType, Dimension, Metadata, PolygonType};
use geoarrow_schema::{Crs, GeometryType as GeoArrowGeometryType};
use support::context::context;
#[cfg(feature = "geos-3_12")]
use wkt::types::{
    Coord, Dimension as WktDimension, LineString as WktLineString, Polygon as WktPolygon,
};

fn coord_values(coord: &impl CoordTrait<T = f64>) -> Vec<f64> {
    (0..coord.dim().size())
        .map(|index| coord.nth_or_panic(index))
        .collect()
}

fn polygon_rings(array: &GeometryArray, index: usize) -> Vec<Vec<Vec<f64>>> {
    let geometry = array.value(index).expect("expected a non-null geometry");
    let GeometryType::Polygon(polygon) = geometry.as_type() else {
        panic!("expected Polygon");
    };
    let mut rings = Vec::new();
    if let Some(exterior) = polygon.exterior() {
        rings.push(
            exterior
                .coords()
                .map(|coord| coord_values(&coord))
                .collect(),
        );
    }
    for interior in polygon.interiors() {
        rings.push(
            interior
                .coords()
                .map(|coord| coord_values(&coord))
                .collect(),
        );
    }
    rings
}

fn canonical_ring(ring: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let closed = ring.first() == ring.last();
    let open = if closed {
        &ring[..ring.len() - 1]
    } else {
        ring
    };
    if open.is_empty() {
        return ring.to_vec();
    }

    let reversed = open.iter().rev().cloned().collect::<Vec<_>>();
    let mut candidates = Vec::new();
    for source in [open, reversed.as_slice()] {
        for offset in 0..source.len() {
            let mut candidate = Vec::with_capacity(source.len());
            candidate.extend_from_slice(&source[offset..]);
            candidate.extend_from_slice(&source[..offset]);
            candidates.push(candidate);
        }
    }
    let mut best = candidates
        .into_iter()
        .min_by(|left, right| left.partial_cmp(right).unwrap())
        .unwrap();
    if closed {
        best.push(best[0].clone());
    }
    best
}

fn output_geometry(batch: &RecordBatch) -> GeometryArray {
    GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0))).unwrap()
}

fn geometry_column(batch: &RecordBatch, index: usize) -> GeometryArray {
    GeometryArray::try_from((
        batch.column(index).as_ref(),
        batch.schema_ref().field(index),
    ))
    .unwrap()
}

#[cfg(feature = "geos-3_12")]
fn rectangle(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> WktPolygon<f64> {
    WktPolygon::new(
        vec![WktLineString::new(
            vec![
                Coord {
                    x: min_x,
                    y: min_y,
                    z: None,
                    m: None,
                },
                Coord {
                    x: max_x,
                    y: min_y,
                    z: None,
                    m: None,
                },
                Coord {
                    x: max_x,
                    y: max_y,
                    z: None,
                    m: None,
                },
                Coord {
                    x: min_x,
                    y: max_y,
                    z: None,
                    m: None,
                },
                Coord {
                    x: min_x,
                    y: min_y,
                    z: None,
                    m: None,
                },
            ],
            WktDimension::XY,
        )],
        WktDimension::XY,
    )
}

#[cfg(feature = "geos-3_12")]
fn window_pair_batch(order: &[usize]) -> RecordBatch {
    let rectangles = [
        rectangle(0.0, 0.0, 2.0, 2.0),
        rectangle(2.0, 0.0, 4.0, 2.0),
        rectangle(10.0, 0.0, 12.0, 2.0),
        rectangle(12.0, 0.0, 14.0, 2.0),
    ];
    let polygons = order
        .iter()
        .map(|&index| rectangles[index].clone())
        .collect::<Vec<_>>();
    let polygon_type = PolygonType::new(Dimension::XY, Arc::new(Metadata::default()))
        .with_coord_type(CoordType::Interleaved);
    let array = PolygonBuilder::from_polygons(&polygons, polygon_type).finish();
    let geometry_field = Arc::new(array.extension_type().to_field("geom", false));
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(Field::new("grp", DataType::Int32, false)),
        geometry_field,
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                order
                    .iter()
                    .map(|&index| index as i32 + 1)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                order
                    .iter()
                    .map(|&index| if index < 2 { 1 } else { 2 })
                    .collect::<Vec<_>>(),
            )),
            array.into_array_ref(),
        ],
    )
    .unwrap()
}

#[cfg(feature = "geos-3_12")]
fn pair_context(order: &[usize], split: usize) -> SessionContext {
    let batch = window_pair_batch(order);
    let schema = batch.schema();
    let first = batch.slice(0, split);
    let second = batch.slice(split, batch.num_rows() - split);
    let table = MemTable::try_new(schema, vec![vec![first], vec![second]]).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table("pairs", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);
    ctx
}

#[cfg(feature = "geos-3_12")]
async fn window_invalid_edge_lengths(ctx: &SessionContext, over: &str) -> Vec<Option<f64>> {
    let sql = format!(
        "SELECT ST_Length(ST_CoverageInvalidEdges(ST_GeomFromText(wkt), 0.0) OVER ({over})) AS len \
         FROM ( \
             SELECT 1 AS id, 1 AS grp, 'POLYGON((0 0,1 0,1 1,0 1,0 0))' AS wkt \
             UNION ALL \
             SELECT 2, 1, 'POLYGON((0.5 0,1.5 0,1.5 1,0.5 1,0.5 0))' \
             UNION ALL \
             SELECT 3, 2, 'POLYGON((10 0,11 0,11 1,10 1,10 0))' \
         ) AS input \
         ORDER BY id"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut values = Vec::new();
    for batch in &batches {
        let lengths = batch
            .column(0)
            .as_primitive::<arrow_array::types::Float64Type>();
        for row in 0..lengths.len() {
            values.push((!lengths.is_null(row)).then(|| lengths.value(row)));
        }
    }
    values
}

#[cfg(feature = "geos-3_12")]
async fn window_simplified_areas(ctx: &SessionContext, over: &str) -> Vec<Option<f64>> {
    let sql = format!(
        "SELECT ST_Area(ST_CoverageSimplify(ST_GeomFromText(wkt), 0.5, false) OVER ({over})) AS area \
         FROM ( \
             SELECT 1 AS id, 1 AS grp, 'POLYGON((0 0,1 0,1 1,0.5 1.2,0 1,0 0))' AS wkt \
             UNION ALL \
             SELECT 2, 1, 'POLYGON((1 0,2 0,2 1,1.5 1.2,1 1,1 0))' \
             UNION ALL \
             SELECT 3, 2, 'POLYGON((10 0,11 0,11 1,10 1,10 0))' \
         ) AS input \
         ORDER BY id"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut values = Vec::new();
    for batch in &batches {
        let areas = batch
            .column(0)
            .as_primitive::<arrow_array::types::Float64Type>();
        for row in 0..areas.len() {
            values.push((!areas.is_null(row)).then(|| areas.value(row)));
        }
    }
    values
}

#[cfg(feature = "geos-3_12")]
fn polygon_area(rings: &[Vec<Vec<f64>>]) -> f64 {
    let ring_area = |ring: &[Vec<f64>]| {
        ring.windows(2)
            .map(|segment| {
                let [x0, y0] = [segment[0][0], segment[0][1]];
                let [x1, y1] = [segment[1][0], segment[1][1]];
                x0 * y1 - x1 * y0
            })
            .sum::<f64>()
            / 2.0
    };
    let mut area = ring_area(&rings[0]).abs();
    for hole in &rings[1..] {
        area -= ring_area(hole).abs();
    }
    area
}

#[cfg(feature = "geos-3_12")]
fn segment_key(left: [f64; 2], right: [f64; 2]) -> ((u64, u64), (u64, u64)) {
    let left = (left[0].to_bits(), left[1].to_bits());
    let right = (right[0].to_bits(), right[1].to_bits());
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

#[cfg(feature = "geos-3_12")]
fn ring_segments(ring: &[Vec<f64>]) -> std::collections::BTreeSet<((u64, u64), (u64, u64))> {
    ring.windows(2)
        .map(|segment| {
            segment_key(
                [segment[0][0], segment[0][1]],
                [segment[1][0], segment[1][1]],
            )
        })
        .collect()
}

fn make_batch(schema: SchemaRef, groups: &[&str], wkts: &[Option<&str>]) -> RecordBatch {
    let groups = Arc::new(StringArray::from(groups.to_vec())) as ArrayRef;
    let wkts = Arc::new(StringArray::from(wkts.to_vec())) as ArrayRef;
    RecordBatch::try_new(schema, vec![groups, wkts]).unwrap()
}

fn partitioned_context() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("grp", DataType::Utf8, false)),
        Arc::new(Field::new("wkt", DataType::Utf8, true)),
    ]));
    let partition_a = vec![
        make_batch(
            Arc::clone(&schema),
            &["a"],
            &[Some("POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        ),
        make_batch(
            Arc::clone(&schema),
            &["a"],
            &[Some("POLYGON((1 0,2 0,2 1,1 1,1 0))")],
        ),
    ];
    let partition_b = vec![
        make_batch(
            Arc::clone(&schema),
            &["b"],
            &[Some("POLYGON((10 0,11 0,11 1,10 1,10 0))")],
        ),
        make_batch(
            Arc::clone(&schema),
            &["b"],
            &[Some("POLYGON((11 0,12 0,12 1,11 1,11 0))")],
        ),
    ];
    let table = MemTable::try_new(schema, vec![partition_a, partition_b]).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table("cover", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);
    ctx
}

fn grouped_context() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("grp", DataType::Utf8, false)),
        Arc::new(Field::new("wkt", DataType::Utf8, true)),
    ]));
    let partition_a = vec![
        make_batch(
            Arc::clone(&schema),
            &["a", "b", "empty"],
            &[
                Some("POLYGON((0 0,1 0,1 1,0 1,0 0))"),
                Some("POLYGON((10 0,11 0,11 1,10 1,10 0))"),
                None,
            ],
        ),
        make_batch(
            Arc::clone(&schema),
            &["a"],
            &[Some("POLYGON((1 0,2 0,2 1,1 1,1 0))")],
        ),
    ];
    let partition_b = vec![
        make_batch(
            Arc::clone(&schema),
            &["b", "z"],
            &[
                Some("POLYGON((11 0,12 0,12 1,11 1,11 0))"),
                Some("POLYGON((20 0,21 0,21 1,20 1,20 0))"),
            ],
        ),
        make_batch(Arc::clone(&schema), &["empty"], &[Some("POLYGON EMPTY")]),
    ];
    let table = MemTable::try_new(schema, vec![partition_a, partition_b]).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table("cover_groups", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);
    ctx
}

fn aggregate_plan_state(
    plan: &Arc<dyn ExecutionPlan>,
    partial: &mut usize,
    final_count: &mut usize,
    state_type: &mut Option<DataType>,
) {
    if let Some(aggregate) = (plan.as_ref() as &dyn std::any::Any).downcast_ref::<AggregateExec>() {
        match aggregate.mode() {
            AggregateMode::Partial => {
                *partial += 1;
                *state_type = aggregate.aggr_expr()[0]
                    .state_fields()
                    .ok()
                    .and_then(|fields| fields.first().map(|field| field.data_type().clone()));
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => *final_count += 1,
            _ => {}
        }
    }
    for child in plan.children() {
        aggregate_plan_state(child, partial, final_count, state_type);
    }
}

#[tokio::test]
async fn coverage_union_merges_partial_state_across_partitions() {
    let ctx = partitioned_context();
    let dataframe = ctx
        .sql("SELECT ST_CoverageUnion(ST_GeomFromText(wkt)) AS geom FROM cover")
        .await
        .unwrap();
    let plan = dataframe.create_physical_plan().await.unwrap();
    let (mut partial, mut final_count, mut state_type) = (0, 0, None);
    aggregate_plan_state(&plan, &mut partial, &mut final_count, &mut state_type);
    assert_eq!(
        (partial, final_count),
        (1, 1),
        "expected Partial -> Final aggregation:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    let DataType::List(item) = state_type.expect("partial aggregate state field") else {
        panic!("coverage union state must be a list");
    };
    assert_eq!(item.data_type(), &DataType::Binary);

    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = &batches[0];
    let geometry = output_geometry(batch);
    let value = geometry.value(0).unwrap();
    let GeometryType::MultiPolygon(multi_polygon) = value.as_type() else {
        panic!("expected a MultiPolygon from disjoint coverage components");
    };
    assert_eq!(multi_polygon.num_polygons(), 2);

    let area = ctx
        .sql("SELECT ST_Area(ST_CoverageUnion(ST_GeomFromText(wkt))) FROM cover")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        area[0]
            .column(0)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(0),
        4.0
    );
}

#[tokio::test]
async fn coverage_union_group_by_partial_final_and_empty_inputs() {
    let ctx = grouped_context();
    let dataframe = ctx
        .sql(
            "SELECT grp, \
                    ST_CoverageUnion(ST_GeomFromText(wkt)) AS geom, \
                    ST_Area(ST_CoverageUnion(ST_GeomFromText(wkt))) AS area \
             FROM cover_groups \
             GROUP BY grp \
             ORDER BY grp",
        )
        .await
        .unwrap();
    let plan = dataframe.create_physical_plan().await.unwrap();
    let (mut partial, mut final_count, mut state_type) = (0, 0, None);
    aggregate_plan_state(&plan, &mut partial, &mut final_count, &mut state_type);
    assert!(
        partial >= 1 && final_count >= 1,
        "expected Partial -> Final group aggregation:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    let DataType::List(item) = state_type.expect("partial aggregate state field") else {
        panic!("coverage union state must be a list");
    };
    assert_eq!(item.data_type(), &DataType::Binary);

    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    let batch = &batches[0];
    let groups = batch.column(0).as_string::<i32>();
    let areas = batch
        .column(2)
        .as_primitive::<arrow_array::types::Float64Type>();
    let geometry = geometry_column(batch, 1);
    assert_eq!(
        (0..4).map(|row| groups.value(row)).collect::<Vec<_>>(),
        vec!["a", "b", "empty", "z"]
    );
    assert_eq!(areas.value(0), 2.0);
    assert_eq!(areas.value(1), 2.0);
    assert!(areas.is_null(2));
    assert_eq!(areas.value(3), 1.0);
    assert!(!geometry.is_null(0));
    assert!(!geometry.is_null(1));
    assert!(geometry.is_null(2));
    assert!(!geometry.is_null(3));

    let empty = ctx
        .sql(
            "SELECT ST_CoverageUnion(ST_GeomFromText(wkt)) AS geom \
             FROM cover_groups \
             WHERE grp = 'missing'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(output_geometry(&empty[0]).is_null(0));

    let empty_group = ctx
        .sql(
            "SELECT grp, ST_CoverageUnion(ST_GeomFromText(wkt)) AS geom \
             FROM cover_groups \
             WHERE grp = 'missing' \
             GROUP BY grp",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        empty_group.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0
    );
}

#[tokio::test]
async fn coverage_union_preserves_crs_and_returns_xy() {
    let ctx = context();
    let batches = ctx
        .sql(
            "SELECT ST_CoverageUnion(g) AS geom \
             FROM ( \
                 SELECT ST_GeomFromEWKT('SRID=4326;POLYGON Z ((0 0 1,1 0 2,1 1 3,0 1 4,0 0 1))') AS g \
                 UNION ALL \
                 SELECT ST_GeomFromEWKT('SRID=4326;POLYGON Z ((1 0 2,2 0 3,2 1 4,1 1 3,1 0 2))') \
             ) AS input",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    let output_type = batch
        .schema_ref()
        .field(0)
        .try_extension_type::<GeoArrowGeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );

    let geometry = output_geometry(batch);
    let value = geometry.value(0).unwrap();
    assert_eq!(value.dim(), Dimensions::Xy);
    let expected = [
        vec![0.0, 0.0],
        vec![0.0, 1.0],
        vec![1.0, 1.0],
        vec![2.0, 1.0],
        vec![2.0, 0.0],
        vec![1.0, 0.0],
        vec![0.0, 0.0],
    ];
    assert_eq!(
        canonical_ring(&polygon_rings(&geometry, 0)[0]),
        canonical_ring(&expected)
    );
}

#[tokio::test]
async fn coverage_union_drops_measure_ordinates() {
    let ctx = context();
    let batches = ctx
        .sql(
            "SELECT ST_CoverageUnion(g) AS geom \
             FROM ( \
                 SELECT ST_GeomFromText('POLYGON M ((0 0 1,1 0 2,1 1 3,0 1 4,0 0 1))') AS g \
                 UNION ALL \
                 SELECT ST_GeomFromText('POLYGON M ((1 0 2,2 0 3,2 1 4,1 1 3,1 0 2))') \
             ) AS input",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let geometry = output_geometry(&batches[0]);
    assert_eq!(geometry.value(0).unwrap().dim(), Dimensions::Xy);
}

#[tokio::test]
#[cfg(feature = "geos-3_12")]
async fn coverage_window_defaults_follow_postgis() {
    let ctx = context();
    let batches = ctx
        .sql(
            "SELECT ST_Length(ST_CoverageInvalidEdges(g) OVER ()) AS edges \
             FROM ( \
                 SELECT ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))') AS g \
                 UNION ALL \
                 SELECT ST_GeomFromText('POLYGON((0.5 0,1.5 0,1.5 1,0.5 1,0.5 0))') \
             ) AS input \
             ORDER BY edges",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let lengths = batches[0]
        .column(0)
        .as_primitive::<arrow_array::types::Float64Type>();
    assert_eq!((lengths.value(0), lengths.value(1)), (3.0, 3.0));

    let batches = ctx
        .sql(
            "SELECT ST_Area(ST_CoverageSimplify(g, 0.5) OVER ()) AS area \
             FROM ( \
                 SELECT ST_GeomFromText('POLYGON((0 0,1 0,1 1,0.5 1.2,0 1,0 0))') AS g \
                 UNION ALL \
                 SELECT ST_GeomFromText('POLYGON((1 0,2 0,2 1,1.5 1.2,1 1,1 0))') \
             ) AS input \
             ORDER BY area",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let areas = batches[0]
        .column(0)
        .as_primitive::<arrow_array::types::Float64Type>();
    assert_eq!((areas.value(0), areas.value(1)), (1.0, 1.0));
}

#[tokio::test]
#[cfg(feature = "geos-3_12")]
async fn coverage_window_uses_whole_partition_across_order_and_frame() {
    let ctx = context();
    let baseline_edges = window_invalid_edge_lengths(
        &ctx,
        "PARTITION BY grp ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW",
    )
    .await;
    assert_eq!(baseline_edges, vec![Some(3.0), Some(3.0), None]);

    for over in [
        "PARTITION BY grp ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
        "PARTITION BY grp ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        "PARTITION BY grp",
    ] {
        assert_eq!(
            window_invalid_edge_lengths(&ctx, over).await,
            baseline_edges,
            "invalid edges changed under OVER ({over})"
        );
    }

    let baseline_areas = window_simplified_areas(&ctx, "PARTITION BY grp ORDER BY id").await;
    assert_eq!(baseline_areas.len(), 3);
    assert!((baseline_areas[0].unwrap() - 1.1).abs() < 1e-12);
    assert!((baseline_areas[1].unwrap() - 1.1).abs() < 1e-12);
    assert_eq!(baseline_areas[2], Some(1.0));

    for over in [
        "PARTITION BY grp ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW",
        "PARTITION BY grp ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
        "PARTITION BY grp",
    ] {
        let actual = window_simplified_areas(&ctx, over).await;
        for (expected, actual) in baseline_areas.iter().zip(&actual) {
            match (expected, actual) {
                (Some(expected), Some(actual)) => {
                    assert!((expected - actual).abs() < 1e-12);
                }
                (None, None) => {}
                _ => panic!("simplified membership changed under OVER ({over})"),
            }
        }
        assert_eq!(actual.len(), baseline_areas.len());
    }
}

#[tokio::test]
#[cfg(feature = "geos-3_12")]
async fn coverage_window_batch_slices_and_row_reordering_are_invariant() {
    let expected_shared_a_b = segment_key([2.0, 0.0], [2.0, 2.0]);
    let expected_shared_c_d = segment_key([12.0, 0.0], [12.0, 2.0]);

    for (order, split) in [([0, 1, 2, 3], 2), ([3, 2, 1, 0], 2), ([2, 0, 3, 1], 1)] {
        let ctx = pair_context(&order, split);
        let batches = ctx
            .sql(
                "SELECT id, \
                        ST_CoverageSimplify(geom, 0.5) \
                            OVER (PARTITION BY grp ORDER BY id DESC) AS simplified \
                 FROM pairs \
                 ORDER BY id",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];
        let ids = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(
            (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        let geometry = geometry_column(batch, 1);
        let mut output_area = 0.0;
        for row in 0..4 {
            assert!(
                !geometry.is_null(row),
                "batch order {order:?} lost row {row}"
            );
            let rings = polygon_rings(&geometry, row);
            assert_eq!(rings.len(), 1);
            output_area += polygon_area(&rings);
        }
        assert_eq!(output_area, 16.0);
        assert!(ring_segments(&polygon_rings(&geometry, 0)[0]).contains(&expected_shared_a_b));
        assert!(ring_segments(&polygon_rings(&geometry, 1)[0]).contains(&expected_shared_a_b));
        assert!(ring_segments(&polygon_rings(&geometry, 2)[0]).contains(&expected_shared_c_d));
        assert!(ring_segments(&polygon_rings(&geometry, 3)[0]).contains(&expected_shared_c_d));
    }
}

#[tokio::test]
#[cfg(feature = "geos-3_12")]
async fn coverage_window_accepts_null_parameters_with_non_nullable_fields() {
    let metadata = Arc::new(Metadata::default());
    let polygon_type =
        PolygonType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let rings = |coords: &[(f64, f64)]| {
        WktLineString::new(
            coords
                .iter()
                .map(|&(x, y)| Coord {
                    x,
                    y,
                    z: None,
                    m: None,
                })
                .collect(),
            WktDimension::XY,
        )
    };
    let polygons = [
        WktPolygon::new(
            vec![rings(&[
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ])],
            WktDimension::XY,
        ),
        WktPolygon::new(
            vec![rings(&[
                (0.5, 0.0),
                (1.5, 0.0),
                (1.5, 1.0),
                (0.5, 1.0),
                (0.5, 0.0),
            ])],
            WktDimension::XY,
        ),
    ];
    let array = PolygonBuilder::from_polygons(&polygons, polygon_type).finish();
    let geometry_field = Arc::new(array.extension_type().to_field("geom", false));
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::clone(&geometry_field),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            array.into_array_ref(),
        ],
    )
    .unwrap();
    let ctx = context();
    ctx.register_batch("coverage_input", batch).unwrap();

    let batches = ctx
        .sql(
            "SELECT ST_CoverageInvalidEdges(geom, CAST(NULL AS DOUBLE)) OVER () IS NULL AS is_null \
             FROM coverage_input \
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let is_null = batches[0].column(0).as_boolean();
    assert!(!is_null.value(0));
    assert!(!is_null.value(1));

    let batches = ctx
        .sql(
            "SELECT ST_CoverageSimplify(geom, CAST(NULL AS DOUBLE)) OVER () IS NULL AS is_null \
             FROM coverage_input \
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let is_null = batches[0].column(0).as_boolean();
    assert!(!is_null.value(0));
    assert!(!is_null.value(1));
}

#[tokio::test]
#[cfg(feature = "geos-3_12")]
async fn coverage_window_handles_slices_empty_and_metadata() {
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let polygon_type =
        PolygonType::new(Dimension::XYZ, metadata).with_coord_type(CoordType::Interleaved);
    let ring = |coords: &[(f64, f64, f64)]| {
        WktLineString::new(
            coords
                .iter()
                .map(|&(x, y, z)| Coord {
                    x,
                    y,
                    z: Some(z),
                    m: None,
                })
                .collect(),
            WktDimension::XYZ,
        )
    };
    let polygons = [
        WktPolygon::new(
            vec![ring(&[
                (20.0, 0.0, 1.0),
                (21.0, 0.0, 2.0),
                (21.0, 1.0, 3.0),
                (20.0, 1.0, 4.0),
                (20.0, 0.0, 1.0),
            ])],
            WktDimension::XYZ,
        ),
        WktPolygon::empty(WktDimension::XYZ),
        WktPolygon::new(
            vec![ring(&[
                (0.0, 0.0, 1.0),
                (1.0, 0.0, 2.0),
                (1.0, 0.4, 3.0),
                (1.0, 0.6, 4.0),
                (1.0, 1.0, 5.0),
                (0.5, 1.2, 6.0),
                (0.0, 1.0, 7.0),
                (0.0, 0.0, 1.0),
            ])],
            WktDimension::XYZ,
        ),
        WktPolygon::new(
            vec![ring(&[
                (1.0, 0.0, 2.0),
                (2.0, 0.0, 3.0),
                (2.0, 1.0, 4.0),
                (1.5, 1.2, 5.0),
                (1.0, 1.0, 5.0),
                (1.0, 0.6, 4.0),
                (1.0, 0.4, 3.0),
                (1.0, 0.0, 2.0),
            ])],
            WktDimension::XYZ,
        ),
        WktPolygon::new(
            vec![ring(&[
                (10.0, 0.0, 1.0),
                (11.0, 0.0, 2.0),
                (11.0, 1.0, 3.0),
                (10.0, 1.0, 4.0),
                (10.0, 0.0, 1.0),
            ])],
            WktDimension::XYZ,
        ),
    ];
    let array = PolygonBuilder::from_polygons(&polygons, polygon_type)
        .finish()
        .slice(1, 4);
    let geometry_field = Arc::new(array.extension_type().to_field("geom", false));
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(Field::new("grp", DataType::Int32, false)),
        geometry_field,
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![1, 1, 1, 2])),
            array.into_array_ref(),
        ],
    )
    .unwrap();
    let ctx = context();
    ctx.register_batch("coverage_windows", batch).unwrap();

    let batches = ctx
        .sql(
            "SELECT ST_CoverageSimplify(geom, 0.5, false) \
                    OVER (PARTITION BY grp ORDER BY id) AS simplified \
             FROM coverage_windows \
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    let output_type = batch
        .schema_ref()
        .field(0)
        .try_extension_type::<GeoArrowGeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert_eq!(output_type.coord_type(), CoordType::Interleaved);
    let geometry = output_geometry(batch);
    assert!(geometry.is_null(0));
    for row in 1..4 {
        assert_eq!(geometry.value(row).unwrap().dim(), Dimensions::Xyz);
    }
    // The outer bumps survive when simplifyBoundary is false, while the
    // collinear vertices on the shared edge are removed from both polygons.
    assert_eq!(polygon_rings(&geometry, 1)[0].len(), 6);
    assert_eq!(polygon_rings(&geometry, 2)[0].len(), 6);

    let batches = ctx
        .sql(
            "SELECT ST_CoverageInvalidEdges(geom) OVER (PARTITION BY grp ORDER BY id) AS edges \
             FROM coverage_windows \
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let geometry = output_geometry(&batches[0]);
    for row in 0..4 {
        assert!(geometry.is_null(row), "row {row} should be valid");
    }
}

#[tokio::test]
#[cfg(feature = "geos-3_12")]
async fn coverage_window_drops_measure_ordinates() {
    let metadata = Arc::new(Metadata::default());
    let polygon_type =
        PolygonType::new(Dimension::XYM, metadata).with_coord_type(CoordType::Interleaved);
    let ring = |coords: &[(f64, f64, f64)]| {
        WktLineString::new(
            coords
                .iter()
                .map(|&(x, y, m)| Coord {
                    x,
                    y,
                    z: None,
                    m: Some(m),
                })
                .collect(),
            WktDimension::XYM,
        )
    };
    let polygons = [
        WktPolygon::new(
            vec![ring(&[
                (0.0, 0.0, 1.0),
                (1.0, 0.0, 2.0),
                (1.0, 1.0, 3.0),
                (0.0, 1.0, 4.0),
                (0.0, 0.0, 1.0),
            ])],
            WktDimension::XYM,
        ),
        WktPolygon::new(
            vec![ring(&[
                (1.0, 0.0, 2.0),
                (2.0, 0.0, 3.0),
                (2.0, 1.0, 4.0),
                (1.0, 1.0, 3.0),
                (1.0, 0.0, 2.0),
            ])],
            WktDimension::XYM,
        ),
    ];
    let array = PolygonBuilder::from_polygons(&polygons, polygon_type).finish();
    let geometry_field = Arc::new(array.extension_type().to_field("geom", false));
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        geometry_field,
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            array.into_array_ref(),
        ],
    )
    .unwrap();
    let ctx = context();
    ctx.register_batch("measure_input", batch).unwrap();

    let batches = ctx
        .sql(
            "SELECT ST_CoverageSimplify(geom, 0.5) OVER (ORDER BY id) AS simplified \
             FROM measure_input \
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let geometry = output_geometry(&batches[0]);
    for row in 0..2 {
        assert_eq!(geometry.value(row).unwrap().dim(), Dimensions::Xy);
    }
}

#[tokio::test]
#[cfg(feature = "geos-3_12")]
async fn coverage_window_arguments_are_validated() {
    let ctx = context();
    for (sql, expected) in [
        (
            "SELECT ST_CoverageSimplify(1, 0.5) OVER ()",
            "must be a geometry",
        ),
        (
            "SELECT ST_CoverageSimplify(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) OVER ()",
            "expects 2 or 3 arguments",
        ),
        (
            "SELECT ST_CoverageInvalidEdges(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 'x') OVER ()",
            "must be numeric",
        ),
        (
            "SELECT ST_CoverageSimplify(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 0.5, 1) OVER ()",
            "must be boolean",
        ),
    ] {
        let error = ctx
            .sql(sql)
            .await
            .expect_err("invalid window arguments must be rejected");
        assert!(error.to_string().contains(expected), "{error}");
    }
}
