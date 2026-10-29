//! SQL contracts for geometry aggregates, their named scalar overloads, and
//! boundary polygon constructors.

mod support;

use std::sync::Arc;

#[cfg(feature = "geos-3_11")]
use arrow_array::BinaryArray;
#[cfg(feature = "geos-3_11")]
use arrow_array::cast::AsArray;
use arrow_array::{ListArray, RecordBatch};
use arrow_buffer::OffsetBuffer;
#[cfg(feature = "geos-3_11")]
use arrow_schema::SchemaRef;
use arrow_schema::{Field, Schema};
use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
#[cfg(feature = "geos-3_11")]
use datafusion::datasource::MemTable;
#[cfg(feature = "geos-3_11")]
use datafusion::physical_plan::ExecutionPlan;
#[cfg(feature = "geos-3_11")]
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
#[cfg(feature = "geos-3_11")]
use datafusion::physical_plan::displayable;
#[cfg(feature = "geos-3_11")]
use datafusion::prelude::{SessionConfig, SessionContext};
use geoarrow_array::test::point;
use geoarrow_array::{GeoArrowArray, IntoArrow};
use geoarrow_schema::{CoordType, Dimension};
#[cfg(feature = "geos-3_11")]
use geoarrow_schema::{Crs, GeometryType, LineStringType, Metadata};
use support::context::context;

#[cfg(feature = "geos-3_11")]
fn aggregate_plan_modes(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize, usize, usize) {
    let (mut partial, mut r#final, mut single) = (0, 0, 0);
    let mut state_fields = usize::MAX;
    collect_aggregate_plan_modes(
        plan,
        &mut partial,
        &mut r#final,
        &mut single,
        &mut state_fields,
    );
    (partial, r#final, single, state_fields)
}

#[cfg(feature = "geos-3_11")]
fn collect_aggregate_plan_modes(
    plan: &Arc<dyn ExecutionPlan>,
    partial: &mut usize,
    r#final: &mut usize,
    single: &mut usize,
    state_fields: &mut usize,
) {
    if let Some(aggregate) = (plan.as_ref() as &dyn std::any::Any).downcast_ref::<AggregateExec>() {
        match aggregate.mode() {
            AggregateMode::Partial => {
                *partial += 1;
                *state_fields =
                    (*state_fields).min(aggregate.aggr_expr()[0].state_fields().unwrap().len());
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => *r#final += 1,
            AggregateMode::Single | AggregateMode::SinglePartitioned => *single += 1,
            AggregateMode::PartialReduce => {}
        }
    }
    for child in plan.children() {
        collect_aggregate_plan_modes(child, partial, r#final, single, state_fields);
    }
}

#[cfg(feature = "geos-3_11")]
fn partitioned_context() -> SessionContext {
    use geoarrow_schema::{Crs, Metadata, WktType};

    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        Default::default(),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", arrow_schema::DataType::Int64, false),
        Field::new("wkt", arrow_schema::DataType::Utf8, true)
            .with_extension_type(WktType::new(metadata)),
    ]));
    let left = vec![make_batch(
        Arc::clone(&schema),
        &[2, 0],
        &[Some("POINT(2 2)"), Some("POINT(0 0)")],
    )];
    let right = vec![make_batch(Arc::clone(&schema), &[1], &[Some("POINT(1 1)")])];
    let table = MemTable::try_new(schema, vec![left, right]).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table("points", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);
    ctx
}

#[cfg(feature = "geos-3_11")]
fn make_batch(schema: SchemaRef, ids: &[i64], wkts: &[Option<&str>]) -> RecordBatch {
    use arrow_array::StringArray;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(wkts.to_vec())),
        ],
    )
    .unwrap()
}

async fn text_query(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Vec<Option<String>> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let formatter =
            ArrayFormatter::try_new(batch.column(0), &FormatOptions::default()).unwrap();
        for row in 0..batch.num_rows() {
            values.push(if batch.column(0).is_null(row) {
                None
            } else {
                Some(formatter.value(row).to_string())
            });
        }
    }
    values
}

#[cfg(feature = "geos-3_11")]
async fn sql_error(ctx: &datafusion::prelude::SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(dataframe) => dataframe
            .collect()
            .await
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default(),
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn makeline_aggregate_and_scalar_overloads() {
    let ctx = context();

    let values = text_query(
        &ctx,
        "SELECT ST_AsText(ST_MakeLineAgg(g ORDER BY id)) FROM (VALUES \
         (2, ST_GeomFromText('POINT(2 2)')), (1, ST_GeomFromText('POINT(1 1)')), \
         (3, ST_GeomFromText('POINT(3 3)'))) AS t(id, g)",
    )
    .await;
    assert_eq!(values, vec![Some("LINESTRING(1 1,2 2,3 3)".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_AsText(ST_MakeLine(a, b)) FROM (VALUES \
         (ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'))) AS t(a, b)",
    )
    .await;
    assert_eq!(values, vec![Some("LINESTRING(0 0,1 1)".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_AsText(ST_MakeLineAgg(g)) FROM (VALUES \
         (ST_GeomFromText('POINT EMPTY')), (ST_GeomFromText('POINT(1 1)'))) AS t(g)",
    )
    .await;
    assert_eq!(values, vec![Some("LINESTRING(1 1)".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_MakeLineAgg(g) IS NULL FROM (SELECT ST_GeomFromText('POINT(1 1)') AS g WHERE false)",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn collect_union_and_polygonize() {
    let ctx = context();

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_Collect(g), ST_GeomFromText('MULTIPOINT(0 0,1 1)')) FROM (VALUES \
         (ST_GeomFromText('POINT(0 0)')), (ST_GeomFromText('POINT(1 1)'))) AS t(g)",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_Union(g ORDER BY id), ST_GeomFromText('POINT(0 0)')) FROM (VALUES \
         (2, ST_GeomFromText('POINT(0 0)')), (1, ST_GeomFromText('POINT(0 0)'))) AS t(id, g)",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_CollectPair(a, b), ST_GeomFromText('MULTIPOINT(0 0,1 1)')) FROM (VALUES \
         (ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'))) AS t(a, b)",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_UnionPair(a, b), ST_GeomFromText('POLYGON((2 0,0 0,0 2,1 2,1 3,3 3,3 1,2 1,2 0))')) FROM (VALUES \
         (ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'), \
          ST_GeomFromText('POLYGON((1 1,3 1,3 3,1 3,1 1))'))) AS t(a, b)",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_Polygonize(g), \
         ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0,0 1,1 1,1 0,0 0)))')) FROM (VALUES \
         (ST_GeomFromText('LINESTRING(0 0,1 0,1 1,0 1,0 0)'))) AS t(g)",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);
}

#[tokio::test]
async fn makeline_array_overload_accepts_geometry_lists() {
    let ctx = context();
    let points = point::array(CoordType::Separated, Dimension::XY);
    let element_field = Arc::new(points.extension_type().to_field("item", true));
    let values = points.into_array_ref();
    let arrays = ListArray::new(
        element_field,
        OffsetBuffer::<i32>::from_lengths([2]),
        values,
        None,
    );
    let schema = Schema::new(vec![Field::new("points", arrays.data_type().clone(), true)]);
    ctx.register_batch(
        "makeline_arrays",
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arrays)]).unwrap(),
    )
    .unwrap();

    let values = text_query(
        &ctx,
        "SELECT ST_AsText(ST_MakeLine(points)) FROM makeline_arrays",
    )
    .await;
    assert_eq!(values, vec![Some("LINESTRING(30 10,40 20)".to_string())]);
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn array_overloads_accept_list_columns() {
    let ctx = context();
    let points = point::array(CoordType::Separated, Dimension::XY);
    let element_field = Arc::new(points.extension_type().to_field("item", true));
    let values = points.into_array_ref();
    let arrays = ListArray::new(
        element_field,
        OffsetBuffer::<i32>::from_lengths([2]),
        values,
        None,
    );
    let schema = Schema::new(vec![Field::new("points", arrays.data_type().clone(), true)]);
    ctx.register_batch(
        "t",
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arrays)]).unwrap(),
    )
    .unwrap();

    let values = text_query(&ctx, "SELECT ST_AsText(ST_MakeLine(points)) FROM t").await;
    assert_eq!(values, vec![Some("LINESTRING(30 10,40 20)".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_CollectArray(points), ST_GeomFromText('MULTIPOINT(30 10,40 20)')) FROM t",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_UnionArray(points), ST_GeomFromText('MULTIPOINT(30 10,40 20)')) FROM t",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn aggregate_empty_null_and_grid_semantics() {
    let ctx = context();

    for sql in [
        "SELECT ST_Collect(g) IS NULL FROM (SELECT ST_GeomFromText('POINT(1 1)') AS g WHERE false) AS t",
        "SELECT ST_MemUnion(g) IS NULL FROM (SELECT ST_GeomFromText('POINT(1 1)') AS g WHERE false) AS t",
        "SELECT ST_Union(g) IS NULL FROM (SELECT ST_GeomFromText('POINT(1 1)') AS g WHERE false) AS t",
        "SELECT ST_Polygonize(g) IS NULL FROM (SELECT ST_GeomFromText('LINESTRING(0 0,1 1)') AS g WHERE false) AS t",
    ] {
        assert_eq!(
            text_query(&ctx, sql).await,
            vec![Some("true".to_string())],
            "{sql}"
        );
    }

    let values = text_query(
        &ctx,
        "SELECT ST_NumGeometries(ST_Collect(g)) FROM (VALUES \
         (ST_GeomFromText('POINT EMPTY')), (ST_GeomFromText('POINT(1 1)'))) AS t(g)",
    )
    .await;
    assert_eq!(values, vec![Some("2".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_Union(g, CAST(NULL AS DOUBLE)) IS NULL FROM (VALUES \
         (ST_GeomFromText('POINT(0.1 0.1)')), (ST_GeomFromText('POINT(0.4 0.4)'))) AS t(g)",
    )
    .await;
    assert_eq!(values, vec![Some("false".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_Union(g, 0.5), ST_GeomFromText('MULTIPOINT(0 0,0.5 0.5)')) FROM (VALUES \
         (ST_GeomFromText('POINT(0.1 0.1)')), (ST_GeomFromText('POINT(0.4 0.4)'))) AS t(g)",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn union_and_polygonize_array_overloads() {
    let ctx = context();
    let points = point::array(CoordType::Separated, Dimension::XY);
    let element_field = Arc::new(points.extension_type().to_field("item", true));
    let values = points.into_array_ref();
    let arrays = ListArray::new(
        element_field,
        OffsetBuffer::<i32>::from_lengths([2]),
        values,
        None,
    );
    let schema = Schema::new(vec![Field::new("geoms", arrays.data_type().clone(), true)]);
    ctx.register_batch(
        "arrays",
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arrays)]).unwrap(),
    )
    .unwrap();

    let values = text_query(
        &ctx,
        "SELECT ST_Equals(ST_UnionArray(geoms), ST_GeomFromText('MULTIPOINT(30 10,40 20)')) FROM arrays",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);

    // ST_PolygonizeArray needs linework, so build a geometry list from WKB.
    let ring = |offset: f64| {
        let wkt = format!(
            "LINESTRING({offset} 0,{end} 0,{end} 1,{offset} 1,{offset} 0)",
            end = offset + 1.0
        );
        let geometry = wkt.parse::<wkt::Wkt<f64>>().unwrap();
        let mut bytes = Vec::new();
        wkb::writer::write_geometry(&mut bytes, &geometry, &Default::default()).unwrap();
        bytes
    };
    let wkb_values = BinaryArray::from_iter_values([ring(0.0), ring(10.0)]);
    let wkb_array = geoarrow_array::array::GenericWkbArray::<i32>::new(
        wkb_values,
        Arc::new(Metadata::default()),
    );
    let element_type =
        geoarrow_schema::GeoArrowType::Geometry(GeometryType::new(Arc::new(Metadata::default())));
    let geometry = geoarrow_array::cast::from_wkb(&wkb_array, element_type.clone()).unwrap();
    let element_field = Arc::new(element_type.to_field("item", true));
    let rings = ListArray::new(
        element_field,
        OffsetBuffer::<i32>::from_lengths([2]),
        geometry.into_array_ref(),
        None,
    );
    let schema = Schema::new(vec![Field::new("rings", rings.data_type().clone(), true)]);
    ctx.register_batch(
        "polygonize_arrays",
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(rings)]).unwrap(),
    )
    .unwrap();
    let values = text_query(
        &ctx,
        "SELECT ST_NumGeometries(ST_PolygonizeArray(rings)) FROM polygonize_arrays",
    )
    .await;
    assert_eq!(values, vec![Some("2".to_string())]);
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn aggregates_preserve_field_metadata_and_xyzm() {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let points =
        point::array(CoordType::Interleaved, Dimension::XYZM).with_metadata(metadata.clone());
    let field = Arc::new(points.extension_type().to_field("geom", true));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![points.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("dims", batch).unwrap();

    let batches = ctx
        .sql("SELECT ST_MakeLineAgg(geom) FROM dims")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let output_field = batches[0].schema().field(0).clone();
    let line_type = output_field.try_extension_type::<LineStringType>().unwrap();
    assert_eq!(line_type.dimension(), Dimension::XYZM);
    assert_eq!(line_type.coord_type(), CoordType::Interleaved);
    assert_eq!(line_type.metadata().crs(), metadata.crs());

    let batches = ctx
        .sql("SELECT ST_Collect(geom) FROM dims")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let output_field = batches[0].schema().field(0).clone();
    let geometry_type = output_field.try_extension_type::<GeometryType>().unwrap();
    assert_eq!(geometry_type.metadata().crs(), metadata.crs());

    assert_eq!(
        text_query(&ctx, "SELECT ST_NDims(ST_Collect(geom)) FROM dims").await,
        vec![Some("4".to_string())]
    );
    assert_eq!(
        text_query(&ctx, "SELECT ST_NDims(ST_Union(geom)) FROM dims").await,
        vec![Some("3".to_string())]
    );
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn nullable_false_inputs_with_null_values_are_handled() {
    let ctx = context();
    let points = point::array(CoordType::Separated, Dimension::XY);
    let field = Arc::new(points.extension_type().to_field("geom", true));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            field.clone(),
            Arc::new(field.as_ref().clone().with_name("geom2")),
        ])),
        vec![points.clone().into_array_ref(), points.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("nullable_false", batch).unwrap();

    let values = text_query(
        &ctx,
        "SELECT ST_NumGeometries(ST_Collect(geom)) FROM nullable_false",
    )
    .await;
    assert_eq!(values, vec![Some("3".to_string())]);

    let values = text_query(
        &ctx,
        "SELECT ST_MakeLine(geom, geom2) IS NULL FROM nullable_false",
    )
    .await;
    assert_eq!(
        values,
        vec![
            Some("false".to_string()),
            Some("false".to_string()),
            Some("true".to_string()),
            Some("false".to_string()),
        ]
    );

    let non_null_points = point::array(CoordType::Separated, Dimension::XY);
    let field = Arc::new(non_null_points.extension_type().to_field("geom", false));
    let non_null_values = non_null_points.into_array_ref().slice(0, 1);
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![non_null_values]).unwrap();
    ctx.register_batch("non_null", batch).unwrap();
    let values = text_query(&ctx, "SELECT ST_Collect(geom) IS NULL FROM non_null").await;
    assert_eq!(values, vec![Some("false".to_string())]);
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn boundary_polygon_constructors() {
    let ctx = context();

    let values = text_query(
        &ctx,
        "SELECT ST_AsText(ST_BdPolyFromText('MULTILINESTRING((0 0,1 0,1 1,0 1,0 0))', 0))",
    )
    .await;
    assert_eq!(
        values,
        vec![Some("POLYGON((0 0,0 1,1 1,1 0,0 0))".to_string())]
    );

    let values = text_query(
        &ctx,
        "SELECT ST_BdPolyFromText('MULTILINESTRING((0 0,1 1))', 0) IS NULL",
    )
    .await;
    assert_eq!(values, vec![Some("true".to_string())]);

    let error = sql_error(
        &ctx,
        "SELECT ST_BdPolyFromText('MULTILINESTRING((0 0,1 0,1 1,0 1,0 0),(10 10,11 10,11 11,10 11,10 10))', 0)",
    )
    .await;
    assert!(
        error.contains("more than a single polygon"),
        "unexpected error: {error}"
    );

    let values = text_query(
        &ctx,
        "SELECT ST_AsEWKT(ST_BdMPolyFromText('MULTILINESTRING((0 0,1 0,1 1,0 1,0 0))', 4326))",
    )
    .await;
    assert_eq!(
        values,
        vec![Some(
            "SRID=4326;MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))".to_string()
        )]
    );
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn aggregates_merge_partial_states_across_partitions() {
    let ctx = partitioned_context();

    let dataframe = ctx
        .sql("SELECT ST_AsText(ST_MakeLineAgg(ST_GeomFromText(wkt) ORDER BY id)) FROM points")
        .await
        .unwrap();
    let plan = dataframe.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let (partial, r#final, single, state_fields) = aggregate_plan_modes(&plan);
    assert_eq!(
        (partial, r#final, single, state_fields),
        (1, 1, 0, 2),
        "expected ordered Partial -> Final plan:\n{plan_text}"
    );
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    let text = batches[0].column(0).as_string::<i32>().value(0);
    assert_eq!(text, "LINESTRING(0 0,1 1,2 2)");

    for (sql, expected) in [
        (
            "SELECT ST_Equals(ST_Collect(ST_GeomFromText(wkt)), ST_GeomFromEWKT('SRID=4326;MULTIPOINT(0 0,1 1,2 2)')) FROM points",
            true,
        ),
        (
            "SELECT ST_Equals(ST_Union(ST_GeomFromText(wkt)), ST_GeomFromEWKT('SRID=4326;MULTIPOINT(0 0,1 1,2 2)')) FROM points",
            true,
        ),
        (
            "SELECT ST_Equals(ST_MemUnion(ST_GeomFromText(wkt)), ST_GeomFromEWKT('SRID=4326;MULTIPOINT(0 0,1 1,2 2)')) FROM points",
            true,
        ),
    ] {
        let dataframe = ctx.sql(sql).await.unwrap();
        let plan = dataframe.create_physical_plan().await.unwrap();
        let plan_text = displayable(plan.as_ref()).indent(true).to_string();
        let (partial, r#final, single, _) = aggregate_plan_modes(&plan);
        assert_eq!(
            (partial, r#final, single),
            (1, 1, 0),
            "expected Partial -> Final plan for {sql}:\n{plan_text}"
        );
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap()
                .value(0),
            expected,
            "{sql}"
        );
    }
}
