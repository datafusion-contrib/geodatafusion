use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use geo_traits::{CoordTrait, RectTrait};
use geoarrow_array::array::RectArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{BoxType, Dimension, Metadata, WktType};

const CRS_AUTHORITY: &str = "EPSG:4326";

fn test_context() -> SessionContext {
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code(CRS_AUTHORITY.to_string()),
        Default::default(),
    ));
    let wkt_field = Arc::new(
        Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata)),
    );
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("grp", DataType::Utf8, false)),
        wkt_field,
    ]));

    let partition_a = vec![
        make_batch(
            Arc::clone(&schema),
            &["a", "a", "b", "empty"],
            &[
                Some("POINT(1 2)"),
                Some("POINT(3 5)"),
                Some("POINT EMPTY"),
                Some("POLYGON EMPTY"),
            ],
        ),
        make_batch(
            Arc::clone(&schema),
            &["null", "null", "a", "b"],
            &[None, None, Some("POINT(-2 4)"), Some("POINT(10 -1)")],
        ),
    ];
    let partition_b = vec![
        make_batch(
            Arc::clone(&schema),
            &["a", "b", "empty"],
            &[Some("POINT(0 10)"), Some("POINT(-5 2)"), None],
        ),
        make_batch(
            Arc::clone(&schema),
            &["null", "a"],
            &[None, Some("POINT(7 0)")],
        ),
    ];

    let table = MemTable::try_new(schema, vec![partition_a, partition_b]).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table("points", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);
    ctx
}

fn extent_3d_test_context() -> SessionContext {
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code(CRS_AUTHORITY.to_string()),
        Default::default(),
    ));
    let wkt_field = Arc::new(
        Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata)),
    );
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("grp", DataType::Utf8, false)),
        wkt_field,
    ]));

    let partition_a = vec![
        make_batch(
            Arc::clone(&schema),
            &["a", "a", "b", "empty"],
            &[
                Some("POINT Z(1 2 3)"),
                Some("POINT(4 5)"),
                Some("POINT M(100 200 999)"),
                Some("POINT EMPTY"),
            ],
        ),
        make_batch(
            Arc::clone(&schema),
            &["null", "null", "a", "b"],
            &[
                None,
                None,
                Some("POINT Z(-2 4 8)"),
                Some("LINESTRING(10 -1)"),
            ],
        ),
    ];
    let partition_b = vec![
        make_batch(
            Arc::clone(&schema),
            &["a", "b", "empty"],
            &[
                Some("POINT M(0 10 500)"),
                Some("LINESTRING Z(-5 2 4)"),
                None,
            ],
        ),
        make_batch(
            Arc::clone(&schema),
            &["null", "a"],
            &[None, Some("POINT Z(7 0 6)")],
        ),
    ];

    let table = MemTable::try_new(schema, vec![partition_a, partition_b]).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table("points_3d", Arc::new(table)).unwrap();
    geodatafusion::register(&ctx);
    ctx
}

fn make_batch(schema: SchemaRef, groups: &[&str], wkts: &[Option<&str>]) -> RecordBatch {
    let groups = Arc::new(StringArray::from(groups.to_vec())) as ArrayRef;
    let wkts = Arc::new(StringArray::from(wkts.to_vec())) as ArrayRef;
    RecordBatch::try_new(schema, vec![groups, wkts]).unwrap()
}

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

fn partial_state_fields(plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<FieldRef>> {
    if let Some(aggregate) = (plan.as_ref() as &dyn std::any::Any).downcast_ref::<AggregateExec>()
        && *aggregate.mode() == AggregateMode::Partial
    {
        return aggregate.aggr_expr()[0].state_fields().ok();
    }

    plan.children().into_iter().find_map(partial_state_fields)
}

#[tokio::test]
async fn extent_global_uses_partial_and_final_across_partitions() {
    let ctx = test_context();
    let dataframe = ctx
        .sql("SELECT ST_Extent(ST_GeomFromText(wkt)) AS extent FROM points")
        .await
        .unwrap();
    let plan = dataframe.create_physical_plan().await.unwrap();

    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let (partial, r#final, single, state_fields) = aggregate_plan_modes(&plan);
    assert_eq!(
        (partial, r#final, single),
        (1, 1, 0),
        "expected a physical Partial -> Final plan:\n{plan_text}"
    );

    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap();
    assert_eq!(
        state_fields, 4,
        "ST_Extent must declare all four partial-state fields"
    );
    let state_fields = partial_state_fields(&plan).expect("partial aggregate state fields");
    for (field, suffix) in state_fields
        .iter()
        .zip(["[xmin]", "[ymin]", "[xmax]", "[ymax]"])
    {
        assert!(
            field.name().ends_with(suffix),
            "unexpected ST_Extent state field name: {}",
            field.name()
        );
    }
    assert!(
        state_fields
            .iter()
            .all(|field| field.data_type() == &DataType::Float64)
    );
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let batch = &batches[0];
    let schema = batch.schema();
    let output_field = schema.field(0);
    let box_type = output_field.try_extension_type::<BoxType>().unwrap();
    assert_eq!(box_type.dimension(), Dimension::XY);
    assert_eq!(
        box_type.metadata().crs(),
        &Crs::from_authority_code(CRS_AUTHORITY.to_string())
    );

    let rects = RectArray::try_from((batch.column(0).as_ref(), output_field)).unwrap();
    let rect = rects.value(0).unwrap();
    assert_eq!(rect.min().x(), -5.0);
    assert_eq!(rect.min().y(), -1.0);
    assert_eq!(rect.max().x(), 10.0);
    assert_eq!(rect.max().y(), 10.0);
}

#[tokio::test]
async fn extent_groups_and_empty_inputs_match_postgis() {
    let ctx = test_context();
    let batches = ctx
        .sql(
            "SELECT grp, ST_Extent(ST_GeomFromText(wkt)) AS extent \
             FROM points GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut actual = Vec::new();
    for batch in &batches {
        let groups = batch.column(0).as_string::<i32>();
        let rects =
            RectArray::try_from((batch.column(1).as_ref(), batch.schema().field(1))).unwrap();
        for row in 0..batch.num_rows() {
            let bounds = (!rects.is_null(row)).then(|| {
                let rect = rects.value(row).unwrap();
                (
                    rect.min().x(),
                    rect.min().y(),
                    rect.max().x(),
                    rect.max().y(),
                )
            });
            actual.push((groups.value(row).to_string(), bounds));
        }
    }
    actual.sort_by(|left, right| left.0.cmp(&right.0));

    assert_eq!(
        actual,
        vec![
            ("a".to_string(), Some((-2.0, 0.0, 7.0, 10.0))),
            ("b".to_string(), Some((-5.0, -1.0, 10.0, 2.0))),
            ("empty".to_string(), None),
            ("null".to_string(), None),
        ]
    );

    let batches = ctx
        .sql("SELECT ST_Extent(ST_GeomFromText(wkt)) FROM points WHERE false")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    let rects = RectArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(rects.is_null(0));
}

#[tokio::test]
async fn extent_propagates_invalid_geometry_errors() {
    let ctx = test_context();
    let result = ctx
        .sql("SELECT ST_Extent(ST_GeomFromText('NOT WKT'))")
        .await
        .unwrap()
        .collect()
        .await;

    let error = result.expect_err("invalid WKT must fail the aggregate");
    assert!(
        error.to_string().to_ascii_lowercase().contains("wkt"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn extent_3d_global_uses_partial_and_final_across_partitions() {
    let ctx = extent_3d_test_context();
    let dataframe = ctx
        .sql("SELECT ST_3DExtent(ST_GeomFromText(wkt)) AS extent FROM points_3d")
        .await
        .unwrap();
    let plan = dataframe.create_physical_plan().await.unwrap();

    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let (partial, r#final, single, state_fields) = aggregate_plan_modes(&plan);
    assert_eq!(
        (partial, r#final, single),
        (1, 1, 0),
        "expected a physical Partial -> Final plan:\n{plan_text}"
    );
    assert_eq!(state_fields, 6, "ST_3DExtent must declare six state fields");

    let state_fields = partial_state_fields(&plan).expect("partial aggregate state fields");
    for (field, suffix) in state_fields
        .iter()
        .zip(["[xmin]", "[ymin]", "[zmin]", "[xmax]", "[ymax]", "[zmax]"])
    {
        assert!(
            field.name().ends_with(suffix),
            "unexpected ST_3DExtent state field name: {}",
            field.name()
        );
    }
    assert!(
        state_fields
            .iter()
            .all(|field| field.data_type() == &DataType::Float64)
    );

    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let batch = &batches[0];
    let schema = batch.schema();
    let output_field = schema.field(0);
    let box_type = output_field.try_extension_type::<BoxType>().unwrap();
    assert_eq!(box_type.dimension(), Dimension::XYZ);
    assert_eq!(
        box_type.metadata().crs(),
        &Crs::from_authority_code(CRS_AUTHORITY.to_string())
    );
    let DataType::Struct(inner_fields) = output_field.data_type() else {
        panic!("ST_3DExtent must return a struct");
    };
    assert_eq!(
        inner_fields
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["xmin", "ymin", "zmin", "xmax", "ymax", "zmax"]
    );

    let rects = RectArray::try_from((batch.column(0).as_ref(), output_field)).unwrap();
    let rect = rects.value(0).unwrap();
    assert_eq!(rect.min().x(), -5.0);
    assert_eq!(rect.min().y(), -1.0);
    assert_eq!(rect.min().nth_or_panic(2), 0.0);
    assert_eq!(rect.max().x(), 100.0);
    assert_eq!(rect.max().y(), 200.0);
    assert_eq!(rect.max().nth_or_panic(2), 8.0);
}

#[tokio::test]
async fn extent_3d_groups_empty_and_null_inputs_match_postgis() {
    let ctx = extent_3d_test_context();
    let batches = ctx
        .sql(
            "SELECT grp, ST_3DExtent(ST_GeomFromText(wkt)) AS extent \
             FROM points_3d GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut actual = Vec::new();
    for batch in &batches {
        let groups = batch.column(0).as_string::<i32>();
        let rects =
            RectArray::try_from((batch.column(1).as_ref(), batch.schema().field(1))).unwrap();
        for row in 0..batch.num_rows() {
            let bounds = (!rects.is_null(row)).then(|| {
                let rect = rects.value(row).unwrap();
                (
                    rect.min().x(),
                    rect.min().y(),
                    rect.min().nth_or_panic(2),
                    rect.max().x(),
                    rect.max().y(),
                    rect.max().nth_or_panic(2),
                )
            });
            actual.push((groups.value(row).to_string(), bounds));
        }
    }
    actual.sort_by(|left, right| left.0.cmp(&right.0));

    assert_eq!(
        actual,
        vec![
            ("a".to_string(), Some((-2.0, 0.0, 0.0, 7.0, 10.0, 8.0)),),
            ("b".to_string(), Some((-5.0, -1.0, 0.0, 100.0, 200.0, 4.0)),),
            ("empty".to_string(), None),
            ("null".to_string(), None),
        ]
    );

    let batches = ctx
        .sql("SELECT ST_3DExtent(ST_GeomFromText(wkt)) FROM points_3d WHERE false")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    let rects = RectArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(rects.is_null(0));
}

#[tokio::test]
async fn extent_bare_null_returns_null() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    for (query, dimension) in [
        ("SELECT ST_Extent(NULL)", Dimension::XY),
        ("SELECT ST_3DExtent(NULL)", Dimension::XYZ),
    ] {
        let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
        let batch = &batches[0];
        let schema = batch.schema();
        let output_field = schema.field(0);
        let box_type = output_field.try_extension_type::<BoxType>().unwrap();
        assert_eq!(box_type.dimension(), dimension);
        let rects = RectArray::try_from((batch.column(0).as_ref(), output_field)).unwrap();
        assert!(rects.is_null(0));
    }
}
