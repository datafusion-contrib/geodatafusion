use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use geo_traits::{CoordTrait, PointTrait};
use geoarrow_array::array::{PointArray, from_arrow_array};
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, Metadata, PointType};
use wkt::wkt;

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

async fn collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

async fn one_batch(ctx: &SessionContext, sql: &str) -> Result<RecordBatch> {
    let batches = collect(ctx, sql).await?;
    Ok(batches.into_iter().next().expect("query returned a batch"))
}

async fn binary_value(ctx: &SessionContext, sql: &str) -> Result<Vec<u8>> {
    let batch = one_batch(ctx, sql).await?;
    let value = batch.column(0).as_binary::<i32>().value(0);
    Ok(value.to_vec())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn aggregate_bytes(ctx: &SessionContext) -> Result<Vec<u8>> {
    binary_value(
        ctx,
        "SELECT ST_AsFlatGeobuf(ST_PointFromText(wkt, 4326), id, label) AS fgb
         FROM (VALUES
            (1, 'point', 'POINT ZM(1 2 3 4)'),
            (2, NULL, 'POINT EMPTY'),
            (3, 'null-geom', NULL)
         ) AS input(id, label, wkt)",
    )
    .await
}

fn point_type(dimension: Dimension, coord_type: CoordType) -> PointType {
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    PointType::new(dimension, metadata).with_coord_type(coord_type)
}

fn point_array(values: &[Option<wkt::types::Point<f64>>], coord_type: CoordType) -> PointArray {
    let mut builder = PointBuilder::new(point_type(Dimension::XY, coord_type));
    for value in values {
        builder.push_point(value.as_ref());
    }
    builder.finish()
}

fn point_batch(ids: ArrayRef, labels: ArrayRef, points: ArrayRef, nullable: bool) -> RecordBatch {
    let geometry = Arc::new(point_type(Dimension::XY, CoordType::Separated).to_field("geom", true));
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, nullable)),
        Arc::new(Field::new("label", DataType::Utf8, true)),
        geometry,
    ]));
    RecordBatch::try_new(schema, vec![ids, labels, points]).unwrap()
}

fn separated_batch(
    ids: Vec<Option<i32>>,
    labels: Vec<Option<&str>>,
    wkts: &[Option<wkt::types::Point<f64>>],
) -> RecordBatch {
    let ids = Arc::new(Int32Array::from(ids)) as ArrayRef;
    let labels = Arc::new(StringArray::from(labels)) as ArrayRef;
    let points = point_array(wkts, CoordType::Separated).into_array_ref();
    point_batch(ids, labels, points, true)
}

fn aggregate_plan_modes(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
    let mut partial = 0;
    let mut r#final = 0;
    collect_aggregate_plan_modes(plan, &mut partial, &mut r#final);
    (partial, r#final)
}

fn collect_aggregate_plan_modes(
    plan: &Arc<dyn ExecutionPlan>,
    partial: &mut usize,
    r#final: &mut usize,
) {
    if let Some(aggregate) = (plan.as_ref() as &dyn std::any::Any).downcast_ref::<AggregateExec>() {
        match aggregate.mode() {
            AggregateMode::Partial => *partial += 1,
            AggregateMode::Final | AggregateMode::FinalPartitioned => *r#final += 1,
            _ => {}
        }
    }
    for child in plan.children() {
        collect_aggregate_plan_modes(child, partial, r#final);
    }
}

#[tokio::test]
async fn aggregate_and_reader_roundtrip_geometry_properties_and_crs() -> Result<()> {
    let ctx = context();
    let bytes = aggregate_bytes(&ctx).await?;
    assert!(!bytes.is_empty());

    let sql = format!(
        "SELECT id, label, geom FROM st_fromflatgeobuf(decode('{}', 'hex')) ORDER BY id",
        hex(&bytes)
    );
    let dataframe = ctx.sql(&sql).await?;
    let schema = dataframe.schema().as_arrow().clone();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "label");
    assert_eq!(schema.field(2).name(), "geom");

    let geom_field = schema.field(2);
    let geo_type = GeoArrowType::try_from(geom_field).unwrap();
    assert!(matches!(geo_type, GeoArrowType::Point(_)));
    assert_eq!(
        geo_type
            .metadata()
            .crs()
            .crs_value()
            .and_then(|value| value.as_str()),
        Some("EPSG:4326")
    );

    let batches = dataframe.collect().await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let batch = &batches[0];
    let ids = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    let labels = batch.column(1).as_string::<i32>();
    assert_eq!(ids.values(), &[1, 2, 3]);
    assert_eq!(labels.value(0), "point");
    assert!(labels.is_null(1));
    assert_eq!(labels.value(2), "null-geom");
    assert!(!batch.column(2).is_null(0));
    assert!(batch.column(2).is_null(1));
    assert!(batch.column(2).is_null(2));

    let geometry = from_arrow_array(batch.column(2), geom_field).expect("geoarrow array");
    let points = geometry
        .as_any()
        .downcast_ref::<PointArray>()
        .expect("typed point array");
    let coord = points.value(0).unwrap().coord().unwrap();
    assert_eq!(coord.x(), 1.0);
    assert_eq!(coord.y(), 2.0);
    assert_eq!(coord.nth(2), Some(3.0));
    assert_eq!(coord.nth(3), Some(4.0));
    Ok(())
}

#[tokio::test]
async fn multi_partition_state_merge_and_slices_roundtrip() -> Result<()> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    geodatafusion::register(&ctx);

    let partition_a = vec![
        separated_batch(
            vec![Some(1), Some(2)],
            vec![Some("a"), Some("b")],
            &[Some(wkt!(POINT(0.0 0.0))), Some(wkt!(POINT(1.0 1.0)))],
        ),
        separated_batch(
            vec![Some(3), Some(4), Some(5)],
            vec![Some("c"), Some("d"), Some("e")],
            &[
                Some(wkt!(POINT(2.0 2.0))),
                Some(wkt!(POINT(3.0 3.0))),
                Some(wkt!(POINT(4.0 4.0))),
            ],
        )
        .slice(1, 2),
    ];
    let partition_b = vec![separated_batch(
        vec![Some(6)],
        vec![Some("f")],
        &[Some(wkt!(POINT(5.0 5.0)))],
    )];
    let schema: SchemaRef = partition_a[0].schema();
    let table = MemTable::try_new(schema, vec![partition_a, partition_b])?;
    ctx.register_table("points", Arc::new(table))?;

    let dataframe = ctx
        .sql("SELECT ST_AsFlatGeobuf(geom, id, label) AS fgb FROM points")
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert_eq!(
        aggregate_plan_modes(&plan),
        (1, 1),
        "expected a Partial -> Final aggregate plan:\n{plan_text}"
    );
    let bytes = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await?
        .into_iter()
        .next()
        .expect("aggregate batch")
        .column(0)
        .as_binary::<i32>()
        .value(0)
        .to_vec();

    let batches = collect(
        &ctx,
        &format!(
            "SELECT id, label, geom FROM st_fromflatgeobuf(decode('{}', 'hex')) ORDER BY id",
            hex(&bytes)
        ),
    )
    .await?;
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2, 4, 5, 6]);
    Ok(())
}

#[tokio::test]
async fn interleaved_coord_type_roundtrips() -> Result<()> {
    let ctx = context();
    let ids = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
    let labels = Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef;
    let points = point_array(
        &[Some(wkt!(POINT(1.0 2.0))), Some(wkt!(POINT(3.0 4.0)))],
        CoordType::Interleaved,
    )
    .into_array_ref();
    let geometry =
        Arc::new(point_type(Dimension::XY, CoordType::Interleaved).to_field("geom", true));
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, true)),
        Arc::new(Field::new("label", DataType::Utf8, true)),
        geometry,
    ]));
    let batch = RecordBatch::try_new(schema, vec![ids, labels, points])?;
    ctx.register_batch("interleaved", batch)?;

    let bytes = binary_value(
        &ctx,
        "SELECT ST_AsFlatGeobuf(geom, id, label) FROM interleaved",
    )
    .await?;
    let batches = collect(
        &ctx,
        &format!(
            "SELECT id, geom FROM st_fromflatgeobuf(decode('{}', 'hex')) ORDER BY id",
            hex(&bytes)
        ),
    )
    .await?;
    let batch = &batches[0];
    let geometry = from_arrow_array(batch.column(1), batch.schema().field(1)).unwrap();
    let points = geometry.as_any().downcast_ref::<PointArray>().unwrap();
    let second = points.value(1).unwrap().coord().unwrap();
    assert_eq!((second.x(), second.y()), (3.0, 4.0));
    Ok(())
}

#[tokio::test]
async fn multi_partition_merge_supports_generic_geometry_and_all_null_groups() -> Result<()> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    geodatafusion::register(&ctx);

    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, true)),
        Arc::new(Field::new("label", DataType::Utf8, true)),
        Arc::new(Field::new("wkt", DataType::Utf8, true)),
    ]));
    let first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("POINT(1 2)"),
                Some("POINT(3 4)"),
            ])) as ArrayRef,
        ],
    )?;
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("c")])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![first], vec![second]])?;
    ctx.register_table("generic_points", Arc::new(table))?;

    let dataframe = ctx
        .sql(
            "SELECT id, ST_AsFlatGeobuf(ST_PointFromText(wkt, 4326), id, label) AS fgb
             FROM generic_points
             GROUP BY id
             ORDER BY id",
        )
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    assert_eq!(
        aggregate_plan_modes(&plan),
        (1, 1),
        "expected a Partial -> Final aggregate plan"
    );
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

    let batch = &batches[0];
    let ids = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    let fgb = batch.column(1).as_binary::<i32>();
    let first_bytes = hex(fgb.value(0));
    let null_bytes = hex(fgb.value(1));
    assert_eq!(
        ids.values(),
        &[1, 2],
        "partition-local groups must both survive the final merge"
    );

    let decoded = collect(
        &ctx,
        &format!(
            "SELECT id, geom IS NULL AS geom_is_null \
             FROM st_fromflatgeobuf(decode('{first_bytes}', 'hex')) ORDER BY id"
        ),
    )
    .await?;
    assert_eq!(decoded.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(!decoded[0].column(1).as_boolean().value(0));
    assert!(!decoded[0].column(1).as_boolean().value(1));

    let decoded = collect(
        &ctx,
        &format!(
            "SELECT geom IS NULL AS geom_is_null \
             FROM st_fromflatgeobuf(decode('{null_bytes}', 'hex'))"
        ),
    )
    .await?;
    assert_eq!(decoded.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(decoded[0].column(0).as_boolean().value(0));
    Ok(())
}

#[tokio::test]
async fn xym_coordinate_dimension_roundtrips() -> Result<()> {
    let ctx = context();
    let mut builder = PointBuilder::new(point_type(Dimension::XYM, CoordType::Separated));
    builder.push_coord(Some(&wkt::types::Coord {
        x: 1.0,
        y: 2.0,
        z: None,
        m: Some(5.0),
    }));
    builder.push_coord(Some(&wkt::types::Coord {
        x: 3.0,
        y: 4.0,
        z: None,
        m: Some(6.0),
    }));
    let points = builder.finish().into_array_ref();
    let ids = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
    let geometry =
        Arc::new(point_type(Dimension::XYM, CoordType::Separated).to_field("geom", true));
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, true)),
        geometry,
    ]));
    let batch = RecordBatch::try_new(schema, vec![ids, points])?;
    ctx.register_batch("xym", batch)?;

    let bytes = binary_value(&ctx, "SELECT ST_AsFlatGeobuf(geom, id) FROM xym").await?;
    let batches = collect(
        &ctx,
        &format!(
            "SELECT id, geom FROM st_fromflatgeobuf(decode('{}', 'hex')) ORDER BY id",
            hex(&bytes)
        ),
    )
    .await?;
    let batch = &batches[0];
    let geometry = from_arrow_array(batch.column(1), batch.schema().field(1)).unwrap();
    let points = geometry
        .as_any()
        .downcast_ref::<PointArray>()
        .expect("typed point array");
    let first = points.value(0).unwrap().coord().unwrap();
    let second = points.value(1).unwrap().coord().unwrap();
    assert_eq!(first.dim(), geo_traits::Dimensions::Xym);
    assert_eq!(first.nth(2), Some(5.0));
    assert_eq!(second.nth(2), Some(6.0));
    Ok(())
}

#[tokio::test]
async fn typed_wkb_sql_roundtrip_preserves_crs_null_and_attributes() -> Result<()> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    geodatafusion::register(&ctx);

    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, true)),
        Arc::new(Field::new("label", DataType::Utf8, true)),
        Arc::new(Field::new("wkt", DataType::Utf8, true)),
    ]));
    let first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("POINT(1 2)"),
                Some("POINT(3 4)"),
            ])) as ArrayRef,
        ],
    )?;
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("c")])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ],
    )?;
    ctx.register_table(
        "typed_wkb_points",
        Arc::new(MemTable::try_new(schema, vec![vec![first], vec![second]])?),
    )?;

    let dataframe = ctx
        .sql(
            "SELECT id, ST_AsFlatGeobuf(ST_AsBinary(ST_PointFromText(wkt, 4326)), id, label) AS fgb
             FROM typed_wkb_points
             GROUP BY id
             ORDER BY id",
        )
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    assert_eq!(
        aggregate_plan_modes(&plan),
        (1, 1),
        "expected a Partial -> Final aggregate plan"
    );
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    let batch = &batches[0];
    let ids = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    let fgb = batch.column(1).as_binary::<i32>();
    assert_eq!(ids.values(), &[1, 2]);

    let point_bytes = hex(fgb.value(0));
    let null_bytes = hex(fgb.value(1));
    let decoded = collect(
        &ctx,
        &format!(
            "SELECT id, geom FROM st_fromflatgeobuf(decode('{point_bytes}', 'hex')) ORDER BY id"
        ),
    )
    .await?;
    assert_eq!(decoded.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(
        decoded[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>()
            .values(),
        &[1, 1]
    );
    let decoded_schema = decoded[0].schema();
    let geom_field = decoded_schema.field(1);
    let geo_type = GeoArrowType::try_from(geom_field).unwrap();
    assert!(matches!(geo_type, GeoArrowType::Point(_)));
    assert_eq!(
        geo_type
            .metadata()
            .crs()
            .crs_value()
            .and_then(|value| value.as_str()),
        Some("EPSG:4326")
    );

    let decoded = collect(
        &ctx,
        &format!(
            "SELECT id, geom, geom IS NULL AS geom_is_null \
             FROM st_fromflatgeobuf(decode('{null_bytes}', 'hex'))"
        ),
    )
    .await?;
    assert_eq!(decoded.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(decoded[0].column(2).as_boolean().value(0));
    let geo_type = GeoArrowType::try_from(decoded[0].schema().field(1)).unwrap();
    assert!(matches!(geo_type, GeoArrowType::Geometry(_)));
    assert_eq!(
        geo_type
            .metadata()
            .crs()
            .crs_value()
            .and_then(|value| value.as_str()),
        Some("EPSG:4326")
    );
    Ok(())
}

#[tokio::test]
async fn typed_wkb_rejects_mixed_types_and_dimensions() -> Result<()> {
    async fn error_for(wkts: [&str; 2]) -> String {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        geodatafusion::register(&ctx);
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "wkt",
            DataType::Utf8,
            true,
        ))]));
        let partitions = wkts
            .into_iter()
            .map(|wkt| {
                vec![
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(StringArray::from(vec![Some(wkt)])) as ArrayRef],
                    )
                    .unwrap(),
                ]
            })
            .collect::<Vec<_>>();
        ctx.register_table(
            "wkb_input",
            Arc::new(MemTable::try_new(schema, partitions).unwrap()),
        )
        .unwrap();

        collect(
            &ctx,
            "SELECT ST_AsFlatGeobuf(ST_AsBinary(ST_GeomFromText(wkt))) FROM wkb_input",
        )
        .await
        .unwrap_err()
        .to_string()
    }

    let error = error_for(["POINT(1 2)", "LINESTRING(0 0,1 1)"]).await;
    assert!(error.contains("mixed geometry types"), "{error}");
    let error = error_for(["POINT(1 2)", "POINT Z(3 4 5)"]).await;
    assert!(error.contains("mixed coordinate dimensions"), "{error}");
    Ok(())
}

#[tokio::test]
async fn non_nullable_metadata_and_null_properties_are_preserved() -> Result<()> {
    let ctx = context();
    let ids = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
    let labels = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
    let points = point_array(
        &[Some(wkt!(POINT(1.0 2.0))), Some(wkt!(POINT(3.0 4.0)))],
        CoordType::Separated,
    )
    .into_array_ref();
    let batch = point_batch(ids, labels, points, false);
    ctx.register_batch("non_nullable", batch)?;

    let bytes = binary_value(
        &ctx,
        "SELECT ST_AsFlatGeobuf(geom, id, label) FROM non_nullable",
    )
    .await?;
    let batches = collect(
        &ctx,
        &format!(
            "SELECT id IS NULL AS id_is_null, label IS NULL AS label_is_null \
             FROM st_fromflatgeobuf(decode('{}', 'hex')) ORDER BY id",
            hex(&bytes)
        ),
    )
    .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let batch = &batches[0];
    assert!(!batch.column(0).as_boolean().value(0));
    assert!(!batch.column(0).as_boolean().value(1));
    assert!(!batch.column(1).as_boolean().value(0));
    assert!(batch.column(1).as_boolean().value(1));
    assert!(!batch.schema().field(0).is_nullable());
    Ok(())
}

#[tokio::test]
async fn row_mode_supports_index_and_preserves_metadata() -> Result<()> {
    let ctx = context();
    let geometry = Arc::new(point_type(Dimension::XY, CoordType::Separated).to_field("geom", true));
    let row_fields = Fields::from(vec![
        Arc::new(Field::new("id", DataType::Int32, true)),
        Arc::clone(&geometry),
    ]);
    let ids = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
    let points = point_array(
        &[Some(wkt!(POINT(1.0 2.0))), Some(wkt!(POINT(3.0 4.0)))],
        CoordType::Separated,
    )
    .into_array_ref();
    let rows = Arc::new(StructArray::new(
        row_fields.clone(),
        vec![ids, points],
        None,
    )) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
        "row",
        DataType::Struct(row_fields),
        true,
    ))]));
    let batch = RecordBatch::try_new(schema, vec![rows])?;
    ctx.register_batch("rows", batch)?;

    let bytes = binary_value(&ctx, "SELECT ST_AsFlatGeobuf(row, true, 'geom') FROM rows").await?;
    let batches = collect(
        &ctx,
        &format!(
            "SELECT id, geom FROM st_fromflatgeobuf(decode('{}', 'hex')) ORDER BY id",
            hex(&bytes)
        ),
    )
    .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    Ok(())
}

#[tokio::test]
async fn from_flatgeobuf_rejects_malformed_bytes_and_extra_arguments() -> Result<()> {
    let ctx = context();
    let error = ctx
        .sql("SELECT * FROM st_fromflatgeobuf(decode('00', 'hex'))")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("invalid FlatGeobuf data"),
        "unexpected error: {error}"
    );

    let error = ctx
        .sql("SELECT * FROM st_fromflatgeobuf()")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("expects"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn flattened_geometry_accepts_the_index_option_and_rejects_geom_name() -> Result<()> {
    let ctx = context();
    for (sql, expected_index) in [
        (
            "SELECT ST_AsFlatGeobuf(ST_PointFromText('POINT(1 2)', 4326), true)",
            true,
        ),
        (
            "SELECT ST_AsFlatGeobuf(ST_PointFromText('POINT(1 2)', 4326), false)",
            false,
        ),
    ] {
        let bytes = binary_value(&ctx, sql).await?;
        let reader = flatgeobuf::FgbReader::open(std::io::Cursor::new(&bytes))
            .map_err(|error| datafusion::error::DataFusionError::Execution(error.to_string()))?;
        assert_eq!(reader.header().index_node_size() == 16, expected_index);
        drop(reader);
        let batches = collect(
            &ctx,
            &format!(
                "SELECT * FROM st_fromflatgeobuf(decode('{}', 'hex'))",
                hex(&bytes)
            ),
        )
        .await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "geom");
    }

    let error = binary_value(
        &ctx,
        "SELECT ST_AsFlatGeobuf(ST_PointFromText('POINT(1 2)', 4326), false, 'geom')",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("geom_name option is not supported by the flattened form"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn from_flatgeobuf_rejects_the_postgis_template_argument() -> Result<()> {
    let ctx = context();
    let bytes = aggregate_bytes(&ctx).await?;
    for template in ["NULL", "named_struct('fid', 1)"] {
        let error = ctx
            .sql(&format!(
                "SELECT id FROM st_fromflatgeobuf({template}, decode('{}', 'hex'))",
                hex(&bytes)
            ))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not support the PostgreSQL row-template argument"),
            "unexpected error for template {template}: {error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn flatgeobuf_functions_are_registered_only_as_table_functions() {
    let ctx = context();
    let state = ctx.state();
    for name in ["st_fromflatgeobuf", "st_fromflatgeobuftotable"] {
        assert!(
            state.table_functions().get(name).is_some(),
            "{name} must be registered as a table function"
        );
        assert!(
            state.scalar_functions().get(name).is_none(),
            "{name} must not be registered as a scalar function; the PostgreSQL \
             call shape cannot trigger a catalog side effect or set-returning read"
        );
    }
}

#[tokio::test]
async fn aggregate_rejects_missing_geometry_and_wrong_arguments() -> Result<()> {
    let ctx = context();
    let error = ctx.sql("SELECT ST_AsFlatGeobuf(1, 2)").await.unwrap_err();
    assert!(
        error.to_string().contains("not a geometry column"),
        "unexpected error: {error}"
    );

    let error = ctx
        .sql("SELECT ST_AsFlatGeobuf(ST_GeomFromText('POINT(1.0 2.0)'), named_struct('id', 1))")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("cannot represent"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn flattened_form_rejects_lost_metadata_and_unrepresentable_properties() -> Result<()> {
    let ctx = context();
    // DataFusion's `named_struct` drops GeoArrow extension metadata, so the
    // struct row carries no geometry column and must fail instead of writing a
    // file without geometry.
    let error = ctx
        .sql("SELECT ST_AsFlatGeobuf(named_struct('geom', ST_GeomFromText('POINT(1 2)')))")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("row contains no geometry column"),
        "unexpected error: {error}"
    );

    let error = ctx
        .sql("SELECT ST_AsFlatGeobuf(ST_GeomFromText('POINT(1 2)'), ARRAY[1, 2])")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("which FlatGeobuf cannot represent"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn to_table_defers_catalog_mutation_until_stream_execution() -> Result<()> {
    let bytes = aggregate_bytes(&context()).await?;
    let hex = hex(&bytes);

    let ctx = context();
    let dataframe = ctx
        .sql(&format!(
            "SELECT * FROM st_fromflatgeobuftotable('public', 'fgb_uncollected', decode('{hex}', 'hex'))"
        ))
        .await?;
    assert!(!ctx.table_exist("public.fgb_uncollected")?);
    drop(dataframe);
    assert!(!ctx.table_exist("public.fgb_uncollected")?);

    let ctx = context();
    let _ = collect(
        &ctx,
        &format!(
            "EXPLAIN SELECT * FROM st_fromflatgeobuftotable('public', 'fgb_explain', decode('{hex}', 'hex'))"
        ),
    )
    .await?;
    assert!(!ctx.table_exist("public.fgb_explain")?);

    let ctx = context();
    let _ = collect(
        &ctx,
        &format!(
            "SELECT * FROM st_fromflatgeobuftotable('public', 'fgb_limit', decode('{hex}', 'hex')) LIMIT 0"
        ),
    )
    .await?;
    assert!(!ctx.table_exist("public.fgb_limit")?);

    let ctx = context();
    let dataframe = ctx
        .sql(&format!(
            "SELECT * FROM st_fromflatgeobuftotable('public', 'fgb_created', decode('{hex}', 'hex'))"
        ))
        .await?;
    assert!(!ctx.table_exist("public.fgb_created")?);
    let batches = dataframe.collect().await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert!(ctx.table_exist("public.fgb_created")?);

    let stored = collect(&ctx, "SELECT id, label FROM fgb_created ORDER BY id").await?;
    assert_eq!(stored.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

    let error = ctx
        .sql(&format!(
            "SELECT * FROM st_fromflatgeobuftotable('public', 'fgb_created', decode('{hex}', 'hex'))"
        ))
        .await?
        .collect()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("already exists"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn to_table_count_star_and_explain_analyze_execute_and_create() -> Result<()> {
    let bytes = aggregate_bytes(&context()).await?;
    let hex = hex(&bytes);

    let ctx = context();
    let batch = one_batch(
        &ctx,
        &format!(
            "SELECT COUNT(*) AS count \
             FROM st_fromflatgeobuftotable('public', 'fgb_count', decode('{hex}', 'hex'))"
        ),
    )
    .await?;
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        3
    );
    assert!(ctx.table_exist("public.fgb_count")?);

    let ctx = context();
    let _ = collect(
        &ctx,
        &format!(
            "EXPLAIN ANALYZE SELECT * FROM st_fromflatgeobuftotable('public', 'fgb_analyze', decode('{hex}', 'hex'))"
        ),
    )
    .await?;
    assert!(ctx.table_exist("public.fgb_analyze")?);
    Ok(())
}

#[tokio::test]
async fn to_table_missing_schema_fails_when_the_stream_runs() -> Result<()> {
    let bytes = aggregate_bytes(&context()).await?;
    let ctx = context();
    let dataframe = ctx
        .sql(&format!(
            "SELECT * FROM st_fromflatgeobuftotable('missing_schema', 'fgb_missing', decode('{}', 'hex'))",
            hex(&bytes)
        ))
        .await?;
    let error = dataframe.collect().await.unwrap_err();
    assert!(
        error.to_string().contains("does not exist"),
        "unexpected error: {error}"
    );
    Ok(())
}
