use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

fn input_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("matrix", DataType::Utf8, true),
        Field::new("pattern", DataType::Utf8, true),
    ]));
    let a = StringArray::from(vec![
        Some("POINT(1 2)"),
        Some("LINESTRING(0 0,1 1)"),
        Some("POINT EMPTY"),
        Some("GEOMETRYCOLLECTION(POINT(1 2))"),
        None,
    ]);
    let b = StringArray::from(vec![
        Some("POINT(1 2)"),
        Some("LINESTRING(1 1,0 0)"),
        Some("POINT EMPTY"),
        Some("GEOMETRYCOLLECTION(POINT(1 2))"),
        Some("POINT(0 0)"),
    ]);
    let matrix = StringArray::from(vec![
        Some("101010101"),
        Some("101010101"),
        Some("101010101"),
        Some("101010101"),
        None,
    ]);
    let pattern = StringArray::from(vec![
        Some("T*T***T**"),
        Some("FF*FF****"),
        Some("T*T***T**"),
        Some("T*T***T**"),
        Some("T*T***T**"),
    ]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(a),
            Arc::new(b),
            Arc::new(matrix),
            Arc::new(pattern),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn multi_row_sliced_arrays_and_nulls() {
    let ctx = context();
    ctx.register_batch("inputs", input_batch().slice(2, 3))
        .unwrap();
    let batch = ctx
        .sql(
            "SELECT
                ST_OrderingEquals(ST_GeomFromText(a), ST_GeomFromText(b)) AS ordering_equal,
                ST_RelateMatch(matrix, pattern) AS matrix_match
             FROM inputs",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(batch.num_rows(), 3);
    let ordering = batch.column(0).as_boolean();
    assert!(ordering.value(1));
    assert!(ordering.value(0));
    assert!(ordering.is_null(2));
    let matching = batch.column(1).as_boolean();
    assert!(matching.value(0));
    assert!(matching.value(1));
    assert!(matching.is_null(2));
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn geos_multi_row_sliced_arrays() {
    let ctx = context();
    ctx.register_batch("inputs", input_batch().slice(0, 3))
        .unwrap();
    let batch = ctx
        .sql(
            "SELECT
                ST_ContainsProperly(
                    ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'),
                    ST_GeomFromText(a)
                ) AS contained
             FROM inputs",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let contained = batch.column(0).as_boolean();
    assert!(contained.value(0));
    assert!(!contained.value(1));
    assert!(!contained.value(2));
}

#[tokio::test]
async fn ordering_equals_single_member_collection_and_metadata() {
    let ctx = context();
    let batch = ctx
        .sql(
            "SELECT
                ST_OrderingEquals(
                    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'),
                    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))')
                ) AS collection_same,
                ST_OrderingEquals(
                    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'),
                    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(2 1))')
                ) AS collection_different,
                ST_OrderingEquals(
                    ST_PointFromText('POINT(1 2)', 4326),
                    ST_PointFromText('POINT(1 2)', 3857)
                ) AS srid_different",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(batch.column(0).as_boolean().value(0));
    assert!(!batch.column(1).as_boolean().value(0));
    assert!(!batch.column(2).as_boolean().value(0));
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn geos_relation_rejects_mixed_srid() {
    let ctx = context();
    for sql in [
        "SELECT ST_ContainsProperly(
            ST_PointFromText('POINT(1 2)', 4326),
            ST_PointFromText('POINT(1 2)', 3857)
        )",
        "SELECT ST_Relate(
            ST_PointFromText('POINT(1 2)', 4326),
            ST_PointFromText('POINT(1 2)', 3857)
        )",
    ] {
        let error = ctx
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("mixed SRID"), "{error}");
    }
}

#[tokio::test]
async fn relate_match_errors_are_runtime_errors() {
    let ctx = context();
    let error = ctx
        .sql("SELECT ST_RelateMatch('10101010X', 'T*T***T**')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Unknown dimension symbol: X"), "{error}");
}

fn boolean_column(batch: &RecordBatch, name: &str) -> Vec<Option<bool>> {
    let array = batch.column_by_name(name).unwrap().as_boolean();
    (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array.value(row)))
        .collect()
}

#[tokio::test]
async fn topological_predicates_follow_postgis_across_slices_nulls_and_empty() {
    let ctx = context();
    ctx.register_batch("inputs", input_batch()).unwrap();
    ctx.register_batch("tail", input_batch().slice(2, 3))
        .unwrap();

    let expressions = PREDICATE_NAMES
        .iter()
        .map(|name| {
            format!(
                "{name}(ST_GeomFromText(a), ST_GeomFromText(b)) AS {}",
                name.to_lowercase()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let batch = ctx
        .sql(&format!("SELECT {expressions} FROM inputs"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    // Values reviewed against the pinned PostGIS 3.6.4 oracle. Rows are
    // POINT, reversed LINESTRING, POINT EMPTY, single-member collection, NULL.
    let expected: &[(&str, &[Option<bool>])] = &[
        (
            "st_intersects",
            &[Some(true), Some(true), Some(false), Some(true), None],
        ),
        (
            "st_contains",
            &[Some(true), Some(true), Some(false), Some(true), None],
        ),
        (
            "st_equals",
            &[Some(true), Some(true), Some(true), Some(true), None],
        ),
        (
            "st_disjoint",
            &[Some(false), Some(false), Some(true), Some(false), None],
        ),
        (
            "st_covers",
            &[Some(true), Some(true), Some(false), Some(true), None],
        ),
        (
            "st_coveredby",
            &[Some(true), Some(true), Some(false), Some(true), None],
        ),
        (
            "st_touches",
            &[Some(false), Some(false), Some(false), Some(false), None],
        ),
        (
            "st_crosses",
            &[Some(false), Some(false), Some(false), Some(false), None],
        ),
        (
            "st_overlaps",
            &[Some(false), Some(false), Some(false), Some(false), None],
        ),
        (
            "st_within",
            &[Some(true), Some(true), Some(false), Some(true), None],
        ),
    ];
    for (name, values) in expected {
        assert_eq!(&boolean_column(&batch, name), values, "{name}");
    }

    // The sliced batch keeps the offset rows, so EMPTY, collection, and NULL
    // still line up with their Reviewed values.
    let sliced = ctx
        .sql(&format!("SELECT {expressions} FROM tail"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        boolean_column(&sliced, "st_intersects"),
        vec![Some(false), Some(true), None]
    );
    assert_eq!(
        boolean_column(&sliced, "st_equals"),
        vec![Some(true), Some(true), None]
    );
    assert_eq!(
        boolean_column(&sliced, "st_disjoint"),
        vec![Some(true), Some(false), None]
    );

    let validity = ctx
        .sql("SELECT ST_IsValid(ST_GeomFromText(a)) AS valid, ST_IsValidReason(ST_GeomFromText(a)) AS reason FROM inputs")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        boolean_column(&validity, "valid"),
        vec![Some(true), Some(true), Some(true), Some(true), None]
    );
    let reason = validity.column_by_name("reason").unwrap().as_string_view();
    assert_eq!(reason.value(0), "Valid Geometry");
    assert!(reason.is_null(4));
}

const PREDICATE_NAMES: &[&str] = &[
    "ST_Contains",
    "ST_CoveredBy",
    "ST_Covers",
    "ST_Crosses",
    "ST_Disjoint",
    "ST_Equals",
    "ST_Intersects",
    "ST_Overlaps",
    "ST_Touches",
    "ST_Within",
];

#[tokio::test]
async fn topological_predicates_ignore_z_and_m_but_keep_boundary_rules() {
    let ctx = context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let a = StringArray::from(vec![
        Some("POINT Z (1 2 5)"),
        Some("POINT M (1 2 7)"),
        Some("POLYGON((0 0,2 0,2 2,0 2,0 0))"),
        Some("POLYGON Z ((0 0 1,2 0 2,2 2 3,0 2 4,0 0 1))"),
        Some("POLYGON M ((0 0 1,2 0 2,2 2 3,0 2 4,0 0 1))"),
        Some("POINT(2 2)"),
        Some("POLYGON((0 0,2 0,2 2,0 2,0 0))"),
        Some("POLYGON((0 0,2 0,2 2,0 2,0 0))"),
        Some("LINESTRING(-1 1,3 1)"),
    ]);
    let b = StringArray::from(vec![
        Some("POINT(1 2)"),
        Some("POINT(1 2)"),
        Some("POINT(0 1)"),
        Some("POINT(1 1)"),
        Some("POINT(1 1)"),
        Some("POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))"),
        Some("POLYGON((2 0,4 0,4 2,2 2,2 0))"),
        Some("POLYGON((2 2,4 2,4 4,2 4,2 2))"),
        Some("POLYGON((0 0,2 0,2 2,0 2,0 0))"),
    ]);
    ctx.register_batch(
        "dimensional",
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap(),
    )
    .unwrap();

    let expressions = PREDICATE_NAMES
        .iter()
        .map(|name| {
            format!(
                "{name}(ST_GeomFromText(a), ST_GeomFromText(b)) AS {}",
                name.to_lowercase()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let batch = ctx
        .sql(&format!("SELECT {expressions} FROM dimensional"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    // Values reviewed against PostGIS 3.6.4: Z and M ordinates are ignored,
    // a boundary point is covered but not contained, a hole interior is
    // disjoint, and shared edges or corners only touch.
    let expected: &[(&str, &[Option<bool>])] = &[
        (
            "st_intersects",
            &[
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                Some(true),
            ],
        ),
        (
            "st_contains",
            &[
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
            ],
        ),
        (
            "st_equals",
            &[
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
            ],
        ),
        (
            "st_disjoint",
            &[
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
            ],
        ),
        (
            "st_covers",
            &[
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
            ],
        ),
        (
            "st_coveredby",
            &[
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
            ],
        ),
        (
            "st_touches",
            &[
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                Some(false),
            ],
        ),
        (
            "st_crosses",
            &[
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(true),
            ],
        ),
        ("st_overlaps", &[Some(false); 9]),
        (
            "st_within",
            &[
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
            ],
        ),
    ];
    for (name, values) in expected {
        assert_eq!(&boolean_column(&batch, name), values, "{name}");
    }
}

#[tokio::test]
async fn topological_predicates_reject_mixed_srid_arguments() {
    let ctx = context();
    for name in PREDICATE_NAMES {
        let sql = format!(
            "SELECT {name}(
                ST_PointFromText('POINT(1 2)', 4326),
                ST_PointFromText('POINT(1 2)', 3857)
            )"
        );
        let error = predicate_error(&ctx, &sql).await;
        assert!(error.contains("mixed SRID"), "{name}: {error}");

        // PostGIS treats SRID 0 as an unknown CRS that still conflicts with a
        // declared SRID, so this must fail as well.
        let sql = format!(
            "SELECT {name}(
                ST_PointFromText('POINT(1 2)', 0),
                ST_PointFromText('POINT(1 2)', 4326)
            )"
        );
        let error = predicate_error(&ctx, &sql).await;
        assert!(error.contains("mixed SRID"), "{name}: {error}");
    }

    let batch = ctx
        .sql(
            "SELECT
            ST_Intersects(
                ST_PointFromText('POINT(1 2)', 3857),
                ST_PointFromText('POINT(1 2)', 3857)
            ) AS same_srid,
            ST_Intersects(
                ST_PointFromText('POINT(1 2)', 0),
                ST_PointFromText('POINT(1 2)', 0)
            ) AS both_unknown,
            ST_IsValid(ST_PointFromText('POINT(1 2)', 4326)) AS valid
        ",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(boolean_column(&batch, "same_srid"), vec![Some(true)]);
    assert_eq!(boolean_column(&batch, "both_unknown"), vec![Some(true)]);
    assert_eq!(boolean_column(&batch, "valid"), vec![Some(true)]);
}

async fn predicate_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(dataframe) => match dataframe.collect().await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected `{sql}` to fail"),
        },
    }
}

#[tokio::test]
async fn topological_predicates_expose_postgis_field_contract() {
    let ctx = context();
    let plan = ctx
        .sql(
            "SELECT
            ST_Intersects(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)')) AS intersects,
            ST_IsValidReason(ST_GeomFromText('POINT(1 2)')) AS reason
        ",
        )
        .await
        .unwrap();
    // DataFusion names an unaliased scalar call after its arguments, so the
    // public contract of these UDFs is the return type and nullability.
    let schema = plan.schema();
    let intersects = schema.field(0);
    assert_eq!(intersects.name(), "intersects");
    assert_eq!(intersects.data_type(), &DataType::Boolean);
    assert!(intersects.is_nullable());
    let reason = schema.field(1);
    assert_eq!(reason.name(), "reason");
    assert_eq!(reason.data_type(), &DataType::Utf8View);
    assert!(reason.is_nullable());
}
