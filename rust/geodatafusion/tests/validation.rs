//! Public SQL contracts for the 2D validity checks.

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

/// WKT values whose PostGIS 3.6.4 answers are `true` and `Valid Geometry`.
const VALID_GEOMETRIES: &[&str] = &[
    "POINT EMPTY",
    "LINESTRING EMPTY",
    "POLYGON EMPTY",
    "MULTIPOINT EMPTY",
    "MULTIPOINT(EMPTY,1 1)",
    "MULTILINESTRING EMPTY",
    "MULTIPOLYGON EMPTY",
    "MULTIPOLYGON(EMPTY)",
    "MULTIPOLYGON(EMPTY,((0 0,1 0,1 1,0 1,0 0)))",
    "GEOMETRYCOLLECTION EMPTY",
    "GEOMETRYCOLLECTION(POINT EMPTY)",
    "GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING EMPTY)",
    "GEOMETRYCOLLECTION(POINT(1 2),POINT EMPTY)",
    "POINT Z (1 2 3)",
    "POINT M (1 2 3)",
    "LINESTRING Z (0 0 1,1 1 2)",
    "POLYGON((0 0,2 0,2 2,0 2,0 0))",
];

fn validity_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("geom", DataType::Utf8, true)]));
    let mut values = VALID_GEOMETRIES
        .iter()
        .map(|wkt| Some(*wkt))
        .collect::<Vec<_>>();
    values.push(None);
    let geom = StringArray::from(values);
    RecordBatch::try_new(schema, vec![Arc::new(geom)]).unwrap()
}

fn assert_valid_rows(batch: &RecordBatch, rows: impl Iterator<Item = usize>) {
    let valid = batch.column_by_name("valid").unwrap().as_boolean();
    let reason = batch.column_by_name("reason").unwrap().as_string_view();
    for row in rows {
        assert!(valid.value(row), "row {row} should be valid");
        assert_eq!(reason.value(row), "Valid Geometry", "row {row}");
    }
}

#[tokio::test]
async fn is_valid_handles_empty_geometries_nulls_and_slices() {
    let ctx = context();
    ctx.register_batch("geometries", validity_batch()).unwrap();
    ctx.register_batch("tail", validity_batch().slice(VALID_GEOMETRIES.len(), 1))
        .unwrap();

    let sql = "SELECT
        ST_IsValid(ST_GeomFromText(geom)) AS valid,
        ST_IsValidReason(ST_GeomFromText(geom)) AS reason
    FROM ";
    let batch = ctx
        .sql(&format!("{sql}geometries"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_valid_rows(&batch, 0..VALID_GEOMETRIES.len());
    assert!(
        batch
            .column_by_name("valid")
            .unwrap()
            .as_boolean()
            .is_null(VALID_GEOMETRIES.len())
    );
    assert!(
        batch
            .column_by_name("reason")
            .unwrap()
            .as_string_view()
            .is_null(VALID_GEOMETRIES.len())
    );

    let sliced = ctx
        .sql(&format!("{sql}tail"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(
        sliced
            .column_by_name("valid")
            .unwrap()
            .as_boolean()
            .is_null(0)
    );
    assert!(
        sliced
            .column_by_name("reason")
            .unwrap()
            .as_string_view()
            .is_null(0)
    );
}

#[tokio::test]
async fn is_valid_reports_invalid_geometry_reasons() {
    let ctx = context();
    let batch = ctx
        .sql("SELECT
            ST_IsValid(ST_GeomFromText('POLYGON((0 0,2 2,2 0,0 2,0 0))')) AS valid,
            ST_IsValidReason(ST_GeomFromText('POLYGON((0 0,2 2,2 0,0 2,0 0))')) AS reason,
            ST_IsValid(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))')) AS holed
        ")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(!batch.column_by_name("valid").unwrap().as_boolean().value(0));
    // PostGIS reports `Self-intersection[1 1]` for this geometry. The native
    // checker reports the geo crate wording instead; the value is documented in
    // README.md under Geometry Validation.
    assert_eq!(
        batch
            .column_by_name("reason")
            .unwrap()
            .as_string_view()
            .value(0),
        "exterior ring has a self-intersection"
    );
    assert!(batch.column_by_name("holed").unwrap().as_boolean().value(0));
}
