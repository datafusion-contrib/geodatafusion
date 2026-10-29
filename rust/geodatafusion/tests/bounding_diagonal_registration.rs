use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, LineStringTrait};
use geoarrow_array::array::GeometryArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{Crs, GeometryType as GeoArrowGeometryType};

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

fn coord_values(coord: &impl CoordTrait<T = f64>) -> Vec<f64> {
    match coord.dim() {
        geo_traits::Dimensions::Xy => vec![coord.x(), coord.y()],
        geo_traits::Dimensions::Xyz => vec![coord.x(), coord.y(), coord.nth_or_panic(2)],
        geo_traits::Dimensions::Xym => vec![coord.x(), coord.y(), coord.nth_or_panic(2)],
        geo_traits::Dimensions::Xyzm => vec![
            coord.x(),
            coord.y(),
            coord.nth_or_panic(2),
            coord.nth_or_panic(3),
        ],
        geo_traits::Dimensions::Unknown(dim) => {
            panic!("unexpected coordinate dimension: {dim}")
        }
    }
}

fn line_values(array: &GeometryArray, index: usize) -> Vec<Vec<f64>> {
    let geometry = array.value(index).unwrap();
    let line_string = match geometry.as_type() {
        GeometryType::LineString(line_string) => line_string,
        _ => panic!("expected LineString"),
    };
    line_string
        .coords()
        .map(|coord| coord_values(&coord))
        .collect()
}

#[tokio::test]
async fn bounding_diagonal_preserves_dimensions_and_nulls() {
    let ctx = context();
    let batches = ctx
        .sql(
            "SELECT ST_BoundingDiagonal(ST_GeomFromText(wkt)) AS diag \
             FROM (VALUES \
                 ('LINESTRING ZM(1 2 3 9,3 4 5 6)'), \
                 ('LINESTRING M(1 2 9,3 4 5)'), \
                 ('POINT EMPTY'), \
                 (NULL) \
             ) AS input(wkt)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 4);

    let schema = batch.schema();
    let output_field = schema.field(0);
    assert_eq!(
        output_field.extension_type_name(),
        Some("geoarrow.geometry")
    );
    let output_type = output_field
        .try_extension_type::<GeoArrowGeometryType>()
        .unwrap();
    assert_eq!(output_type.metadata().crs(), &Crs::default());

    let output = GeometryArray::try_from((batch.column(0).as_ref(), output_field)).unwrap();
    assert_eq!(
        line_values(&output, 0),
        vec![vec![1.0, 2.0, 3.0, 6.0], vec![3.0, 4.0, 5.0, 9.0]]
    );
    assert_eq!(
        line_values(&output, 1),
        vec![vec![1.0, 2.0, 5.0], vec![3.0, 4.0, 9.0]]
    );
    assert!(line_values(&output, 2).is_empty());
    assert!(output.is_null(3));
}

#[tokio::test]
async fn bounding_diagonal_fits_controls_empty_results() {
    let ctx = context();
    let batches = ctx
        .sql(
            "SELECT ST_BoundingDiagonal(ST_GeomFromText(wkt), fits) AS diag \
             FROM (VALUES \
                 ('POINT Z EMPTY', FALSE), \
                 ('POINT M EMPTY', TRUE), \
                 ('POINT(1 2)', CAST(NULL AS BOOLEAN)), \
                 (NULL, TRUE) \
             ) AS input(wkt, fits)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 4);

    let schema = batch.schema();
    let output = GeometryArray::try_from((batch.column(0).as_ref(), schema.field(0))).unwrap();
    assert!(line_values(&output, 0).is_empty());
    assert_eq!(
        line_values(&output, 1),
        vec![vec![0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0]]
    );
    assert!(output.is_null(2));
    assert!(output.is_null(3));
}

#[tokio::test]
async fn bounding_diagonal_preserves_crs_metadata() {
    let ctx = context();
    let batches = ctx
        .sql("SELECT ST_BoundingDiagonal(ST_PointFromText('POINT Z(1 2 3)', 4326)) AS diag")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    let schema = batch.schema();
    let output_type = schema
        .field(0)
        .try_extension_type::<GeoArrowGeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
}

#[tokio::test]
async fn bounding_diagonal_bare_null_returns_null() {
    let ctx = context();
    for query in [
        "SELECT ST_BoundingDiagonal(NULL) AS diag",
        "SELECT ST_BoundingDiagonal(NULL, NULL) AS diag",
    ] {
        let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let schema = batch.schema();
        assert_eq!(
            schema.field(0).extension_type_name(),
            Some("geoarrow.geometry")
        );
        let output = GeometryArray::try_from((batch.column(0).as_ref(), schema.field(0))).unwrap();
        assert!(output.is_null(0));
    }
}

#[tokio::test]
async fn bounding_diagonal_requires_boolean_fits() {
    let ctx = context();
    let error = ctx
        .sql("SELECT ST_BoundingDiagonal(ST_GeomFromText('POINT(1 2)'), 1)")
        .await
        .expect_err("numeric fits must be rejected");
    assert!(
        error.to_string().contains("argument 2 must be boolean"),
        "unexpected error: {error}"
    );
}
