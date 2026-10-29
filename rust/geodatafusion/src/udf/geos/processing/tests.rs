use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo::{Area, CoordsIter};
use geo_traits::{CoordTrait, Dimensions, GeometryTrait, PointTrait};
use geoarrow_array::array::GeometryArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{CoordType, Edges, GeometryType, Metadata};
use wkb::reader::Wkb;

use crate::register;
use crate::udf::native::geometry::StrictGeometryBuilder;

// Expected values marked as PostGIS examples were checked against the pinned
// PostGIS 3.6.4 / GEOS 3.14.1 oracle.
fn context() -> SessionContext {
    let context = SessionContext::new();
    register(&context);
    context
}

async fn execute(context: &SessionContext, sql: &str) -> RecordBatch {
    let dataframe = context
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let batches = dataframe
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    assert_eq!(batches.len(), 1, "unexpected batch count for {sql}");
    batches.into_iter().next().unwrap()
}

async fn text_value(context: &SessionContext, sql: &str) -> Option<String> {
    let batch = execute(context, sql).await;
    let values = batch.column(0).as_string::<i32>();
    (!values.is_null(0)).then(|| values.value(0).to_string())
}

async fn query_error(context: &SessionContext, sql: &str) -> String {
    match context.sql(sql).await {
        Ok(dataframe) => dataframe.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    }
}

fn geometry_array(batch: &RecordBatch) -> GeometryArray {
    GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0))).unwrap()
}

fn output_geometry(batch: &RecordBatch) -> geo::Geometry<f64> {
    let array = geometry_array(batch);
    let geometry = array.value(0).unwrap();
    geometry_to_geo(&geometry).unwrap()
}

#[tokio::test]
async fn postgis_overlay_examples() {
    let context = context();

    let difference = text_value(
        &context,
        "SELECT ST_AsText(ST_Difference(
            ST_GeomFromText('LINESTRING(50 100, 50 200)'),
            ST_GeomFromText('LINESTRING(50 50, 50 150)')
        ))",
    )
    .await;
    assert_eq!(difference.as_deref(), Some("LINESTRING(50 150,50 200)"));

    let disjoint = text_value(
        &context,
        "SELECT ST_AsText(ST_Intersection(
            ST_GeomFromText('POINT(0 0)'),
            ST_GeomFromText('LINESTRING(2 0, 0 2)')
        ))",
    )
    .await;
    // Pinned PostGIS 3.6.4 / GEOS 3.14.1 returns a typed empty here.
    assert_eq!(disjoint.as_deref(), Some("POINT EMPTY"));

    let touching = text_value(
        &context,
        "SELECT ST_AsText(ST_Intersection(
            ST_GeomFromText('POINT(0 0)'),
            ST_GeomFromText('LINESTRING(0 0, 0 2)')
        ))",
    )
    .await;
    assert_eq!(touching.as_deref(), Some("POINT(0 0)"));

    let symmetric = text_value(
        &context,
        "SELECT ST_AsText(ST_SymDifference(
            ST_GeomFromText('LINESTRING(50 100, 50 200)'),
            ST_GeomFromText('LINESTRING(50 50, 50 150)')
        ))",
    )
    .await;
    assert_eq!(
        symmetric.as_deref(),
        Some("MULTILINESTRING((50 150,50 200),(50 50,50 100))")
    );
}

#[tokio::test]
async fn overlay_supports_scalar_array_mixing_and_grid_size() {
    let context = context();
    let batch = execute(
        &context,
        "WITH lines(wkt) AS (
            VALUES ('LINESTRING(0 0, 2 0)'), ('LINESTRING(0 1, 2 1)')
        )
        SELECT ST_AsText(ST_Intersection(
            ST_GeomFromText(wkt),
            ST_GeomFromText('POLYGON((1 -1, 3 -1, 3 1, 1 1, 1 -1))')
        )) FROM lines",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_horizontal_line(values.value(0), "0");
    assert_horizontal_line(values.value(1), "1");

    let batch = execute(
        &context,
        "WITH lines(wkt) AS (
            VALUES ('LINESTRING(0 0, 2 0)'), ('LINESTRING(0 1, 2 1)')
        )
        SELECT ST_AsText(ST_Intersection(
            ST_GeomFromText('POLYGON((1 -1, 3 -1, 3 1, 1 1, 1 -1))'),
            ST_GeomFromText(wkt)
        )) FROM lines",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_horizontal_line(values.value(0), "0");
    assert_horizontal_line(values.value(1), "1");

    let batch = execute(
        &context,
        "WITH lines(wkt) AS (
            VALUES ('LINESTRING(0.2 0.5, 9.8 0.5)')
        )
        SELECT
            ST_AsText(ST_Intersection(
                ST_GeomFromText(wkt),
                ST_GeomFromText('POLYGON((0 0, 10 0, 10 1, 0 1, 0 0))')
            )),
            ST_AsText(ST_Intersection(
                ST_GeomFromText(wkt),
                ST_GeomFromText('POLYGON((0 0, 10 0, 10 1, 0 1, 0 0))'),
                1
            ))
        FROM lines",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "LINESTRING(0.2 0.5,9.8 0.5)"
    );
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        "LINESTRING(0 1,10 1)"
    );

    let batch = execute(
        &context,
        "WITH inputs(grid_size) AS (
            VALUES (CAST(-1 AS DOUBLE)), (CAST(1 AS DOUBLE))
        )
        SELECT ST_AsText(ST_Intersection(
            ST_GeomFromText('LINESTRING(0.2 0.5, 9.8 0.5)'),
            ST_GeomFromText('POLYGON((0 0, 10 0, 10 1, 0 1, 0 0))'),
            grid_size
        )) FROM inputs",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_eq!(values.value(0), "LINESTRING(0.2 0.5,9.8 0.5)");
    assert_eq!(values.value(1), "LINESTRING(0 1,10 1)");

    for function in ["ST_Difference", "ST_SymDifference"] {
        let sql = format!(
            "SELECT ST_AsText({function}(
                ST_GeomFromText('LINESTRING(0.2 0.5,9.8 0.5)'),
                ST_GeomFromText('LINESTRING(4.2 0.5,6.2 0.5)'),
                1
            ))"
        );
        let actual = text_value(&context, &sql).await;
        assert_eq!(
            actual.as_deref(),
            Some("MULTILINESTRING((0 1,4 1),(6 1,10 1))")
        );
    }

    let batch = execute(
        &context,
        "SELECT ST_Intersection(
            ST_Point(0, 0),
            ST_Point(0, 0),
            CAST(NULL AS DOUBLE)
        ) IS NULL",
    )
    .await;
    assert!(batch.column(0).as_boolean().value(0));
}

#[tokio::test]
async fn overlay_preserves_postgis_empty_operands() {
    let context = context();
    let cases = [
        (
            "ST_Intersection(
                ST_GeomFromText('POINT EMPTY'),
                ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))')
            )",
            "POINT EMPTY",
        ),
        (
            "ST_Difference(
                ST_GeomFromText('LINESTRING(0 0,1 1)'),
                ST_GeomFromText('POLYGON EMPTY')
            )",
            "LINESTRING(0 0,1 1)",
        ),
        (
            "ST_SymDifference(
                ST_GeomFromText('POINT EMPTY'),
                ST_GeomFromText('POINT(2 3)')
            )",
            "POINT(2 3)",
        ),
    ];

    for (expression, expected) in cases {
        let sql = format!("SELECT ST_AsText({expression})");
        let actual = text_value(&context, &sql).await;
        assert_eq!(actual.as_deref(), Some(expected), "{expression}");
    }
}

#[tokio::test]
async fn overlay_preserves_single_member_geometry_collections() {
    let context = context();
    let point = wkt::types::Point::from_coord(wkt::types::Coord {
        x: 1.0,
        y: 2.0,
        z: None,
        m: None,
    });
    let collection =
        wkt::types::GeometryCollection::new(vec![point.clone().into()], wkt::types::Dimension::XY);

    let mut builder = StrictGeometryBuilder::new(GeometryType::new(Default::default()));
    builder.push_geometry(Some(&point)).unwrap();
    builder.push_geometry(Some(&collection)).unwrap();
    builder
        .push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)
        .unwrap();
    let array = builder.finish();
    let field = array.data_type().to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![array.into_array_ref()],
    )
    .unwrap();
    context.register_batch("inputs", batch).unwrap();

    let batch = execute(
        &context,
        "SELECT
            ST_AsText(ST_Difference(
                geometry,
                ST_GeomFromText('POLYGON EMPTY')
            )),
            ST_AsText(ST_SymDifference(
                geometry,
                ST_GeomFromText('POLYGON EMPTY')
            )),
            ST_AsText(ST_SymDifference(
                ST_GeomFromText('POLYGON EMPTY'),
                geometry
            ))
        FROM inputs",
    )
    .await;
    let difference = batch.column(0).as_string::<i32>();
    let symmetric_left = batch.column(1).as_string::<i32>();
    let symmetric_right = batch.column(2).as_string::<i32>();
    assert_eq!(difference.value(0), "POINT(1 2)");
    assert_eq!(difference.value(1), "GEOMETRYCOLLECTION(POINT(1 2))");
    assert!(difference.is_null(2));
    assert_eq!(symmetric_left.value(0), "POINT(1 2)");
    assert_eq!(symmetric_left.value(1), "GEOMETRYCOLLECTION(POINT(1 2))");
    assert!(symmetric_left.is_null(2));
    assert_eq!(symmetric_right.value(0), "POINT(1 2)");
    assert_eq!(symmetric_right.value(1), "GEOMETRYCOLLECTION(POINT(1 2))");
    assert!(symmetric_right.is_null(2));
}

#[tokio::test]
async fn unary_union_dissolves_overlapping_components() {
    let context = context();
    let batch = execute(
        &context,
        "SELECT ST_UnaryUnion(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 15 5, 15 15, 5 15, 5 5)))'
        ))",
    )
    .await;
    let geometry = output_geometry(&batch);
    assert!((geometry.unsigned_area() - 175.0).abs() < 1e-9);
    assert_eq!(geometry.coords_count(), 9);

    let empty = text_value(
        &context,
        "SELECT ST_AsText(ST_UnaryUnion(ST_GeomFromText('LINESTRING EMPTY')))",
    )
    .await;
    assert_eq!(empty.as_deref(), Some("LINESTRING EMPTY"));

    let batch = execute(
        &context,
        "SELECT ST_UnaryUnion(
            ST_GeomFromText(
                'MULTIPOLYGON(
                    ((0.2 0.2,5.8 0.2,5.8 5.8,0.2 5.8,0.2 0.2)),
                    ((4.2 4.2,7.8 4.2,7.8 7.8,4.2 7.8,4.2 4.2))
                )'
            ),
            1
        )",
    )
    .await;
    let geometry = output_geometry(&batch);
    assert!((geometry.unsigned_area() - 48.0).abs() < 1e-9);
    assert_eq!(bounds(&geometry), (0.0, 0.0, 8.0, 8.0));
}

#[tokio::test]
async fn node_matches_postgis_linework_examples() {
    let context = context();
    let cases = [
        (
            "MULTILINESTRING((0 0, 10 0),(5 -5, 5 5))",
            "MULTILINESTRING((0 0,5 0),(5 -5,5 0),(5 0,10 0),(5 0,5 5))",
        ),
        (
            "MULTILINESTRING((0 0, 10 0, 20 0),(25 0, 15 0, 8 0))",
            "MULTILINESTRING((0 0,8 0),(8 0,10 0,15 0,20 0),(20 0,25 0))",
        ),
        (
            "LINESTRING Z (0 0 0, 10 10 10, 0 10 5, 10 0 3)",
            "MULTILINESTRING Z((0 0 0,5 5 4.5),(5 5 4.5,10 10 10,0 10 5,5 5 4.5),(5 5 4.5,10 0 3))",
        ),
    ];

    for (input, expected) in cases {
        let sql = format!("SELECT ST_AsText(ST_Node(ST_GeomFromText('{input}')))");
        let actual = text_value(&context, &sql).await;
        assert_eq!(actual.as_deref(), Some(expected), "{input}");
    }

    for input in ["LINESTRING EMPTY", "MULTILINESTRING EMPTY"] {
        let sql = format!("SELECT ST_AsText(ST_Node(ST_GeomFromText('{input}')))");
        let actual = text_value(&context, &sql).await;
        assert_eq!(actual.as_deref(), Some(input));
    }

    let error = context
        .sql("SELECT ST_Node(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))'))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("topological dimension 1"), "{error}");
}

#[tokio::test]
async fn buffer_supports_style_and_integer_overloads() {
    let context = context();
    let batch = execute(
        &context,
        "SELECT ST_Buffer(ST_Point(0, 0), 1, 'quad_segs=2')",
    )
    .await;
    let geometry = output_geometry(&batch);
    assert_eq!(geometry.coords_count(), 9);
    assert!((geometry.unsigned_area() - 2.8284271247461903).abs() < 1e-9);

    let batch = execute(&context, "SELECT ST_Buffer(ST_Point(0, 0), 1, 2)").await;
    assert_eq!(output_geometry(&batch).coords_count(), 9);

    let batch = execute(
        &context,
        "SELECT ST_Buffer(
            ST_GeomFromText('LINESTRING(0 0, 10 0)'),
            2,
            'quad_segs=2 endcap=flat'
        )",
    )
    .await;
    let geometry = output_geometry(&batch);
    let (min_x, min_y, max_x, max_y) = bounds(&geometry);
    assert!((min_x - 0.0).abs() < 1e-9);
    assert!((max_x - 10.0).abs() < 1e-9);
    assert!((min_y + 2.0).abs() < 1e-9);
    assert!((max_y - 2.0).abs() < 1e-9);

    let batch = execute(
        &context,
        "SELECT ST_Buffer(
            ST_GeomFromText('LINESTRING(0 0, 10 0)'),
            2,
            'side=left'
        )",
    )
    .await;
    let (_, min_y, _, max_y) = bounds(&output_geometry(&batch));
    assert!(min_y >= -1e-9, "left buffer crossed to the right side");
    assert!((max_y - 2.0).abs() < 1e-9);

    let error = context
        .sql("SELECT ST_Buffer(ST_Point(0, 0), 1, 'unknown=1')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("does not support buffer parameter 'unknown'"),
        "{error}"
    );

    let batch = execute(
        &context,
        "WITH inputs(radius) AS (VALUES (CAST(0 AS DOUBLE)), (CAST(2 AS DOUBLE)))
        SELECT ST_Buffer(ST_Point(0, 0), radius, 2) FROM inputs",
    )
    .await;
    let array = geometry_array(&batch);
    assert_eq!(array.len(), 2);
    assert_eq!(array.value(0).unwrap().dim(), Dimensions::Xy);
    assert_eq!(array.value(1).unwrap().dim(), Dimensions::Xy);
}

#[tokio::test]
async fn build_area_handles_area_empty_and_non_areal_inputs() {
    let context = context();
    let batch = execute(
        &context,
        "SELECT ST_BuildArea(ST_GeomFromText(
            'LINESTRING(0 0, 10 0, 10 10, 0 10, 0 0)'
        ))",
    )
    .await;
    assert!((output_geometry(&batch).unsigned_area() - 100.0).abs() < 1e-9);

    let batch = execute(
        &context,
        "SELECT ST_BuildArea(ST_GeomFromText(
            'GEOMETRYCOLLECTION(
                LINESTRING(0 0, 10 0, 10 10, 0 10, 0 0),
                LINESTRING(4 4, 6 4, 6 6, 4 6, 4 4)
            )'
        ))",
    )
    .await;
    let geometry = output_geometry(&batch);
    assert!(matches!(
        &geometry,
        geo::Geometry::Polygon(polygon) if polygon.interiors().len() == 1
    ));
    assert!((geometry.unsigned_area() - 96.0).abs() < 1e-9);

    let non_areal = text_value(
        &context,
        "SELECT ST_AsText(ST_BuildArea(ST_GeomFromText('LINESTRING(0 0, 1 1)')))",
    )
    .await;
    assert_eq!(non_areal, None);

    let empty = text_value(
        &context,
        "SELECT ST_AsText(ST_BuildArea(ST_GeomFromText('POINT EMPTY')))",
    )
    .await;
    assert_eq!(empty.as_deref(), Some("POLYGON EMPTY"));
}

#[tokio::test]
async fn output_preserves_crs_metadata_and_native_dimension_behavior() {
    let context = context();
    let batch = execute(
        &context,
        "SELECT ST_Intersection(ST_Point(0, 0, 4326), ST_Point(0, 0, 4326))",
    )
    .await;
    let output_type = batch
        .schema_ref()
        .field(0)
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert!(output_type.metadata().edges().is_none());

    let batch = execute(&context, "SELECT ST_Buffer(ST_Point(0, 0, 4326), 1)").await;
    let output_type = batch
        .schema_ref()
        .field(0)
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert!(output_type.metadata().edges().is_none());

    let batch = execute(
        &context,
        "SELECT ST_Intersection(
            ST_GeomFromText('LINESTRING Z (0 0 0, 2 2 2)'),
            ST_GeomFromText('LINESTRING Z (0 2 4, 2 0 8)')
        )",
    )
    .await;
    let array = geometry_array(&batch);
    let geometry = array.value(0).unwrap();
    assert_eq!(geometry.dim(), Dimensions::Xyz);
    let wkt = text_value(
        &context,
        "SELECT ST_AsText(ST_Intersection(
            ST_GeomFromText('LINESTRING Z (0 0 0, 2 2 2)'),
            ST_GeomFromText('LINESTRING Z (0 2 4, 2 0 8)')
        ))",
    )
    .await;
    assert_eq!(wkt.as_deref(), Some("POINT Z(1 1 3.5)"));

    let z_queries = [
        "SELECT ST_Difference(
            ST_GeomFromText('MULTIPOINT Z (-118.58 38.38 5,-118.60 38.329 6,-118.614 38.281 7)'),
            ST_GeomFromText('POINT Z (-118.614 38.281 5)')
        )",
        "SELECT ST_SymDifference(
            ST_GeomFromText('LINESTRING Z (1 2 1,1 4 2)'),
            ST_GeomFromText('LINESTRING Z (1 1 3,1 3 4)')
        )",
        "SELECT ST_UnaryUnion(ST_GeomFromText(
            'MULTILINESTRING Z ((0 0 1,10 0 2),(5 0 3,15 0 4))'
        ))",
        "SELECT ST_Node(ST_GeomFromText(
            'LINESTRING Z (0 0 0, 10 10 10, 0 10 5, 10 0 3)'
        ))",
        "SELECT ST_BuildArea(ST_GeomFromText(
            'LINESTRING Z (0 0 1,10 0 2,10 10 3,0 10 4,0 0 1)'
        ))",
    ];
    for sql in z_queries {
        let batch = execute(&context, sql).await;
        let array = geometry_array(&batch);
        assert_eq!(array.value(0).unwrap().dim(), Dimensions::Xyz, "{sql}");
    }

    let batch = execute(
        &context,
        "SELECT ST_Buffer(ST_GeomFromText('POINT Z (0 0 5)'), 1)",
    )
    .await;
    let array = geometry_array(&batch);
    let geometry = array.value(0).unwrap();
    assert_eq!(geometry.dim(), Dimensions::Xy);

    let error = query_error(
        &context,
        "SELECT ST_Intersection(ST_Point(0, 0), ST_Point(0, 0, 4326))",
    )
    .await;
    assert!(error.contains("mixed SRID geometries"), "{error}");

    let error = query_error(
        &context,
        "SELECT ST_Intersection(ST_Point(0, 0, 4326), ST_Point(0, 0, 3857))",
    )
    .await;
    assert!(error.contains("mixed SRID geometries"), "{error}");

    let batch = execute(
        &context,
        "SELECT ST_Intersection(NULL, ST_Point(0, 0, 4326))",
    )
    .await;
    let output_type = batch
        .schema_ref()
        .field(0)
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert!(output_type.metadata().edges().is_none());
    let batch = execute(
        &context,
        "SELECT ST_Intersection(NULL, ST_Point(0, 0, 4326)) IS NULL",
    )
    .await;
    assert!(batch.column(0).as_boolean().value(0));

    for (table, edges) in [("spherical", Edges::Spherical), ("karney", Edges::Karney)] {
        let metadata = Arc::new(Metadata::new(Crs::default(), Some(edges)));
        let typ = GeometryType::new(metadata).with_coord_type(CoordType::Separated);
        let mut builder = StrictGeometryBuilder::new(typ);
        builder
            .push_geometry(Some(&wkt::types::Point::from_coord(wkt::types::Coord {
                x: 1.0,
                y: 2.0,
                z: None,
                m: None,
            })))
            .unwrap();
        let array = builder.finish();
        let field = array.data_type().to_field("geometry", true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new([Arc::new(field)])),
            vec![array.into_array_ref()],
        )
        .unwrap();
        context.register_batch(table, batch).unwrap();

        let error = query_error(
            &context,
            &format!("SELECT ST_Buffer(geometry, 1) FROM {table}"),
        )
        .await;
        assert!(
            error.contains("supports only planar edge metadata"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn null_arguments_propagate() {
    let context = context();
    let batch = execute(
        &context,
        "SELECT
            ST_Buffer(
                ST_GeomFromText('POINT M (1 2 3)'),
                CAST(NULL AS DOUBLE)
            ) IS NULL,
            ST_UnaryUnion(
                ST_GeomFromText('POINT M (1 2 3)'),
                CAST(NULL AS DOUBLE)
            ) IS NULL",
    )
    .await;
    assert!(batch.column(0).as_boolean().value(0));
    assert!(batch.column(1).as_boolean().value(0));

    let queries = [
        "ST_Difference(
            ST_GeomFromText('POINT(0 0)'),
            ST_GeomFromText(CAST(NULL AS TEXT))
        )",
        "ST_UnaryUnion(
            ST_GeomFromText('POINT(0 0)'),
            CAST(NULL AS DOUBLE)
        )",
        "ST_Buffer(
            ST_GeomFromText('POINT(0 0)'),
            CAST(NULL AS DOUBLE),
            'quad_segs=2'
        )",
        "ST_Intersection(
            ST_GeomFromText(CAST(NULL AS TEXT)),
            ST_GeomFromText('POINT(0 0)')
        )",
        "ST_SymDifference(
            ST_GeomFromText(CAST(NULL AS TEXT)),
            ST_GeomFromText('POINT(0 0)')
        )",
        "ST_UnaryUnion(ST_GeomFromText(CAST(NULL AS TEXT)))",
        "ST_Node(ST_GeomFromText(CAST(NULL AS TEXT)))",
        "ST_Buffer(ST_GeomFromText(CAST(NULL AS TEXT)), 1)",
        "ST_BuildArea(ST_GeomFromText(CAST(NULL AS TEXT)))",
    ];
    for expression in queries {
        let batch = execute(&context, &format!("SELECT {expression} IS NULL")).await;
        assert!(
            batch.column(0).as_boolean().value(0),
            "strict NULL did not win for {expression}"
        );
    }

    let queries = [
        "ST_Buffer(
                ST_GeomFromText('POINT M (1 2 3)'),
                CAST(NULL AS DOUBLE)
            )",
        "ST_UnaryUnion(
                ST_GeomFromText('POINT M (1 2 3)'),
                CAST(NULL AS DOUBLE)
            )",
        "ST_Intersection(
                ST_GeomFromText('POINT M (1 2 3)'),
                ST_GeomFromText('POINT M (1 2 3)'),
                CAST(NULL AS DOUBLE)
            )",
        "ST_Buffer(
                ST_GeomFromText('POINT M (1 2 3)'),
                1,
                CAST(NULL AS TEXT)
            )",
        "ST_LineMerge(
                ST_GeomFromText('LINESTRING(0 0, 1 1)'),
                CAST(NULL AS BOOLEAN)
            )",
    ];
    for expression in queries {
        let batch = execute(&context, &format!("SELECT {expression} IS NULL")).await;
        assert!(
            batch.column(0).as_boolean().value(0),
            "strict NULL did not win for {expression}"
        );
    }
}

#[tokio::test]
async fn measure_dimensions_are_stripped_before_geos() {
    let context = context();
    let z_query = "LINESTRING ZM (0 0 0 0,4 0 4 4)";
    let crossing = "LINESTRING ZM (2 -1 10 100,2 1 20 200)";

    let intersection = text_value(
        &context,
        &format!(
            "SELECT ST_AsText(ST_Intersection(
                ST_GeomFromText('{z_query}'),
                ST_GeomFromText('{crossing}')
            ))"
        ),
    )
    .await;
    assert_eq!(intersection.as_deref(), Some("POINT Z(2 0 8.5)"));

    let z_queries = [
        format!(
            "ST_Difference(
                ST_GeomFromText('{z_query}'),
                ST_GeomFromText('{crossing}')
            )"
        ),
        format!(
            "ST_SymDifference(
                ST_GeomFromText('{z_query}'),
                ST_GeomFromText('{crossing}')
            )"
        ),
        format!("ST_UnaryUnion(ST_GeomFromText('{z_query}'))"),
        format!("ST_Node(ST_GeomFromText('{z_query}'))"),
        "ST_BuildArea(ST_GeomFromText(
            'LINESTRING ZM (0 0 1 2,10 0 2 3,10 10 3 4,0 10 4 5,0 0 1 2)'
        ))"
        .to_string(),
        "ST_LineMerge(ST_GeomFromText(
            'MULTILINESTRING ZM ((0 0 1 2,4 0 4 5),(4 0 4 5,8 0 8 9))'
        ))"
        .to_string(),
        "ST_DelaunayTriangles(ST_GeomFromText(
            'MULTIPOINT ZM (0 0 5 50,1 0 6 60,0 1 7 70)'
        ))"
        .to_string(),
        "ST_TriangulatePolygon(ST_GeomFromText(
            'POLYGON ZM ((0 0 1 2,4 0 2 3,4 4 3 4,2 2 4 5,0 4 5 6,0 0 1 2))'
        ))"
        .to_string(),
        "ST_ReducePrecision(ST_GeomFromText(
            'POINT ZM (1.2345 2.3456 3.4567 4.5678)'
        ), 0.1)"
            .to_string(),
    ];
    for expression in z_queries {
        let batch = execute(&context, &format!("SELECT {expression}")).await;
        assert_eq!(
            geometry_array(&batch).value(0).unwrap().dim(),
            Dimensions::Xyz,
            "{expression}"
        );
    }

    let measure_queries = [
        "ST_Buffer(ST_GeomFromText('POINT ZM (0 0 1 2)'), 1)".to_string(),
        "ST_DelaunayTriangles(ST_GeomFromText(
            'MULTIPOINT M (0 0 50,1 0 60,0 1 70)'
        ))"
        .to_string(),
        "ST_OffsetCurve(ST_GeomFromText('LINESTRING ZM (0 0 1 2,10 0 2 3,10 10 3 4)'), 1)"
            .to_string(),
        "ST_SharedPaths(
            ST_GeomFromText('LINESTRING ZM (0 0 1 2,10 0 2 3)'),
            ST_GeomFromText('LINESTRING ZM (0 0 3 4,10 0 4 5)')
        )"
        .to_string(),
        "ST_VoronoiLines(ST_GeomFromText(
            'MULTIPOINT ZM (0 0 5 50,1 0 6 60,0 1 7 70)'
        ))"
        .to_string(),
    ];
    for expression in measure_queries {
        let batch = execute(&context, &format!("SELECT {expression}")).await;
        let dimension = geometry_array(&batch).value(0).unwrap().dim();
        assert!(
            matches!(dimension, Dimensions::Xy | Dimensions::Xyz),
            "{expression}: {dimension:?}"
        );
    }

    // Inputs returned unchanged keep their original M ordinate.
    let passthrough = [
        "ST_Difference(
            ST_GeomFromText('LINESTRING ZM (0 0 0 0,4 0 4 4)'),
            ST_GeomFromText('POINT EMPTY')
        )"
        .to_string(),
        "ST_SymDifference(
            ST_GeomFromText('POINT EMPTY'),
            ST_GeomFromText('LINESTRING ZM (0 0 0 0,4 0 4 4)')
        )"
        .to_string(),
        "ST_UnaryUnion(ST_GeomFromText('LINESTRING ZM EMPTY'))".to_string(),
        "ST_LineMerge(ST_GeomFromText('LINESTRING ZM EMPTY'))".to_string(),
        "ST_OffsetCurve(ST_GeomFromText(
            'LINESTRING ZM (0 0 1 2,10 0 2 3,10 10 3 4)'
        ), 0)"
            .to_string(),
        "ST_ReducePrecision(ST_GeomFromText('POINT ZM EMPTY'), 0.1)".to_string(),
    ];
    for expression in passthrough {
        let batch = execute(&context, &format!("SELECT {expression}")).await;
        assert_eq!(
            geometry_array(&batch).value(0).unwrap().dim(),
            Dimensions::Xyzm,
            "{expression}"
        );
    }

    let empty_dimensions = [
        (
            "ST_Node(ST_GeomFromText('LINESTRING ZM EMPTY'))",
            Dimensions::Xy,
        ),
        (
            "ST_BuildArea(ST_GeomFromText('POINT ZM EMPTY'))",
            Dimensions::Xyz,
        ),
        (
            "ST_DelaunayTriangles(ST_GeomFromText('POINT ZM (0 0 1 2)'))",
            Dimensions::Xy,
        ),
        (
            "ST_TriangulatePolygon(ST_GeomFromText('POLYGON ZM EMPTY'))",
            Dimensions::Xy,
        ),
        (
            "ST_VoronoiLines(ST_GeomFromText('MULTIPOINT ZM EMPTY'))",
            Dimensions::Xy,
        ),
    ];
    for (expression, expected) in empty_dimensions {
        let batch = execute(&context, &format!("SELECT {expression}")).await;
        assert_eq!(
            geometry_array(&batch).value(0).unwrap().dim(),
            expected,
            "{expression}"
        );
    }
}

#[tokio::test]
async fn delaunay_triangles_supports_polygon_edge_and_tin_flags() {
    let context = context();
    let input = "MULTIPOINT(0 0,1 0,0 1)";

    let polygons = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_DelaunayTriangles(ST_GeomFromText('{input}')))"),
    )
    .await;
    assert_eq!(
        polygons.as_deref(),
        Some("GEOMETRYCOLLECTION(POLYGON((0 1,0 0,1 0,0 1)))")
    );

    let edges = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_DelaunayTriangles(ST_GeomFromText('{input}'), 0, 1))"),
    )
    .await;
    assert_eq!(
        edges.as_deref(),
        Some("MULTILINESTRING((0 1,1 0),(0 0,0 1),(0 0,1 0))")
    );

    let empty = text_value(
        &context,
        "SELECT ST_AsText(ST_DelaunayTriangles(ST_GeomFromText('POINT(0 0)')))",
    )
    .await;
    assert_eq!(empty.as_deref(), Some("GEOMETRYCOLLECTION EMPTY"));

    let z = execute(
        &context,
        "SELECT ST_DelaunayTriangles(ST_GeomFromText(
            'MULTIPOINT Z (0 0 5,1 0 6,0 1 7)'
        ))",
    )
    .await;
    assert_eq!(geometry_array(&z).value(0).unwrap().dim(), Dimensions::Xyz);

    let error = query_error(
        &context,
        &format!("SELECT ST_DelaunayTriangles(ST_GeomFromText('{input}'), 0, 2)"),
    )
    .await;
    assert!(error.contains("TIN output"), "{error}");

    let error = query_error(
        &context,
        &format!("SELECT ST_DelaunayTriangles(ST_GeomFromText('{input}'), 0, 3)"),
    )
    .await;
    assert!(error.contains("must be 0, 1, or 2"), "{error}");
}

#[tokio::test]
async fn triangulate_polygon_matches_constrained_delaunay() {
    let context = context();
    let cases = [
        (
            "POLYGON((0 0,4 0,4 4,2 2,0 4,0 0))",
            "GEOMETRYCOLLECTION(POLYGON((0 0,0 4,2 2,0 0)),POLYGON((2 2,4 4,4 0,2 2)),POLYGON((4 0,0 0,2 2,4 0)))",
        ),
        ("POLYGON EMPTY", "GEOMETRYCOLLECTION EMPTY"),
        ("POINT(0 0)", "GEOMETRYCOLLECTION EMPTY"),
    ];
    for (input, expected) in cases {
        let actual = text_value(
            &context,
            &format!("SELECT ST_AsText(ST_TriangulatePolygon(ST_GeomFromText('{input}')))"),
        )
        .await;
        assert_eq!(actual.as_deref(), Some(expected), "{input}");
    }

    let z = execute(
        &context,
        "SELECT ST_TriangulatePolygon(ST_GeomFromText(
            'POLYGON Z ((0 0 1,4 0 2,4 4 3,2 2 4,0 4 5,0 0 1))'
        ))",
    )
    .await;
    assert_eq!(geometry_array(&z).value(0).unwrap().dim(), Dimensions::Xyz);
}

#[tokio::test]
async fn offset_curve_supports_signed_distance_and_style_parameters() {
    let context = context();
    let line = "LINESTRING(0 0,10 0,10 10)";

    let basic = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_OffsetCurve(ST_GeomFromText('{line}'), 1))"),
    )
    .await;
    assert_eq!(basic.as_deref(), Some("LINESTRING(0 1,9 1,9 10)"));

    let reversed_side = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_OffsetCurve(ST_GeomFromText('{line}'), -1, 'join=bevel'))"),
    )
    .await;
    assert_eq!(
        reversed_side.as_deref(),
        Some("LINESTRING(0 -1,10 -1,11 0,11 10)")
    );

    let mitre = execute(
        &context,
        &format!(
            "SELECT ST_OffsetCurve(ST_GeomFromText('{line}'), -1, 'join=mitre mitre_limit=1')"
        ),
    )
    .await;
    let mitre = output_geometry(&mitre);
    assert_eq!(mitre.coords_count(), 4);
    assert_eq!(bounds(&mitre), (0.0, -1.0, 11.0, 10.0));

    let zero = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_OffsetCurve(ST_GeomFromText('{line}'), 0))"),
    )
    .await;
    assert_eq!(zero.as_deref(), Some("LINESTRING(0 0,10 0,10 10)"));

    let empty = text_value(
        &context,
        "SELECT ST_AsText(ST_OffsetCurve(ST_GeomFromText('POLYGON EMPTY'), 1))",
    )
    .await;
    assert_eq!(empty.as_deref(), Some("POLYGON EMPTY"));

    let z = execute(
        &context,
        "SELECT ST_OffsetCurve(ST_GeomFromText('LINESTRING Z (0 0 1,10 0 2,10 10 3)'), 1)",
    )
    .await;
    assert_eq!(geometry_array(&z).value(0).unwrap().dim(), Dimensions::Xy);

    let error = query_error(
        &context,
        &format!("SELECT ST_OffsetCurve(ST_GeomFromText('{line}'), 1, 'side=left')"),
    )
    .await;
    assert!(
        error.contains("does not support buffer parameter 'side'"),
        "{error}"
    );

    let error = query_error(
        &context,
        &format!("SELECT ST_OffsetCurve(ST_GeomFromText('{line}'), 1, 'quad_segs')"),
    )
    .await;
    assert!(error.contains("missing '=value'"), "{error}");
}

#[tokio::test]
async fn reduce_precision_matches_postgis_rounding() {
    let context = context();

    // Decimal grids are rounded by the native GEOS version: 3.11 multiplies by
    // the grid size while 3.14 and newer divide by its reciprocal, which can
    // differ by one ULP. Compare against the grid value with that tolerance
    // instead of asserting a bit-level text match.
    let grid_cases: [(f64, f64, f64); 4] = [
        (1.2345, 2.3456, 0.1),
        (-1.2345, -2.3456, 0.1),
        (0.12345, 0.23456, 0.001),
        (7.5, 8.5, 1.0),
    ];
    for (input_x, input_y, grid_size) in grid_cases {
        let sql = format!(
            "SELECT ST_X(ST_ReducePrecision(ST_GeomFromText('POINT({input_x} {input_y})'), {grid_size})), \
             ST_Y(ST_ReducePrecision(ST_GeomFromText('POINT({input_x} {input_y})'), {grid_size}))"
        );
        let batch = execute(&context, &sql).await;
        for (column, input_value) in [(0usize, input_x), (1, input_y)] {
            let value = batch
                .column(column)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("ST_X/ST_Y return Float64")
                .value(0);
            let expected = (input_value / grid_size).round() * grid_size;
            assert!(
                ulp_distance(value, expected) <= 1,
                "{sql}: {value} is not within one ULP of grid value {expected}"
            );
        }
    }

    // Grids that cannot drift in the native rounding path stay exact.
    let cases = [
        ("POINT(1.5 2.5)", 1.0, Some("POINT(2 3)")),
        ("POINT(1.2345 2.3456)", 0.0, Some("POINT(1.2345 2.3456)")),
        ("POINT(1.2345 2.3456)", -1.0, Some("POINT(1 2)")),
        (
            "POLYGON((0.1 0.1,0.14 0.1,0.14 0.14,0.1 0.14,0.1 0.1))",
            1.0,
            Some("POLYGON EMPTY"),
        ),
    ];
    for (input, grid_size, expected) in cases {
        let actual = text_value(
            &context,
            &format!(
                "SELECT ST_AsText(ST_ReducePrecision(ST_GeomFromText('{input}'), {grid_size}))"
            ),
        )
        .await;
        assert_eq!(actual.as_deref(), expected, "{input} grid={grid_size}");
    }

    let z = execute(
        &context,
        "SELECT ST_ReducePrecision(ST_GeomFromText('POINT Z (1.2345 2.3456 3.4567)'), 0.1)",
    )
    .await;
    let z_geometry = geometry_array(&z);
    assert_eq!(z_geometry.value(0).unwrap().dim(), Dimensions::Xyz);
    // The oracle comparator allows the reviewed decimal-grid difference on
    // every ordinate, so Z preservation is asserted exactly here.
    match z_geometry.value(0).unwrap().as_type() {
        geo_traits::GeometryType::Point(point) => assert_eq!(
            point.coord().unwrap().nth(2),
            Some(3.4567),
            "ST_ReducePrecision must keep Z unchanged"
        ),
        _ => panic!("unexpected ST_ReducePrecision output"),
    }
}

/// Distance between two nearby doubles in representable steps.
fn ulp_distance(left: f64, right: f64) -> u64 {
    if left == right {
        return 0;
    }
    if left == 0.0 || right == 0.0 || left.signum() != right.signum() {
        return u64::MAX;
    }
    let (low, high) = if left.abs() < right.abs() {
        (left.abs(), right.abs())
    } else {
        (right.abs(), left.abs())
    };
    high.to_bits() - low.to_bits()
}

#[tokio::test]
async fn shared_paths_returns_forward_and_backward_collections() {
    let context = context();
    let cases = [
        (
            "LINESTRING(0 0,10 0)",
            "LINESTRING(0 0,10 0)",
            "GEOMETRYCOLLECTION(MULTILINESTRING((0 0,10 0)),MULTILINESTRING EMPTY)",
        ),
        (
            "LINESTRING(0 0,10 0)",
            "LINESTRING(10 0,0 0)",
            "GEOMETRYCOLLECTION(MULTILINESTRING EMPTY,MULTILINESTRING((0 0,10 0)))",
        ),
        (
            "MULTILINESTRING((0 0,5 0),(10 0,15 0))",
            "MULTILINESTRING((0 0,5 0),(15 0,10 0))",
            "GEOMETRYCOLLECTION(MULTILINESTRING((0 0,5 0)),MULTILINESTRING((10 0,15 0)))",
        ),
        (
            "LINESTRING(0 0,10 0)",
            "LINESTRING(0 0,5 0,5 5)",
            "GEOMETRYCOLLECTION(MULTILINESTRING((0 0,5 0)),MULTILINESTRING EMPTY)",
        ),
        (
            "LINESTRING EMPTY",
            "LINESTRING(0 0,1 1)",
            "GEOMETRYCOLLECTION(MULTILINESTRING EMPTY,MULTILINESTRING EMPTY)",
        ),
    ];
    for (left, right, expected) in cases {
        let actual = text_value(
            &context,
            &format!(
                "SELECT ST_AsText(ST_SharedPaths(ST_GeomFromText('{left}'), ST_GeomFromText('{right}')))"
            ),
        )
        .await;
        assert_eq!(actual.as_deref(), Some(expected), "{left} | {right}");
    }

    let z = execute(
        &context,
        "SELECT ST_SharedPaths(
            ST_GeomFromText('LINESTRING Z (0 0 1,10 0 2)'),
            ST_GeomFromText('LINESTRING Z (0 0 3,10 0 4)')
        )",
    )
    .await;
    assert!(matches!(
        geometry_array(&z).value(0).unwrap().dim(),
        Dimensions::Xy | Dimensions::Xyz
    ));

    let error = query_error(
        &context,
        "SELECT ST_SharedPaths(ST_Point(0, 0), ST_GeomFromText('LINESTRING(0 0,1 1)'))",
    )
    .await;
    assert!(error.contains("not lineal"), "{error}");
}

#[tokio::test]
async fn voronoi_returns_lines_polygons_and_handles_null_extend_to() {
    let context = context();
    let input = "MULTIPOINT(0 0,1 0,0 1)";

    let lines = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_VoronoiLines(ST_GeomFromText('{input}')))"),
    )
    .await;
    assert_eq!(
        lines.as_deref(),
        Some("MULTILINESTRING((0.5 -1,0.5 0.5),(0.5 0.5,2 2),(-1 0.5,0.5 0.5))")
    );

    let polygons = execute(
        &context,
        &format!("SELECT ST_VoronoiPolygons(ST_GeomFromText('{input}'))"),
    )
    .await;
    let polygons = output_geometry(&polygons);
    let geo::Geometry::GeometryCollection(polygons) = polygons else {
        panic!("ST_VoronoiPolygons did not return a GeometryCollection");
    };
    assert_eq!(polygons.0.len(), 3);
    assert!((polygons.unsigned_area() - 9.0).abs() < 1e-9);

    let extended = text_value(
        &context,
        &format!(
            "SELECT ST_AsText(ST_VoronoiLines(
                ST_GeomFromText('{input}'),
                0,
                ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))')
            ))"
        ),
    )
    .await;
    assert_eq!(
        extended.as_deref(),
        Some("MULTILINESTRING((0.5 -1,0.5 0.5),(0.5 0.5,10 10),(-1 0.5,0.5 0.5))")
    );

    let null_extend_to = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_VoronoiLines(ST_GeomFromText('{input}'), 0, NULL))"),
    )
    .await;
    assert_eq!(null_extend_to, lines);

    let single = text_value(
        &context,
        "SELECT ST_AsText(ST_VoronoiPolygons(ST_GeomFromText('POINT(0 0)')))",
    )
    .await;
    assert_eq!(single.as_deref(), Some("GEOMETRYCOLLECTION EMPTY"));

    let z = execute(
        &context,
        "SELECT ST_VoronoiLines(ST_GeomFromText('MULTIPOINT Z (0 0 5,1 0 6,0 1 7)'))",
    )
    .await;
    assert_eq!(geometry_array(&z).value(0).unwrap().dim(), Dimensions::Xy);

    let error = query_error(
        &context,
        &format!("SELECT ST_VoronoiLines(ST_GeomFromText('{input}'), -1)"),
    )
    .await;
    assert!(error.contains("tolerance must be non-negative"), "{error}");
}

#[tokio::test]
async fn new_geos_processing_functions_propagate_null_arguments() {
    let context = context();
    let queries = [
        "ST_DelaunayTriangles(ST_GeomFromText(CAST(NULL AS TEXT)))",
        "ST_TriangulatePolygon(ST_GeomFromText(CAST(NULL AS TEXT)))",
        "ST_OffsetCurve(ST_GeomFromText(CAST(NULL AS TEXT)), 1)",
        "ST_ReducePrecision(ST_GeomFromText(CAST(NULL AS TEXT)), 0.1)",
        "ST_SharedPaths(
            ST_GeomFromText(CAST(NULL AS TEXT)),
            ST_GeomFromText('LINESTRING(0 0,1 1)')
        )",
        "ST_VoronoiLines(ST_GeomFromText(CAST(NULL AS TEXT)))",
        "ST_VoronoiPolygons(ST_GeomFromText('POINT(0 0)'), CAST(NULL AS DOUBLE))",
    ];
    for expression in queries {
        let batch = execute(&context, &format!("SELECT {expression} IS NULL")).await;
        assert!(
            batch.column(0).as_boolean().value(0),
            "strict NULL did not win for {expression}"
        );
    }

    let trailing = [
        "ST_DelaunayTriangles(
            ST_GeomFromText('MULTIPOINT(0 0,1 0,0 1)'),
            CAST(NULL AS DOUBLE)
        )",
        "ST_DelaunayTriangles(
            ST_GeomFromText('MULTIPOINT(0 0,1 0,0 1)'),
            0,
            CAST(NULL AS INT)
        )",
        "ST_OffsetCurve(
            ST_GeomFromText('LINESTRING(0 0,1 0)'),
            CAST(NULL AS DOUBLE)
        )",
        "ST_OffsetCurve(
            ST_GeomFromText('LINESTRING(0 0,1 0)'),
            1,
            CAST(NULL AS TEXT)
        )",
        "ST_ReducePrecision(
            ST_GeomFromText('POINT(0 0)'),
            CAST(NULL AS DOUBLE)
        )",
        "ST_SharedPaths(
            ST_GeomFromText('LINESTRING(0 0,1 1)'),
            ST_GeomFromText(CAST(NULL AS TEXT))
        )",
    ];
    for expression in trailing {
        let batch = execute(&context, &format!("SELECT {expression} IS NULL")).await;
        assert!(
            batch.column(0).as_boolean().value(0),
            "strict NULL did not win for {expression}"
        );
    }
}

fn bounds(geometry: &geo::Geometry<f64>) -> (f64, f64, f64, f64) {
    geometry.coords_iter().fold(
        (
            f64::INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
        ),
        |(min_x, min_y, max_x, max_y), coord| {
            (
                min_x.min(coord.x),
                min_y.min(coord.y),
                max_x.max(coord.x),
                max_y.max(coord.y),
            )
        },
    )
}

fn assert_horizontal_line(actual: &str, y: &str) {
    assert!(
        actual == format!("LINESTRING(1 {y},2 {y})")
            || actual == format!("LINESTRING(2 {y},1 {y})"),
        "unexpected line: {actual}"
    );
}

#[tokio::test]
async fn is_valid_detail_matches_postgis_examples() {
    let context = context();
    let invalid = execute(
        &context,
        "SELECT d.valid AS valid, d.reason AS reason, ST_AsText(d.location) AS location
         FROM (
            SELECT ST_IsValidDetail(ST_GeomFromText('POLYGON((0 0,1 1,1 0,0 1,0 0))')) AS d
         ) AS s",
    )
    .await;
    assert!(!invalid.column(0).as_boolean().value(0));
    assert_eq!(
        invalid.column(1).as_string::<i32>().value(0),
        "Self-intersection"
    );
    assert_eq!(
        invalid.column(2).as_string::<i32>().value(0),
        "POINT(0.5 0.5)"
    );

    let valid = execute(
        &context,
        "SELECT d.valid AS valid, d.reason AS reason, ST_AsText(d.location) AS location
         FROM (
            SELECT ST_IsValidDetail(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) AS d
         ) AS s",
    )
    .await;
    assert!(valid.column(0).as_boolean().value(0));
    assert!(valid.column(1).is_null(0));
    assert!(valid.column(2).is_null(0));

    let empty = execute(
        &context,
        "SELECT d.valid AS valid, d.reason AS reason, d.location IS NULL AS no_location
         FROM (SELECT ST_IsValidDetail(ST_GeomFromText('POINT EMPTY')) AS d) AS s",
    )
    .await;
    assert!(empty.column(0).as_boolean().value(0));
    assert!(empty.column(1).is_null(0));
    assert!(empty.column(2).as_boolean().value(0));

    let flags = execute(
        &context,
        "SELECT d.valid AS valid FROM (
            SELECT ST_IsValidDetail(
                ST_GeomFromText('POLYGON((0 0,1 1,1 0,0 1,0 0))'), 1
            ) AS d
         ) AS s",
    )
    .await;
    assert!(!flags.column(0).as_boolean().value(0));
}

#[tokio::test]
async fn is_valid_detail_propagates_null_arguments() {
    let context = context();
    let null_geometry = execute(
        &context,
        "SELECT ST_IsValidDetail(ST_GeomFromText(CAST(NULL AS TEXT))) IS NULL AS is_null",
    )
    .await;
    assert!(null_geometry.column(0).as_boolean().value(0));

    let null_flags = execute(
        &context,
        "SELECT ST_IsValidDetail(
            ST_GeomFromText('POINT(0 0)'), CAST(NULL AS INT)
        ) IS NULL AS is_null",
    )
    .await;
    assert!(null_flags.column(0).as_boolean().value(0));
}

#[tokio::test]
async fn make_valid_matches_postgis_examples() {
    let context = context();
    let bowtie = "POLYGON((0 0,1 1,1 0,0 1,0 0))";

    let default = execute(
        &context,
        &format!(
            "SELECT
                ST_Area(ST_MakeValid(ST_GeomFromText('{bowtie}'))) = 0.5,
                ST_NumGeometries(ST_MakeValid(ST_GeomFromText('{bowtie}'))) = 2,
                ST_IsValid(ST_MakeValid(ST_GeomFromText('{bowtie}')))"
        ),
    )
    .await;
    assert!(default.column(0).as_boolean().value(0));
    assert!(default.column(1).as_boolean().value(0));
    assert!(default.column(2).as_boolean().value(0));

    let structure = execute(
        &context,
        &format!(
            "SELECT
                ST_Area(ST_MakeValid(
                    ST_GeomFromText('{bowtie}'), 'method=structure'
                )) = 0.5,
                ST_NumGeometries(ST_MakeValid(
                    ST_GeomFromText('{bowtie}'), 'method=structure'
                )) = 2,
                ST_IsValid(ST_MakeValid(
                    ST_GeomFromText('{bowtie}'), 'method=structure'
                ))"
        ),
    )
    .await;
    assert!(structure.column(0).as_boolean().value(0));
    assert!(structure.column(1).as_boolean().value(0));
    assert!(structure.column(2).as_boolean().value(0));

    let keep_collapsed = text_value(
        &context,
        "SELECT arrow_cast(ST_GeometryType(ST_MakeValid(
                    ST_GeomFromText('POLYGON((0 0,1 0,1 0,0 0))'),
                    'method=structure keepcollapsed=true'
                )), 'Utf8')",
    )
    .await;
    assert_eq!(keep_collapsed.as_deref(), Some("ST_LineString"));

    let unknown_key = execute(
        &context,
        &format!(
            "SELECT
                ST_Area(ST_MakeValid(ST_GeomFromText('{bowtie}'), 'foo=bar')) = 0.5,
                ST_NumGeometries(ST_MakeValid(ST_GeomFromText('{bowtie}'), 'foo=bar')) = 2"
        ),
    )
    .await;
    assert!(unknown_key.column(0).as_boolean().value(0));
    assert!(unknown_key.column(1).as_boolean().value(0));

    let multi_collapsed = text_value(
        &context,
        "SELECT arrow_cast(ST_GeometryType(ST_MakeValid(
            ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 0,0 0)))'),
            'method=structure keepcollapsed=true'
        )), 'Utf8')",
    )
    .await;
    assert_eq!(multi_collapsed.as_deref(), Some("ST_MultiLineString"));

    let multi_polygon = text_value(
        &context,
        "SELECT arrow_cast(ST_GeometryType(ST_MakeValid(
            ST_GeomFromText('MULTIPOLYGON(((0 0,1 1,1 0,0 1,0 0)))')
        )), 'Utf8')",
    )
    .await;
    assert_eq!(multi_polygon.as_deref(), Some("ST_MultiPolygon"));
}

#[tokio::test]
async fn make_valid_handles_empties_nulls_and_invalid_options() {
    let context = context();

    let empty_z = text_value(
        &context,
        "SELECT ST_AsText(ST_MakeValid(ST_GeomFromText('POLYGON Z EMPTY')))",
    )
    .await;
    assert_eq!(empty_z.as_deref(), Some("POLYGON EMPTY"));

    let empty_zm = text_value(
        &context,
        "SELECT ST_AsText(ST_MakeValid(ST_GeomFromText('POLYGON ZM EMPTY')))",
    )
    .await;
    assert_eq!(empty_zm.as_deref(), Some("POLYGON EMPTY"));

    let zm = text_value(
        &context,
        "SELECT ST_AsText(ST_MakeValid(
            ST_GeomFromText('POLYGON ZM ((0 0 1 9,1 0 2 9,1 1 3 9,0 1 4 9,0 0 1 9))')
        ))",
    )
    .await;
    assert_eq!(
        zm.as_deref(),
        Some("POLYGON Z((0 0 1,1 0 2,1 1 3,0 1 4,0 0 1))")
    );

    let null_geometry = execute(
        &context,
        "SELECT ST_MakeValid(ST_GeomFromText(CAST(NULL AS TEXT))) IS NULL AS is_null",
    )
    .await;
    assert!(null_geometry.column(0).as_boolean().value(0));

    let null_params = execute(
        &context,
        "SELECT ST_MakeValid(
            ST_GeomFromText('POINT(0 0)'), CAST(NULL AS TEXT)
        ) IS NULL AS is_null",
    )
    .await;
    assert!(null_params.column(0).as_boolean().value(0));

    let bad_method = query_error(
        &context,
        "SELECT ST_MakeValid(ST_GeomFromText('POINT(0 0)'), 'method=bogus')",
    )
    .await;
    assert!(
        bad_method.contains("Unsupported value for 'method', 'bogus'"),
        "{bad_method}"
    );

    let missing_separator = query_error(
        &context,
        "SELECT ST_MakeValid(ST_GeomFromText('POINT(0 0)'), 'method')",
    )
    .await;
    assert!(
        missing_separator.contains("lacks separator '='"),
        "{missing_separator}"
    );
}

#[tokio::test]
async fn clip_by_box_2d_matches_postgis_examples() {
    let context = context();
    let box_expr = "ST_MakeBox2D(ST_Point(5, 5), ST_Point(15, 15))";

    let clipped = text_value(
        &context,
        &format!(
            "SELECT ST_AsText(ST_ClipByBox2D(
                ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'),
                {box_expr}
            ))"
        ),
    )
    .await;
    assert_eq!(
        clipped.as_deref(),
        Some("POLYGON((5 5,5 10,10 10,10 5,5 5))")
    );

    let inside = text_value(
        &context,
        "SELECT ST_AsText(ST_ClipByBox2D(
            ST_GeomFromText('POLYGON((1 1,2 1,2 2,1 2,1 1))'),
            ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))
        ))",
    )
    .await;
    assert_eq!(inside.as_deref(), Some("POLYGON((1 1,2 1,2 2,1 2,1 1))"));

    let outside = text_value(
        &context,
        "SELECT ST_AsText(ST_ClipByBox2D(
            ST_GeomFromText('POINT(50 50)'),
            ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))
        ))",
    )
    .await;
    assert_eq!(outside.as_deref(), Some("POINT EMPTY"));

    let z_outside = text_value(
        &context,
        "SELECT ST_AsText(ST_ClipByBox2D(
            ST_GeomFromText('POLYGON Z ((50 50 1,60 50 2,60 60 3,50 60 4,50 50 1))'),
            ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))
        ))",
    )
    .await;
    assert_eq!(z_outside.as_deref(), Some("POLYGON EMPTY"));

    // Z ordinates are restored from the input linework and the three-by-three
    // elevation model, so these values do not depend on the native GEOS
    // version even though GEOS 3.11's ClipByRect drops Z entirely.
    let z_cases = [
        (
            "POLYGON Z ((0 0 1,10 0 2,10 10 3,0 10 4,0 0 1))",
            box_expr.to_string(),
            "POLYGON Z((5 5 2.5,5 10 3.5,10 10 3,10 5 2.5,5 5 2.5))",
        ),
        (
            "POLYGON Z ((0 0 1,100 0 2,100 40 3,60 40 4,60 100 5,0 100 6,0 0 1))",
            "ST_MakeBox2D(ST_Point(25, 25), ST_Point(75, 75))".to_string(),
            "POLYGON Z((25 25 1,25 75 6,60 75 4.583333333333333,60 40 4,75 40 3.625,75 25 2,25 25 1))",
        ),
        (
            "LINESTRING Z (0 0 0,40 0 40,60 0 20,100 0 60)",
            "ST_MakeBox2D(ST_Point(25, -1), ST_Point(75, 1))".to_string(),
            "LINESTRING Z(25 0 25,40 0 40,60 0 20,75 0 35)",
        ),
        (
            "POLYGON Z ((0 0 1,200 0 99,200 100 3,0 100 4,0 0 1))",
            "ST_MakeBox2D(ST_Point(25, 25), ST_Point(75, 75))".to_string(),
            "POLYGON Z((25 25 1,25 75 4,75 75 26.75,75 25 26.75,25 25 1))",
        ),
        (
            "POLYGON Z ((1 1 1,2 1 2,2 2 3,1 2 4,1 1 1))",
            "ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))".to_string(),
            "POLYGON Z((1 1 1,2 1 2,2 2 3,1 2 4,1 1 1))",
        ),
        (
            "POLYGON ZM ((0 0 1 9,10 0 2 9,10 10 3 9,0 10 4 9,0 0 1 9))",
            box_expr.to_string(),
            "POLYGON Z((5 5 2.5,5 10 3.5,10 10 3,10 5 2.5,5 5 2.5))",
        ),
    ];
    for (wkt, box_expr, expected) in z_cases {
        let clipped = text_value(
            &context,
            &format!("SELECT ST_AsText(ST_ClipByBox2D(ST_GeomFromText('{wkt}'), {box_expr}))"),
        )
        .await;
        assert_eq!(clipped.as_deref(), Some(expected), "{wkt} in {box_expr}");
    }

    let z_collection = text_value(
        &context,
        "SELECT ST_AsText(ST_ClipByBox2D(
            ST_GeomFromText('GEOMETRYCOLLECTION Z (
                POINT Z (50 50 7),
                LINESTRING Z (0 50 1,100 50 9),
                POLYGON Z ((0 0 1,100 0 2,100 100 3,0 100 4,0 0 1))
            )'),
            ST_MakeBox2D(ST_Point(25, 25), ST_Point(75, 75))
        ))",
    )
    .await;
    assert_eq!(
        z_collection.as_deref(),
        Some(
            "GEOMETRYCOLLECTION Z(\
             POLYGON Z((25 25 1,25 75 4,75 75 3,75 25 2,25 25 1)),\
             LINESTRING Z(25 50 3,75 50 7),\
             POINT Z(50 50 7))"
        )
    );

    let collection = text_value(
        &context,
        "SELECT ST_AsText(ST_ClipByBox2D(
            ST_GeomFromText('GEOMETRYCOLLECTION(
                POINT(1 1),
                LINESTRING(-5 5,15 5),
                POLYGON((-1 -1,11 -1,11 11,-1 11,-1 -1))
            )'),
            ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))
        ))",
    )
    .await;
    assert_eq!(
        collection.as_deref(),
        Some(
            "GEOMETRYCOLLECTION(POLYGON((0 0,0 10,10 10,10 0,0 0)),\
             LINESTRING(0 5,10 5),POINT(1 1))"
        )
    );

    let degenerate = execute(
        &context,
        "SELECT ST_ClipByBox2D(
            ST_GeomFromText('LINESTRING(0 0,5 5)'),
            ST_MakeBox2D(ST_Point(0, 0), ST_Point(0, 0))
        ) IS NULL AS is_null",
    )
    .await;
    assert!(degenerate.column(0).as_boolean().value(0));

    let nulls = execute(
        &context,
        "SELECT
            ST_ClipByBox2D(
                ST_GeomFromText(CAST(NULL AS TEXT)),
                ST_MakeBox2D(ST_Point(0, 0), ST_Point(1, 1))
            ) IS NULL AS null_geometry,
            ST_ClipByBox2D(
                ST_GeomFromText('POINT(0 0)'),
                ST_MakeBox2D(
                    ST_Point(CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)),
                    ST_Point(1, 1)
                )
            ) IS NULL AS null_box",
    )
    .await;
    assert!(nulls.column(0).as_boolean().value(0));
    assert!(nulls.column(1).as_boolean().value(0));
}

#[tokio::test]
async fn clip_by_box_2d_preserves_metadata_and_rejects_bad_arguments() {
    let context = context();

    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let typ = GeometryType::new(metadata);
    let mut builder = StrictGeometryBuilder::new(typ);
    builder
        .push_geometry(Some(&wkt::types::Point::from_coord(wkt::types::Coord {
            x: 1.0,
            y: 2.0,
            z: None,
            m: None,
        })))
        .unwrap();
    let array = builder.finish();
    let field = array.data_type().to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![array.into_array_ref()],
    )
    .unwrap();
    context.register_batch("clip_inputs", batch).unwrap();

    let batch = execute(
        &context,
        "SELECT ST_ClipByBox2D(
            geometry,
            ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))
        )
        FROM clip_inputs",
    )
    .await;
    let output_type = batch
        .schema_ref()
        .field(0)
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );

    let error = query_error(
        &context,
        "SELECT ST_ClipByBox2D(ST_Point(0, 0), ST_Point(0, 0))",
    )
    .await;
    assert!(error.contains("must be a box2d"), "{error}");
}

#[tokio::test]
async fn simplify_polygon_hull_matches_postgis_examples() {
    let context = context();
    let polygon = "POLYGON((0 0,4 0,4 1,4 4,1 4,0 4,0 0))";

    let fraction_zero = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_SimplifyPolygonHull(ST_GeomFromText('{polygon}'), 0))"),
    )
    .await;
    assert_eq!(
        fraction_zero.as_deref(),
        Some("POLYGON((0 0,0 4,4 4,4 0,0 0))")
    );

    let fraction_one = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_SimplifyPolygonHull(ST_GeomFromText('{polygon}'), 1))"),
    )
    .await;
    assert_eq!(fraction_one.as_deref(), Some(polygon));

    let clamped_high = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_SimplifyPolygonHull(ST_GeomFromText('{polygon}'), 1.5))"),
    )
    .await;
    assert_eq!(clamped_high, fraction_one);

    let clamped_low = text_value(
        &context,
        &format!("SELECT ST_AsText(ST_SimplifyPolygonHull(ST_GeomFromText('{polygon}'), -0.5))"),
    )
    .await;
    assert_eq!(clamped_low, fraction_zero);

    let inner = text_value(
        &context,
        &format!(
            "SELECT ST_AsText(ST_SimplifyPolygonHull(ST_GeomFromText('{polygon}'), 0.5, false))"
        ),
    )
    .await;
    assert_eq!(inner.as_deref(), Some("POLYGON((4 0,4 4,0 4,4 0))"));

    let empty = text_value(
        &context,
        "SELECT ST_AsText(ST_SimplifyPolygonHull(
            ST_GeomFromText('POLYGON Z EMPTY'), 0.5
        ))",
    )
    .await;
    assert_eq!(empty.as_deref(), Some("POLYGON EMPTY"));

    let z = text_value(
        &context,
        "SELECT ST_AsText(ST_SimplifyPolygonHull(
            ST_GeomFromText('POLYGON Z ((0 0 1,4 0 2,4 1 3,4 4 4,1 4 5,0 4 6,0 0 1))'),
            0.5
        ))",
    )
    .await;
    assert_eq!(
        z.as_deref(),
        Some("POLYGON Z((0 0 1,0 4 6,4 4 4,4 0 2,0 0 1))")
    );
}

#[tokio::test]
async fn simplify_polygon_hull_handles_nulls_and_rejects_non_polygons() {
    let context = context();

    let nulls = execute(
        &context,
        "SELECT
            ST_SimplifyPolygonHull(
                ST_GeomFromText(CAST(NULL AS TEXT)), 0.5
            ) IS NULL AS null_geometry,
            ST_SimplifyPolygonHull(
                ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), CAST(NULL AS DOUBLE)
            ) IS NULL AS null_fraction,
            ST_SimplifyPolygonHull(
                ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 0.5, CAST(NULL AS BOOLEAN)
            ) IS NULL AS null_outer",
    )
    .await;
    assert!(nulls.column(0).as_boolean().value(0));
    assert!(nulls.column(1).as_boolean().value(0));
    assert!(nulls.column(2).as_boolean().value(0));

    for input in ["POINT(0 0)", "LINESTRING(0 0,1 1)"] {
        let error = query_error(
            &context,
            &format!("SELECT ST_SimplifyPolygonHull(ST_GeomFromText('{input}'), 0.5)"),
        )
        .await;
        assert!(error.contains("polygonal input"), "{input}: {error}");
    }
}

#[tokio::test]
async fn split_lines_matches_postgis_examples() {
    let context = context();

    let cases = [
        (
            "ST_Split(
                ST_GeomFromText('LINESTRING(0 0,10 0)'),
                ST_GeomFromText('POINT(5 0)')
            )",
            "GEOMETRYCOLLECTION(LINESTRING(0 0,5 0),LINESTRING(5 0,10 0))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('LINESTRING(0 0,10 0)'),
                ST_GeomFromText('POINT(0 0)')
            )",
            "GEOMETRYCOLLECTION(LINESTRING(0 0,10 0))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('LINESTRING(0 0,10 0)'),
                ST_GeomFromText('MULTIPOINT(8 0,3 0)')
            )",
            "GEOMETRYCOLLECTION(\
             LINESTRING(8 0,10 0),LINESTRING(3 0,8 0),LINESTRING(0 0,3 0))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('LINESTRING(0 0,10 0)'),
                ST_GeomFromText('LINESTRING(5 -5,5 5)')
            )",
            "GEOMETRYCOLLECTION(LINESTRING(0 0,5 0),LINESTRING(5 0,10 0))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('LINESTRING(0 0,10 0)'),
                ST_GeomFromText('MULTILINESTRING((3 -5,3 5),(7 -5,7 5))')
            )",
            "GEOMETRYCOLLECTION(\
             LINESTRING(0 0,3 0),LINESTRING(3 0,7 0),LINESTRING(7 0,10 0))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('LINESTRING(0 0,10 0)'),
                ST_GeomFromText('POLYGON((5 -5,6 -5,6 5,5 5,5 -5))')
            )",
            "GEOMETRYCOLLECTION(\
             LINESTRING(0 0,5 0),LINESTRING(5 0,6 0),LINESTRING(6 0,10 0))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('MULTILINESTRING((0 0,10 0),(0 1,10 1))'),
                ST_GeomFromText('POINT(5 0)')
            )",
            "GEOMETRYCOLLECTION(\
             LINESTRING(0 0,5 0),LINESTRING(5 0,10 0),LINESTRING(0 1,10 1))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('GEOMETRYCOLLECTION(\
                    LINESTRING(0 0,10 0),LINESTRING(0 1,10 1))'),
                ST_GeomFromText('LINESTRING(5 -5,5 15)')
            )",
            "GEOMETRYCOLLECTION(\
             LINESTRING(0 0,5 0),LINESTRING(5 0,10 0),\
             LINESTRING(0 1,5 1),LINESTRING(5 1,10 1))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('LINESTRING EMPTY'),
                ST_GeomFromText('POINT(1 1)')
            )",
            "GEOMETRYCOLLECTION(LINESTRING EMPTY)",
        ),
        (
            "ST_Split(
                ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'),
                ST_GeomFromText('LINESTRING(5 -5,5 15)')
            )",
            "GEOMETRYCOLLECTION(\
             POLYGON((5 0,0 0,0 10,5 10,5 0)),\
             POLYGON((5 10,10 10,10 0,5 0,5 10)))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'),
                ST_GeomFromText('LINESTRING(20 20,30 30)')
            )",
            "GEOMETRYCOLLECTION(POLYGON((0 0,0 10,10 10,10 0,0 0)))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('MULTIPOLYGON(\
                    ((0 0,10 0,10 10,0 10,0 0)),\
                    ((20 0,30 0,30 10,20 10,20 0)))'),
                ST_GeomFromText('LINESTRING(5 -5,5 15)')
            )",
            "GEOMETRYCOLLECTION(\
             POLYGON((5 0,0 0,0 10,5 10,5 0)),\
             POLYGON((5 10,10 10,10 0,5 0,5 10)),\
             POLYGON((20 0,20 10,30 10,30 0,20 0)))",
        ),
        (
            "ST_Split(
                ST_GeomFromText('POLYGON EMPTY'),
                ST_GeomFromText('LINESTRING(0 0,1 1)')
            )",
            "GEOMETRYCOLLECTION EMPTY",
        ),
    ];

    for (expression, expected) in cases {
        let actual = text_value(&context, &format!("SELECT ST_AsText({expression})")).await;
        assert_eq!(actual.as_deref(), Some(expected), "{expression}");
    }
}

#[tokio::test]
async fn split_preserves_dimensions_and_rejects_unsupported_inputs() {
    let context = context();

    let zm_point = execute(
        &context,
        "SELECT ST_Split(
            ST_GeomFromText('LINESTRING ZM (0 0 0 0,10 0 10 10)'),
            ST_GeomFromText('POINT(5 0)')
        )",
    )
    .await;
    assert_eq!(
        geometry_array(&zm_point).value(0).unwrap().dim(),
        Dimensions::Xyzm
    );

    let zm_line = execute(
        &context,
        "SELECT ST_Split(
            ST_GeomFromText('LINESTRING ZM (0 0 0 0,10 0 10 10)'),
            ST_GeomFromText('LINESTRING(5 -5,5 5)')
        )",
    )
    .await;
    assert_eq!(
        geometry_array(&zm_line).value(0).unwrap().dim(),
        Dimensions::Xyz
    );

    let overlap = query_error(
        &context,
        "SELECT ST_Split(
            ST_GeomFromText('LINESTRING(0 0,10 0)'),
            ST_GeomFromText('LINESTRING(3 0,7 0)')
        )",
    )
    .await;
    assert!(overlap.contains("linear intersection"), "{overlap}");

    let errors = [
        (
            "ST_Split(
                ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'),
                ST_GeomFromText('POINT(5 5)')
            )",
            "Splitting a Polygon by a Point is unsupported",
        ),
        (
            "ST_Split(
                ST_GeomFromText('GEOMETRYCOLLECTION(
                    POINT(1 1),LINESTRING(0 0,10 0))'),
                ST_GeomFromText('POINT(5 0)')
            )",
            "Splitting of Point geometries is unsupported",
        ),
        (
            "ST_Split(
                ST_GeomFromText('LINESTRING(0 0,10 0)'),
                ST_GeomFromText('GEOMETRYCOLLECTION(LINESTRING(5 -1,5 1))')
            )",
            "Splitting a Line by a GeometryCollection is unsupported",
        ),
    ];
    for (expression, expected) in errors {
        let error = query_error(&context, &format!("SELECT {expression}")).await;
        assert!(error.contains(expected), "{expression}: {error}");
    }

    let nulls = execute(
        &context,
        "SELECT
            ST_Split(
                ST_GeomFromText(CAST(NULL AS TEXT)),
                ST_GeomFromText('POINT(1 1)')
            ) IS NULL AS null_input,
            ST_Split(
                ST_GeomFromText('LINESTRING(0 0,1 1)'),
                ST_GeomFromText(CAST(NULL AS TEXT))
            ) IS NULL AS null_blade",
    )
    .await;
    assert!(nulls.column(0).as_boolean().value(0));
    assert!(nulls.column(1).as_boolean().value(0));
}

#[tokio::test]
async fn subdivide_matches_postgis_examples() {
    let context = context();

    let line = execute(
        &context,
        "SELECT ST_AsText(s.comp.geom)
         FROM (
            SELECT unnest(ST_Subdivide(
                ST_GeomFromText('LINESTRING(0 0,1 0,2 0,3 0,4 0,5 0,6 0)'), 5
            )) AS comp
         ) AS s",
    )
    .await;
    let line_values = line.column(0).as_string::<i32>();
    assert_eq!(line_values.len(), 2);
    assert_eq!(line_values.value(0), "LINESTRING(0 0,3 0)");
    assert_eq!(line_values.value(1), "LINESTRING(3 0,6 0)");

    let unsplit = execute(
        &context,
        "SELECT ST_AsText(s.comp.geom)
         FROM (
            SELECT unnest(ST_Subdivide(
                ST_GeomFromText('LINESTRING(0 0,1 0,2 0,3 0,4 0,5 0,6 0)')
            )) AS comp
         ) AS s",
    )
    .await;
    assert_eq!(unsplit.num_rows(), 1);
    assert_eq!(
        unsplit.column(0).as_string::<i32>().value(0),
        "LINESTRING(0 0,1 0,2 0,3 0,4 0,5 0,6 0)"
    );

    let polygon = execute(
        &context,
        "SELECT ST_Area(s.comp.geom) = 2 AS half_area
         FROM (
            SELECT unnest(ST_Subdivide(
                ST_GeomFromText('POLYGON((0 0,1 0,2 0,2 2,0 2,0 0))'), 5
            )) AS comp
         ) AS s",
    )
    .await;
    let polygon_values = polygon.column(0).as_boolean();
    assert_eq!(polygon_values.len(), 2);
    assert!(polygon_values.value(0));
    assert!(polygon_values.value(1));

    let multipoint = execute(
        &context,
        "SELECT ST_AsText(s.comp.geom)
         FROM (
            SELECT unnest(ST_Subdivide(
                ST_GeomFromText('MULTIPOINT(0 0,1 1,2 2,3 3,4 4,5 5)'), 5
            )) AS comp
         ) AS s",
    )
    .await;
    let multipoint_values = multipoint.column(0).as_string::<i32>();
    assert_eq!(multipoint_values.len(), 2);
    assert_eq!(multipoint_values.value(0), "MULTIPOINT((0 0),(1 1),(2 2))");
    assert_eq!(multipoint_values.value(1), "MULTIPOINT((3 3),(4 4),(5 5))");

    let collection = execute(
        &context,
        "SELECT ST_AsText(s.comp.geom)
         FROM (
            SELECT unnest(ST_Subdivide(
                ST_GeomFromText('GEOMETRYCOLLECTION(
                    LINESTRING(0 0,1 0,2 0,3 0,4 0,5 0,6 0),
                    POLYGON((0 0,1 0,2 0,2 2,0 2,0 0))
                )'),
                5
            )) AS comp
         ) AS s",
    )
    .await;
    let collection_values = collection.column(0).as_string::<i32>();
    assert_eq!(collection_values.len(), 2);
    assert_eq!(collection_values.value(0), "POLYGON((0 0,0 1,2 1,2 0,0 0))");

    let point = execute(
        &context,
        "SELECT ST_AsText(s.comp.geom)
         FROM (
            SELECT unnest(ST_Subdivide(ST_GeomFromText('POINT(1 2)'))) AS comp
         ) AS s",
    )
    .await;
    assert_eq!(point.num_rows(), 1);
    assert_eq!(point.column(0).as_string::<i32>().value(0), "POINT(1 2)");

    let zero_rows = [
        "ST_Subdivide(ST_GeomFromText('MULTIPOINT(0 0)'), 5)",
        "ST_Subdivide(ST_GeomFromText('LINESTRING EMPTY'))",
        "ST_Subdivide(ST_GeomFromText(CAST(NULL AS TEXT)))",
        "ST_Subdivide(
            ST_GeomFromText('LINESTRING(0 0,1 1)'), CAST(NULL AS INT)
        )",
        "ST_Subdivide(ST_GeomFromText('POINT EMPTY'), 4)",
    ];
    for expression in zero_rows {
        let batch = execute(
            &context,
            &format!(
                "SELECT count(*) = 0 AS empty FROM (SELECT unnest({expression}) AS comp) AS s"
            ),
        )
        .await;
        assert!(batch.column(0).as_boolean().value(0), "{expression}");
    }

    let error = query_error(
        &context,
        "SELECT ST_Subdivide(ST_GeomFromText('LINESTRING(0 0,1 1)'), 4)",
    )
    .await;
    assert!(error.contains("fewer than 5 vertices"), "{error}");
}

#[tokio::test]
async fn subdivide_preserves_z_and_drops_m() {
    let context = context();
    let batch = execute(
        &context,
        "SELECT s.comp.geom
         FROM (
            SELECT unnest(ST_Subdivide(
                ST_GeomFromText('LINESTRING ZM (
                    0 0 1 1,1 0 2 2,2 0 3 3,3 0 4 4,4 0 5 5,5 0 6 6,6 0 7 7
                )'),
                5
            )) AS comp
         ) AS s",
    )
    .await;
    let geometries = geometry_array(&batch);
    assert_eq!(geometries.len(), 2);
    assert_eq!(geometries.value(0).unwrap().dim(), Dimensions::Xyz);
    assert_eq!(
        text_value(
            &context,
            "SELECT ST_AsText(s.comp.geom)
             FROM (
                SELECT unnest(ST_Subdivide(
                    ST_GeomFromText('LINESTRING Z (
                        0 0 1,1 0 2,2 0 3,3 0 4,4 0 5,5 0 6,6 0 7
                    )'),
                    5
                )) AS comp
             ) AS s"
        )
        .await
        .as_deref(),
        Some("LINESTRING Z(0 0 1,3 0 4)")
    );
}

#[tokio::test]
async fn advanced_geos_arrow_fields_cover_crs_nullable_and_dimensions() {
    let context = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));

    let line = wkt::types::LineString::new(
        vec![
            wkt::types::Coord {
                x: 0.0,
                y: 0.0,
                z: None,
                m: None,
            },
            wkt::types::Coord {
                x: 10.0,
                y: 0.0,
                z: None,
                m: None,
            },
        ],
        wkt::types::Dimension::XY,
    );
    let mut geometry_builder = StrictGeometryBuilder::new(GeometryType::new(metadata.clone()));
    geometry_builder.push_geometry(Some(&line)).unwrap();
    geometry_builder.push_geometry(Some(&line)).unwrap();
    let geometry_values = geometry_builder.finish();
    let geometry_field = Arc::new(geometry_values.data_type().to_field("geometry", false));
    let geometry = geometry_values.into_array_ref();

    let polygon = wkt::types::Polygon::new(
        vec![wkt::types::LineString::new(
            vec![
                wkt::types::Coord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
                wkt::types::Coord {
                    x: 2.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
                wkt::types::Coord {
                    x: 2.0,
                    y: 2.0,
                    z: None,
                    m: None,
                },
                wkt::types::Coord {
                    x: 0.0,
                    y: 2.0,
                    z: None,
                    m: None,
                },
                wkt::types::Coord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
            ],
            wkt::types::Dimension::XY,
        )],
        wkt::types::Dimension::XY,
    );
    let mut polygon_builder = StrictGeometryBuilder::new(GeometryType::new(metadata.clone()));
    polygon_builder.push_geometry(Some(&polygon)).unwrap();
    polygon_builder.push_geometry(Some(&polygon)).unwrap();
    let polygon_values = polygon_builder.finish();
    let polygon_field = Arc::new(polygon_values.data_type().to_field("polygon", false));
    let polygon = polygon_values.into_array_ref();

    let point = wkt::types::Point::from_coord(wkt::types::Coord {
        x: 5.0,
        y: 0.0,
        z: None,
        m: None,
    });
    let mut blade_builder = StrictGeometryBuilder::new(GeometryType::new(metadata.clone()));
    blade_builder.push_geometry(Some(&point)).unwrap();
    blade_builder
        .push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)
        .unwrap();
    let blade_array = blade_builder.finish();
    let blade_field = Arc::new(blade_array.data_type().to_field("blade", true));
    let blade = blade_array.into_array_ref();

    let flags = Arc::new(Int64Array::from(vec![Some(0), None]));
    let factor = Arc::new(Float64Array::from(vec![Some(0.5), None]));
    let params = Arc::new(StringArray::from(vec![Some("method=linework"), None]));
    let maxvertices = Arc::new(Int64Array::from(vec![Some(5), None]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([
            geometry_field.clone(),
            polygon_field,
            Arc::new(Field::new("flags", DataType::Int64, true)),
            Arc::new(Field::new("factor", DataType::Float64, true)),
            Arc::new(Field::new("params", DataType::Utf8, true)),
            Arc::new(Field::new("maxvertices", DataType::Int64, true)),
            blade_field.clone(),
        ])),
        vec![
            geometry.clone(),
            polygon.clone(),
            flags.clone(),
            factor,
            params,
            maxvertices,
            blade.clone(),
        ],
    )
    .unwrap();
    context.register_batch("advanced", batch).unwrap();

    let nulls = execute(
        &context,
        "SELECT
            ST_IsValidDetail(geometry, flags) IS NULL AS detail,
            ST_MakeValid(geometry, params) IS NULL AS make_valid,
            ST_Split(geometry, blade) IS NULL AS split,
            ST_Subdivide(geometry, maxvertices) IS NULL AS subdivide,
            ST_ClipByBox2D(
                geometry,
                ST_MakeBox2D(ST_Point(0, 0), ST_Point(factor, factor))
            ) IS NULL AS clip,
            ST_SimplifyPolygonHull(polygon, factor) IS NULL AS hull
         FROM advanced",
    )
    .await;
    for column in 0..6 {
        assert!(!nulls.column(column).as_boolean().value(0));
        assert!(nulls.column(column).as_boolean().value(1));
    }

    let make_valid = execute(
        &context,
        "SELECT ST_MakeValid(geometry, params) FROM advanced",
    )
    .await;
    let output_type = make_valid
        .schema_ref()
        .field(0)
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert_eq!(
        geometry_array(&make_valid).value(0).unwrap().dim(),
        Dimensions::Xy
    );

    let slice_batch = RecordBatch::try_new(
        Arc::new(Schema::new([
            geometry_field.clone(),
            Arc::new(Field::new("flags", DataType::Int64, true)),
        ])),
        vec![geometry.slice(1, 1), Arc::new(flags.slice(1, 1))],
    )
    .unwrap();
    context.deregister_table("advanced").unwrap();
    context.register_batch("advanced", slice_batch).unwrap();
    let sliced = execute(
        &context,
        "SELECT ST_IsValidDetail(geometry, flags) IS NULL FROM advanced",
    )
    .await;
    assert!(sliced.column(0).as_boolean().value(0));

    let mut zm_wkb = vec![1];
    zm_wkb.extend_from_slice(&3002u32.to_le_bytes());
    zm_wkb.extend_from_slice(&2u32.to_le_bytes());
    for (index, coord) in [(0.0f64, 0.0f64, 0.0f64, 0.0f64), (10.0, 0.0, 10.0, 10.0)]
        .into_iter()
        .enumerate()
    {
        assert!(index < 2);
        zm_wkb.extend_from_slice(&coord.0.to_le_bytes());
        zm_wkb.extend_from_slice(&coord.1.to_le_bytes());
        zm_wkb.extend_from_slice(&coord.2.to_le_bytes());
        zm_wkb.extend_from_slice(&coord.3.to_le_bytes());
    }
    let zm = Wkb::try_new(&zm_wkb).unwrap();
    let mut zm_builder = StrictGeometryBuilder::new(GeometryType::new(metadata.clone()));
    zm_builder.push_geometry(Some(&zm)).unwrap();
    let zm = zm_builder.finish();
    let zm_field = Arc::new(zm.data_type().to_field("geometry", false));
    let mut zm_blade_builder = StrictGeometryBuilder::new(GeometryType::new(metadata.clone()));
    zm_blade_builder.push_geometry(Some(&point)).unwrap();
    let zm_blade = zm_blade_builder.finish();
    let zm_blade_field = Arc::new(zm_blade.data_type().to_field("blade", false));
    let zm_batch = RecordBatch::try_new(
        Arc::new(Schema::new([zm_field, zm_blade_field])),
        vec![zm.into_array_ref(), zm_blade.into_array_ref()],
    )
    .unwrap();
    context.register_batch("advanced_zm", zm_batch).unwrap();

    let split = execute(
        &context,
        "SELECT ST_Split(geometry, blade) FROM advanced_zm",
    )
    .await;
    assert_eq!(
        geometry_array(&split).value(0).unwrap().dim(),
        Dimensions::Xyzm
    );
    let make_valid = execute(
        &context,
        "SELECT ST_MakeValid(geometry, 'method=linework') FROM advanced_zm",
    )
    .await;
    assert_eq!(
        geometry_array(&make_valid).value(0).unwrap().dim(),
        Dimensions::Xyz
    );
}
