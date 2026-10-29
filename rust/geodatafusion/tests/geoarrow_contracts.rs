//! Minimal raw Arrow checks for schema, dimension, and CRS boundaries.
//!
//! SQLLogicTest compares formatted values, which cannot describe extension
//! metadata or nested field structure. These checks stay deliberately small
//! and use the public registration entry point.

mod support;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::Schema;
use datafusion::arrow::array::RecordBatch;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::test::point;
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{CoordType, Dimension, GeometryType, Metadata, PointType, WkbType, WktType};
use support::context::context;
use support::geoarrow::collect;
use wkt::Wkt;
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, Point as WktPoint};

fn xyz_point(x: f64, y: f64, z: f64) -> Wkt<f64> {
    Wkt::Point(WktPoint::new(
        Some(WktCoord {
            x,
            y,
            z: Some(z),
            m: None,
        }),
        WktDimension::XYZ,
    ))
}

fn first_batch(batches: &[RecordBatch]) -> &RecordBatch {
    assert!(!batches.is_empty(), "query must return at least one batch");
    &batches[0]
}

#[tokio::test]
async fn constructors_emit_expected_extension_and_dimension() {
    let ctx = context();

    for (sql, expected_dimension) in [
        ("SELECT ST_MakePoint(1, 2) AS geom", Dimension::XY),
        ("SELECT ST_MakePoint(1, 2, 3) AS geom", Dimension::XYZ),
        ("SELECT ST_MakePoint(1, 2, 3, 4) AS geom", Dimension::XYZM),
        ("SELECT ST_MakePointM(1, 2, 3) AS geom", Dimension::XYM),
        ("SELECT ST_Point(1, 2) AS geom", Dimension::XY),
        ("SELECT ST_PointZ(1, 2, 3) AS geom", Dimension::XYZ),
        ("SELECT ST_PointM(1, 2, 3) AS geom", Dimension::XYM),
        ("SELECT ST_PointZM(1, 2, 3, 4) AS geom", Dimension::XYZM),
    ] {
        let batches = collect(&ctx, sql).await.unwrap();
        let field = first_batch(&batches).schema().field(0).clone();
        assert_eq!(field.extension_type_name(), Some("geoarrow.point"), "{sql}");
        assert_eq!(
            field.extension_type::<PointType>().dimension(),
            expected_dimension,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn wkt_decode_and_encode_preserve_crs_metadata() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let points = point::array(CoordType::Separated, Dimension::XY).with_metadata(metadata);
    let field = points.data_type().to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![points.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_AsText(geometry) AS text, ST_AsBinary(geometry) AS binary FROM t",
    )
    .await
    .unwrap();
    let text_field = first_batch(&batches).schema().field(0).clone();
    let binary_field = first_batch(&batches).schema().field(1).clone();
    assert_eq!(
        &crs,
        text_field.extension_type::<WktType>().metadata().crs()
    );
    assert_eq!(
        &crs,
        binary_field.extension_type::<WkbType>().metadata().crs()
    );

    let batches = collect(
        &ctx,
        "SELECT ST_GeomFromText(ST_AsText(geometry)) AS geom, \
         ST_GeomFromWKB(ST_AsBinary(geometry)) AS wkb_geom FROM t",
    )
    .await
    .unwrap();
    let geom_field = first_batch(&batches).schema().field(0).clone();
    let wkb_geom_field = first_batch(&batches).schema().field(1).clone();
    assert_eq!(
        &crs,
        geom_field.extension_type::<GeometryType>().metadata().crs()
    );
    assert_eq!(
        &crs,
        wkb_geom_field
            .extension_type::<GeometryType>()
            .metadata()
            .crs()
    );
}

#[tokio::test]
async fn dump_keeps_nested_geometry_and_crs_fields() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let points = point::array(CoordType::Separated, Dimension::XY).with_metadata(metadata);
    let field = points.data_type().to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![points.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(&ctx, "SELECT ST_Dump(geometry) AS dumped FROM t")
        .await
        .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    let datafusion::arrow::datatypes::DataType::List(item_field) = field.data_type() else {
        panic!("ST_Dump must return a list");
    };
    let datafusion::arrow::datatypes::DataType::Struct(fields) = item_field.data_type() else {
        panic!("ST_Dump list items must be structs");
    };
    assert_eq!(fields[0].name(), "path");
    assert_eq!(fields[1].name(), "geom");
    let geometry_field = fields[1].as_ref();
    assert_eq!(
        &crs,
        geometry_field
            .extension_type::<GeometryType>()
            .metadata()
            .crs()
    );
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn line_merge_preserves_geometry_extension_metadata() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_LineMerge(ST_GeomFromText(\
         'MULTILINESTRING((10 160, 60 120), (120 140, 60 120), (120 140, 180 120))'\
         )) AS geom",
    )
    .await
    .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert_eq!(
        field.extension_type_name(),
        Some("geoarrow.geometry"),
        "ST_LineMerge must return a generic Geometry extension field"
    );
    assert_eq!(
        field.extension_type::<GeometryType>().coord_type(),
        CoordType::Separated
    );
}

#[tokio::test]
async fn three_d_outputs_preserve_geometry_extension_and_crs() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let mut left_builder = PointBuilder::new(
        PointType::new(Dimension::XYZ, metadata.clone()).with_coord_type(CoordType::Separated),
    );
    let mut right_builder = PointBuilder::new(
        PointType::new(Dimension::XYZ, metadata).with_coord_type(CoordType::Separated),
    );
    left_builder
        .push_geometry(Some(&xyz_point(0.0, 0.0, 0.0)))
        .unwrap();
    left_builder
        .push_geometry(Some(&xyz_point(3.0, 4.0, 12.0)))
        .unwrap();
    right_builder
        .push_geometry(Some(&xyz_point(3.0, 4.0, 12.0)))
        .unwrap();
    right_builder
        .push_geometry(Some(&xyz_point(0.0, 0.0, 0.0)))
        .unwrap();
    let left = left_builder.finish();
    let right = right_builder.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            left.data_type().to_field("left", true),
            right.data_type().to_field("right", true),
        ])),
        vec![left.to_array_ref(), right.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("t_3d", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_3DClosestPoint(left, right) AS cp, \
         ST_3DShortestLine(left, right) AS sl FROM t_3d",
    )
    .await
    .unwrap();
    for column_index in 0..2 {
        let field = first_batch(&batches).schema().field(column_index).clone();
        assert_eq!(
            field.extension_type_name(),
            Some("geoarrow.geometry"),
            "column {column_index}"
        );
        assert_eq!(
            &crs,
            field.extension_type::<GeometryType>().metadata().crs(),
            "column {column_index}"
        );
        assert!(field.is_nullable(), "column {column_index}");
    }
}

#[tokio::test]
async fn three_d_udfs_propagate_nulls_from_nonnullable_fields() {
    let ctx = context();
    let mut builder = PointBuilder::new(
        PointType::new(Dimension::XYZ, Default::default()).with_coord_type(CoordType::Separated),
    );
    builder
        .push_geometry(Some(&xyz_point(0.0, 0.0, 3.0)))
        .unwrap();
    builder
        .push_geometry(Some(&Wkt::Point(WktPoint::empty(WktDimension::XYZ))))
        .unwrap();
    let values = builder.finish();
    let field = values.data_type().to_field("geom", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![values.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("t_3d_null", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT geom IS NULL AS raw_null, \
         ST_3DDistance(geom, ST_GeomFromText('POINT Z (0 0 0)')) IS NULL AS dist_null, \
         ST_3DDistance(geom, ST_GeomFromText('POINT Z (0 0 0)')) AS dist, \
         ST_3DIntersects(geom, ST_GeomFromText('POINT Z (0 0 3)')) IS NULL AS int_null \
         FROM t_3d_null",
    )
    .await
    .unwrap();
    let batch = first_batch(&batches);
    assert!(!batch.column(0).as_boolean().value(0));
    assert!(!batch.column(1).as_boolean().value(0));
    assert!(!batch.column(3).as_boolean().value(0));
    assert_eq!(batch.column(2).as_primitive::<Float64Type>().value(0), 3.0);
    assert!(!batch.column(0).as_boolean().value(1));
    assert!(batch.column(1).as_boolean().value(1));
    assert!(batch.column(2).is_null(1));
    assert!(!batch.column(3).as_boolean().value(1));
}

#[tokio::test]
async fn three_d_udfs_handle_nonfinite_coordinates() {
    let ctx = context();
    let mut builder = PointBuilder::new(
        PointType::new(Dimension::XYZ, Default::default()).with_coord_type(CoordType::Separated),
    );
    builder
        .push_geometry(Some(&xyz_point(f64::NAN, 0.0, 3.0)))
        .unwrap();
    let values = builder.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![values.data_type().to_field("geom", true)])),
        vec![values.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("t_3d_nan", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_3DDistance(geom, ST_GeomFromText('POINT Z (0 0 3)')) IS NULL AS dist_null, \
         ST_3DIntersects(geom, ST_GeomFromText('POINT Z (0 0 3)')) AS intersects, \
         ST_AsText(ST_3DClosestPoint(geom, ST_GeomFromText('POINT Z (0 0 3)'))) IS NULL AS cp_null \
         FROM t_3d_nan",
    )
    .await
    .unwrap();
    let batch = first_batch(&batches);
    assert!(batch.column(0).as_boolean().value(0));
    assert!(!batch.column(1).as_boolean().value(0));
    assert!(batch.column(2).as_boolean().value(0));
}
