//! Public-entry contracts for planar `ST_Project`.
//!
//! These checks use the same `register()` entry point as applications and
//! inspect raw Arrow schema metadata, nullability, and dimension layout in
//! addition to scalar values.

mod support;

use std::sync::Arc;

use arrow_array::array::Float64Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{CoordType, Dimension, Metadata, PointType};
use support::context::context;
use support::geoarrow::collect;
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, Point as WktPoint};

fn point(x: f64, y: f64) -> WktPoint {
    WktPoint::new(
        Some(WktCoord {
            x,
            y,
            z: None,
            m: None,
        }),
        WktDimension::XY,
    )
}

fn point_z(x: f64, y: f64, z: f64) -> WktPoint {
    WktPoint::new(
        Some(WktCoord {
            x,
            y,
            z: Some(z),
            m: None,
        }),
        WktDimension::XYZ,
    )
}

fn point_m(x: f64, y: f64, m: f64) -> WktPoint {
    WktPoint::new(
        Some(WktCoord {
            x,
            y,
            z: None,
            m: Some(m),
        }),
        WktDimension::XYM,
    )
}

fn typed_points(
    points: &[WktPoint],
    dimension: Dimension,
    coord_type: CoordType,
    crs: Option<&Crs>,
    nullable_geometry: bool,
) -> (Field, arrow_array::ArrayRef) {
    let metadata = Arc::new(
        crs.map(|crs| Metadata::new(crs.clone(), None))
            .unwrap_or_default(),
    );
    let typ = PointType::new(dimension, metadata).with_coord_type(coord_type);
    let mut builder = PointBuilder::new(typ.clone());
    for point in points {
        builder.push_point(Some(point));
    }
    let array = builder.finish();
    (
        typ.to_field("geom", nullable_geometry),
        array.into_array_ref(),
    )
}

fn register_points(
    ctx: &SessionContext,
    geometry_field: Field,
    geometry: arrow_array::ArrayRef,
    distances: Option<Float64Array>,
    azimuths: Option<Float64Array>,
) {
    let mut fields = vec![geometry_field];
    let mut arrays: Vec<arrow_array::ArrayRef> = vec![geometry];
    if let Some(distances) = distances {
        fields.push(Field::new("distance", DataType::Float64, true));
        arrays.push(Arc::new(distances));
    }
    if let Some(azimuths) = azimuths {
        fields.push(Field::new("azimuth", DataType::Float64, true));
        arrays.push(Arc::new(azimuths));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    ctx.register_batch("t", batch).unwrap();
}

fn float_value(batch: &RecordBatch, column: usize, row: usize) -> f64 {
    batch
        .column(column)
        .as_primitive::<Float64Type>()
        .value(row)
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= 1e-9,
        "expected {expected}, got {actual}"
    );
}

#[tokio::test]
async fn project_preserves_typed_metadata_and_nullability() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let (field, geometry) = typed_points(
        &[point(0.0, 0.0)],
        Dimension::XY,
        CoordType::Separated,
        Some(&crs),
        false,
    );
    register_points(
        &ctx,
        field,
        geometry,
        Some(Float64Array::from(vec![Some(1000.0)])),
        Some(Float64Array::from(vec![Some(std::f64::consts::FRAC_PI_4)])),
    );

    let batches = collect(
        &ctx,
        "SELECT ST_Project(geom, distance, azimuth) AS p FROM t",
    )
    .await
    .unwrap();
    assert_eq!(batches.len(), 1);
    let field = batches[0].schema().field(0).clone();
    assert!(
        field.is_nullable(),
        "nullable distance must make output nullable"
    );
    assert_eq!(field.extension_type_name(), Some("geoarrow.point"));
    let typ = field.extension_type::<PointType>();
    assert_eq!(typ.dimension(), Dimension::XY);
    assert_eq!(typ.coord_type(), CoordType::Separated);
    assert_eq!(&crs, typ.metadata().crs());
}

#[tokio::test]
async fn project_preserves_z_m_dimensions_and_values() {
    let ctx = context();
    let (field, geometry) = typed_points(
        &[point_z(3.0, 4.0, 7.0)],
        Dimension::XYZ,
        CoordType::Separated,
        None,
        false,
    );
    register_points(
        &ctx,
        field,
        geometry,
        Some(Float64Array::from(vec![Some(1000.0)])),
        Some(Float64Array::from(vec![Some(std::f64::consts::FRAC_PI_2)])),
    );
    let batches = collect(
        &ctx,
        "SELECT ST_Project(geom, distance, azimuth) AS p, \
         ST_X(ST_Project(geom, distance, azimuth)) AS x, \
         ST_Z(ST_Project(geom, distance, azimuth)) AS z FROM t",
    )
    .await
    .unwrap();
    let typ = batches[0]
        .schema()
        .field(0)
        .clone()
        .extension_type::<PointType>();
    assert_eq!(typ.dimension(), Dimension::XYZ);
    assert_close(float_value(&batches[0], 1, 0), 1003.0);
    assert_eq!(float_value(&batches[0], 2, 0), 7.0);

    let ctx = context();
    let (field, geometry) = typed_points(
        &[point_m(3.0, 4.0, 9.0)],
        Dimension::XYM,
        CoordType::Interleaved,
        None,
        false,
    );
    register_points(
        &ctx,
        field,
        geometry,
        Some(Float64Array::from(vec![Some(1000.0)])),
        Some(Float64Array::from(vec![Some(std::f64::consts::FRAC_PI_2)])),
    );
    let batches = collect(
        &ctx,
        "SELECT ST_Project(geom, distance, azimuth) AS p, \
         ST_M(ST_Project(geom, distance, azimuth)) AS m FROM t",
    )
    .await
    .unwrap();
    let typ = batches[0]
        .schema()
        .field(0)
        .clone()
        .extension_type::<PointType>();
    assert_eq!(typ.dimension(), Dimension::XYM);
    assert_eq!(typ.coord_type(), CoordType::Interleaved);
    assert_eq!(float_value(&batches[0], 1, 0), 9.0);
}

#[tokio::test]
async fn two_geometry_project_uses_start_dimension() {
    let ctx = context();
    let (from_field, from) = typed_points(
        &[point_z(0.0, 0.0, 10.0)],
        Dimension::XYZ,
        CoordType::Separated,
        None,
        false,
    );
    let (to_field, to) = typed_points(
        &[point_z(3.0, 4.0, 20.0)],
        Dimension::XYZ,
        CoordType::Separated,
        None,
        false,
    );
    let schema = Arc::new(Schema::new(vec![
        from_field.with_name("from_geom"),
        to_field.with_name("to_geom"),
        Field::new("distance", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![from, to, Arc::new(Float64Array::from(vec![5.0]))],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_Project(from_geom, to_geom, distance) AS p, \
         ST_X(ST_Project(from_geom, to_geom, distance)) AS x, \
         ST_Y(ST_Project(from_geom, to_geom, distance)) AS y, \
         ST_Z(ST_Project(from_geom, to_geom, distance)) AS z FROM t",
    )
    .await
    .unwrap();
    let typ = batches[0]
        .schema()
        .field(0)
        .clone()
        .extension_type::<PointType>();
    assert_eq!(typ.dimension(), Dimension::XYZ);
    assert_close(float_value(&batches[0], 1, 0), 6.0);
    assert_close(float_value(&batches[0], 2, 0), 8.0);
    assert_close(float_value(&batches[0], 3, 0), 30.0);
}

#[tokio::test]
async fn null_and_empty_arguments_produce_nulls() {
    let ctx = context();
    let (field, geometry) = typed_points(
        &[point(0.0, 0.0), point(3.0, 4.0), point(10.0, 20.0)],
        Dimension::XY,
        CoordType::Separated,
        None,
        true,
    );
    register_points(
        &ctx,
        field,
        geometry,
        Some(Float64Array::from(vec![Some(1.0), None, Some(1000000.0)])),
        Some(Float64Array::from(vec![Some(0.0); 3])),
    );

    let batches = collect(
        &ctx,
        "SELECT ST_Project(geom, distance, azimuth) IS NULL AS is_null, \
         ST_Project(geom, distance, azimuth) IS NOT NULL AS is_not_null \
         FROM t",
    )
    .await
    .unwrap();
    let is_null = batches[0].column(0).as_boolean();
    assert!(!is_null.value(0));
    assert!(is_null.value(1));
    assert!(!is_null.value(2));

    let batches = collect(
        &ctx,
        "SELECT ST_Project(ST_GeomFromText('POINT EMPTY'), 1000.0, 0.0) IS NULL AS empty, \
         ST_Project(ST_GeomFromText('POINT(0 0)'), 1000.0, NULL::DOUBLE) IS NULL AS null_azimuth, \
         ST_Project(ST_GeomFromText(NULL), 1000.0, 0.0) IS NULL AS null_geometry, \
         ST_Project(\
           ST_GeomFromText('POINT(0 0)'), \
           ST_GeomFromText('POINT(1 0)'), \
           NULL::DOUBLE\
         ) IS NULL AS null_two_distance, \
         ST_Project(\
           ST_GeomFromText(NULL), \
           ST_GeomFromText('POINT(1 0)'), \
           1000.0\
         ) IS NULL AS null_two_from",
    )
    .await
    .unwrap();
    let row = &batches[0];
    assert!(row.column(0).as_boolean().value(0));
    assert!(row.column(1).as_boolean().value(0));
    assert!(row.column(2).as_boolean().value(0));
    assert!(row.column(3).as_boolean().value(0));
    assert!(row.column(4).as_boolean().value(0));
}

#[tokio::test]
async fn sliced_and_multibatch_geometry_inputs_keep_row_alignment() {
    let ctx = context();
    let (geometry_field, geometry) = typed_points(
        &[point(0.0, 0.0), point(1.0, 0.0), point(2.0, 0.0)],
        Dimension::XY,
        CoordType::Separated,
        None,
        false,
    );
    let schema = Arc::new(Schema::new(vec![
        geometry_field,
        Field::new("distance", DataType::Float64, false),
        Field::new("azimuth", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            geometry,
            Arc::new(Float64Array::from(vec![0.0, 1000.0, 2000.0])),
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0])),
        ],
    )
    .unwrap();
    let table =
        MemTable::try_new(schema, vec![vec![batch.slice(0, 2), batch.slice(2, 1)]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_X(ST_Project(geom, distance, azimuth)) AS x FROM t \
         ORDER BY distance",
    )
    .await
    .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Float64Type>()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values.len(), 3);
    assert_close(values[0], 0.0);
    assert_close(values[1], 1.0);
    assert_close(values[2], 2.0);
}

#[tokio::test]
async fn arbitrary_crs_is_accepted() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:3857".to_string());
    let (field, geometry) = typed_points(
        &[point(0.0, 0.0)],
        Dimension::XY,
        CoordType::Separated,
        Some(&crs),
        false,
    );
    ctx.register_batch(
        "g",
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![geometry]).unwrap(),
    )
    .unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_X(ST_Project(geom, 1000.0, pi() / 2)) AS x, \
         ST_Y(ST_Project(geom, 1000.0, pi() / 2)) AS y FROM g",
    )
    .await
    .unwrap();
    assert_close(float_value(&batches[0], 0, 0), 1000.0);
    assert_close(float_value(&batches[0], 1, 0), 0.0);

    let batches = collect(
        &ctx,
        "SELECT ST_Project(geom, 1000.0, pi() / 2) AS p FROM g",
    )
    .await
    .unwrap();
    let typ = batches[0]
        .schema()
        .field(0)
        .clone()
        .extension_type::<PointType>();
    assert_eq!(typ.dimension(), Dimension::XY);
    assert_eq!(typ.metadata().crs(), &crs);

    let ctx = context();
    let (field, geometry) = typed_points(
        &[point(0.0, 0.0)],
        Dimension::XY,
        CoordType::Separated,
        None,
        false,
    );
    ctx.register_batch(
        "g",
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![geometry]).unwrap(),
    )
    .unwrap();
    let batches = collect(
        &ctx,
        "SELECT ST_X(ST_Project(geom, 1000.0, 0.0)) AS x FROM g",
    )
    .await
    .unwrap();
    assert_close(float_value(&batches[0], 0, 0), 0.0);
}

#[tokio::test]
async fn planar_sql_contracts_match_postgis() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT \
           ST_X(ST_Project(ST_GeomFromText('POINT(3 4)'), 1000.0, 0.0)) AS north_x, \
           ST_Y(ST_Project(ST_GeomFromText('POINT(3 4)'), 1000.0, 0.0)) AS north_y, \
           ST_X(ST_Project(ST_GeomFromText('POINT(3 4)'), 1000.0, pi() / 2)) AS east_x, \
           ST_Y(ST_Project(ST_GeomFromText('POINT(3 4)'), 1000.0, pi() / 2)) AS east_y, \
           ST_X(ST_Project(ST_GeomFromText('POINT(3 4)'), 1000.0, pi())) AS south_x, \
           ST_Y(ST_Project(ST_GeomFromText('POINT(3 4)'), 1000.0, pi())) AS south_y, \
           ST_X(ST_Project(ST_GeomFromText('POINT(3 4)'), 1000.0, -pi() / 2)) AS west_x, \
           ST_Y(ST_Project(ST_GeomFromText('POINT(3 4)'), 1000.0, -pi() / 2)) AS west_y, \
           ST_X(ST_Project(ST_GeomFromText('POINT(3 4)'), -1000.0, pi() / 2)) AS negative_east_x \
         FROM (SELECT 1) AS one",
    )
    .await
    .unwrap();
    let row = &batches[0];
    assert_close(float_value(row, 0, 0), 3.0);
    assert_close(float_value(row, 1, 0), 1004.0);
    assert_close(float_value(row, 2, 0), 1003.0);
    assert_close(float_value(row, 3, 0), 4.0);
    assert_close(float_value(row, 4, 0), 3.0);
    assert_close(float_value(row, 5, 0), -996.0);
    assert_close(float_value(row, 6, 0), -997.0);
    assert_close(float_value(row, 7, 0), 4.0);
    assert_close(float_value(row, 8, 0), -997.0);
}

#[tokio::test]
async fn two_geometry_sql_contracts_match_postgis() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT \
           ST_X(ST_Project(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'), 50000.0)) AS x, \
           ST_Y(ST_Project(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'), 50000.0)) AS y, \
           ST_X(ST_Project(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'), -1000.0)) AS negative_x, \
           ST_Y(ST_Project(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'), -1000.0)) AS negative_y, \
           ST_X(ST_Project(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)'), 50000.0)) AS same_x, \
           ST_Y(ST_Project(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)'), 50000.0)) AS same_y \
         FROM (SELECT 1) AS one",
    )
    .await
    .unwrap();
    let row = &batches[0];
    assert_close(float_value(row, 0, 0), 30003.0);
    assert_close(float_value(row, 1, 0), 40004.0);
    assert_close(float_value(row, 2, 0), -597.0);
    assert_close(float_value(row, 3, 0), -796.0);
    assert_close(float_value(row, 4, 0), 1.0);
    assert_close(float_value(row, 5, 0), 2.0);
}

#[tokio::test]
async fn non_point_arguments_are_rejected() {
    let ctx = context();
    let error = collect(
        &ctx,
        "SELECT ST_Project(ST_GeomFromText('LINESTRING(0 0,1 1)'), 1000.0, 0.0)",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Argument must be POINT geometry"),
        "{error}"
    );

    let error = collect(
        &ctx,
        "SELECT ST_Project(\
           ST_GeomFromText('LINESTRING(0 0,1 1)'), \
           ST_GeomFromText('POINT(3 4)'), \
           1000.0)",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Arguments must be POINT geometries"),
        "{error}"
    );
}
