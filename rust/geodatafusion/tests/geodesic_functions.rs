//! Public-entry contracts for spheroidal measurement functions.
//!
//! These checks use the same `register()` entry point as applications and
//! inspect raw Arrow schema metadata, nullability, and dimension layout in
//! addition to scalar values.

mod support;

use std::sync::Arc;

use arrow_schema::{Field, Schema};
use datafusion::arrow::array::{AsArray, RecordBatch};
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
#[tokio::test]
async fn projected_crs_is_rejected_for_spheroidal_measurements() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:3857".to_string());
    let (field, geometry) = typed_points(
        &[point(0.0, 0.0), point(1.0, 0.0)],
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

    for sql in [
        "SELECT ST_DistanceSphere(a, b) FROM (SELECT geom AS a FROM g) x, (SELECT geom AS b FROM g) y",
        "SELECT ST_LengthSpheroid(geom, 'SPHEROID[\"WGS 84\",6378137,298.257223563]') FROM g",
    ] {
        let error = collect(&ctx, sql).await.unwrap_err();
        assert!(
            error.to_string().contains("lon/lat"),
            "expected lon/lat CRS error for {sql}, got {error}"
        );
    }
}

#[tokio::test]
async fn non_wgs84_geographic_crs_is_rejected_explicitly() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4269".to_string());
    let (field, geometry) = typed_points(
        &[point(0.0, 0.0), point(1.0, 1.0)],
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

    for sql in [
        "SELECT ST_DistanceSphere(a, b) FROM (SELECT geom AS a FROM g) x, (SELECT geom AS b FROM g) y",
        "SELECT ST_DistanceSpheroid(a, b) FROM (SELECT geom AS a FROM g) x, (SELECT geom AS b FROM g) y",
        "SELECT ST_LengthSpheroid(geom, 'SPHEROID[\"WGS 84\",6378137,298.257223563]') FROM g",
    ] {
        let error = collect(&ctx, sql).await.unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("WGS84") && message.contains("EPSG:4269"),
            "expected an explicit WGS84 boundary error for {sql}, got {error}"
        );
    }
}

#[tokio::test]
async fn empty_and_null_distance_contracts() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_DistanceSphere(ST_GeomFromText('POINT EMPTY'), ST_GeomFromText('POINT(0 0)')) AS sphere_empty, \
         ST_DistanceSpheroid(ST_GeomFromText('POINT EMPTY'), ST_GeomFromText('POINT(0 0)')) AS spheroid_empty, \
         ST_LengthSpheroid(ST_GeomFromText('LINESTRING EMPTY'), 'SPHEROID[\"WGS 84\",6378137,298.257223563]') AS length_empty, \
         ST_DistanceSphere(ST_GeomFromText(NULL), ST_GeomFromText('POINT(0 0)')) AS null_value, \
         ST_DistanceSphere(ST_GeomFromText('POLYGON EMPTY'), ST_GeomFromText('POINT(0 0)'), 6371008.771415059) AS explicit_empty, \
         ST_DistanceSpheroid(ST_GeomFromText('POLYGON EMPTY'), ST_GeomFromText('POINT(0 0)')) AS spheroid_polygon_empty",
    )
    .await
    .unwrap();
    let row = 0;
    assert!(batches[0].column(0).is_null(row));
    assert_eq!(
        batches[0]
            .column(1)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(row),
        -1.0
    );
    assert_eq!(
        batches[0]
            .column(2)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(row),
        0.0
    );
    assert!(batches[0].column(3).is_null(row));
    assert_eq!(
        batches[0]
            .column(4)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(row),
        -1.0
    );
    assert_eq!(
        batches[0]
            .column(5)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(row),
        -1.0
    );
}
