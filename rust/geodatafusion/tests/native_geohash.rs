//! Raw Arrow and boundary contracts for the GeoHash functions.

mod support;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use geo_traits::{CoordTrait, PointTrait, RectTrait};
use geoarrow_array::array::{PointArray, RectArray, WktArray};
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{BoxType, CoordType, Crs, Dimension, Metadata, PointType};
use support::context::context;
use support::geoarrow::collect;

fn first_batch(batches: &[RecordBatch]) -> &RecordBatch {
    assert!(!batches.is_empty(), "query must return at least one batch");
    &batches[0]
}

#[tokio::test]
async fn geohash_functions_emit_expected_extension_metadata() {
    let ctx = context();

    let batches = collect(&ctx, "SELECT ST_PointFromGeoHash('ww8p1r4t8')")
        .await
        .unwrap();
    let batch = first_batch(&batches);
    let field = batch.schema().field(0).clone();
    assert_eq!(field.extension_type_name(), Some("geoarrow.point"));
    let point_type = field.try_extension_type::<PointType>().unwrap();
    assert_eq!(point_type.dimension(), Dimension::XY);
    assert_eq!(point_type.coord_type(), CoordType::Separated);
    assert!(point_type.metadata().crs().crs_value().is_none());
    let points = PointArray::try_from((batch.column(0).as_ref(), field.as_ref())).unwrap();
    let point = points.value(0).unwrap();
    assert_eq!(point.coord().unwrap().x(), 112.55838632583618);
    assert_eq!(point.coord().unwrap().y(), 37.83238649368286);

    let batches = collect(&ctx, "SELECT ST_Box2dFromGeoHash('ww8p1r4t8')")
        .await
        .unwrap();
    let batch = first_batch(&batches);
    let field = batch.schema().field(0).clone();
    assert_eq!(field.extension_type_name(), Some("geoarrow.box"));
    let box_type = field.try_extension_type::<BoxType>().unwrap();
    assert_eq!(box_type.dimension(), Dimension::XY);
    assert!(box_type.metadata().crs().crs_value().is_none());
    let rects = RectArray::try_from((batch.column(0).as_ref(), field.as_ref())).unwrap();
    let rect = rects.value(0).unwrap();
    assert_eq!(rect.min().x(), 112.55836486816406);
    assert_eq!(rect.min().y(), 37.83236503601074);
    assert_eq!(rect.max().x(), 112.5584077835083);
    assert_eq!(rect.max().y(), 37.83240795135498);

    let batches = collect(&ctx, "SELECT ST_GeoHash(ST_MakePoint(-126,48))")
        .await
        .unwrap();
    let batch = first_batch(&batches);
    let field = batch.schema().field(0).clone();
    assert_eq!(field.data_type(), &DataType::Utf8View);
    assert_eq!(
        batch.column(0).as_string_view().value(0),
        "c0w3hf1s70w3hf1s70w3"
    );
}

#[tokio::test]
async fn typed_wkt_and_wkb_inputs_ignore_crs_and_preserve_row_alignment() {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:3857".to_string()),
        None,
    ));
    let wkt = WktArray::new(
        StringArray::from(vec![
            Some("LINESTRING(1 2,3 4)"),
            Some("POINT(-126 48)"),
            None,
        ]),
        metadata.clone(),
    );
    let wkt_field = wkt.data_type().to_field("geometry", true);
    ctx.register_batch(
        "typed_wkt",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![wkt_field])),
            vec![wkt.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();

    let wkb = to_wkb::<i32>(&wkt).unwrap();
    let wkb_field = wkb.data_type().to_field("geometry", true);
    ctx.register_batch(
        "typed_wkb",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![wkb_field])),
            vec![wkb.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();

    for table in ["typed_wkt", "typed_wkb"] {
        let sql = format!("SELECT ST_GeoHash(geometry) FROM {table}");
        let batches = collect(&ctx, &sql).await.unwrap();
        let values = first_batch(&batches).column(0).as_string_view();
        assert_eq!(values.value(0), "s0", "{sql}");
        assert_eq!(values.value(1), "c0w3hf1s70w3hf1s70w3", "{sql}");
        assert!(values.is_null(2), "{sql}");
    }

    let sliced = wkt.slice(1, 2);
    let sliced_field = wkt.data_type().to_field("geometry", true);
    ctx.register_batch(
        "typed_slice",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![sliced_field])),
            vec![sliced.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();
    let batches = collect(&ctx, "SELECT ST_GeoHash(geometry) FROM typed_slice")
        .await
        .unwrap();
    let values = first_batch(&batches).column(0).as_string_view();
    assert_eq!(values.value(0), "c0w3hf1s70w3hf1s70w3");
    assert!(values.is_null(1));
}

#[tokio::test]
async fn nullable_columns_and_null_precision_follow_postgis() {
    let ctx = context();
    let hashes = StringArray::from(vec![
        Some("ww8p1r4t8"),
        None,
        Some("WW8P1R4T8"),
        Some("c0w3hf1s70w3hf1s70w3"),
    ]);
    let precision = Int64Array::from(vec![Some(4), Some(0), None, Some(20)]);
    let hash_field = Arc::new(Field::new("hash", DataType::Utf8, true));
    let precision_field = Arc::new(Field::new("precision", DataType::Int64, true));
    ctx.register_batch(
        "hashes",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![hash_field, precision_field])),
            vec![Arc::new(hashes), Arc::new(precision)],
        )
        .unwrap(),
    )
    .unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_AsEWKT(ST_PointFromGeoHash(hash, precision)) FROM hashes",
    )
    .await
    .unwrap();
    let values = first_batch(&batches).column(0).as_string::<i32>();
    assert_eq!(values.value(0), "POINT(112.67578125 37.880859375)");
    assert!(values.is_null(1));
    assert_eq!(
        values.value(2),
        "POINT(112.55838632583618 37.83238649368286)"
    );
    assert_eq!(
        values.value(3),
        "POINT(-126.00000000000003 48.00000000000007)"
    );

    let batches = collect(
        &ctx,
        "SELECT ST_AsEWKT(ST_Box2dFromGeoHash(hash, precision)) FROM hashes",
    )
    .await
    .unwrap();
    let values = first_batch(&batches).column(0).as_string::<i32>();
    assert_eq!(
        values.value(0),
        "POLYGON((112.5 37.79296875,112.5 37.96875,112.8515625 37.96875,112.8515625 37.79296875,112.5 37.79296875))"
    );
    assert!(values.is_null(1));

    let points = WktArray::new(
        StringArray::from(vec![Some("POINT(-126 48)"), Some("POINT(-126 48)")]),
        Arc::new(Metadata::new(
            Crs::from_authority_code("EPSG:4326".to_string()),
            None,
        )),
    );
    let point_field = points.data_type().to_field("geometry", false);
    let maxchars = Int64Array::from(vec![Some(12), None]);
    ctx.register_batch(
        "points",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                point_field,
                Field::new("maxchars", DataType::Int64, true),
            ])),
            vec![points.to_array_ref(), Arc::new(maxchars)],
        )
        .unwrap(),
    )
    .unwrap();

    let batches = collect(&ctx, "SELECT ST_GeoHash(geometry, maxchars) FROM points")
        .await
        .unwrap();
    let values = first_batch(&batches).column(0).as_string_view();
    assert_eq!(values.value(0), "c0w3hf1s70w3");
    assert!(values.is_null(1));

    let strict_maxchars = Int64Array::from(vec![Some(12), Some(20)]);
    ctx.register_batch(
        "strict_points",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                points.data_type().to_field("geometry", false),
                Field::new("maxchars", DataType::Int64, false),
            ])),
            vec![points.to_array_ref(), Arc::new(strict_maxchars)],
        )
        .unwrap(),
    )
    .unwrap();
    let batches = collect(
        &ctx,
        "SELECT ST_GeoHash(geometry, maxchars) FROM strict_points",
    )
    .await
    .unwrap();
    let batch = first_batch(&batches);
    assert!(
        batch.schema().field(0).is_nullable(),
        "ST_GeoHash must stay nullable because EMPTY input returns NULL"
    );
    let values = batch.column(0).as_string_view();
    assert_eq!(values.value(0), "c0w3hf1s70w3");
    assert_eq!(values.value(1), "c0w3hf1s70w3hf1s70w3");
}

#[tokio::test]
async fn invalid_and_non_finite_inputs_fail_explicitly() {
    let ctx = context();

    let error = collect(&ctx, "SELECT ST_PointFromGeoHash('abc')")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("decode_geohash_bbox: Invalid character 'a'"),
        "{error}"
    );

    let error = collect(&ctx, "SELECT ST_Box2dFromGeoHash('abc')")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("decode_geohash_bbox: Invalid character 'a'"),
        "{error}"
    );

    let error = collect(&ctx, "SELECT ST_GeoHash(ST_MakePoint(190,0))")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Geohash requires inputs in decimal degrees"),
        "{error}"
    );

    let error = collect(
        &ctx,
        "SELECT ST_GeoHash(ST_MakePoint(CAST('NaN' AS DOUBLE),2))",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("requires finite coordinates"), "{error}");

    let error = ctx
        .sql("SELECT ST_GeoHash(1)")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("expects a geometry argument"), "{error}");

    let error = ctx
        .sql("SELECT ST_PointFromGeoHash(1)")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("expects a text argument"), "{error}");

    let error = ctx
        .sql("SELECT ST_GeoHash(ST_MakePoint(0,0), 'twelve')")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("expects an integer maxchars argument"),
        "{error}"
    );
}
