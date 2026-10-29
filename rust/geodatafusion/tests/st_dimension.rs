use std::str::FromStr;
use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, BinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_schema::{CoordType, Crs, GeometryType, Metadata, WkbType, WktType};
use geodatafusion::udf::native::accessors::Dimension as StDimension;
use wkt::Wkt;

const CRS_AUTHORITY: &str = "EPSG:4326";

fn metadata() -> Arc<Metadata> {
    Arc::new(Metadata::new(
        Crs::from_authority_code(CRS_AUTHORITY.to_string()),
        Default::default(),
    ))
}

fn parse_wkt(value: &str) -> Wkt<f64> {
    Wkt::from_str(value).unwrap()
}

async fn collect_one(ctx: &SessionContext, sql: &str) -> RecordBatch {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    batches.into_iter().next().unwrap()
}

fn assert_dimensions(batch: &RecordBatch, expected: &[Option<i32>]) {
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
    let values = batch.column(0).as_primitive::<Int32Type>();
    assert_eq!(values.len(), expected.len());
    for (row, expected) in expected.iter().enumerate() {
        match expected {
            Some(expected) => {
                assert!(!values.is_null(row), "row {row} must be non-null");
                assert_eq!(values.value(row), *expected, "row {row}");
            }
            None => assert!(values.is_null(row), "row {row} must be NULL"),
        }
    }
}

#[tokio::test]
async fn untyped_null_and_all_null_columns_return_null_int32() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let batch = collect_one(&ctx, "SELECT ST_Dimension(NULL) AS dimension").await;
    assert_dimensions(&batch, &[None]);

    let batch = collect_one(
        &ctx,
        "SELECT ST_Dimension(value) AS dimension \
         FROM (VALUES (NULL), (NULL)) AS inputs(value)",
    )
    .await;
    assert_dimensions(&batch, &[None, None]);
}

#[tokio::test]
async fn native_geometry_arrays_preserve_type_dimension_for_every_dimension_and_layout() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dimension in [
            geoarrow_schema::Dimension::XY,
            geoarrow_schema::Dimension::XYZ,
            geoarrow_schema::Dimension::XYM,
            geoarrow_schema::Dimension::XYZM,
        ] {
            let rows = match dimension {
                geoarrow_schema::Dimension::XY => vec![
                    Some("POINT(9 9)"),
                    Some("POINT(1 2)"),
                    Some("LINESTRING(0 0,1 1)"),
                    Some("POLYGON((0 0,1 0,1 1,0 0))"),
                    Some("MULTIPOINT((1 2),(3 4))"),
                    Some("MULTILINESTRING((0 0,1 1))"),
                    Some("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))"),
                    Some("POINT EMPTY"),
                    Some("LINESTRING EMPTY"),
                    Some("POLYGON EMPTY"),
                    Some("GEOMETRYCOLLECTION(POINT EMPTY,POLYGON EMPTY)"),
                    None,
                ],
                geoarrow_schema::Dimension::XYZ => vec![
                    Some("POINT Z (9 9 9)"),
                    Some("POINT Z (1 2 3)"),
                    Some("LINESTRING Z (0 0 1,1 1 2)"),
                    Some("POLYGON Z ((0 0 1,1 0 2,1 1 3,0 0 1))"),
                    Some("MULTIPOINT Z ((1 2 3),(4 5 6))"),
                    Some("MULTILINESTRING Z ((0 0 1,1 1 2))"),
                    Some("MULTIPOLYGON Z (((0 0 1,1 0 2,1 1 3,0 0 1)))"),
                    Some("POINT Z EMPTY"),
                    Some("LINESTRING Z EMPTY"),
                    Some("POLYGON Z EMPTY"),
                    Some("GEOMETRYCOLLECTION Z (POINT Z EMPTY,POLYGON Z EMPTY)"),
                    None,
                ],
                geoarrow_schema::Dimension::XYM => vec![
                    Some("POINT M (9 9 9)"),
                    Some("POINT M (1 2 3)"),
                    Some("LINESTRING M (0 0 1,1 1 2)"),
                    Some("POLYGON M ((0 0 1,1 0 2,1 1 3,0 0 1))"),
                    Some("MULTIPOINT M ((1 2 3),(4 5 6))"),
                    Some("MULTILINESTRING M ((0 0 1,1 1 2))"),
                    Some("MULTIPOLYGON M (((0 0 1,1 0 2,1 1 3,0 0 1)))"),
                    Some("POINT M EMPTY"),
                    Some("LINESTRING M EMPTY"),
                    Some("POLYGON M EMPTY"),
                    Some("GEOMETRYCOLLECTION M (POINT M EMPTY,POLYGON M EMPTY)"),
                    None,
                ],
                geoarrow_schema::Dimension::XYZM => vec![
                    Some("POINT ZM (9 9 9 9)"),
                    Some("POINT ZM (1 2 3 4)"),
                    Some("LINESTRING ZM (0 0 1 2,1 1 3 4)"),
                    Some("POLYGON ZM ((0 0 1 2,1 0 2 3,1 1 3 4,0 0 1 2))"),
                    Some("MULTIPOINT ZM ((1 2 3 4),(4 5 6 7))"),
                    Some("MULTILINESTRING ZM ((0 0 1 2,1 1 3 4))"),
                    Some("MULTIPOLYGON ZM (((0 0 1 2,1 0 2 3,1 1 3 4,0 0 1 2)))"),
                    Some("POINT ZM EMPTY"),
                    Some("LINESTRING ZM EMPTY"),
                    Some("POLYGON ZM EMPTY"),
                    Some("GEOMETRYCOLLECTION ZM (POINT ZM EMPTY,POLYGON ZM EMPTY)"),
                    None,
                ],
            };
            let geometries: Vec<Option<Wkt<f64>>> =
                rows.into_iter().map(|wkt| wkt.map(parse_wkt)).collect();
            let geometry_type = GeometryType::new(metadata()).with_coord_type(coord_type);
            let geometries = GeometryBuilder::from_nullable_geometries(&geometries, geometry_type)
                .unwrap()
                .finish()
                .slice(1, 11);

            let input = RecordBatch::try_new(
                Arc::new(Schema::new([Arc::new(
                    geometries.data_type().to_field("geometry", true),
                )])),
                vec![geometries.into_array_ref()],
            )
            .unwrap();

            let ctx = SessionContext::new();
            ctx.register_udf(StDimension::new().into());
            ctx.register_batch("geometries", input).unwrap();
            let batch = collect_one(
                &ctx,
                "SELECT ST_Dimension(geometry) AS dimension FROM geometries",
            )
            .await;

            assert_dimensions(
                &batch,
                &[
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(2),
                    None,
                ],
            );
        }
    }
}

fn wkb_array(values: &[Option<&str>]) -> BinaryArray {
    let mut builder = BinaryBuilder::new();
    for value in values {
        match value {
            Some(value) => builder.append_value(hex::decode(value).unwrap()),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

#[tokio::test]
async fn raw_wkt_and_wkb_inputs_include_empty_collection_and_atomic_dimensions() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let wkt_values = [
        Some("POINT(9 9)"),
        Some("POINT EMPTY"),
        Some("LINESTRING EMPTY"),
        Some("POLYGON EMPTY"),
        Some("GEOMETRYCOLLECTION EMPTY"),
        Some("GEOMETRYCOLLECTION(POLYGON EMPTY)"),
        Some("GEOMETRYCOLLECTION(LINESTRING EMPTY,POINT EMPTY)"),
        Some("GEOMETRYCOLLECTION(POINT(1 2),POLYGON EMPTY)"),
        Some("MULTIPOLYGON EMPTY"),
        None,
    ];
    let wkt = StringArray::from(wkt_values.to_vec()).slice(1, 9);
    let wkt_field = Arc::new(
        Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata())),
    );
    let wkt_input =
        RecordBatch::try_new(Arc::new(Schema::new([wkt_field])), vec![Arc::new(wkt)]).unwrap();
    ctx.register_batch("wkt_dimensions", wkt_input).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_Dimension(wkt) AS dimension FROM wkt_dimensions",
    )
    .await;
    assert_dimensions(
        &batch,
        &[
            Some(0),
            Some(1),
            Some(2),
            Some(0),
            Some(2),
            Some(1),
            Some(2),
            Some(2),
            None,
        ],
    );

    let wkb_values = [
        // Sentinel point.
        Some("010100000000000000000022400000000000002240"),
        // POINT EMPTY.
        Some("0101000000000000000000f87f000000000000f87f"),
        // LINESTRING EMPTY.
        Some("010200000000000000"),
        // POLYGON EMPTY.
        Some("010300000000000000"),
        // MULTIPOLYGON EMPTY.
        Some("010600000000000000"),
        // GEOMETRYCOLLECTION EMPTY.
        Some("010700000000000000"),
        // GEOMETRYCOLLECTION(POLYGON EMPTY).
        Some("010700000001000000010300000000000000"),
        None,
    ];
    let wkb = wkb_array(&wkb_values).slice(1, 7);
    let wkb_field = Arc::new(
        Field::new("wkb", DataType::Binary, true).with_extension_type(WkbType::new(metadata())),
    );
    let wkb_input =
        RecordBatch::try_new(Arc::new(Schema::new([wkb_field])), vec![Arc::new(wkb)]).unwrap();
    ctx.register_batch("wkb_dimensions", wkb_input).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_Dimension(wkb) AS dimension FROM wkb_dimensions",
    )
    .await;
    assert_dimensions(
        &batch,
        &[Some(0), Some(1), Some(2), Some(2), Some(0), Some(2), None],
    );
}
