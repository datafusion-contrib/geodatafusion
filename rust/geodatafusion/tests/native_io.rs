use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, BinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::GeometryArray;
use geoarrow_schema::{CoordType, Crs, GeometryType, Metadata, WkbType, WktType};

const WKT_ROWS: [Option<&str>; 7] = [
    Some("POINT(1 2)"),
    Some("GEOMETRYCOLLECTION(POINT(3 4))"),
    None,
    Some("POINT EMPTY"),
    Some("POINT ZM(5 6 7 8)"),
    Some("GEOMETRYCOLLECTION ZM(POINT ZM(9 10 11 12))"),
    Some("GEOMETRYCOLLECTION EMPTY"),
];

const WKB_ROWS: [Option<&str>; 7] = [
    // POINT(1 2)
    Some("0101000000000000000000f03f0000000000000040"),
    // GEOMETRYCOLLECTION(POINT(3 4))
    Some("010700000001000000010100000000000000000008400000000000001040"),
    None,
    // POINT EMPTY
    Some("0101000000000000000000f87f000000000000f87f"),
    // POINT ZM(5 6 7 8)
    Some("01b90b0000000000000000144000000000000018400000000000001c400000000000002040"),
    // GEOMETRYCOLLECTION ZM(POINT ZM(9 10 11 12))
    Some(
        "01bf0b00000100000001b90b00000000000000002240000000000000244000000000000026400000000000002840",
    ),
    // GEOMETRYCOLLECTION EMPTY
    Some("010700000000000000"),
];

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

async fn collect_one(ctx: &SessionContext, sql: &str) -> Result<RecordBatch> {
    let batches = ctx.sql(sql).await?.collect().await?;
    assert_eq!(batches.len(), 1);
    Ok(batches.into_iter().next().unwrap())
}

fn wkb_array(rows: impl IntoIterator<Item = Option<&'static str>>) -> BinaryArray {
    let mut builder = BinaryBuilder::new();
    for row in rows {
        match row {
            Some(value) => builder.append_value(hex::decode(value).unwrap()),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

fn assert_io_contract(batch: &RecordBatch, expected_crs: &Crs) {
    assert_eq!(batch.num_rows(), 7);

    let schema = batch.schema();
    let geometry_field = schema.field(3);
    let geometry_type = geometry_field.try_extension_type::<GeometryType>().unwrap();
    assert_eq!(geometry_type.coord_type(), CoordType::Separated);
    assert_eq!(geometry_type.metadata().crs(), expected_crs);

    let text_field = schema.field(1);
    let text_type = text_field.try_extension_type::<WktType>().unwrap();
    assert_eq!(text_type.metadata().crs(), expected_crs);

    let binary_field = schema.field(2);
    let binary_type = binary_field.try_extension_type::<WkbType>().unwrap();
    assert_eq!(binary_type.metadata().crs(), expected_crs);

    let type_names = batch.column(0).as_string_view();
    let expected_types = [
        Some("ST_Point"),
        Some("ST_GeometryCollection"),
        None,
        Some("ST_Point"),
        Some("ST_Point"),
        Some("ST_GeometryCollection"),
        Some("ST_GeometryCollection"),
    ];
    for (row, expected) in expected_types.into_iter().enumerate() {
        assert_eq!(
            type_names.is_null(row),
            expected.is_none(),
            "type row {row}"
        );
        if let Some(expected) = expected {
            assert_eq!(type_names.value(row), expected, "type row {row}");
        }
    }

    let texts = batch.column(1).as_string::<i32>();
    let expected_texts = [
        Some("POINT(1 2)"),
        Some("GEOMETRYCOLLECTION(POINT(3 4))"),
        None,
        Some("POINT EMPTY"),
        Some("POINT ZM(5 6 7 8)"),
        Some("GEOMETRYCOLLECTION ZM(POINT ZM(9 10 11 12))"),
        Some("GEOMETRYCOLLECTION EMPTY"),
    ];
    for (row, expected) in expected_texts.into_iter().enumerate() {
        assert_eq!(texts.is_null(row), expected.is_none(), "text row {row}");
        if let Some(expected) = expected {
            assert_eq!(texts.value(row), expected, "text row {row}");
        }
    }
    assert!(!batch.column(2).is_null(3));

    let geometries = GeometryArray::try_from((batch.column(3).as_ref(), geometry_field)).unwrap();
    assert_eq!(geometries.len(), 7);
    assert!(geometries.is_null(2));
    // GeoArrow union type IDs encode geometry kind and dimension.
    assert_eq!(geometries.type_ids().as_ref(), &[1, 7, 1, 1, 31, 37, 7]);

    let dumps = batch.column(4).as_list::<i32>();
    assert!(dumps.is_null(2));
    let collection_rows = dumps.value(1);
    let collection_rows = collection_rows.as_struct();
    let paths = collection_rows.column(0).as_list::<i32>();
    assert_eq!(paths.len(), 1);
    let path_values = paths.value(0);
    let path = path_values.as_primitive::<Int32Type>();
    assert_eq!(path.values().as_ref(), &[1]);
    assert_eq!(dumps.value(3).len(), 0);
}

#[tokio::test]
async fn wkt_input_preserves_geometry_collection_identity_and_arrow_metadata() -> Result<()> {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let field = Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata));

    let mut values = vec![Some("POLYGON((0 0,0 1,1 1,0 0))")];
    values.extend(WKT_ROWS);
    values.push(Some("POLYGON((0 0,0 1,1 1,0 0))"));
    let wkt = StringArray::from(values).slice(1, WKT_ROWS.len());
    let input = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(wkt)],
    )
    .unwrap();
    ctx.register_batch("wkt_input", input).unwrap();

    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_GeometryType(ST_GeomFromText(wkt)) AS type_name,
            ST_AsText(ST_GeomFromText(wkt)) AS text,
            ST_AsBinary(ST_GeomFromText(wkt)) AS binary,
            ST_GeomFromText(wkt) AS geometry,
            ST_Dump(ST_GeomFromText(wkt)) AS dumped
        FROM wkt_input
        "#,
    )
    .await?;

    assert_io_contract(&batch, &crs);
    Ok(())
}

#[tokio::test]
async fn wkb_input_preserves_geometry_collection_identity_and_arrow_metadata() -> Result<()> {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:3857".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let field =
        Field::new("wkb", DataType::Binary, true).with_extension_type(WkbType::new(metadata));

    let sentinel = "01030000000100000004000000000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f000000000000f03f00000000000000000000000000000000";
    let mut rows = vec![Some(sentinel)];
    rows.extend(WKB_ROWS);
    rows.push(Some(sentinel));
    let wkb = wkb_array(rows).slice(1, WKB_ROWS.len());
    let input = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(wkb)],
    )
    .unwrap();
    ctx.register_batch("wkb_input", input).unwrap();

    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_GeometryType(ST_GeomFromWKB(wkb)) AS type_name,
            ST_AsText(ST_GeomFromWKB(wkb)) AS text,
            ST_AsBinary(ST_GeomFromWKB(wkb)) AS binary,
            ST_GeomFromWKB(wkb) AS geometry,
            ST_Dump(ST_GeomFromWKB(wkb)) AS dumped
        FROM wkb_input
        "#,
    )
    .await?;

    assert_io_contract(&batch, &crs);
    Ok(())
}
