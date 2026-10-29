use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array};
use arrow_schema::{DataType, Field};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
use geoarrow_array::array::PolygonArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Crs, Dimension, Metadata, PolygonType};
use geodatafusion::udf::native::constructors::MakeEnvelope;

#[tokio::test]
async fn make_envelope_public_registration_sets_crs_and_broadcasts_rows() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let df = ctx
        .sql(
            r#"
            SELECT
                ST_MakeEnvelope(0, 1, 10, 6, CAST(4326 AS BIGINT)) AS with_srid,
                ST_MakeEnvelope(0, 1, 10, 6) AS without_srid,
                ST_MakeEnvelope(xmin, ymin, xmax, ymax, CAST(4326 AS BIGINT)) AS row_arrays
            FROM (
                VALUES (0.0, 1.0, 10.0, 6.0), (2.0, 3.0, 4.0, 5.0)
            ) AS envelopes(xmin, ymin, xmax, ymax)
            "#,
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = &batches[0];
    let schema = batch.schema();

    let with_srid = schema.field_with_name("with_srid").unwrap();
    let with_srid_type = with_srid.extension_type::<PolygonType>();
    assert_eq!(with_srid_type.dimension(), Dimension::XY);
    assert_eq!(with_srid_type.coord_type(), CoordType::Separated);
    assert_eq!(
        with_srid_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );

    let without_srid = schema.field_with_name("without_srid").unwrap();
    let without_srid_type = without_srid.extension_type::<PolygonType>();
    assert!(without_srid_type.metadata().crs().crs_value().is_none());

    for name in ["with_srid", "without_srid", "row_arrays"] {
        let index = schema.index_of(name).unwrap();
        let polygons =
            PolygonArray::try_from((batch.column(index).as_ref(), schema.field(index))).unwrap();
        assert_eq!(polygons.len(), 2);
    }

    let row_arrays = PolygonArray::try_from((
        batch
            .column(schema.index_of("row_arrays").unwrap())
            .as_ref(),
        schema.field_with_name("row_arrays").unwrap(),
    ))
    .unwrap();
    let ring = row_arrays.value(1).unwrap().exterior().unwrap();
    let coords = (0..ring.num_coords())
        .map(|index| {
            let coord = ring.coord(index).unwrap();
            (coord.x(), coord.y())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        coords,
        vec![(2.0, 3.0), (2.0, 5.0), (4.0, 5.0), (4.0, 3.0), (2.0, 3.0),]
    );
}

#[test]
fn make_envelope_accepts_ffi_constant_srid_array() {
    let (udf, args, return_field) =
        make_envelope_args(CoordType::Interleaved, vec![Some(4326), Some(4326)]);
    assert_eq!(
        return_field.extension_type::<PolygonType>().coord_type(),
        CoordType::Interleaved
    );

    let ColumnarValue::Array(array) = udf.invoke_with_args(args).unwrap() else {
        panic!("expected an array result");
    };
    let polygons = PolygonArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();

    assert_eq!(polygons.len(), 2);
    assert_eq!(
        polygons.data_type().metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert_eq!(
        polygons.value(0).unwrap().exterior().unwrap().num_coords(),
        5
    );
    assert_eq!(
        polygons.value(1).unwrap().exterior().unwrap().num_coords(),
        5
    );
}

#[test]
fn make_envelope_rejects_non_constant_row_srids() {
    let (udf, args, _) = make_envelope_args(CoordType::Separated, vec![Some(4326), Some(3857)]);
    let error = udf.invoke_with_args(args).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("ST_MakeEnvelope only supports SRID as a scalar integer")
    );
}

#[test]
fn make_envelope_treats_all_null_srid_array_as_scalar_null() {
    let (udf, args, return_field) = make_envelope_args(CoordType::Separated, vec![None, None]);
    let ColumnarValue::Array(array) = udf.invoke_with_args(args).unwrap() else {
        panic!("expected an array result");
    };
    let polygons = PolygonArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();

    assert_eq!(polygons.len(), 2);
    assert!(polygons.is_null(0));
    assert!(polygons.is_null(1));
}

fn make_envelope_args(
    coord_type: CoordType,
    srids: Vec<Option<i64>>,
) -> (MakeEnvelope, ScalarFunctionArgs, Arc<Field>) {
    assert_eq!(srids.len(), 2);
    let udf = MakeEnvelope::new(coord_type);
    let metadata = if srids.iter().all(Option::is_some) {
        Metadata::new(Crs::from_authority_code("EPSG:4326".to_string()), None)
    } else {
        Metadata::default()
    };
    let return_field = Arc::new(
        PolygonType::new(Dimension::XY, Arc::new(metadata))
            .with_coord_type(coord_type)
            .to_field("envelope", true),
    );
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![0.0, 2.0]))),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![10.0, 4.0]))),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(6.0))),
            ColumnarValue::Array(Arc::new(Int64Array::from(srids))),
        ],
        arg_fields: vec![
            Arc::new(Field::new("xmin", DataType::Float64, true)),
            Arc::new(Field::new("ymin", DataType::Float64, true)),
            Arc::new(Field::new("xmax", DataType::Float64, true)),
            Arc::new(Field::new("ymax", DataType::Float64, true)),
            Arc::new(Field::new("srid", DataType::Int64, true)),
        ],
        number_rows: 2,
        return_field: Arc::clone(&return_field),
        config_options: Arc::new(ConfigOptions::default()),
    };
    (udf, args, return_field)
}
