use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array};
use arrow_schema::{DataType, Field};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::PointArray;
use geoarrow_schema::{CoordType, Crs, Dimension, GeoArrowType, Metadata, PointType};
use geodatafusion::udf::native::constructors::Point;

#[tokio::test]
async fn point_null_srid_fields_match_arrays() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let df = ctx
        .sql(
            r#"
            SELECT
                ST_Point(1, 2, CAST(NULL AS BIGINT)) AS point_null,
                ST_PointZ(1, 2, 3, CAST(NULL AS BIGINT)) AS pointz_null,
                ST_PointM(1, 2, 3, CAST(NULL AS BIGINT)) AS pointm_null,
                ST_PointZM(1, 2, 3, 4, CAST(NULL AS BIGINT)) AS pointzm_null,
                ST_Point(1, 2, CAST(4326 AS BIGINT)) AS point_srid
            "#,
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = &batches[0];
    let schema = batch.schema();

    let cases = [
        ("point_null", Dimension::XY, None),
        ("pointz_null", Dimension::XYZ, None),
        ("pointm_null", Dimension::XYM, None),
        ("pointzm_null", Dimension::XYZM, None),
        ("point_srid", Dimension::XY, Some(4326)),
    ];
    for (index, (name, dimension, srid)) in cases.into_iter().enumerate() {
        let field = schema.field_with_name(name).unwrap();
        let point_type = field.extension_type::<PointType>();
        assert_eq!(point_type.dimension(), dimension);
        assert_eq!(point_type.coord_type(), CoordType::Separated);
        if let Some(srid) = srid {
            assert_eq!(
                point_type.metadata().crs(),
                &Crs::from_authority_code(format!("EPSG:{srid}"))
            );
        } else {
            assert!(point_type.metadata().crs().crs_value().is_none());
        }

        let point_array = PointArray::try_from((batch.column(index).as_ref(), field)).unwrap();
        assert_eq!(
            point_array.data_type(),
            GeoArrowType::Point(point_type.clone())
        );
        assert_eq!(point_array.is_null(0), srid.is_none());
    }

    let err = async {
        let df = ctx
            .sql(
                "SELECT ST_Point(x, y, srid) \
                 FROM (VALUES (1.0, 2.0, 4326)) AS points(x, y, srid)",
            )
            .await?;
        df.collect().await
    }
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("ST_Point only supports SRID as a scalar integer")
    );
}

#[tokio::test]
async fn point_null_srid_preserves_interleaved_layout() {
    let ctx = SessionContext::new();
    ctx.register_udf(Point::new(CoordType::Interleaved).into());

    let df = ctx
        .sql("SELECT ST_Point(1, 2, CAST(NULL AS BIGINT)) AS point")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = &batches[0];
    let schema = batch.schema();
    let field = schema.field_with_name("point").unwrap();
    let point_type = field.extension_type::<PointType>();

    assert_eq!(point_type.coord_type(), CoordType::Interleaved);
    let point_array = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    assert_eq!(
        point_array.data_type(),
        GeoArrowType::Point(point_type.clone())
    );
    assert!(point_array.is_null(0));
}

#[test]
fn point_accepts_broadcast_srid_arrays() {
    for (srid, crs, null) in [
        (vec![Some(4326), Some(4326)], Some("EPSG:4326"), false),
        (vec![None, None], None, true),
    ] {
        let (udf, args, return_field) = point_args(Int64Array::from(srid), crs);
        let ColumnarValue::Array(array) = udf.invoke_with_args(args).unwrap() else {
            panic!("expected an array result");
        };
        let points = PointArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();

        assert_eq!(points.len(), 2);
        assert_eq!(points.is_null(0), null);
        assert_eq!(points.is_null(1), null);
    }
}

fn point_args(srid: Int64Array, crs: Option<&str>) -> (Point, ScalarFunctionArgs, Arc<Field>) {
    let udf = Point::new(CoordType::Separated);
    let crs = crs
        .map(|crs| Crs::from_authority_code(crs.to_string()))
        .unwrap_or_default();
    let metadata = Metadata::new(crs, None);
    let return_field = Arc::new(
        PointType::new(Dimension::XY, Arc::new(metadata))
            .with_coord_type(CoordType::Separated)
            .to_field("point", true),
    );
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![1.0, 3.0]))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![2.0, 4.0]))),
            ColumnarValue::Array(Arc::new(srid)),
        ],
        arg_fields: vec![
            Arc::new(Field::new("x", DataType::Float64, true)),
            Arc::new(Field::new("y", DataType::Float64, true)),
            Arc::new(Field::new("srid", DataType::Int64, true)),
        ],
        number_rows: 2,
        return_field: Arc::clone(&return_field),
        config_options: Arc::new(ConfigOptions::default()),
    };
    (udf, args, return_field)
}
