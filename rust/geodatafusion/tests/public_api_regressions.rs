use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait, PointTrait, RectTrait};
use geoarrow_array::builder::LineStringBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Dimension, LineStringType, PointType};
use geodatafusion::udf::geo::processing::Simplify;
use geodatafusion::udf::native::constructors::{
    MakePoint, MakePointM, Point, PointM, PointZ, PointZM,
};
use wkt::wkt;

fn context() -> SessionContext {
    let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
    geodatafusion::register(&ctx);
    ctx
}

async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

#[tokio::test]
async fn point_constructors_preserve_component_nulls_in_both_layouts() {
    let x = Float64Array::from(vec![Some(1.0), None, Some(1.0), Some(1.0)]);
    let y = Float64Array::from(vec![Some(2.0), Some(2.0), None, Some(2.0)]);
    let z = Float64Array::from(vec![Some(3.0), Some(3.0), Some(3.0), None]);
    let m = Float64Array::from(vec![Some(4.0), Some(4.0), Some(4.0), Some(4.0)]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Float64, true),
        Field::new("y", DataType::Float64, true),
        Field::new("z", DataType::Float64, true),
        Field::new("m", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(x), Arc::new(y), Arc::new(z), Arc::new(m)],
    )
    .unwrap();

    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
        ctx.register_udf(Point::new(coord_type).into());
        ctx.register_udf(PointZ::new(coord_type).into());
        ctx.register_udf(PointM::new(coord_type).into());
        ctx.register_udf(PointZM::new(coord_type).into());
        ctx.register_udf(MakePoint::new(coord_type).into());
        ctx.register_udf(MakePointM::new(coord_type).into());
        ctx.register_batch("points", batch.clone()).unwrap();

        let batches = collect(
            &ctx,
            "SELECT \
               ST_Point(x, y), ST_PointZ(x, y, z), ST_PointM(x, y, m), \
               ST_PointZM(x, y, z, m), ST_MakePoint(x, y), ST_MakePoint(x, y, z), \
               ST_MakePoint(x, y, z, m), ST_MakePointM(x, y, m) \
             FROM points",
        )
        .await;
        let batch = &batches[0];
        let schema = batch.schema();
        let expected_nulls: [&[usize]; 8] = [
            &[1, 2],
            &[1, 2, 3],
            &[1, 2],
            &[1, 2, 3],
            &[1, 2],
            &[1, 2, 3],
            &[1, 2, 3],
            &[1, 2],
        ];
        for (index, expected) in expected_nulls.iter().enumerate() {
            let field = schema.field(index);
            let array =
                geoarrow_array::array::from_arrow_array(batch.column(index).as_ref(), field)
                    .unwrap();
            let actual = (0..array.len())
                .filter(|row| array.is_null(*row))
                .collect::<Vec<_>>();
            assert_eq!(
                actual, *expected,
                "column {index} null rows for {coord_type:?}"
            );
        }
    }
}

#[tokio::test]
async fn point_m_interleaved_uses_xym_dimension() {
    let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
    ctx.register_udf(PointM::new(CoordType::Interleaved).into());
    ctx.register_udf(MakePointM::new(CoordType::Interleaved).into());

    let batches = collect(
        &ctx,
        "SELECT ST_PointM(1, 2, 3) AS pointm, ST_MakePointM(4, 5, 6) AS makepointm",
    )
    .await;
    let batch = &batches[0];

    for (index, expected) in [(0, (1.0, 2.0, 3.0)), (1, (4.0, 5.0, 6.0))] {
        let schema = batch.schema();
        let field = schema.field(index);
        let typ = field.extension_type::<PointType>();
        assert_eq!(typ.dimension(), Dimension::XYM);
        assert_eq!(typ.coord_type(), CoordType::Interleaved);

        let points: geoarrow_array::array::PointArray =
            geoarrow_array::array::PointArray::try_from((batch.column(index).as_ref(), field))
                .unwrap();
        let point = points.value(0).unwrap();
        let coord = point.coord().unwrap();
        assert_eq!(coord.x(), expected.0);
        assert_eq!(coord.y(), expected.1);
        assert_eq!(coord.nth(2), Some(expected.2));
    }
}

#[tokio::test]
async fn coordinate_accessors_accept_wkb_wkt_and_null_inputs() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT \
           ST_X(wkt), ST_X(ST_AsBinary(ST_GeomFromText(wkt))), \
           ST_Z(wkt), ST_M(wkt) \
         FROM (VALUES ('POINT ZM(1 2 3 4)'), (NULL)) AS inputs(wkt)",
    )
    .await;
    let batch = &batches[0];

    assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 1.0);
    assert_eq!(batch.column(1).as_primitive::<Float64Type>().value(0), 1.0);
    assert_eq!(batch.column(2).as_primitive::<Float64Type>().value(0), 3.0);
    assert_eq!(batch.column(3).as_primitive::<Float64Type>().value(0), 4.0);
    for index in 0..4 {
        assert!(batch.column(index).is_null(1));
    }

    let batches = collect(
        &ctx,
        "SELECT ST_Z(ST_GeomFromText('POINT M(1 2 3)')), \
         ST_M(ST_GeomFromText('POINT Z(1 2 3)')), ST_X(NULL)",
    )
    .await;
    let batch = &batches[0];
    assert!(batch.column(0).is_null(0));
    assert!(batch.column(1).is_null(0));
    assert!(batch.column(2).is_null(0));
}

#[tokio::test]
async fn coord_dim_preserves_native_point_nulls() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_CoordDim(ST_Point(NULL, NULL)), ST_CoordDim(NULL), \
         ST_CoordDim(ST_GeomFromText('POINT EMPTY'))",
    )
    .await;
    let batch = &batches[0];
    assert!(batch.column(0).is_null(0));
    assert!(batch.column(1).is_null(0));
    assert_eq!(
        batch
            .column(2)
            .as_primitive::<arrow_array::types::UInt8Type>()
            .value(0),
        2
    );

    let mut builder = geoarrow_array::builder::PointBuilder::new(PointType::new(
        Dimension::XY,
        Default::default(),
    ));
    builder.push_point(Some(&wkt!(POINT(1.0 2.0))));
    builder.push_null();
    let points = builder.finish();
    let field = points.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![points.into_array_ref()]).unwrap();
    ctx.register_batch("native_points", batch).unwrap();

    let batches = collect(&ctx, "SELECT ST_CoordDim(geometry) FROM native_points").await;
    let values = batches[0]
        .column(0)
        .as_primitive::<arrow_array::types::UInt8Type>();
    assert_eq!(values.value(0), 2);
    assert!(values.is_null(1));
}

#[tokio::test]
async fn geometry_accessor_fields_are_nullable_for_table_inputs() {
    let ctx = context();
    let mut builder = geoarrow_array::builder::PointBuilder::new(PointType::new(
        Dimension::XY,
        Default::default(),
    ));
    builder.push_null();
    let points = builder.finish();
    let field = points.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![points.into_array_ref()]).unwrap();
    ctx.register_batch("points", batch).unwrap();

    let batches = collect(&ctx, "SELECT ST_X(geometry) AS x FROM points").await;
    let batch = &batches[0];
    assert!(batch.schema().field(0).is_nullable());
    assert!(batch.column(0).is_null(0));
}

#[tokio::test]
async fn simplify_interleaved_native_high_dimension_outputs_xy() {
    let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
    ctx.register_udf(Simplify::new(CoordType::Interleaved).into());

    let mut builder =
        LineStringBuilder::new(LineStringType::new(Dimension::XYZ, Default::default()));
    builder
        .push_line_string(Some(
            &wkt!(LINESTRING Z(0.0 0.0 1.0, 1.0 0.1 2.0, 2.0 0.0 3.0, 3.0 0.0 4.0)),
        ))
        .unwrap();
    let geometry = builder.finish();
    let field = geometry.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let batch = RecordBatch::try_new(schema.clone(), vec![geometry.into_array_ref()]).unwrap();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_Simplify(geometry, 0.5) AS simplified FROM t",
    )
    .await;
    let batch = &batches[0];
    let schema = batch.schema();
    let field = schema.field(0);
    let typ = field.extension_type::<LineStringType>();
    assert_eq!(typ.dimension(), Dimension::XY);
    assert_eq!(typ.coord_type(), CoordType::Interleaved);

    let array = geoarrow_array::array::LineStringArray::try_from((batch.column(0).as_ref(), field))
        .unwrap();
    let value = array.value(0).unwrap();
    let line = value.as_type();
    let geo_traits::GeometryType::LineString(line) = line else {
        panic!("expected linestring");
    };
    assert_eq!(line.num_coords(), 2);
}

#[tokio::test]
async fn simplify_null_tolerance_returns_typed_nulls() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_Simplify(ST_GeomFromText('LINESTRING(0 0, 1 1)'), NULL) AS simplified",
    )
    .await;
    let batch = &batches[0];
    let schema = batch.schema();
    let field = schema.field(0);
    let array = geoarrow_array::array::from_arrow_array(batch.column(0).as_ref(), field).unwrap();
    assert!(array.is_null(0));
    assert!(field.extension_type_name().is_some());
}

#[tokio::test]
async fn geohash_uses_bbox_center_and_nulls_empty_geometries() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_GeoHash(ST_GeomFromText('POINT(-126 48)')), \
         ST_GeoHash(ST_GeomFromText('POLYGON((-127 47, -125 47, -125 49, -127 49, -127 47))')), \
         ST_GeoHash(ST_GeomFromText('POINT EMPTY'))",
    )
    .await;
    let batch = &batches[0];
    assert_eq!(batch.column(0).as_string_view().value(0), "c0w3hf1s70w3");
    assert_eq!(batch.column(1).as_string_view().value(0), "c0w3hf1s70w3");
    assert!(batch.column(2).is_null(0));
}

#[tokio::test]
async fn make_box_accepts_generic_points_and_preserves_crs() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_MakeBox2D(ST_GeomFromText('POINT(0 5)'), \
           ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT(10 20)')))) AS b, \
         ST_MakeBox2D(ST_Point(0, 5, 4326), ST_Point(10, 20, 4326)) AS with_crs, \
         ST_3DMakeBox(ST_GeomFromText('POINT Z(0 5 1)'), ST_GeomFromText('POINT Z(10 20 30)')) AS b3d",
    )
    .await;
    let batch = &batches[0];

    let rect: geoarrow_array::array::RectArray = geoarrow_array::array::RectArray::try_from((
        batch.column(0).as_ref(),
        batch.schema().field(0),
    ))
    .unwrap();
    let rect = rect.value(0).unwrap();
    assert_eq!(rect.min().x(), 0.0);
    assert_eq!(rect.max().y(), 20.0);

    let schema = batch.schema();
    let field = schema.field(1);
    assert_eq!(
        field
            .extension_type::<geoarrow_schema::BoxType>()
            .metadata()
            .crs(),
        &geoarrow_schema::Crs::from_authority_code("EPSG:4326".to_string())
    );

    let rect3d: geoarrow_array::array::RectArray = geoarrow_array::array::RectArray::try_from((
        batch.column(2).as_ref(),
        batch.schema().field(2),
    ))
    .unwrap();
    let rect3d = rect3d.value(0).unwrap();
    assert_eq!(rect3d.min().nth_or_panic(2), 1.0);
    assert_eq!(rect3d.max().nth_or_panic(2), 30.0);
}

#[tokio::test]
async fn make_box_rejects_non_point_inputs_with_execution_error() {
    let ctx = context();
    let err = async {
        let df = ctx
            .sql("SELECT ST_MakeBox2D(ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_Point(1, 1))")
            .await?;
        df.collect().await
    }
    .await
    .unwrap_err();
    assert!(err.to_string().contains("require Point inputs"), "{err}");
}

#[tokio::test]
async fn bare_null_is_accepted_by_geometry_udfs() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_X(NULL), ST_CoordDim(NULL), ST_GeoHash(NULL), \
         ST_Simplify(ST_GeomFromText('LINESTRING(0 0, 1 1)'), NULL), \
         ST_MakeBox2D(NULL, NULL)",
    )
    .await;
    let batch = &batches[0];
    let schema = batch.schema();
    for index in 0..batch.num_columns() {
        let field = schema.field(index);
        let is_null = if field.extension_type_name().is_some() {
            geoarrow_array::array::from_arrow_array(batch.column(index).as_ref(), field)
                .unwrap()
                .is_null(0)
        } else {
            batch.column(index).is_null(0)
        };
        assert!(is_null, "column {index}");
    }
}

#[tokio::test]
async fn unary_null_geometry_signatures_propagate_null() {
    use datafusion::logical_expr::{Signature, TypeSignature};

    fn accepts_unary_null(signature: &Signature) -> bool {
        fn accepts(signature: &TypeSignature) -> bool {
            match signature {
                TypeSignature::Uniform(1, types) => types.contains(&DataType::Null),
                TypeSignature::Exact(types) => {
                    types.len() == 1 && types[0] == DataType::Null
                }
                TypeSignature::OneOf(signatures) => signatures.iter().any(accepts),
                TypeSignature::Any(1) => true,
                _ => false,
            }
        }
        accepts(&signature.type_signature)
    }

    let ctx = context();
    let state = ctx.state();
    let mut names = state
        .scalar_functions()
        .iter()
        .filter(|(name, udf)| name.as_str() == udf.name() && accepts_unary_null(udf.signature()))
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    names.sort();
    assert!(names.iter().any(|name| name == "st_area"));

    let mut failures = Vec::new();
    for name in names {
        let result = async {
            ctx.sql(&format!("SELECT \"{name}\"(NULL) AS result"))
                .await?
                .collect()
                .await
        }
        .await;
        let batches = match result {
            Ok(batches) => batches,
            Err(err) => {
                failures.push(format!("{name}: {err}"));
                continue;
            }
        };
        for batch in batches {
            let field = batch.schema().field(0).clone();
            let is_null = if field.extension_type_name().is_some() {
                geoarrow_array::array::from_arrow_array(batch.column(0).as_ref(), &field)
                    .unwrap()
                    .is_null(0)
            } else {
                batch.column(0).is_null(0)
            };
            assert!(is_null, "{name} returned non-NULL for a bare NULL input");
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
