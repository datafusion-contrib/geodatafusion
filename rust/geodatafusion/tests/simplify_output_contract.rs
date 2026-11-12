use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::Schema;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{WktArray, from_arrow_array};
use geoarrow_array::cast::from_wkt;
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, GeometryType, LineStringType, MultiLineStringType,
    MultiPolygonType, PolygonType,
};
use geodatafusion::udf::geo::processing::{Simplify, SimplifyPreserveTopology, SimplifyVW};

#[tokio::test]
async fn native_simplify_variants_match_output_layout_dimension_and_nulls() {
    let fixtures: [(GeoArrowType, &str); 4] = [
        (
            LineStringType::new(Dimension::XYZM, Default::default()).into(),
            "LINESTRING ZM(0 0 1 2,1 0.1 3 4,2 0 5 6)",
        ),
        (
            MultiLineStringType::new(Dimension::XYZM, Default::default()).into(),
            "MULTILINESTRING ZM((0 0 1 2,1 0.1 3 4,2 0 5 6))",
        ),
        (
            PolygonType::new(Dimension::XYZM, Default::default()).into(),
            "POLYGON ZM((0 0 1 2,4 0 3 4,4 4 5 6,0 4 7 8,0 0 1 2))",
        ),
        (
            MultiPolygonType::new(Dimension::XYZM, Default::default()).into(),
            "MULTIPOLYGON ZM(((0 0 1 2,4 0 3 4,4 4 5 6,0 4 7 8,0 0 1 2)))",
        ),
    ];

    for output_layout in [CoordType::Separated, CoordType::Interleaved] {
        let input_layout = match output_layout {
            CoordType::Separated => CoordType::Interleaved,
            CoordType::Interleaved => CoordType::Separated,
        };
        for (typ, wkt) in &fixtures {
            let input_type = typ.clone().with_coord_type(input_layout);
            let expected = typ
                .clone()
                .with_dimension(Dimension::XY)
                .with_coord_type(output_layout);
            let encoded = WktArray::new(
                StringArray::from(vec![Some(*wkt), None]),
                Default::default(),
            );
            let input = from_wkt(&encoded, input_type).unwrap();
            let schema = Arc::new(Schema::new(vec![input.data_type().to_field("g", true)]));
            let batch = RecordBatch::try_new(schema, vec![input.to_array_ref()]).unwrap();
            let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
            ctx.register_udf(Simplify::new(output_layout).into());
            ctx.register_udf(SimplifyVW::new(output_layout).into());
            ctx.register_udf(SimplifyPreserveTopology::new(output_layout).into());
            ctx.register_batch("input", batch).unwrap();

            for function in [
                "st_simplify",
                "st_simplifyvw",
                "st_simplifypreservetopology",
            ] {
                let sql = format!(
                    "SELECT {function}(g, 0.1) AS result, {function}(g, NULL) AS absent FROM input"
                );
                let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                assert_eq!(batches.len(), 1);
                let batch = &batches[0];
                assert_eq!(batch.num_rows(), 2);
                for column in 0..2 {
                    let field = batch.schema().field(column).clone();
                    assert_eq!(GeoArrowType::from_arrow_field(&field).unwrap(), expected);
                    assert_eq!(batch.column(column).data_type(), field.data_type());
                    let actual = from_arrow_array(batch.column(column).as_ref(), &field).unwrap();
                    assert_eq!(actual.is_null(0), column == 1, "{function}, {expected:?}");
                    assert!(actual.is_null(1), "{function}, {expected:?}");
                }
            }
        }
    }
}

#[tokio::test]
async fn encoded_simplify_variants_match_output_layout_and_xy_values() {
    for output_layout in [CoordType::Separated, CoordType::Interleaved] {
        let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
        geodatafusion::register(&ctx);
        ctx.register_udf(Simplify::new(output_layout).into());
        ctx.register_udf(SimplifyVW::new(output_layout).into());
        ctx.register_udf(SimplifyPreserveTopology::new(output_layout).into());
        for function in [
            "st_simplify",
            "st_simplifyvw",
            "st_simplifypreservetopology",
        ] {
            for input in [
                "wkt",
                "ST_GeomFromText(wkt)",
                "ST_AsBinary(ST_GeomFromText(wkt))",
            ] {
                let sql = format!(
                    "SELECT result, ST_AsText(result) AS text FROM (
                        SELECT {function}({input}, 0.5) AS result
                        FROM (VALUES ('LINESTRING ZM(0 0 1 2,1 0.1 3 4,2 0 5 6)'), (NULL)) AS inputs(wkt)
                    )"
                );
                let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                let batch = &batches[0];
                assert_eq!(batch.num_rows(), 2);
                let field = batch.schema().field(0).clone();
                let expected: GeoArrowType = GeometryType::new(Default::default())
                    .with_coord_type(output_layout)
                    .into();
                assert_eq!(GeoArrowType::from_arrow_field(&field).unwrap(), expected);
                assert_eq!(batch.column(0).data_type(), field.data_type());
                let actual = from_arrow_array(batch.column(0).as_ref(), &field).unwrap();
                assert!(!actual.is_null(0));
                assert!(actual.is_null(1));
                assert_eq!(
                    batch.column(1).as_string::<i32>().value(0),
                    "LINESTRING(0 0,2 0)"
                );
            }
        }
    }
}
