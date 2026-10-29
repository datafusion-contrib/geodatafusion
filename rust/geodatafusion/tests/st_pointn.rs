use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, GeometryTrait, PointTrait};
use geoarrow_array::array::{GeometryArray, PointArray};
use geoarrow_array::builder::{GeometryBuilder, LineStringBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Crs, Dimension, GeoArrowType, GeometryType, Metadata, PointType};
use geodatafusion::udf::native::accessors::PointN;
use geodatafusion::udf::native::io::AsText;
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

fn parse_line_string(value: &str) -> wkt::types::LineString<f64> {
    match parse_wkt(value) {
        Wkt::LineString(line_string) => line_string,
        _ => panic!("expected LineString WKT, got {value}"),
    }
}

fn line_string_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "LINESTRING(0 0,1 2,3 4)",
        Dimension::XYZ => "LINESTRING Z (0 0 10,1 2 11,3 4 12)",
        Dimension::XYM => "LINESTRING M (0 0 20,1 2 21,3 4 22)",
        Dimension::XYZM => "LINESTRING ZM (0 0 10 20,1 2 11 21,3 4 12 22)",
    }
}

fn sentinel_line_string_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "LINESTRING(20 20,21 21)",
        Dimension::XYZ => "LINESTRING Z (20 20 90,21 21 91)",
        Dimension::XYM => "LINESTRING M (20 20 90,21 21 91)",
        Dimension::XYZM => "LINESTRING ZM (20 20 90 190,21 21 91 191)",
    }
}

fn empty_line_string_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "LINESTRING EMPTY",
        Dimension::XYZ => "LINESTRING Z EMPTY",
        Dimension::XYM => "LINESTRING M EMPTY",
        Dimension::XYZM => "LINESTRING ZM EMPTY",
    }
}

fn expected_point_coords(dimension: Dimension, position: usize) -> Vec<f64> {
    let (x, y) = [(0.0, 0.0), (1.0, 2.0), (3.0, 4.0)][position];
    let z = 10.0 + position as f64;
    let m = 20.0 + position as f64;
    match dimension {
        Dimension::XY => vec![x, y],
        Dimension::XYZ => vec![x, y, z],
        Dimension::XYM => vec![x, y, m],
        Dimension::XYZM => vec![x, y, z, m],
    }
}

fn coord_values(coord: &impl CoordTrait<T = f64>) -> Vec<f64> {
    (0..coord.dim().size())
        .map(|index| coord.nth(index).unwrap())
        .collect()
}

async fn collect_one(ctx: &SessionContext, sql: &str) -> RecordBatch {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    batches.into_iter().next().unwrap()
}

#[tokio::test]
async fn untyped_null_geometry_and_index_return_typed_null_geometry() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    for sql in [
        "SELECT ST_PointN(NULL, 1) AS point",
        "SELECT ST_PointN(NULL, NULL) AS point",
    ] {
        let batch = collect_one(&ctx, sql).await;
        let output_field = batch.schema().field(0).clone();
        let output_type = output_field.try_extension_type::<GeometryType>().unwrap();
        assert_eq!(output_type.coord_type(), CoordType::default());
        assert!(output_type.metadata().crs().crs_value().is_none());

        let points =
            GeometryArray::try_from((batch.column(0).as_ref(), output_field.as_ref())).unwrap();
        assert_eq!(
            points.data_type(),
            GeoArrowType::Geometry(output_type.clone())
        );
        assert_eq!(points.len(), 1);
        assert!(points.is_null(0));
    }

    let batch = collect_one(
        &ctx,
        "SELECT ST_PointN(value, n) AS point \
         FROM (VALUES (NULL, 1), (NULL, -1)) AS inputs(value, n)",
    )
    .await;
    let output_field = batch.schema().field(0).clone();
    let output_type = output_field.try_extension_type::<GeometryType>().unwrap();
    let points =
        GeometryArray::try_from((batch.column(0).as_ref(), output_field.as_ref())).unwrap();
    assert_eq!(
        points.data_type(),
        GeoArrowType::Geometry(output_type.clone())
    );
    assert_eq!(points.len(), 2);
    assert!(points.is_null(0));
    assert!(points.is_null(1));
}

#[tokio::test]
async fn typed_line_string_raw_arrow_preserves_dimensions_layouts_crs_slices_and_extremes() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dimension in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let line_strings = vec![
                Some(parse_line_string(sentinel_line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                Some(parse_line_string(line_string_wkt(dimension))),
                None,
                Some(parse_line_string(empty_line_string_wkt(dimension))),
            ];
            let line_type = geoarrow_schema::LineStringType::new(dimension, metadata())
                .with_coord_type(coord_type);
            let lines = LineStringBuilder::from_nullable_line_strings(&line_strings, line_type)
                .finish()
                .slice(1, 11);
            let indices = Int64Array::from(vec![
                Some(1),
                Some(-1),
                Some(-2),
                None,
                Some(0),
                Some(4),
                Some(-4),
                Some(i64::MAX),
                Some(i64::MIN),
                Some(1),
                Some(1),
            ]);

            let geometry_field = lines.data_type().to_field("geometry", true);
            let input = RecordBatch::try_new(
                Arc::new(Schema::new([
                    Arc::new(geometry_field),
                    Arc::new(Field::new("n", DataType::Int64, true)),
                ])),
                vec![lines.into_array_ref(), Arc::new(indices)],
            )
            .unwrap();

            let ctx = SessionContext::new();
            ctx.register_udf(PointN::new(coord_type).into());
            ctx.register_udf(AsText.into());
            ctx.register_batch("lines", input).unwrap();

            let batch = collect_one(
                &ctx,
                "SELECT ST_PointN(geometry, n) AS point, \
                        ST_AsText(ST_PointN(geometry, n)) AS text \
                 FROM lines",
            )
            .await;
            assert_eq!(batch.num_rows(), 11);

            let output_field = batch.schema().field(0).clone();
            let output_type = output_field.try_extension_type::<PointType>().unwrap();
            assert_eq!(output_type.dimension(), dimension);
            assert_eq!(output_type.coord_type(), coord_type);
            assert_eq!(
                output_type.metadata().crs(),
                &Crs::from_authority_code(CRS_AUTHORITY.to_string())
            );

            let points =
                PointArray::try_from((batch.column(0).as_ref(), output_field.as_ref())).unwrap();
            assert_eq!(points.data_type(), GeoArrowType::Point(output_type.clone()));
            assert_eq!(
                coord_values(&points.value(0).unwrap().coord().unwrap()),
                expected_point_coords(dimension, 0)
            );
            assert_eq!(
                coord_values(&points.value(1).unwrap().coord().unwrap()),
                expected_point_coords(dimension, 2)
            );
            assert_eq!(
                coord_values(&points.value(2).unwrap().coord().unwrap()),
                expected_point_coords(dimension, 1)
            );
            for row in 3..11 {
                assert!(points.is_null(row), "row {row} must be NULL");
            }

            let text = batch.column(1).as_string::<i32>();
            assert!(!text.is_null(0));
            assert!(!text.is_null(1));
            assert!(!text.is_null(2));
            for row in 3..11 {
                assert!(text.is_null(row), "text row {row} must be NULL");
            }

            let broadcast =
                collect_one(&ctx, "SELECT ST_PointN(geometry, 2) AS point FROM lines").await;
            let broadcast_field = broadcast.schema().field(0).clone();
            let broadcast_type = broadcast_field.try_extension_type::<PointType>().unwrap();
            let broadcast_points =
                PointArray::try_from((broadcast.column(0).as_ref(), broadcast_field.as_ref()))
                    .unwrap();
            assert_eq!(
                broadcast_points.data_type(),
                GeoArrowType::Point(broadcast_type.clone())
            );
            for row in 0..9 {
                assert_eq!(
                    coord_values(&broadcast_points.value(row).unwrap().coord().unwrap()),
                    expected_point_coords(dimension, 1)
                );
            }
            for row in 9..11 {
                assert!(broadcast_points.is_null(row), "row {row} must be NULL");
            }

            let null_index =
                collect_one(&ctx, "SELECT ST_PointN(geometry, NULL) AS point FROM lines").await;
            let null_index_field = null_index.schema().field(0).clone();
            let null_index_type = null_index_field.try_extension_type::<PointType>().unwrap();
            let null_index_points =
                PointArray::try_from((null_index.column(0).as_ref(), null_index_field.as_ref()))
                    .unwrap();
            assert_eq!(
                null_index_points.data_type(),
                GeoArrowType::Point(null_index_type.clone())
            );
            for row in 0..11 {
                assert!(
                    null_index_points.is_null(row),
                    "null-index row {row} must be NULL"
                );
            }
        }
    }
}

#[tokio::test]
async fn generic_mixed_geometry_raw_arrow_uses_per_row_indices_and_returns_points_only() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let geometries = vec![
            Some(parse_wkt("POINT(9 9)")),
            Some(parse_wkt(line_string_wkt(Dimension::XYZM))),
            Some(parse_wkt(line_string_wkt(Dimension::XYM))),
            Some(parse_wkt("POINT(1 2)")),
            Some(parse_wkt("MULTILINESTRING((0 0,1 1))")),
            Some(parse_wkt("POLYGON((0 0,1 0,1 1,0 0))")),
            Some(parse_wkt("LINESTRING ZM EMPTY")),
            None,
        ];
        let geometry_type = GeometryType::new(metadata()).with_coord_type(coord_type);
        let geometries = GeometryBuilder::from_nullable_geometries(&geometries, geometry_type)
            .unwrap()
            .finish()
            .slice(1, 7);
        let indices = Int32Array::from(vec![1, 2, 1, 1, 1, 1, 1]);

        let geometry_field = geometries.data_type().to_field("geometry", true);
        let input = RecordBatch::try_new(
            Arc::new(Schema::new([
                Arc::new(geometry_field),
                Arc::new(Field::new("n", DataType::Int32, true)),
            ])),
            vec![geometries.into_array_ref(), Arc::new(indices)],
        )
        .unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(PointN::new(coord_type).into());
        ctx.register_udf(AsText.into());
        ctx.register_batch("mixed_geometries", input).unwrap();

        let batch = collect_one(
            &ctx,
            "SELECT ST_PointN(geometry, n) AS point, \
                    ST_AsText(ST_PointN(geometry, n)) AS text \
             FROM mixed_geometries",
        )
        .await;
        assert_eq!(batch.num_rows(), 7);

        let output_field = batch.schema().field(0).clone();
        let output_type = output_field.try_extension_type::<GeometryType>().unwrap();
        assert_eq!(output_type.coord_type(), coord_type);
        assert_eq!(
            output_type.metadata().crs(),
            &Crs::from_authority_code(CRS_AUTHORITY.to_string())
        );

        let points =
            GeometryArray::try_from((batch.column(0).as_ref(), output_field.as_ref())).unwrap();
        assert_eq!(
            points.data_type(),
            GeoArrowType::Geometry(output_type.clone())
        );

        let zm_point = points.value(0).unwrap();
        let geo_traits::GeometryType::Point(zm_point) = zm_point.as_type() else {
            panic!("expected Point for LineString ZM row");
        };
        assert_eq!(
            coord_values(&zm_point.coord().unwrap()),
            expected_point_coords(Dimension::XYZM, 0)
        );

        let m_point = points.value(1).unwrap();
        let geo_traits::GeometryType::Point(m_point) = m_point.as_type() else {
            panic!("expected Point for LineString M row");
        };
        assert_eq!(
            coord_values(&m_point.coord().unwrap()),
            expected_point_coords(Dimension::XYM, 1)
        );

        for row in 2..7 {
            assert!(points.is_null(row), "row {row} must be NULL");
        }

        let text = batch.column(1).as_string::<i32>();
        assert!(!text.is_null(0));
        assert!(!text.is_null(1));
        for row in 2..7 {
            assert!(text.is_null(row), "text row {row} must be NULL");
        }
    }
}
