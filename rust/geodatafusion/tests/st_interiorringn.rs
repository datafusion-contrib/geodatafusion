use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray};
use geoarrow_array::builder::{GeometryBuilder, PolygonBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeoArrowType, GeometryType, LineStringType, Metadata, PolygonType,
};
use geodatafusion::udf::native::accessors::InteriorRingN;
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

fn parse_polygon(value: &str) -> wkt::types::Polygon<f64> {
    match parse_wkt(value) {
        Wkt::Polygon(polygon) => polygon,
        _ => panic!("expected polygon WKT, got {value}"),
    }
}

fn polygon_with_two_holes_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 1),(8 8,9 8,9 9,8 8))",
        Dimension::XYZ => {
            "POLYGON Z ((0 0 1,10 0 2,10 10 3,0 10 4,0 0 1),(1 1 5,2 1 6,2 2 7,1 1 5),(8 8 9,9 8 10,9 9 11,8 8 9))"
        }
        Dimension::XYM => {
            "POLYGON M ((0 0 10,10 0 11,10 10 12,0 10 13,0 0 10),(1 1 20,2 1 21,2 2 22,1 1 20),(8 8 30,9 8 31,9 9 32,8 8 30))"
        }
        Dimension::XYZM => {
            "POLYGON ZM ((0 0 1 10,10 0 2 11,10 10 3 12,0 10 4 13,0 0 1 10),(1 1 5 20,2 1 6 21,2 2 7 22,1 1 5 20),(8 8 9 30,9 8 10 31,9 9 11 32,8 8 9 30))"
        }
    }
}

fn sentinel_polygon_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "POLYGON((20 20,21 20,21 21,20 20),(20.5 20.5,21 20.5,21 21,20.5 20.5))",
        Dimension::XYZ => {
            "POLYGON Z ((20 20 90,21 20 91,21 21 92,20 20 90),(20.5 20.5 93,21 20.5 94,21 21 95,20.5 20.5 93))"
        }
        Dimension::XYM => {
            "POLYGON M ((20 20 90,21 20 91,21 21 92,20 20 90),(20.5 20.5 93,21 20.5 94,21 21 95,20.5 20.5 93))"
        }
        Dimension::XYZM => {
            "POLYGON ZM ((20 20 90 190,21 20 91 191,21 21 92 192,20 20 90 190),(20.5 20.5 93 193,21 20.5 94 194,21 21 95 195,20.5 20.5 93 193))"
        }
    }
}

fn empty_polygon_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "POLYGON EMPTY",
        Dimension::XYZ => "POLYGON Z EMPTY",
        Dimension::XYM => "POLYGON M EMPTY",
        Dimension::XYZM => "POLYGON ZM EMPTY",
    }
}

fn expected_hole_coords(dimension: Dimension, hole_index: usize) -> Vec<Vec<f64>> {
    let coordinates = if hole_index == 1 {
        [(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 1.0)]
    } else {
        [(8.0, 8.0), (9.0, 8.0), (9.0, 9.0), (8.0, 8.0)]
    };
    let (z_start, m_start) = if hole_index == 1 {
        (5.0, 20.0)
    } else {
        (9.0, 30.0)
    };

    coordinates
        .into_iter()
        .enumerate()
        .map(|(index, (x, y))| {
            let closing = if index == 3 { 0 } else { index };
            let z = z_start + closing as f64;
            let m = m_start + closing as f64;
            match dimension {
                Dimension::XY => vec![x, y],
                Dimension::XYZ => vec![x, y, z],
                Dimension::XYM => vec![x, y, m],
                Dimension::XYZM => vec![x, y, z, m],
            }
        })
        .collect()
}

fn line_string_coords(value: &impl LineStringTrait<T = f64>) -> Vec<Vec<f64>> {
    value
        .coords()
        .map(|coord| {
            (0..coord.dim().size())
                .map(|index| coord.nth(index).unwrap())
                .collect()
        })
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
        "SELECT ST_InteriorRingN(NULL, 1) AS ring",
        "SELECT ST_InteriorRingN(NULL, NULL) AS ring",
    ] {
        let batch = collect_one(&ctx, sql).await;
        let output_field = batch.schema().field(0).clone();
        let output_type = output_field.try_extension_type::<GeometryType>().unwrap();
        assert_eq!(output_type.coord_type(), CoordType::default());
        assert!(output_type.metadata().crs().crs_value().is_none());

        let rings =
            GeometryArray::try_from((batch.column(0).as_ref(), output_field.as_ref())).unwrap();
        assert_eq!(
            rings.data_type(),
            GeoArrowType::Geometry(output_type.clone())
        );
        assert_eq!(rings.len(), 1);
        assert!(rings.is_null(0));
    }

    let batch = collect_one(
        &ctx,
        "SELECT ST_InteriorRingN(value, n) AS ring \
         FROM (VALUES (NULL, 1), (NULL, 2)) AS inputs(value, n)",
    )
    .await;
    let output_field = batch.schema().field(0).clone();
    let output_type = output_field.try_extension_type::<GeometryType>().unwrap();
    let rings = GeometryArray::try_from((batch.column(0).as_ref(), output_field.as_ref())).unwrap();
    assert_eq!(
        rings.data_type(),
        GeoArrowType::Geometry(output_type.clone())
    );
    assert_eq!(rings.len(), 2);
    assert!(rings.is_null(0));
    assert!(rings.is_null(1));
}

#[tokio::test]
async fn typed_polygon_raw_arrow_handles_index_arrays_extremes_slices_layouts_and_nulls() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dimension in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let polygons = vec![
                Some(parse_polygon(sentinel_polygon_wkt(dimension))),
                Some(parse_polygon(polygon_with_two_holes_wkt(dimension))),
                Some(parse_polygon(polygon_with_two_holes_wkt(dimension))),
                Some(parse_polygon(polygon_with_two_holes_wkt(dimension))),
                Some(parse_polygon(polygon_with_two_holes_wkt(dimension))),
                Some(parse_polygon(polygon_with_two_holes_wkt(dimension))),
                Some(parse_polygon(polygon_with_two_holes_wkt(dimension))),
                Some(parse_polygon(polygon_with_two_holes_wkt(dimension))),
                Some(parse_polygon(polygon_with_two_holes_wkt(dimension))),
                None,
                Some(parse_polygon(empty_polygon_wkt(dimension))),
            ];
            let polygon_type = PolygonType::new(dimension, metadata()).with_coord_type(coord_type);
            let polygons = PolygonBuilder::from_nullable_polygons(&polygons, polygon_type)
                .finish()
                .slice(1, 10);
            let indices = Int64Array::from(vec![
                Some(1),
                Some(2),
                None,
                Some(0),
                Some(-1),
                Some(3),
                Some(i64::MAX),
                Some(i64::MIN),
                Some(1),
                Some(1),
            ]);

            let geometry_field = polygons.data_type().to_field("geometry", true);
            let input = RecordBatch::try_new(
                Arc::new(Schema::new([
                    Arc::new(geometry_field),
                    Arc::new(Field::new("n", DataType::Int64, true)),
                ])),
                vec![polygons.into_array_ref(), Arc::new(indices)],
            )
            .unwrap();

            let ctx = SessionContext::new();
            ctx.register_udf(InteriorRingN::new(coord_type).into());
            ctx.register_udf(AsText.into());
            ctx.register_batch("typed_polygons", input).unwrap();

            let batch = collect_one(
                &ctx,
                "SELECT ST_InteriorRingN(geometry, n) AS ring, \
                        ST_AsText(ST_InteriorRingN(geometry, n)) AS text \
                 FROM typed_polygons",
            )
            .await;
            assert_eq!(batch.num_rows(), 10);

            let output_field = batch.schema().field(0).clone();
            let output_type = output_field.try_extension_type::<LineStringType>().unwrap();
            assert_eq!(output_type.dimension(), dimension);
            assert_eq!(output_type.coord_type(), coord_type);
            assert_eq!(
                output_type.metadata().crs(),
                &Crs::from_authority_code(CRS_AUTHORITY.to_string())
            );

            let rings =
                LineStringArray::try_from((batch.column(0).as_ref(), output_field.as_ref()))
                    .unwrap();
            assert_eq!(
                rings.data_type(),
                GeoArrowType::LineString(output_type.clone())
            );
            assert_eq!(
                line_string_coords(&rings.value(0).unwrap()),
                expected_hole_coords(dimension, 1)
            );
            assert_eq!(
                line_string_coords(&rings.value(1).unwrap()),
                expected_hole_coords(dimension, 2)
            );
            for row in 2..10 {
                assert!(rings.is_null(row), "row {row} must be NULL");
            }

            let text = batch.column(1).as_string::<i32>();
            assert!(!text.is_null(0));
            assert!(!text.is_null(1));
            for row in 2..10 {
                assert!(text.is_null(row), "text row {row} must be NULL");
            }

            let broadcast = collect_one(
                &ctx,
                "SELECT ST_InteriorRingN(geometry, 2) AS ring FROM typed_polygons",
            )
            .await;
            let broadcast_field = broadcast.schema().field(0).clone();
            let broadcast_type = broadcast_field
                .try_extension_type::<LineStringType>()
                .unwrap();
            let broadcast_rings =
                LineStringArray::try_from((broadcast.column(0).as_ref(), broadcast_field.as_ref()))
                    .unwrap();
            assert_eq!(
                broadcast_rings.data_type(),
                GeoArrowType::LineString(broadcast_type.clone())
            );
            for row in 0..8 {
                assert_eq!(
                    line_string_coords(&broadcast_rings.value(row).unwrap()),
                    expected_hole_coords(dimension, 2)
                );
            }
            for row in 8..10 {
                assert!(broadcast_rings.is_null(row), "row {row} must be NULL");
            }

            let null_index = collect_one(
                &ctx,
                "SELECT ST_InteriorRingN(geometry, NULL) AS ring FROM typed_polygons",
            )
            .await;
            let null_index_field = null_index.schema().field(0).clone();
            let null_index_type = null_index_field
                .try_extension_type::<LineStringType>()
                .unwrap();
            let null_index_rings = LineStringArray::try_from((
                null_index.column(0).as_ref(),
                null_index_field.as_ref(),
            ))
            .unwrap();
            assert_eq!(
                null_index_rings.data_type(),
                GeoArrowType::LineString(null_index_type.clone())
            );
            for row in 0..10 {
                assert!(
                    null_index_rings.is_null(row),
                    "null-index row {row} must be NULL"
                );
            }
        }
    }
}

#[tokio::test]
async fn generic_mixed_geometry_raw_arrow_uses_array_indices_and_returns_polygon_rings_only() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let geometries = vec![
            Some(parse_wkt("POINT(9 9)")),
            Some(parse_wkt(polygon_with_two_holes_wkt(Dimension::XYZM))),
            Some(parse_wkt(polygon_with_two_holes_wkt(Dimension::XYM))),
            Some(parse_wkt("POINT(1 2)")),
            Some(parse_wkt("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))")),
            Some(parse_wkt(
                "GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 0)),POINT(3 4))",
            )),
            Some(parse_wkt("POLYGON ZM EMPTY")),
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
        ctx.register_udf(InteriorRingN::new(coord_type).into());
        ctx.register_udf(AsText.into());
        ctx.register_batch("mixed_geometries", input).unwrap();

        let batch = collect_one(
            &ctx,
            "SELECT ST_InteriorRingN(geometry, n) AS ring, \
                    ST_AsText(ST_InteriorRingN(geometry, n)) AS text \
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

        let rings =
            GeometryArray::try_from((batch.column(0).as_ref(), output_field.as_ref())).unwrap();
        assert_eq!(
            rings.data_type(),
            GeoArrowType::Geometry(output_type.clone())
        );

        let zm_ring = rings.value(0).unwrap();
        let geo_traits::GeometryType::LineString(zm_ring) = zm_ring.as_type() else {
            panic!("expected LineString for Polygon ZM row");
        };
        assert_eq!(zm_ring.dim(), geo_traits::Dimensions::Xyzm);
        assert_eq!(
            line_string_coords(&zm_ring),
            expected_hole_coords(Dimension::XYZM, 1)
        );

        let m_ring = rings.value(1).unwrap();
        let geo_traits::GeometryType::LineString(m_ring) = m_ring.as_type() else {
            panic!("expected LineString for Polygon M row");
        };
        assert_eq!(m_ring.dim(), geo_traits::Dimensions::Xym);
        assert_eq!(
            line_string_coords(&m_ring),
            expected_hole_coords(Dimension::XYM, 2)
        );

        for row in 2..7 {
            assert!(rings.is_null(row), "row {row} must be NULL");
        }

        let text = batch.column(1).as_string::<i32>();
        assert!(!text.is_null(0));
        assert!(!text.is_null(1));
        for row in 2..7 {
            assert!(text.is_null(row), "text row {row} must be NULL");
        }
    }
}
