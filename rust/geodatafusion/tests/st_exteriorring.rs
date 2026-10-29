use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray};
use geoarrow_array::builder::{GeometryBuilder, PolygonBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeoArrowType, GeometryType, LineStringType, Metadata, PolygonType,
};
use geodatafusion::udf::native::accessors::ExteriorRing;
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

fn polygon_with_hole_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,1 2,2 2,2 1,1 1))",
        Dimension::XYZ => {
            "POLYGON Z ((0 0 10,4 0 11,4 4 12,0 4 13,0 0 10),(1 1 14,1 2 15,2 2 16,2 1 17,1 1 14))"
        }
        Dimension::XYM => {
            "POLYGON M ((0 0 20,4 0 21,4 4 22,0 4 23,0 0 20),(1 1 24,1 2 25,2 2 26,2 1 27,1 1 24))"
        }
        Dimension::XYZM => {
            "POLYGON ZM ((0 0 10 20,4 0 11 21,4 4 12 22,0 4 13 23,0 0 10 20),(1 1 14 24,1 2 15 25,2 2 16 26,2 1 17 27,1 1 14 24))"
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

fn sentinel_polygon_wkt(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "POLYGON((9 9,10 9,10 10,9 9))",
        Dimension::XYZ => "POLYGON Z ((9 9 29,10 9 30,10 10 31,9 9 29))",
        Dimension::XYM => "POLYGON M ((9 9 39,10 9 40,10 10 41,9 9 39))",
        Dimension::XYZM => "POLYGON ZM ((9 9 29 39,10 9 30 40,10 10 31 41,9 9 29 39))",
    }
}

fn expected_exterior_coords(dimension: Dimension) -> Vec<Vec<f64>> {
    [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
        .into_iter()
        .enumerate()
        .map(|(index, (x, y))| {
            let ring_index = if index == 4 { 0 } else { index };
            let z = 10.0 + ring_index as f64;
            let m = 20.0 + ring_index as f64;
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
async fn untyped_null_sql_returns_typed_null_geometry() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let batch = collect_one(&ctx, "SELECT ST_ExteriorRing(NULL) AS ring").await;
    let output_field = batch.schema().field(0).clone();
    let output_type = output_field.try_extension_type::<GeometryType>().unwrap();
    assert_eq!(output_type.coord_type(), CoordType::default());
    assert!(output_type.metadata().crs().crs_value().is_none());

    let rings = GeometryArray::try_from((batch.column(0).as_ref(), output_field.as_ref())).unwrap();
    assert_eq!(
        rings.data_type(),
        GeoArrowType::Geometry(output_type.clone())
    );
    assert_eq!(rings.len(), 1);
    assert!(rings.is_null(0));

    let batch = collect_one(
        &ctx,
        "SELECT ST_ExteriorRing(value) AS ring \
         FROM (VALUES (NULL), (NULL)) AS inputs(value)",
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
async fn typed_polygon_raw_arrow_preserves_dimension_crs_layout_slices_and_nulls() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dimension in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let polygons = vec![
                Some(parse_polygon(sentinel_polygon_wkt(dimension))),
                Some(parse_polygon(polygon_with_hole_wkt(dimension))),
                None,
                Some(parse_polygon(empty_polygon_wkt(dimension))),
            ];
            let polygon_type = PolygonType::new(dimension, metadata()).with_coord_type(coord_type);
            let polygons = PolygonBuilder::from_nullable_polygons(&polygons, polygon_type)
                .finish()
                .slice(1, 3);

            let input_field = polygons.data_type().to_field("geometry", true);
            let input = RecordBatch::try_new(
                Arc::new(Schema::new([Arc::new(input_field)])),
                vec![polygons.into_array_ref()],
            )
            .unwrap();

            let ctx = SessionContext::new();
            ctx.register_udf(ExteriorRing::new(coord_type).into());
            ctx.register_udf(AsText.into());
            ctx.register_batch("typed_polygons", input).unwrap();

            let batch = collect_one(
                &ctx,
                "SELECT ST_ExteriorRing(geometry) AS ring, \
                        ST_AsText(ST_ExteriorRing(geometry)) AS text \
                 FROM typed_polygons",
            )
            .await;
            assert_eq!(batch.num_rows(), 3);

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
                expected_exterior_coords(dimension)
            );
            assert!(rings.is_null(1));
            assert_eq!(rings.value(2).unwrap().num_coords(), 0);

            let text = batch.column(1).as_string::<i32>();
            assert!(!text.is_null(0));
            assert!(text.is_null(1));
            assert!(text.value(2).ends_with("EMPTY"));
        }
    }
}

#[tokio::test]
async fn generic_mixed_geometry_raw_arrow_matches_postgis_polygon_only_semantics() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let geometries = vec![
            Some(parse_wkt("POINT(9 9)")),
            Some(parse_wkt(
                "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 1))",
            )),
            Some(parse_wkt(
                "POLYGON Z ((0 0 10,4 0 11,4 4 12,0 4 13,0 0 10))",
            )),
            Some(parse_wkt("POLYGON ZM EMPTY")),
            Some(parse_wkt("POINT(1 2)")),
            Some(parse_wkt("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))")),
            Some(parse_wkt(
                "GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 0)),POINT(3 4))",
            )),
            None,
        ];
        let geometry_type = GeometryType::new(metadata()).with_coord_type(coord_type);
        let geometries = GeometryBuilder::from_nullable_geometries(&geometries, geometry_type)
            .unwrap()
            .finish()
            .slice(1, 7);

        let input_field = geometries.data_type().to_field("geometry", true);
        let input = RecordBatch::try_new(
            Arc::new(Schema::new([Arc::new(input_field)])),
            vec![geometries.into_array_ref()],
        )
        .unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(ExteriorRing::new(coord_type).into());
        ctx.register_udf(AsText.into());
        ctx.register_batch("mixed_geometries", input).unwrap();

        let batch = collect_one(
            &ctx,
            "SELECT ST_ExteriorRing(geometry) AS ring, \
                    ST_AsText(ST_ExteriorRing(geometry)) AS text \
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

        let xy_ring = rings.value(0).unwrap();
        let geo_traits::GeometryType::LineString(xy_ring) = xy_ring.as_type() else {
            panic!("expected LineString for Polygon row");
        };
        assert_eq!(xy_ring.dim(), geo_traits::Dimensions::Xy);
        assert_eq!(
            line_string_coords(&xy_ring),
            expected_exterior_coords(Dimension::XY)
        );

        let z_ring = rings.value(1).unwrap();
        let geo_traits::GeometryType::LineString(z_ring) = z_ring.as_type() else {
            panic!("expected LineString for Polygon Z row");
        };
        assert_eq!(z_ring.dim(), geo_traits::Dimensions::Xyz);

        let empty_ring = rings.value(2).unwrap();
        let geo_traits::GeometryType::LineString(empty_ring) = empty_ring.as_type() else {
            panic!("expected empty LineString for empty Polygon ZM row");
        };
        assert_eq!(empty_ring.dim(), geo_traits::Dimensions::Xyzm);
        assert_eq!(empty_ring.num_coords(), 0);

        for row in 3..7 {
            assert!(rings.is_null(row), "row {row} must be NULL");
        }

        let text = batch.column(1).as_string::<i32>();
        assert_eq!(text.value(0), "LINESTRING(0 0,4 0,4 4,0 4,0 0)");
        assert!(!text.is_null(1));
        assert!(text.value(2).ends_with("EMPTY"));
        for row in 3..7 {
            assert!(text.is_null(row), "text row {row} must be NULL");
        }
    }
}
