use std::sync::Arc;

use approx::relative_eq;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
use geoarrow_array::array::{
    LineStringArray, MultiPointArray, PointArray, PolygonArray, from_arrow_array,
};
use geoarrow_array::builder::{LineStringBuilder, MultiPointBuilder, PointBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow};
use geoarrow_schema::{
    CoordType, Crs, Dimension, LineStringType, Metadata, MultiPointType, PointType,
};
use wkt::types::{
    Coord, Dimension as WktDimension, LineString as WktLineString, MultiPoint as WktMultiPoint,
    Point as WktPoint,
};

fn metadata(srid: i32) -> Arc<Metadata> {
    if srid == 0 {
        Arc::new(Metadata::default())
    } else {
        Arc::new(Metadata::new(
            Crs::from_authority_code(format!("EPSG:{srid}")),
            None,
        ))
    }
}

fn coord(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Coord<f64> {
    Coord { x, y, z, m }
}

fn point_array(
    dim: Dimension,
    srid: i32,
    points: impl IntoIterator<Item = WktPoint<f64>>,
) -> PointArray {
    let typ = PointType::new(dim, metadata(srid)).with_coord_type(CoordType::Separated);
    let points = points.into_iter().collect::<Vec<_>>();
    PointBuilder::from_points(points.iter(), typ).finish()
}

fn multi_point_array(
    dim: Dimension,
    srid: i32,
    multi_points: Vec<WktMultiPoint<f64>>,
) -> MultiPointArray {
    let typ = MultiPointType::new(dim, metadata(srid)).with_coord_type(CoordType::Separated);
    MultiPointBuilder::from_multi_points(&multi_points, typ).finish()
}

fn line_string_array(dim: Dimension, srid: i32, lines: Vec<WktLineString<f64>>) -> LineStringArray {
    let typ = LineStringType::new(dim, metadata(srid)).with_coord_type(CoordType::Separated);
    LineStringBuilder::from_line_strings(&lines, typ).finish()
}

fn wkt_point(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> WktPoint<f64> {
    WktPoint::new(Some(wkt_coord(x, y, z, m)), wkt_dimension_of(z, m))
}

fn wkt_coord(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Coord<f64> {
    Coord { x, y, z, m }
}

fn wkt_dimension_of(z: Option<f64>, m: Option<f64>) -> WktDimension {
    match (z.is_some(), m.is_some()) {
        (false, false) => WktDimension::XY,
        (true, false) => WktDimension::XYZ,
        (false, true) => WktDimension::XYM,
        (true, true) => WktDimension::XYZM,
    }
}

fn crs_is(field: &Field, expected: &str) -> bool {
    Metadata::try_from(field)
        .ok()
        .and_then(|metadata| {
            metadata
                .crs()
                .crs_value()
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .as_deref()
        == Some(expected)
}

fn crs_is_unknown(field: &Field) -> bool {
    Metadata::try_from(field)
        .map(|metadata| metadata.crs().crs_value().is_none())
        .unwrap_or(false)
}

async fn run_sql(ctx: &SessionContext, sql: &str) -> arrow_array::RecordBatch {
    ctx.sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

async fn sql_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(df) => df.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn public_register_preserves_geometry_dimensions_and_crs() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let p1 = point_array(
        Dimension::XYZ,
        4326,
        [
            WktPoint::new(Some(coord(0.0, 0.0, Some(5.0), None)), WktDimension::XYZ),
            WktPoint::new(Some(coord(2.0, 2.0, Some(6.0), None)), WktDimension::XYZ),
        ],
    );
    let p2 = point_array(
        Dimension::XYM,
        4326,
        [
            WktPoint::new(Some(coord(1.0, 1.0, None, Some(7.0))), WktDimension::XYM),
            WktPoint::new(Some(coord(3.0, 3.0, None, Some(8.0))), WktDimension::XYM),
        ],
    );
    let multi_point = WktMultiPoint::new(
        vec![
            wkt_point(0.0, 0.0, None, Some(1.0)),
            wkt_point(1.0, 1.0, None, Some(2.0)),
        ],
        WktDimension::XYM,
    );
    let mp = multi_point_array(Dimension::XYM, 4326, vec![multi_point.clone(), multi_point]);
    let shell = WktLineString::new(
        vec![
            coord(0.0, 0.0, Some(1.0), Some(10.0)),
            coord(0.0, 10.0, Some(2.0), Some(11.0)),
            coord(10.0, 10.0, Some(3.0), Some(12.0)),
            coord(0.0, 0.0, Some(1.0), Some(10.0)),
        ],
        WktDimension::XYZM,
    );
    let hole = WktLineString::new(
        vec![
            coord(1.0, 1.0, Some(4.0), Some(20.0)),
            coord(2.0, 1.0, Some(5.0), Some(21.0)),
            coord(1.0, 2.0, Some(6.0), Some(22.0)),
            coord(1.0, 1.0, Some(4.0), Some(20.0)),
        ],
        WktDimension::XYZM,
    );
    let shell_array = line_string_array(Dimension::XYZM, 4326, vec![shell.clone(), shell.clone()]);
    let hole_array = line_string_array(Dimension::XYZM, 4326, vec![hole.clone(), hole.clone()]);
    let hole_field = Arc::new(hole_array.extension_type().to_field("item", true));
    let hole_values = hole_array.into_array_ref();
    let holes = ListArray::new(
        hole_field,
        OffsetBuffer::<i32>::from_lengths([1, 0]),
        hole_values,
        Some(NullBuffer::from(vec![true, false])),
    );
    let line_points = point_array(
        Dimension::XY,
        4326,
        [
            WktPoint::new(Some(coord(0.0, 0.0, None, None)), WktDimension::XY),
            WktPoint::new(Some(coord(1.0, 1.0, None, None)), WktDimension::XY),
        ],
    );
    let line_point_field = Arc::new(line_points.extension_type().to_field("item", true));
    let line_point_values = line_points.into_array_ref();
    let point_list = ListArray::new(
        line_point_field,
        OffsetBuffer::<i32>::from_lengths([2, 0]),
        line_point_values,
        None,
    );

    let p1_field = p1.extension_type().to_field("p1", true);
    let p2_field = p2.extension_type().to_field("p2", true);
    let mp_field = mp.extension_type().to_field("mp", true);
    let shell_field = shell_array.extension_type().to_field("shell", true);
    let schema = Schema::new(vec![
        p1_field,
        p2_field,
        mp_field,
        shell_field,
        Field::new("holes", holes.data_type().clone(), true),
        Field::new("line_points", point_list.data_type().clone(), true),
    ]);
    let p1_values = p1.into_array_ref();
    let p2_values = p2.into_array_ref();
    let mp_values = mp.into_array_ref();
    let shell_values = shell_array.into_array_ref();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            p1_values,
            p2_values,
            mp_values,
            shell_values,
            Arc::new(holes),
            Arc::new(point_list),
        ],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    let output = run_sql(
        &ctx,
        "SELECT \
            ST_MakeLine(p1, p2) AS line, \
            ST_LineFromMultiPoint(mp) AS mp_line, \
            ST_MakePolygon(shell) AS shell_poly, \
            ST_MakePolygon(shell, holes) AS holed_poly, \
            ST_Polygon(shell, 3857) AS poly, \
            ST_MakeEnvelope(10, 10, 11, 11, 4326) AS env, \
            ST_TileEnvelope(2, 1, 1) AS tile, \
            ST_MakeLine(line_points) AS array_line, \
            ST_MakeEnvelope(0, 0, 1, 1) AS env_unknown \
         FROM t",
    )
    .await;

    let line_field = output.schema().field(0).clone();
    assert!(crs_is(&line_field, "EPSG:4326"));
    let line_type = line_field.try_extension_type::<LineStringType>().unwrap();
    assert_eq!(line_type.dimension(), Dimension::XYZM);
    let line = LineStringArray::try_from((output.column(0).as_ref(), line_field.as_ref())).unwrap();
    let line_value = line.value(0).unwrap();
    let line_coords = line_value.coords().collect::<Vec<_>>();
    assert_eq!(line_coords.len(), 2);
    assert_eq!(line_coords[0].nth(2), Some(5.0));
    assert_eq!(line_coords[0].nth(3), Some(0.0));
    assert_eq!(line_coords[1].nth(2), Some(0.0));
    assert_eq!(line_coords[1].nth(3), Some(7.0));

    let mp_line_field = output.schema().field(1).clone();
    let mp_line_type = mp_line_field
        .try_extension_type::<LineStringType>()
        .unwrap();
    assert_eq!(mp_line_type.dimension(), Dimension::XYM);
    let mp_line =
        LineStringArray::try_from((output.column(1).as_ref(), mp_line_field.as_ref())).unwrap();
    assert_eq!(mp_line.value(0).unwrap().num_coords(), 2);

    let holed_poly_field = output.schema().field(3).clone();
    assert!(crs_is(&holed_poly_field, "EPSG:4326"));
    let holed_poly =
        PolygonArray::try_from((output.column(3).as_ref(), holed_poly_field.as_ref())).unwrap();
    let holed_poly_value = holed_poly.value(0).unwrap();
    assert_eq!(holed_poly_value.num_interiors(), 1);
    assert_eq!(holed_poly_value.exterior().unwrap().num_coords(), 4);
    assert_eq!(holed_poly_value.interiors().next().unwrap().num_coords(), 4);
    assert!(holed_poly.logical_nulls().unwrap().is_null(1));

    let polygon_field = output.schema().field(4).clone();
    let polygon_type = polygon_field
        .try_extension_type::<geoarrow_schema::PolygonType>()
        .unwrap();
    assert_eq!(polygon_type.dimension(), Dimension::XYZM);
    assert!(crs_is(&polygon_field, "EPSG:3857"));

    let env_field = output.schema().field(5).clone();
    assert!(crs_is(&env_field, "EPSG:4326"));
    let env = PolygonArray::try_from((output.column(5).as_ref(), env_field.as_ref())).unwrap();
    let env_ring = env.value(0).unwrap().exterior().unwrap();
    assert_eq!(env_ring.num_coords(), 5);
    assert!(env_ring.coord(0).unwrap().x_y() == env_ring.coord(4).unwrap().x_y());

    let tile_field = output.schema().field(6).clone();
    assert!(crs_is(&tile_field, "EPSG:3857"));
    let tile = PolygonArray::try_from((output.column(6).as_ref(), tile_field.as_ref())).unwrap();
    let tile_ring = tile.value(0).unwrap().exterior().unwrap();
    assert_eq!(tile_ring.num_coords(), 5);
    assert!(relative_eq!(
        tile_ring.coord(0).unwrap().x(),
        -10_018_754.171_394_622
    ));
    assert!(relative_eq!(tile_ring.coord(0).unwrap().y(), 0.0));

    let array_line_field = output.schema().field(7).clone();
    let array_line =
        LineStringArray::try_from((output.column(7).as_ref(), array_line_field.as_ref())).unwrap();
    let array_line_value = array_line.value(0).unwrap();
    assert_eq!(array_line_value.num_coords(), 2);
    assert_eq!(array_line_value.coord(0).unwrap().x_y(), (0.0, 0.0));
    assert_eq!(array_line_value.coord(1).unwrap().x_y(), (1.0, 1.0));
    assert!(array_line.logical_nulls().unwrap().is_null(1));

    assert!(crs_is_unknown(output.schema().field(8)));
}

#[tokio::test]
async fn public_sql_generic_inputs_and_controlled_errors() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let batch = run_sql(
        &ctx,
        "SELECT ST_AsText(ST_LineFromMultiPoint(ST_GeomFromText('MULTIPOINT Z (1 2 3, 4 5 6)')))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "LINESTRING Z(1 2 3,4 5 6)"
    );
    let deduplicated = run_sql(
        &ctx,
        "SELECT ST_AsText(ST_MakeLine( \
            ST_GeomFromText('LINESTRING(0 0,1 1)'), \
            ST_GeomFromText('LINESTRING(1 1,2 2)')))",
    )
    .await;
    assert_eq!(
        deduplicated.column(0).as_string::<i32>().value(0),
        "LINESTRING(0 0,1 1,2 2)"
    );

    let repeated_points = run_sql(
        &ctx,
        "SELECT ST_AsText(ST_MakeLine( \
            ST_GeomFromText('POINT(1 1)'), \
            ST_GeomFromText('POINT(1 1)')))",
    )
    .await;
    assert_eq!(
        repeated_points.column(0).as_string::<i32>().value(0),
        "LINESTRING(1 1,1 1)"
    );

    let empty = run_sql(
        &ctx,
        "SELECT ST_AsText(ST_LineFromMultiPoint(ST_GeomFromText('MULTIPOINT Z EMPTY')))",
    )
    .await;
    assert_eq!(
        empty.column(0).as_string::<i32>().value(0),
        "LINESTRING Z EMPTY"
    );

    let null_envelope = run_sql(
        &ctx,
        "SELECT ST_AsText(ST_MakeEnvelope(CAST(NULL AS DOUBLE), 0, 1, 1))",
    )
    .await;
    assert!(null_envelope.column(0).is_null(0));
    let null_envelope_srid = run_sql(
        &ctx,
        "SELECT ST_AsText(ST_MakeEnvelope(0, 0, 1, 1, CAST(NULL AS BIGINT)))",
    )
    .await;
    assert!(null_envelope_srid.column(0).is_null(0));
    let null_polygon = run_sql(
        &ctx,
        "SELECT ST_AsText(ST_Polygon( \
            ST_GeomFromText('LINESTRING(0 0,0 1,1 1,0 0)'), \
            CAST(NULL AS BIGINT)))",
    )
    .await;
    assert!(null_polygon.column(0).is_null(0));

    let unclosed = sql_error(
        &ctx,
        "SELECT ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0,0 1,1 1,1 0)'))",
    )
    .await;
    assert!(unclosed.contains("must be closed"), "{unclosed}");

    let mixed = sql_error(
        &ctx,
        "SELECT ST_MakeLine(ST_GeomFromText('LINESTRING(0 0,1 1)'), \
         ST_GeomFromText('LINESTRING Z (1 1 2,2 2 3)'))",
    )
    .await;
    assert!(mixed.contains("mixed-dimensional"), "{mixed}");

    let invalid_type = sql_error(
        &ctx,
        "SELECT ST_MakeLine(ST_GeomFromText('POINT(0 0)'), \
         ST_GeomFromText('POLYGON((0 0,0 1,1 1,0 0))'))",
    )
    .await;
    assert!(
        invalid_type.contains("Point or LineString"),
        "message should describe scalar inputs: {invalid_type}"
    );

    let scalar_multipoint = sql_error(
        &ctx,
        "SELECT ST_MakeLine(ST_GeomFromText('MULTIPOINT((0 0),(1 1))'), \
         ST_GeomFromText('POINT(2 2)'))",
    )
    .await;
    assert!(
        scalar_multipoint.contains("Point or LineString"),
        "{scalar_multipoint}"
    );

    let invalid_tile = sql_error(&ctx, "SELECT ST_TileEnvelope(2, 4, 0)").await;
    assert!(
        invalid_tile.contains("invalid tile x value"),
        "{invalid_tile}"
    );

    let invalid_margin = sql_error(&ctx, "SELECT ST_TileEnvelope(0, 0, 0, -0.51)").await;
    assert!(invalid_margin.contains("margin"), "{invalid_margin}");

    let invalid_zoom = sql_error(&ctx, "SELECT ST_TileEnvelope(32, 0, 0)").await;
    assert!(
        invalid_zoom.contains("invalid tile zoom value"),
        "{invalid_zoom}"
    );
    let invalid_y = sql_error(&ctx, "SELECT ST_TileEnvelope(0, 0, 1)").await;
    assert!(invalid_y.contains("invalid tile y value"), "{invalid_y}");
    let nonfinite_margin = sql_error(
        &ctx,
        "SELECT ST_TileEnvelope(0, 0, 0, CAST('NaN' AS DOUBLE))",
    )
    .await;
    assert!(
        nonfinite_margin.contains("margin must be finite"),
        "{nonfinite_margin}"
    );
}

#[tokio::test]
async fn point_srid_contract_and_null_literals() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let nulls = run_sql(
        &ctx,
        "SELECT \
            ST_Point(1, 2, CAST(NULL AS BIGINT)) IS NULL AS point_null, \
            ST_PointZ(1, 2, 3, CAST(NULL AS BIGINT)) IS NULL AS pointz_null, \
            ST_PointM(1, 2, 4, CAST(NULL AS BIGINT)) IS NULL AS pointm_null, \
            ST_PointZM(1, 2, 3, 4, CAST(NULL AS BIGINT)) IS NULL AS pointzm_null",
    )
    .await;
    assert!(nulls.column(0).as_boolean().value(0));
    assert!(nulls.column(1).as_boolean().value(0));
    assert!(nulls.column(2).as_boolean().value(0));
    assert!(nulls.column(3).as_boolean().value(0));

    let srids = run_sql(
        &ctx,
        "SELECT \
            ST_Point(1, 2, -1) AS point_negative, \
            ST_Point(1, 2, 0) AS point_zero, \
            ST_Point(1, 2, 1000000) AS point_clamped, \
            ST_PointZ(1, 2, 3, -1) AS pointz_negative, \
            ST_PointM(1, 2, 4, -1) AS pointm_negative, \
            ST_PointZM(1, 2, 3, 4, -1) AS pointzm_negative",
    )
    .await;
    let schema = srids.schema();
    assert!(crs_is_unknown(schema.field(0)));
    assert!(crs_is_unknown(schema.field(1)));
    assert!(crs_is(schema.field(2), "EPSG:999001"));
    assert!(crs_is_unknown(schema.field(3)));
    assert!(crs_is_unknown(schema.field(4)));
    assert!(crs_is_unknown(schema.field(5)));
}

#[tokio::test]
async fn tile_envelope_properties_for_small_zoom_levels() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    for zoom in 0..4 {
        let tiles = 1usize << zoom;
        for x in 0..tiles {
            for y in 0..tiles {
                let batch =
                    run_sql(&ctx, &format!("SELECT ST_TileEnvelope({zoom}, {x}, {y})")).await;
                let schema = batch.schema();
                let field = schema.field(0);
                let array = PolygonArray::try_from((batch.column(0).as_ref(), field)).unwrap();
                let ring = array.value(0).unwrap().exterior().unwrap();
                assert_eq!(ring.num_coords(), 5);
                assert_eq!(ring.coord(0).unwrap().x_y(), ring.coord(4).unwrap().x_y());
                assert!(ring.coord(0).unwrap().x() <= ring.coord(2).unwrap().x());
                assert!(ring.coord(0).unwrap().y() <= ring.coord(2).unwrap().y());
            }
        }
    }

    let expanded = run_sql(&ctx, "SELECT ST_TileEnvelope(1, 0, 0, 0.1)").await;
    let expanded_field = expanded.schema().field(0).clone();
    let expanded_array =
        PolygonArray::try_from((expanded.column(0).as_ref(), expanded_field.as_ref())).unwrap();
    let expanded_ring = expanded_array.value(0).unwrap().exterior().unwrap();
    assert!(expanded_ring.coord(0).unwrap().y() < 0.0);
}

#[tokio::test]
async fn public_sql_null_geometry_inputs_and_nullable_outputs() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let literals = run_sql(
        &ctx,
        "SELECT \
            ST_MakePolygon(NULL) IS NULL, \
            ST_LineFromMultiPoint(NULL) IS NULL, \
            ST_MakeLine(NULL, NULL) IS NULL, \
            ST_Polygon(NULL, 0) IS NULL, \
            ST_MakePolygon( \
                ST_GeomFromText('LINESTRING(0 0,0 1,1 1,0 0)'), NULL \
            ) IS NULL",
    )
    .await;
    assert!((0..literals.num_columns()).all(|index| literals.column(index).as_boolean().value(0)));

    let shell = WktLineString::new(
        vec![
            coord(0.0, 0.0, None, None),
            coord(0.0, 1.0, None, None),
            coord(1.0, 1.0, None, None),
            coord(0.0, 0.0, None, None),
        ],
        WktDimension::XY,
    );
    let mut shell_builder = LineStringBuilder::new(
        LineStringType::new(Dimension::XY, metadata(0)).with_coord_type(CoordType::Separated),
    );
    shell_builder.push_line_string(Some(&shell)).unwrap();
    shell_builder
        .push_line_string(Option::<&WktLineString<f64>>::None)
        .unwrap();
    let shells = shell_builder.finish();

    let known = point_array(
        Dimension::XY,
        4326,
        [
            WktPoint::new(Some(coord(1.0, 1.0, None, None)), WktDimension::XY),
            WktPoint::new(Some(coord(1.0, 1.0, None, None)), WktDimension::XY),
        ],
    );
    let unknown = point_array(
        Dimension::XY,
        0,
        [
            WktPoint::new(Some(coord(1.0, 1.0, None, None)), WktDimension::XY),
            WktPoint::new(Some(coord(1.0, 1.0, None, None)), WktDimension::XY),
        ],
    );

    let element_type =
        LineStringType::new(Dimension::XY, metadata(0)).with_coord_type(CoordType::Separated);
    let empty_lines = LineStringBuilder::new(element_type.clone()).finish();
    let element_field = Arc::new(element_type.to_field("item", true));
    let null_lines = ListArray::new(
        element_field,
        OffsetBuffer::<i32>::from_lengths([0, 0]),
        empty_lines.into_array_ref(),
        Some(NullBuffer::from(vec![false, false])),
    );

    let schema = Schema::new(vec![
        shells.extension_type().to_field("shell", true),
        known.extension_type().to_field("known", true),
        unknown.extension_type().to_field("unknown", true),
        Field::new("lines", null_lines.data_type().clone(), true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            shells.into_array_ref(),
            known.into_array_ref(),
            unknown.into_array_ref(),
            Arc::new(null_lines),
        ],
    )
    .unwrap();
    ctx.register_batch("null_inputs", batch).unwrap();

    let output = run_sql(
        &ctx,
        "SELECT \
            ST_MakePolygon(shell) AS polygon, \
            ST_MakeLine(NULL, known) AS line_with_literal_null, \
            ST_MakeLine(lines) AS line_from_null_array, \
            ST_MakePolygon(shell, lines) AS polygon_with_null_holes, \
            ST_Polygon(shell, 3857) AS polygon_with_srid \
         FROM null_inputs",
    )
    .await;

    let polygon_field = output.schema().field(0).clone();
    assert!(polygon_field.is_nullable());
    let polygons =
        PolygonArray::try_from((output.column(0).as_ref(), polygon_field.as_ref())).unwrap();
    assert!(polygons.value(0).is_ok());
    assert!(polygons.logical_nulls().unwrap().is_null(1));

    let srid_field = output.schema().field(4).clone();
    assert!(srid_field.is_nullable());
    let srid_polygons =
        PolygonArray::try_from((output.column(4).as_ref(), srid_field.as_ref())).unwrap();
    assert!(srid_polygons.value(0).is_ok());
    assert!(srid_polygons.logical_nulls().unwrap().is_null(1));

    let output_schema = output.schema();
    for column in 1..4 {
        let field = output_schema.field(column);
        assert!(field.is_nullable());
        let array = from_arrow_array(output.column(column).as_ref(), field).unwrap();
        let nulls = array.logical_nulls().unwrap();
        assert!(nulls.is_null(0));
        assert!(nulls.is_null(1));
    }

    let error = sql_error(&ctx, "SELECT ST_MakeLine(unknown, known) FROM null_inputs").await;
    assert!(error.contains("mixed SRID"), "{error}");
}
