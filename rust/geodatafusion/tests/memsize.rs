//! ST_MemSize Arrow row-buffer contract tests.
//!
//! The tests build native GeoArrow arrays, assert the buffer layout they
//! actually have, and derive the expected size from those buffers. They do not
//! re-run the implementation formula.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, BinaryViewBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{
    GeometryBuilder, GeometryCollectionBuilder, LineStringBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, GeometryCollectionType, GeometryType, LineStringType,
    Metadata, PointType, PolygonType, WkbType,
};
use wkt::{Wkt, types as wt};

fn new_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

fn register_geometry(ctx: &SessionContext, name: &str, array: impl GeoArrowArray) {
    let field = array.data_type().to_field("geometry", true);
    register_batch(ctx, name, field, array.to_array_ref());
}

fn register_batch(ctx: &SessionContext, name: &str, field: Field, array: ArrayRef) {
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new([Arc::new(field)])), vec![array]).unwrap();
    ctx.register_batch(name, batch).unwrap();
}

async fn mem_sizes(ctx: &SessionContext, table: &str) -> Vec<Option<i64>> {
    let batch = ctx
        .sql(&format!("SELECT ST_MemSize(geometry) FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let sizes = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    (0..sizes.len())
        .map(|row| (!sizes.is_null(row)).then(|| sizes.value(row)))
        .collect()
}

fn coord(x: f64, y: f64) -> wt::Coord {
    wt::Coord {
        x,
        y,
        z: None,
        m: None,
    }
}

fn line(coords: Vec<(f64, f64)>) -> wt::LineString {
    wt::LineString::new(
        coords.into_iter().map(|(x, y)| coord(x, y)).collect(),
        wt::Dimension::XY,
    )
}

#[tokio::test]
async fn point_rows_use_fixed_coordinate_slots() {
    let ctx = new_ctx();
    let point_type =
        PointType::new(Dimension::XY, Default::default()).with_coord_type(CoordType::Interleaved);
    let mut points = PointBuilder::with_capacity(point_type, 4);
    points.push_coord(Some(&coord(1., 2.)));
    points.push_empty();
    points.push_null();
    points.push_coord(Some(&coord(3., 4.)));
    let points = points.finish();

    // Structural evidence: one XY point is exactly two f64 slots, and even the
    // EMPTY row occupies those fixed slots. The null row returns NULL.
    let data = points.to_array_ref();
    let fixed = data.as_fixed_size_list();
    assert_eq!(fixed.value_length(), 2);
    let coordinates = fixed.values().as_primitive::<Float64Type>();
    assert_eq!(coordinates.len(), 4 * 2);
    assert!(coordinates.value(2).is_nan());
    assert_eq!(2 * std::mem::size_of::<f64>() as i64, 16);

    register_geometry(&ctx, "points", points);
    assert_eq!(
        mem_sizes(&ctx, "points").await,
        vec![Some(16), Some(16), None, Some(16)]
    );
}

#[tokio::test]
async fn linestring_rows_count_offsets_and_coordinates() {
    let ctx = new_ctx();
    let line_type = LineStringType::new(Dimension::XY, Default::default())
        .with_coord_type(CoordType::Interleaved);
    let lines = LineStringBuilder::from_nullable_line_strings(
        &[
            Some(line(vec![(0., 0.), (1., 1.)])),
            Some(line(vec![(0., 0.), (1., 0.), (1., 1.), (0., 0.)])),
            Some(line(vec![])),
            None,
        ],
        line_type,
    )
    .finish();

    // Structural evidence: the list offsets decide how many coordinates each
    // row references; an EMPTY row still references its two offset entries.
    let data = lines.to_array_ref();
    let offsets = data.as_list::<i32>().value_offsets();
    assert_eq!(offsets, &[0, 2, 6, 6, 6]);
    let expected = (0..4)
        .map(|row| {
            (row != 3).then(|| {
                (2 * std::mem::size_of::<i32>() as i64)
                    + i64::from(offsets[row + 1] - offsets[row])
                        * 2
                        * std::mem::size_of::<f64>() as i64
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(expected, vec![Some(40), Some(72), Some(8), None]);

    register_geometry(&ctx, "lines", lines.clone());
    assert_eq!(mem_sizes(&ctx, "lines").await, expected);

    // A slice changes the visible offset range without changing per-row bytes.
    let sliced = lines.slice(1, 2);
    let sliced_offsets = sliced
        .to_array_ref()
        .as_list::<i32>()
        .value_offsets()
        .to_vec();
    // List slices keep absolute child offsets; only the visible range changes.
    assert_eq!(sliced_offsets, vec![2, 6, 6]);
    register_geometry(&ctx, "sliced_lines", sliced);
    assert_eq!(
        mem_sizes(&ctx, "sliced_lines").await,
        vec![Some(72), Some(8)]
    );
}

#[tokio::test]
async fn polygon_rows_count_nested_offsets() {
    let ctx = new_ctx();
    let polygon_type =
        PolygonType::new(Dimension::XY, Default::default()).with_coord_type(CoordType::Interleaved);
    let polygon = wt::Polygon::new(
        vec![
            line(vec![(0., 0.), (4., 0.), (4., 4.), (0., 4.), (0., 0.)]),
            line(vec![(1., 1.), (2., 1.), (2., 2.), (1., 2.), (1., 1.)]),
        ],
        wt::Dimension::XY,
    );
    let polygons = PolygonBuilder::from_polygons(
        &[polygon, wt::Polygon::new(vec![], wt::Dimension::XY)],
        polygon_type,
    )
    .finish();

    // Structural evidence: the outer list references polygons, and the inner
    // list references ring boundaries.
    let data = polygons.to_array_ref();
    let outer_offsets = data.as_list::<i32>().value_offsets();
    assert_eq!(outer_offsets, &[0, 2, 2]);
    let rings = data.as_list::<i32>().values().as_list::<i32>();
    let ring_offsets = rings.value_offsets();
    assert_eq!(ring_offsets, &[0, 5, 10]);
    let expected = vec![
        Some(
            (2 * std::mem::size_of::<i32>() as i64)
                + i64::try_from(ring_offsets.len()).unwrap() * std::mem::size_of::<i32>() as i64
                + i64::from(ring_offsets[2]) * 2 * std::mem::size_of::<f64>() as i64,
        ),
        Some((2 * std::mem::size_of::<i32>() as i64) + std::mem::size_of::<i32>() as i64),
    ];
    assert_eq!(expected, vec![Some(180), Some(12)]);

    register_geometry(&ctx, "polygons", polygons);
    assert_eq!(mem_sizes(&ctx, "polygons").await, expected);
}

#[tokio::test]
async fn union_rows_count_type_ids_offsets_and_children() {
    let ctx = new_ctx();
    let geometry_type =
        GeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
    let geometries = GeometryBuilder::from_nullable_geometries(
        &[
            Some(Wkt::Point(wt::Point::from_coord(coord(1., 2.)))),
            Some(Wkt::LineString(line(vec![(0., 0.), (1., 1.)]))),
            None,
        ],
        geometry_type,
    )
    .unwrap()
    .finish();

    // Structural evidence: a dense union stores one i8 type id and one i32
    // offset per row; the referenced point child stores two f64 slots.
    let data = geometries.to_array_ref();
    let union = data.as_union();
    assert_eq!(union.type_ids().len(), 3);
    assert_eq!(union.offsets().unwrap().len(), 3);
    assert_eq!(union.offsets().unwrap()[0], 0);
    let point_child = union.child(union.type_id(0)).as_fixed_size_list();
    assert_eq!(point_child.value_length(), 2);
    assert_eq!(
        point_child.value(0).len(),
        2,
        "the referenced point child must store two coordinates"
    );
    let linestring_offsets = union
        .child(union.type_id(1))
        .as_list::<i32>()
        .value_offsets()
        .to_vec();
    assert_eq!(linestring_offsets, vec![0, 2]);
    let expected = vec![
        Some(1 + 4 + 2 * std::mem::size_of::<f64>() as i64),
        Some(
            1 + 4
                + 2 * std::mem::size_of::<i32>() as i64
                + 2 * 2 * std::mem::size_of::<f64>() as i64,
        ),
        None,
    ];
    assert_eq!(expected, vec![Some(21), Some(45), None]);

    register_geometry(&ctx, "geometries", geometries);
    assert_eq!(mem_sizes(&ctx, "geometries").await, expected);
}

#[tokio::test]
async fn geometry_collection_rows_count_nested_union_bytes() {
    let ctx = new_ctx();
    let collection_type = GeometryCollectionType::new(Dimension::XY, Default::default())
        .with_coord_type(CoordType::Interleaved);
    let collections = GeometryCollectionBuilder::from_geometry_collections(
        &[
            wt::GeometryCollection::new(
                vec![Wkt::Point(wt::Point::from_coord(coord(1., 2.)))],
                wt::Dimension::XY,
            ),
            wt::GeometryCollection::new(
                vec![Wkt::LineString(line(vec![(0., 0.), (1., 1.)]))],
                wt::Dimension::XY,
            ),
            wt::GeometryCollection::new(vec![], wt::Dimension::XY),
        ],
        collection_type,
    )
    .unwrap()
    .finish();

    // Structural evidence: the collection list references child geometry
    // union rows, each of which stores a type id and a dense offset.
    let data = collections.to_array_ref();
    let outer_offsets = data.as_list::<i32>().value_offsets();
    assert_eq!(outer_offsets, &[0, 1, 2, 2]);
    let children = data.as_list::<i32>().values().as_union();
    assert_eq!(children.type_ids().len(), 2);
    assert_eq!(children.offsets().unwrap().len(), 2);
    let point_child = children.child(children.type_id(0)).as_fixed_size_list();
    assert_eq!(point_child.value_length(), 2);
    let line_child = children.child(children.type_id(1)).as_list::<i32>();
    assert_eq!(line_child.value_offsets(), &[0, 2]);

    let expected = vec![
        Some(2 * std::mem::size_of::<i32>() as i64 + 1 + 4 + 2 * std::mem::size_of::<f64>() as i64),
        Some(
            2 * std::mem::size_of::<i32>() as i64
                + 1
                + 4
                + 2 * std::mem::size_of::<i32>() as i64
                + 2 * 2 * std::mem::size_of::<f64>() as i64,
        ),
        Some(2 * std::mem::size_of::<i32>() as i64),
    ];
    assert_eq!(expected, vec![Some(29), Some(53), Some(8)]);

    register_geometry(&ctx, "collections", collections);
    assert_eq!(mem_sizes(&ctx, "collections").await, expected);
}

#[tokio::test]
async fn nullable_wkb_rows_exclude_the_validity_bitmap() {
    let ctx = new_ctx();
    let point = hex::decode("0101000000000000000000f03f0000000000000040").unwrap();
    let nested = hex::decode(
        "0107000000020000000107000000010000000101000000000000000000f03f0000000000000040\
         010100000000000000000008400000000000001040",
    )
    .unwrap();

    let mut builder = BinaryBuilder::new();
    builder.append_value(&nested);
    builder.append_null();
    builder.append_value(&point);
    let array = builder.finish();

    // The bitmap is present, but valid rows must not be charged for it.
    assert!(array.nulls().is_some());
    let expected = vec![
        Some(2 * 4 + array.value(0).len() as i64),
        None,
        Some(2 * 4 + array.value(2).len() as i64),
    ];
    assert_eq!(expected, vec![Some(68), None, Some(29)]);

    let field =
        GeoArrowType::Wkb(WkbType::new(Arc::new(Metadata::default()))).to_field("geometry", true);
    register_batch(&ctx, "nullable_wkb", field.clone(), Arc::new(array.clone()));
    assert_eq!(mem_sizes(&ctx, "nullable_wkb").await, expected);

    // Slicing keeps the bitmap and the payload bytes, but not extra overhead.
    let sliced = array.slice(1, 2);
    register_batch(&ctx, "sliced_wkb", field, Arc::new(sliced));
    assert_eq!(mem_sizes(&ctx, "sliced_wkb").await, vec![None, Some(29)]);
}

#[tokio::test]
async fn binary_view_rows_count_external_payloads() {
    let ctx = new_ctx();
    let point = hex::decode("0101000000000000000000f03f0000000000000040").unwrap();
    let nested = hex::decode(
        "0107000000020000000107000000010000000101000000000000000000f03f0000000000000040\
         010100000000000000000008400000000000001040",
    )
    .unwrap();
    assert!(point.len() > 12 && nested.len() > 12);

    let mut builder = BinaryViewBuilder::new();
    builder.append_value(&point);
    builder.append_null();
    builder.append_value(&nested);
    let array = builder.finish();

    // Structural evidence: both values are external views referencing their
    // own payload ranges, so each row counts its 16-byte view plus its payload.
    assert_eq!(array.views()[0] as u32 as usize, point.len());
    assert_eq!(array.views()[2] as u32 as usize, nested.len());
    assert!(!array.data_buffers().is_empty());
    let expected = vec![
        Some(16 + point.len() as i64),
        None,
        Some(16 + nested.len() as i64),
    ];
    assert_eq!(expected, vec![Some(37), None, Some(76)]);

    let field = GeoArrowType::WkbView(WkbType::new(Arc::new(Metadata::default())))
        .to_field("geometry", true);
    register_batch(&ctx, "wkb_view", field.clone(), Arc::new(array.clone()));
    assert_eq!(mem_sizes(&ctx, "wkb_view").await, expected);

    let sliced = array.slice(1, 2);
    register_batch(&ctx, "sliced_wkb_view", field, Arc::new(sliced));
    assert_eq!(
        mem_sizes(&ctx, "sliced_wkb_view").await,
        vec![None, Some(76)]
    );
}
