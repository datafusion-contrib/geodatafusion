use arrow_array::Array;
use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_schema::Field;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::{GeometryArray, WkbArray, WktArray};
use geoarrow_array::builder::{GeometryBuilder, GeometryCollectionBuilder, WkbBuilder};
use geoarrow_array::cast::{AsGeoArrowArray, to_wkb, to_wkt};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::type_id::GeometryTypeId;
use geoarrow_schema::{Dimension, GeometryCollectionType, GeometryType};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

pub(crate) const GEOMETRY_SUBTYPE_META_KEY: &str = "geodatafusion:geometry_subtype";
pub(crate) const POLYHEDRAL_SURFACE_SUBTYPE: &str = "polyhedralsurface";

const NESTED_GC_SENTINEL_X_BITS: u64 = 0x7ff8_4744_465f_4743;
const NESTED_GC_SENTINEL_Y_PREFIX: u64 = 0x4330_0000_0000_0000;
const NESTED_GC_SENTINEL_Y_MASK: u64 = 0xffff_ffff_0000_0000;

/// Builds a generic GeoArrow geometry array while preserving top-level and nested
/// `GeometryCollection` structure.
///
/// GeoArrow 0.9's `GeometryBuilder` canonicalizes a one-member
/// top-level `GeometryCollection` to its child geometry, and its `MixedGeometryBuilder`
/// rejects multi-member nested `GeometryCollection`s. This builder routes top-level
/// collections through a dedicated child builder and pre-order encodes nested
/// `GeometryCollection` nodes using sentinel points so collection hierarchy survives.
#[derive(Debug)]
pub(crate) struct GeometryArrayBuilder {
    inner: GeometryBuilder,
    collections: [GeometryCollectionBuilder; 4],
    collection_lengths: [usize; 4],
    collection_rows: Vec<(usize, usize, i32)>,
    len: usize,
}

impl GeometryArrayBuilder {
    pub(crate) fn new(typ: GeometryType) -> Self {
        let coord_type = typ.coord_type();
        let metadata = typ.metadata().clone();
        let collections = std::array::from_fn(|index| {
            GeometryCollectionBuilder::new(
                GeometryCollectionType::new(dimension_from_index(index), Default::default())
                    .with_coord_type(coord_type),
            )
        });

        Self {
            inner: GeometryBuilder::new(GeometryType::new(metadata).with_coord_type(coord_type)),
            collections,
            collection_lengths: [0; 4],
            collection_rows: Vec::new(),
            len: 0,
        }
    }

    pub(crate) fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        let Some(geometry) = geometry else {
            self.inner
                .push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            self.len += 1;
            return Ok(());
        };

        let geo_traits::GeometryType::GeometryCollection(collection) = geometry.as_type() else {
            self.inner.push_geometry(Some(geometry))?;
            self.len += 1;
            return Ok(());
        };

        let dimension_index = dimension_index(collection.dim())?;
        let collection_index = self.collection_lengths[dimension_index];

        // Keep the regular union row count aligned, then retarget this row to the collection
        // child in finish().
        self.inner
            .push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
        push_geometry_collection_preserving_nesting(
            &mut self.collections[dimension_index],
            Some(collection),
        )?;
        self.collection_lengths[dimension_index] += 1;
        self.collection_rows.push((
            self.len,
            dimension_index,
            i32::try_from(collection_index).expect("geometry collection count fits in i32"),
        ));
        self.len += 1;
        Ok(())
    }

    pub(crate) fn finish(self) -> GeometryArray {
        let inner = self.inner.finish();
        let mut type_ids = inner.type_ids().to_vec();
        let mut offsets = inner.offsets().to_vec();

        for (row, dimension_index, collection_index) in self.collection_rows {
            type_ids[row] = self.collections[dimension_index].geometry_type_id();
            offsets[row] = collection_index;
        }

        let dimensions = [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ];
        GeometryArray::new(
            type_ids.into(),
            offsets.into(),
            dimensions.map(|dim| inner.point_child(dim)),
            dimensions.map(|dim| inner.line_string_child(dim)),
            dimensions.map(|dim| inner.polygon_child(dim)),
            dimensions.map(|dim| inner.multi_point_child(dim)),
            dimensions.map(|dim| inner.multi_line_string_child(dim)),
            dimensions.map(|dim| inner.multi_polygon_child(dim)),
            self.collections.map(GeometryCollectionBuilder::finish),
            inner.data_type().metadata().clone(),
        )
    }
}

pub(crate) fn is_polyhedral_surface_field(field: &Field) -> bool {
    field
        .metadata()
        .get(GEOMETRY_SUBTYPE_META_KEY)
        .is_some_and(|v| v == POLYHEDRAL_SURFACE_SUBTYPE)
}

pub(crate) fn with_polyhedral_surface_metadata(mut field: Field) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        GEOMETRY_SUBTYPE_META_KEY.to_string(),
        POLYHEDRAL_SURFACE_SUBTYPE.to_string(),
    );
    field.set_metadata(metadata);
    field
}

pub(crate) fn rewrite_multipolygon_wkt_as_polyhedral_surface(wkt: &str) -> String {
    if let Some(rest) = wkt.strip_prefix("MULTIPOLYGON") {
        format!("POLYHEDRALSURFACE{rest}")
    } else {
        wkt.to_string()
    }
}

pub(crate) fn rewrite_multipolygon_wkb_as_polyhedral_surface(bytes: &mut [u8]) {
    if bytes.len() < 5 {
        return;
    }
    match bytes[0] {
        0 => {
            let raw = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
            if (raw & 0x1fff_ffff) % 1000 == 6 {
                bytes[1..5].copy_from_slice(&(raw + 9).to_be_bytes());
            }
        }
        1 => {
            let raw = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
            if (raw & 0x1fff_ffff) % 1000 == 6 {
                bytes[1..5].copy_from_slice(&(raw + 9).to_le_bytes());
            }
        }
        _ => {}
    }
}

pub(crate) fn push_geometry_collection_preserving_nesting(
    builder: &mut GeometryCollectionBuilder,
    collection: Option<&impl GeometryCollectionTrait<T = f64>>,
) -> GeoArrowResult<()> {
    let Some(collection) = collection else {
        return builder.push_geometry_collection(None::<&GeometryCollection<f64>>);
    };
    if has_nested_geometry_collection(collection) {
        let encoded = encode_nested_geometry_collection(collection)?;
        builder.push_geometry_collection(Some(&encoded))
    } else {
        builder.push_geometry_collection(Some(collection))
    }
}

fn has_nested_geometry_collection(collection: &impl GeometryCollectionTrait<T = f64>) -> bool {
    collection
        .geometries()
        .any(|g| matches!(g.as_type(), geo_traits::GeometryType::GeometryCollection(_)))
}

fn make_nested_gc_sentinel_point(dim: WktDimension, len: usize) -> Point<f64> {
    let x = f64::from_bits(NESTED_GC_SENTINEL_X_BITS);
    let y = f64::from_bits(NESTED_GC_SENTINEL_Y_PREFIX | (len as u64));
    let (z, m) = match dim {
        WktDimension::XY => (None, None),
        WktDimension::XYZ => (Some(0.0), None),
        WktDimension::XYM => (None, Some(0.0)),
        WktDimension::XYZM => (Some(0.0), Some(0.0)),
    };
    Point::from_coord(Coord { x, y, z, m })
}

pub(crate) fn is_nested_gc_sentinel_point(point: &impl PointTrait<T = f64>) -> Option<usize> {
    let coord = point.coord()?;
    if coord.x().to_bits() != NESTED_GC_SENTINEL_X_BITS {
        return None;
    }
    let y_bits = coord.y().to_bits();
    if (y_bits & NESTED_GC_SENTINEL_Y_MASK) != NESTED_GC_SENTINEL_Y_PREFIX {
        return None;
    }
    Some((y_bits & 0xffff_ffff) as usize)
}

fn encode_nested_geometry_collection(
    collection: &impl GeometryCollectionTrait<T = f64>,
) -> GeoArrowResult<GeometryCollection<f64>> {
    let dim = wkt_dimension_from_dimensions(collection.dim())?;
    let mut flat_items = Vec::new();
    if let Some(decoded) = decode_geometry_collection(collection) {
        for item in decoded.geometries() {
            append_encoded_member(item, dim, &mut flat_items)?;
        }
    } else {
        for item in collection.geometries() {
            append_encoded_member(&item, dim, &mut flat_items)?;
        }
    }
    Ok(GeometryCollection::new(flat_items, dim))
}

fn append_encoded_member(
    geom: &impl GeometryTrait<T = f64>,
    dim: WktDimension,
    out: &mut Vec<Wkt<f64>>,
) -> GeoArrowResult<()> {
    if let geo_traits::GeometryType::GeometryCollection(inner) = geom.as_type() {
        if let Some(decoded_inner) = decode_geometry_collection(inner) {
            out.push(Wkt::Point(make_nested_gc_sentinel_point(
                dim,
                decoded_inner.num_geometries(),
            )));
            for child in decoded_inner.geometries() {
                append_encoded_member(child, dim, out)?;
            }
        } else {
            out.push(Wkt::Point(make_nested_gc_sentinel_point(
                dim,
                inner.num_geometries(),
            )));
            for child in inner.geometries() {
                append_encoded_member(&child, dim, out)?;
            }
        }
    } else {
        out.push(geometry_to_wkt(geom)?);
    }
    Ok(())
}

pub(crate) fn decode_geometry_collection(
    collection: &impl GeometryCollectionTrait<T = f64>,
) -> Option<GeometryCollection<f64>> {
    let has_sentinel_or_nested = collection.geometries().any(|g| match g.as_type() {
        geo_traits::GeometryType::Point(pt) => is_nested_gc_sentinel_point(pt).is_some(),
        geo_traits::GeometryType::GeometryCollection(_) => true,
        _ => false,
    });
    if !has_sentinel_or_nested {
        return None;
    }

    let dim = wkt_dimension_from_dimensions(collection.dim()).ok()?;
    let mut cursor = 0;
    let mut top_children = Vec::new();
    while cursor < collection.num_geometries() {
        top_children.push(decode_next_geometry(collection, &mut cursor, dim)?);
    }
    Some(GeometryCollection::new(top_children, dim))
}

fn decode_next_geometry(
    collection: &impl GeometryCollectionTrait<T = f64>,
    cursor: &mut usize,
    dim: WktDimension,
) -> Option<Wkt<f64>> {
    if *cursor >= collection.num_geometries() {
        return None;
    }
    let item = collection.geometry(*cursor)?;
    *cursor += 1;
    match item.as_type() {
        geo_traits::GeometryType::Point(pt) => {
            if let Some(child_count) = is_nested_gc_sentinel_point(pt) {
                let mut children = Vec::with_capacity(child_count);
                for _ in 0..child_count {
                    children.push(decode_next_geometry(collection, cursor, dim)?);
                }
                return Some(Wkt::GeometryCollection(GeometryCollection::new(
                    children, dim,
                )));
            }
            geometry_to_wkt(&item).ok()
        }
        geo_traits::GeometryType::GeometryCollection(inner) => {
            if let Some(decoded) = decode_geometry_collection(inner) {
                Some(Wkt::GeometryCollection(decoded))
            } else {
                geometry_to_wkt(&item).ok()
            }
        }
        _ => geometry_to_wkt(&item).ok(),
    }
}

fn decoded_row_geometry_collection(
    geo_array: &dyn GeoArrowArray,
    row: usize,
) -> GeoArrowResult<Option<GeometryCollection<f64>>> {
    if let Some(gc_arr) = geo_array.as_geometry_collection_opt() {
        let Some(gc) = gc_arr.get(row)? else {
            return Ok(None);
        };
        return Ok(decode_geometry_collection(&gc));
    }
    if let Some(geom_arr) = geo_array.as_geometry_opt() {
        let Some(geom) = geom_arr.get(row)? else {
            return Ok(None);
        };
        if let geo_traits::GeometryType::GeometryCollection(gc) = geom.as_type() {
            return Ok(decode_geometry_collection(gc));
        }
    }
    Ok(None)
}

fn has_encoded_nested_collections(geo_array: &dyn GeoArrowArray) -> GeoArrowResult<bool> {
    if geo_array.as_geometry_collection_opt().is_none() && geo_array.as_geometry_opt().is_none() {
        return Ok(false);
    }
    for row in 0..geo_array.len() {
        if decoded_row_geometry_collection(geo_array, row)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn array_to_wkt(
    geo_array: &dyn GeoArrowArray,
    field: &Field,
) -> GeoArrowResult<WktArray> {
    let is_polyhedral = is_polyhedral_surface_field(field);
    let has_nested_gc = has_encoded_nested_collections(geo_array)?;
    let wkt_arr = to_wkt::<i32>(geo_array)?;
    if !is_polyhedral && !has_nested_gc {
        return Ok(wkt_arr);
    }

    let metadata = wkt_arr.data_type().metadata().clone();
    let strings = wkt_arr.into_arrow();
    let mut builder = StringBuilder::with_capacity(strings.len(), strings.values().len());
    for row in 0..strings.len() {
        if strings.is_null(row) {
            builder.append_null();
        } else if let Some(decoded_gc) = decoded_row_geometry_collection(geo_array, row)? {
            builder.append_value(Wkt::GeometryCollection(decoded_gc).to_string());
        } else {
            let value = strings.value(row);
            if is_polyhedral {
                builder.append_value(rewrite_multipolygon_wkt_as_polyhedral_surface(value));
            } else {
                builder.append_value(value);
            }
        }
    }
    Ok(WktArray::new(builder.finish(), metadata))
}

pub(crate) fn array_to_wkb(
    geo_array: &dyn GeoArrowArray,
    field: &Field,
) -> GeoArrowResult<WkbArray> {
    let is_polyhedral = is_polyhedral_surface_field(field);
    let has_nested_gc = has_encoded_nested_collections(geo_array)?;
    let wkb_arr = to_wkb::<i32>(geo_array)?;
    if !is_polyhedral && !has_nested_gc {
        return Ok(wkb_arr);
    }

    let metadata = wkb_arr.data_type().metadata().clone();
    let binaries = wkb_arr.into_arrow();
    let mut builder = BinaryBuilder::with_capacity(binaries.len(), binaries.values().len());
    for row in 0..binaries.len() {
        if binaries.is_null(row) {
            builder.append_null();
        } else if let Some(decoded_gc) = decoded_row_geometry_collection(geo_array, row)? {
            let mut single = WkbBuilder::<i32>::new(Default::default());
            single.push_geometry(Some(&Wkt::GeometryCollection(decoded_gc)))?;
            let single_arr = single.finish().into_arrow();
            builder.append_value(single_arr.value(0));
        } else {
            let value = binaries.value(row);
            if is_polyhedral {
                let mut buf = value.to_vec();
                rewrite_multipolygon_wkb_as_polyhedral_surface(&mut buf);
                builder.append_value(&buf);
            } else {
                builder.append_value(value);
            }
        }
    }
    Ok(WkbArray::new(builder.finish(), metadata))
}

fn coord_to_wkt(coord: &impl CoordTrait<T = f64>) -> Coord<f64> {
    let (z, m) = match coord.dim() {
        Dimensions::Xy => (None, None),
        Dimensions::Xyz => (coord.nth(2), None),
        Dimensions::Xym => (None, coord.nth(2)),
        Dimensions::Xyzm | Dimensions::Unknown(_) => (coord.nth(2), coord.nth(3)),
    };
    Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    }
}

fn point_to_wkt(point: &impl PointTrait<T = f64>, dim: WktDimension) -> Point<f64> {
    match point.coord() {
        Some(coord) => Point::from_coord(coord_to_wkt(&coord)),
        None => Point::empty(dim),
    }
}

fn line_string_to_wkt(ls: &impl LineStringTrait<T = f64>, dim: WktDimension) -> LineString<f64> {
    LineString::new(ls.coords().map(|c| coord_to_wkt(&c)).collect(), dim)
}

fn polygon_to_wkt(poly: &impl PolygonTrait<T = f64>, dim: WktDimension) -> Polygon<f64> {
    let mut rings =
        Vec::with_capacity(usize::from(poly.exterior().is_some()) + poly.num_interiors());
    if let Some(ext) = poly.exterior() {
        rings.push(line_string_to_wkt(&ext, dim));
    }
    for int in poly.interiors() {
        rings.push(line_string_to_wkt(&int, dim));
    }
    Polygon::new(rings, dim)
}

fn geometry_to_wkt(geom: &impl GeometryTrait<T = f64>) -> GeoArrowResult<Wkt<f64>> {
    let dim = wkt_dimension_from_dimensions(geom.dim())?;
    Ok(match geom.as_type() {
        geo_traits::GeometryType::Point(pt) => Wkt::Point(point_to_wkt(pt, dim)),
        geo_traits::GeometryType::LineString(ls) => Wkt::LineString(line_string_to_wkt(ls, dim)),
        geo_traits::GeometryType::Polygon(poly) => Wkt::Polygon(polygon_to_wkt(poly, dim)),
        geo_traits::GeometryType::MultiPoint(mp) => Wkt::MultiPoint(MultiPoint::new(
            mp.points().map(|p| point_to_wkt(&p, dim)).collect(),
            dim,
        )),
        geo_traits::GeometryType::MultiLineString(mls) => {
            Wkt::MultiLineString(MultiLineString::new(
                mls.line_strings()
                    .map(|ls| line_string_to_wkt(&ls, dim))
                    .collect(),
                dim,
            ))
        }
        geo_traits::GeometryType::MultiPolygon(mpoly) => Wkt::MultiPolygon(MultiPolygon::new(
            mpoly.polygons().map(|p| polygon_to_wkt(&p, dim)).collect(),
            dim,
        )),
        geo_traits::GeometryType::GeometryCollection(gc) => {
            if let Some(decoded) = decode_geometry_collection(gc) {
                Wkt::GeometryCollection(decoded)
            } else {
                let children = gc
                    .geometries()
                    .map(|g| geometry_to_wkt(&g))
                    .collect::<GeoArrowResult<Vec<_>>>()?;
                Wkt::GeometryCollection(GeometryCollection::new(children, dim))
            }
        }
        geo_traits::GeometryType::Rect(rect) => {
            let min = coord_to_wkt(&rect.min());
            let max = coord_to_wkt(&rect.max());
            let ring = LineString::new(
                vec![
                    Coord {
                        x: min.x,
                        y: min.y,
                        z: min.z,
                        m: min.m,
                    },
                    Coord {
                        x: min.x,
                        y: max.y,
                        z: min.z,
                        m: min.m,
                    },
                    Coord {
                        x: max.x,
                        y: max.y,
                        z: max.z,
                        m: max.m,
                    },
                    Coord {
                        x: max.x,
                        y: min.y,
                        z: min.z,
                        m: min.m,
                    },
                    Coord {
                        x: min.x,
                        y: min.y,
                        z: min.z,
                        m: min.m,
                    },
                ],
                dim,
            );
            Wkt::Polygon(Polygon::new(vec![ring], dim))
        }
        geo_traits::GeometryType::Line(line) => Wkt::LineString(LineString::new(
            vec![coord_to_wkt(&line.start()), coord_to_wkt(&line.end())],
            dim,
        )),
        geo_traits::GeometryType::Triangle(tri) => {
            let first = coord_to_wkt(&tri.first());
            let second = coord_to_wkt(&tri.second());
            let third = coord_to_wkt(&tri.third());
            Wkt::Polygon(Polygon::new(
                vec![LineString::new(vec![first, second, third, first], dim)],
                dim,
            ))
        }
    })
}

fn wkt_dimension_from_dimensions(dim: Dimensions) -> GeoArrowResult<WktDimension> {
    match dim {
        Dimensions::Xy => Ok(WktDimension::XY),
        Dimensions::Xyz => Ok(WktDimension::XYZ),
        Dimensions::Xym => Ok(WktDimension::XYM),
        Dimensions::Xyzm => Ok(WktDimension::XYZM),
        Dimensions::Unknown(_) => Err(GeoArrowError::InvalidGeoArrow(format!(
            "unsupported geometry dimensions: {dim:?}"
        ))),
    }
}

fn dimension_index(dim: Dimensions) -> GeoArrowResult<usize> {
    match dim {
        Dimensions::Xy => Ok(0),
        Dimensions::Xyz => Ok(1),
        Dimensions::Xym => Ok(2),
        Dimensions::Xyzm => Ok(3),
        Dimensions::Unknown(_) => Err(GeoArrowError::InvalidGeoArrow(format!(
            "unsupported geometry dimensions: {dim:?}"
        ))),
    }
}

fn dimension_from_index(index: usize) -> Dimension {
    match index {
        0 => Dimension::XY,
        1 => Dimension::XYZ,
        2 => Dimension::XYM,
        3 => Dimension::XYZM,
        _ => unreachable!("GeoArrow has four dimensions"),
    }
}
