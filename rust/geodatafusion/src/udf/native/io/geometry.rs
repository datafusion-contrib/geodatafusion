use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, RwLock};

use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{Array, GenericListArray, UnionArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::Field;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::{
    GeometryArray, GeometryCollectionArray, MultiPolygonArray, WkbArray, WktArray,
};
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

const SIDECAR_SWEEP_INTERVAL: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CollectionNode {
    Leaf(usize),
    Collection(Vec<CollectionNode>),
}

#[derive(Debug)]
struct SidecarEntry {
    geom_offsets: ScalarBuffer<i32>,
    geom_offsets_base: usize,
    geom_offsets_end: usize,
    mixed_type_ids_base: usize,
    marker_buf: Buffer,
    trees: BTreeMap<usize, Vec<CollectionNode>>,
}

#[derive(Debug, Default)]
struct NestedCollectionSidecar {
    entries: HashMap<usize, SidecarEntry>,
    inserts_since_sweep: usize,
}

static NESTED_COLLECTION_COUNT: AtomicUsize = AtomicUsize::new(0);
static NESTED_COLLECTION_SIDECAR: LazyLock<RwLock<NestedCollectionSidecar>> =
    LazyLock::new(|| RwLock::new(NestedCollectionSidecar::default()));

#[derive(Debug)]
struct PolyhedralSidecarEntry {
    geom_offsets: ScalarBuffer<i32>,
    geom_offsets_base: usize,
    geom_offsets_end: usize,
    poly_offsets_base: usize,
    marker_buf: Buffer,
    rows: BTreeSet<usize>,
}

#[derive(Debug, Default)]
struct PolyhedralSidecar {
    entries: HashMap<usize, PolyhedralSidecarEntry>,
    inserts_since_sweep: usize,
}

static POLYHEDRAL_COUNT: AtomicUsize = AtomicUsize::new(0);
static POLYHEDRAL_SIDECAR: LazyLock<RwLock<PolyhedralSidecar>> =
    LazyLock::new(|| RwLock::new(PolyhedralSidecar::default()));

fn extract_mpoly_list_identity(
    list: &GenericListArray<i32>,
) -> Option<(ScalarBuffer<i32>, ScalarBuffer<i32>, Buffer)> {
    let geom_offsets = list.offsets().inner().clone();
    let poly_list = list.values().as_list_opt::<i32>()?;
    let poly_offsets = poly_list.offsets().inner().clone();
    let ring_list = poly_list.values().as_list_opt::<i32>()?;
    let marker_buf = ring_list.offsets().inner().inner().clone();
    Some((geom_offsets, poly_offsets, marker_buf))
}

fn extract_mpoly_array_identity(
    mpoly_arr: &MultiPolygonArray,
) -> Option<(ScalarBuffer<i32>, usize, Buffer)> {
    let list_arr = mpoly_arr.clone().into_array_ref();
    let list = list_arr.as_list_opt::<i32>()?;
    let (geom_offsets, poly_offsets, marker_buf) = extract_mpoly_list_identity(list)?;
    let poly_offsets_base = poly_offsets.as_ptr() as usize;
    Some((geom_offsets, poly_offsets_base, marker_buf))
}

pub(crate) fn register_polyhedral_sidecar(mpoly_arr: &MultiPolygonArray, rows: BTreeSet<usize>) {
    if rows.is_empty() {
        return;
    }
    let Some((geom_offsets, poly_offsets_base, marker_buf)) =
        extract_mpoly_array_identity(mpoly_arr)
    else {
        return;
    };
    let geom_offsets_base = geom_offsets.as_ptr() as usize;
    let geom_offsets_end = geom_offsets_base + geom_offsets.len() * std::mem::size_of::<i32>();
    let marker_ptr = marker_buf.as_ptr() as usize;

    let mut sidecar = POLYHEDRAL_SIDECAR.write().unwrap();
    sidecar.inserts_since_sweep += 1;
    if sidecar.inserts_since_sweep >= SIDECAR_SWEEP_INTERVAL {
        sidecar
            .entries
            .retain(|_, entry| entry.marker_buf.strong_count() > 1);
        sidecar.inserts_since_sweep = 0;
    }
    sidecar.entries.insert(
        marker_ptr,
        PolyhedralSidecarEntry {
            geom_offsets,
            geom_offsets_base,
            geom_offsets_end,
            poly_offsets_base,
            marker_buf,
            rows,
        },
    );
    POLYHEDRAL_COUNT.store(sidecar.entries.len(), Ordering::Release);
}

fn lookup_polyhedral_in_mpoly_list(list: &GenericListArray<i32>, mpoly_row: usize) -> bool {
    if POLYHEDRAL_COUNT.load(Ordering::Acquire) == 0 {
        return false;
    }
    let Some((geom_offsets, poly_offsets, marker_buf)) = extract_mpoly_list_identity(list) else {
        return false;
    };
    let marker_ptr = marker_buf.as_ptr() as usize;

    let sidecar = POLYHEDRAL_SIDECAR.read().unwrap();
    let Some(entry) = sidecar.entries.get(&marker_ptr) else {
        return false;
    };

    let curr_offsets_ptr = geom_offsets.as_ptr() as usize;
    if curr_offsets_ptr >= entry.geom_offsets_base && curr_offsets_ptr < entry.geom_offsets_end {
        let slice_offset =
            (curr_offsets_ptr - entry.geom_offsets_base) / std::mem::size_of::<i32>();
        return entry.rows.contains(&(slice_offset + mpoly_row));
    }

    if entry.geom_offsets.len() == 2 && mpoly_row == 0 {
        return entry.rows.contains(&0);
    }

    if poly_offsets.len() > 1 {
        let curr_poly_ptr = poly_offsets.as_ptr() as usize;
        if curr_poly_ptr >= entry.poly_offsets_base {
            let poly_start =
                ((curr_poly_ptr - entry.poly_offsets_base) / std::mem::size_of::<i32>()) as i32;
            for &r in &entry.rows {
                if entry.geom_offsets.get(r).copied() == Some(poly_start) {
                    return true;
                }
            }
        }
    }

    entry.rows.len() == 1 && mpoly_row == 0
}

pub(crate) fn is_polyhedral_surface_row(
    geo_array: &dyn GeoArrowArray,
    field: &Field,
    row: usize,
) -> bool {
    if is_polyhedral_surface_field(field) {
        return true;
    }
    if matches!(
        geo_array.data_type(),
        geoarrow_schema::GeoArrowType::Wkb(_)
            | geoarrow_schema::GeoArrowType::LargeWkb(_)
            | geoarrow_schema::GeoArrowType::WkbView(_)
    ) {
        if row >= geo_array.len() || geo_array.is_null(row) {
            return false;
        }
        let raw = geo_array.to_array_ref();
        if let Some(bin) = raw.as_binary_opt::<i32>() {
            return is_polyhedral_surface_wkb(bin.value(row));
        }
        if let Some(bin) = raw.as_binary_opt::<i64>() {
            return is_polyhedral_surface_wkb(bin.value(row));
        }
        if let Some(bin) = raw.as_binary_view_opt() {
            return is_polyhedral_surface_wkb(bin.value(row));
        }
    }
    if POLYHEDRAL_COUNT.load(Ordering::Acquire) == 0 {
        return false;
    }
    if let Some(mpoly_arr) = geo_array.as_multi_polygon_opt() {
        if row >= mpoly_arr.len() || mpoly_arr.is_null(row) {
            return false;
        }
        let list_arr = mpoly_arr.clone().into_array_ref();
        let Some(list) = list_arr.as_list_opt::<i32>() else {
            return false;
        };
        return lookup_polyhedral_in_mpoly_list(list, row);
    }
    if let Some(geom_arr) = geo_array.as_geometry_opt() {
        if row >= geom_arr.len() || geom_arr.is_null(row) {
            return false;
        }
        let type_id = geom_arr.type_ids()[row];
        if type_id % 10 != 6 {
            return false;
        }
        let mpoly_row = geom_arr.offsets()[row] as usize;
        let union_arr = geom_arr.clone().into_array_ref();
        let Some(union) = union_arr.as_any().downcast_ref::<UnionArray>() else {
            return false;
        };
        let Some(list) = union.child(type_id).as_list_opt::<i32>() else {
            return false;
        };
        return lookup_polyhedral_in_mpoly_list(list, mpoly_row);
    }
    false
}

fn extract_list_identity(
    list: &GenericListArray<i32>,
    dim_order: i8,
) -> Option<(ScalarBuffer<i32>, ScalarBuffer<i8>, Buffer)> {
    let geom_offsets = list.offsets().inner().clone();
    let mixed_union = list.values().as_any().downcast_ref::<UnionArray>()?;
    let mixed_type_ids = mixed_union.type_ids().clone();
    let ls_type_id = dim_order * 10 + 2;
    let ls_arr = mixed_union.child(ls_type_id).as_list_opt::<i32>()?;
    let marker_buf = ls_arr.offsets().inner().inner().clone();
    Some((geom_offsets, mixed_type_ids, marker_buf))
}

fn extract_gc_array_identity(
    gc_arr: &GeometryCollectionArray,
) -> Option<(ScalarBuffer<i32>, usize, Buffer)> {
    let dim = gc_arr.data_type().dimension()?;
    let dim_order = dimension_order(dim);
    let list_arr = gc_arr.clone().into_array_ref();
    let list = list_arr.as_list_opt::<i32>()?;
    let (geom_offsets, mixed_type_ids, marker_buf) = extract_list_identity(list, dim_order)?;
    let mixed_type_ids_base = mixed_type_ids.inner().as_ptr() as usize;
    Some((geom_offsets, mixed_type_ids_base, marker_buf))
}

fn register_nested_collection_sidecar(
    gc_arr: &GeometryCollectionArray,
    trees: BTreeMap<usize, Vec<CollectionNode>>,
) {
    if trees.is_empty() {
        return;
    }
    let Some((geom_offsets, mixed_type_ids_base, marker_buf)) = extract_gc_array_identity(gc_arr)
    else {
        return;
    };
    let geom_offsets_base = geom_offsets.as_ptr() as usize;
    let geom_offsets_end = geom_offsets_base + geom_offsets.len() * std::mem::size_of::<i32>();
    let marker_ptr = marker_buf.as_ptr() as usize;

    let mut sidecar = NESTED_COLLECTION_SIDECAR.write().unwrap();
    sidecar.inserts_since_sweep += 1;
    if sidecar.inserts_since_sweep >= SIDECAR_SWEEP_INTERVAL {
        sidecar
            .entries
            .retain(|_, entry| entry.marker_buf.strong_count() > 1);
        sidecar.inserts_since_sweep = 0;
    }
    sidecar.entries.insert(
        marker_ptr,
        SidecarEntry {
            geom_offsets,
            geom_offsets_base,
            geom_offsets_end,
            mixed_type_ids_base,
            marker_buf,
            trees,
        },
    );
    NESTED_COLLECTION_COUNT.store(sidecar.entries.len(), Ordering::Release);
}

fn lookup_nested_tree_in_list(
    list: &GenericListArray<i32>,
    dim_order: i8,
    gc_row: usize,
) -> Option<Vec<CollectionNode>> {
    if NESTED_COLLECTION_COUNT.load(Ordering::Acquire) == 0 {
        return None;
    }
    let (geom_offsets, mixed_type_ids, marker_buf) = extract_list_identity(list, dim_order)?;
    let marker_ptr = marker_buf.as_ptr() as usize;

    let sidecar = NESTED_COLLECTION_SIDECAR.read().unwrap();
    let entry = sidecar.entries.get(&marker_ptr)?;

    let curr_offsets_ptr = geom_offsets.as_ptr() as usize;
    if curr_offsets_ptr >= entry.geom_offsets_base && curr_offsets_ptr < entry.geom_offsets_end {
        let slice_offset =
            (curr_offsets_ptr - entry.geom_offsets_base) / std::mem::size_of::<i32>();
        let orig_gc_row = slice_offset + gc_row;
        return entry.trees.get(&orig_gc_row).cloned();
    }

    if entry.geom_offsets.len() == 2 && gc_row == 0 {
        return entry.trees.get(&0).cloned();
    }

    if !mixed_type_ids.is_empty() {
        let curr_type_ids_ptr = mixed_type_ids.inner().as_ptr() as usize;
        if curr_type_ids_ptr >= entry.mixed_type_ids_base {
            let leaf_start = (curr_type_ids_ptr - entry.mixed_type_ids_base) as i32;
            for (&r, tree) in &entry.trees {
                if entry.geom_offsets.get(r).copied() == Some(leaf_start) {
                    return Some(tree.clone());
                }
            }
        }
    }

    if entry.trees.len() == 1 && gc_row == 0 {
        return entry.trees.values().next().cloned();
    }

    None
}

fn lookup_nested_tree(geo_array: &dyn GeoArrowArray, row: usize) -> Option<Vec<CollectionNode>> {
    if NESTED_COLLECTION_COUNT.load(Ordering::Acquire) == 0 {
        return None;
    }
    if let Some(gc_arr) = geo_array.as_geometry_collection_opt() {
        if row >= gc_arr.len() || gc_arr.is_null(row) {
            return None;
        }
        let list_arr = gc_arr.clone().into_array_ref();
        let list = list_arr.as_list_opt::<i32>()?;
        let dim_order = dimension_order(gc_arr.data_type().dimension()?);
        return lookup_nested_tree_in_list(list, dim_order, row);
    }
    if let Some(geom_arr) = geo_array.as_geometry_opt() {
        if row >= geom_arr.len() || geom_arr.is_null(row) {
            return None;
        }
        let type_id = geom_arr.type_ids()[row];
        if type_id % 10 != 7 {
            return None;
        }
        let dim_order = type_id / 10;
        let gc_row = geom_arr.offsets()[row] as usize;
        let union_arr = geom_arr.clone().into_array_ref();
        let union = union_arr.as_any().downcast_ref::<UnionArray>()?;
        let list = union.child(type_id).as_list_opt::<i32>()?;
        return lookup_nested_tree_in_list(list, dim_order, gc_row);
    }
    None
}

/// Builds a `GeometryCollectionArray` while preserving nested `GeometryCollection`
/// hierarchy in a sidecar keyed by the array's physical buffer identity, without
/// inserting synthetic sentinel geometries into physical coordinate buffers.
#[derive(Debug)]
pub(crate) struct GeometryCollectionArrayBuilder {
    inner: GeometryCollectionBuilder,
    nested_trees: BTreeMap<usize, Vec<CollectionNode>>,
    len: usize,
}

impl GeometryCollectionArrayBuilder {
    pub(crate) fn new(typ: GeometryCollectionType) -> Self {
        Self {
            inner: GeometryCollectionBuilder::new(typ),
            nested_trees: BTreeMap::new(),
            len: 0,
        }
    }

    pub(crate) fn geometry_type_id(&self) -> i8 {
        self.inner.geometry_type_id()
    }

    pub(crate) fn push_geometry_collection(
        &mut self,
        collection: Option<&impl GeometryCollectionTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        let row = self.len;
        let Some(collection) = collection else {
            self.inner
                .push_geometry_collection(None::<&GeometryCollection<f64>>)?;
            self.len += 1;
            return Ok(());
        };

        if has_nested_geometry_collection(collection) {
            let (flat, nodes) = flatten_geometry_collection(collection)?;
            self.inner.push_geometry_collection(Some(&flat))?;
            self.nested_trees.insert(row, nodes);
        } else {
            self.inner.push_geometry_collection(Some(collection))?;
        }
        self.len += 1;
        Ok(())
    }

    pub(crate) fn finish(self) -> GeometryCollectionArray {
        let arr = self.inner.finish();
        if !self.nested_trees.is_empty() {
            register_nested_collection_sidecar(&arr, self.nested_trees);
        }
        arr
    }
}

/// Builds a generic GeoArrow geometry array while preserving top-level and nested
/// `GeometryCollection` structure.
///
/// GeoArrow 0.9's `GeometryBuilder` canonicalizes a one-member
/// top-level `GeometryCollection` to its child geometry, and its `MixedGeometryBuilder`
/// rejects multi-member nested `GeometryCollection`s. This builder routes top-level
/// collections through `GeometryCollectionArrayBuilder` so collection hierarchy survives
/// without polluting physical coordinate buffers.
#[derive(Debug)]
pub(crate) struct GeometryArrayBuilder {
    inner: GeometryBuilder,
    collections: [GeometryCollectionArrayBuilder; 4],
    collection_lengths: [usize; 4],
    collection_rows: Vec<(usize, usize, i32)>,
    polyhedral_rows: Vec<usize>,
    len: usize,
}

impl GeometryArrayBuilder {
    pub(crate) fn new(typ: GeometryType) -> Self {
        let coord_type = typ.coord_type();
        let metadata = typ.metadata().clone();
        let collections = std::array::from_fn(|index| {
            GeometryCollectionArrayBuilder::new(
                GeometryCollectionType::new(dimension_from_index(index), Default::default())
                    .with_coord_type(coord_type),
            )
        });

        Self {
            inner: GeometryBuilder::new(GeometryType::new(metadata).with_coord_type(coord_type)),
            collections,
            collection_lengths: [0; 4],
            collection_rows: Vec::new(),
            polyhedral_rows: Vec::new(),
            len: 0,
        }
    }

    pub(crate) fn push_polyhedral_surface(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if geometry.is_some() {
            self.polyhedral_rows.push(self.len);
        }
        self.push_geometry(geometry)
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
        self.collections[dimension_index].push_geometry_collection(Some(collection))?;
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
        let mpolygons = dimensions.map(|dim| inner.multi_polygon_child(dim));
        if !self.polyhedral_rows.is_empty() {
            let mut by_dim: [BTreeSet<usize>; 4] = Default::default();
            for row in self.polyhedral_rows {
                let type_id = type_ids[row];
                if type_id % 10 == 6 {
                    let dim_index = (type_id / 10) as usize;
                    by_dim[dim_index].insert(offsets[row] as usize);
                }
            }
            for (dim_index, rows) in by_dim.into_iter().enumerate() {
                if !rows.is_empty() {
                    register_polyhedral_sidecar(&mpolygons[dim_index], rows);
                }
            }
        }
        GeometryArray::new(
            type_ids.into(),
            offsets.into(),
            dimensions.map(|dim| inner.point_child(dim)),
            dimensions.map(|dim| inner.line_string_child(dim)),
            dimensions.map(|dim| inner.polygon_child(dim)),
            dimensions.map(|dim| inner.multi_point_child(dim)),
            dimensions.map(|dim| inner.multi_line_string_child(dim)),
            mpolygons,
            self.collections.map(GeometryCollectionArrayBuilder::finish),
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

pub(crate) fn is_polyhedral_surface_wkb(bytes: &[u8]) -> bool {
    if bytes.len() < 5 {
        return false;
    }
    match bytes[0] {
        0 => {
            let raw = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
            (raw & 0x1fff_ffff) % 1000 == 15
        }
        1 => {
            let raw = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
            (raw & 0x1fff_ffff) % 1000 == 15
        }
        _ => false,
    }
}

pub(crate) fn polyhedral_wkb_to_multipolygon(
    geom: &impl GeometryTrait<T = f64>,
) -> GeoArrowResult<Wkt<f64>> {
    let dim = wkt_dimension_from_dimensions(geom.dim())?;
    match geom.as_type() {
        geo_traits::GeometryType::GeometryCollection(gc) => {
            let mut polys = Vec::with_capacity(gc.num_geometries());
            for child in gc.geometries() {
                let geo_traits::GeometryType::Polygon(poly) = child.as_type() else {
                    return Err(GeoArrowError::IncorrectGeometryType(
                        "Expected Polygon in PolyhedralSurface".to_string(),
                    ));
                };
                polys.push(polygon_to_wkt(poly, dim));
            }
            Ok(Wkt::MultiPolygon(MultiPolygon::new(polys, dim)))
        }
        geo_traits::GeometryType::MultiPolygon(mp) => Ok(Wkt::MultiPolygon(MultiPolygon::new(
            mp.polygons().map(|p| polygon_to_wkt(&p, dim)).collect(),
            dim,
        ))),
        _ => Err(GeoArrowError::IncorrectGeometryType(
            "Expected PolyhedralSurface geometry".to_string(),
        )),
    }
}

fn has_nested_geometry_collection(collection: &impl GeometryCollectionTrait<T = f64>) -> bool {
    collection
        .geometries()
        .any(|g| matches!(g.as_type(), geo_traits::GeometryType::GeometryCollection(_)))
}

fn flatten_geometry_collection(
    collection: &impl GeometryCollectionTrait<T = f64>,
) -> GeoArrowResult<(GeometryCollection<f64>, Vec<CollectionNode>)> {
    let dim = wkt_dimension_from_dimensions(collection.dim())?;
    let mut leaves = Vec::new();
    let mut nodes = Vec::with_capacity(collection.num_geometries());
    for item in collection.geometries() {
        flatten_member(&item, &mut leaves, &mut nodes)?;
    }
    Ok((GeometryCollection::new(leaves, dim), nodes))
}

fn flatten_member(
    geom: &impl GeometryTrait<T = f64>,
    leaves: &mut Vec<Wkt<f64>>,
    nodes: &mut Vec<CollectionNode>,
) -> GeoArrowResult<()> {
    if let geo_traits::GeometryType::GeometryCollection(inner) = geom.as_type() {
        let mut child_nodes = Vec::with_capacity(inner.num_geometries());
        for child in inner.geometries() {
            flatten_member(&child, leaves, &mut child_nodes)?;
        }
        nodes.push(CollectionNode::Collection(child_nodes));
    } else {
        let leaf_idx = leaves.len();
        leaves.push(geometry_to_wkt(geom)?);
        nodes.push(CollectionNode::Leaf(leaf_idx));
    }
    Ok(())
}

fn reconstruct_geometry_collection(
    flat_gc: &impl GeometryCollectionTrait<T = f64>,
    nodes: &[CollectionNode],
) -> GeoArrowResult<GeometryCollection<f64>> {
    let dim = wkt_dimension_from_dimensions(flat_gc.dim())?;
    let children = reconstruct_nodes(flat_gc, nodes, dim)?;
    Ok(GeometryCollection::new(children, dim))
}

fn reconstruct_nodes(
    flat_gc: &impl GeometryCollectionTrait<T = f64>,
    nodes: &[CollectionNode],
    dim: WktDimension,
) -> GeoArrowResult<Vec<Wkt<f64>>> {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        match node {
            CollectionNode::Leaf(leaf_idx) => {
                let geom = flat_gc.geometry(*leaf_idx).ok_or_else(|| {
                    GeoArrowError::InvalidGeoArrow(format!(
                        "leaf index {leaf_idx} out of bounds for GeometryCollection of length {}",
                        flat_gc.num_geometries()
                    ))
                })?;
                out.push(geometry_to_wkt(&geom)?);
            }
            CollectionNode::Collection(child_nodes) => {
                let grand_children = reconstruct_nodes(flat_gc, child_nodes, dim)?;
                out.push(Wkt::GeometryCollection(GeometryCollection::new(
                    grand_children,
                    dim,
                )));
            }
        }
    }
    Ok(out)
}

pub(crate) fn nested_collection_top_len(
    geo_array: &dyn GeoArrowArray,
    row: usize,
) -> Option<usize> {
    lookup_nested_tree(geo_array, row).map(|nodes| nodes.len())
}

pub(crate) fn decoded_row_geometry_collection(
    geo_array: &dyn GeoArrowArray,
    row: usize,
) -> GeoArrowResult<Option<GeometryCollection<f64>>> {
    let Some(nodes) = lookup_nested_tree(geo_array, row) else {
        return Ok(None);
    };
    if let Some(gc_arr) = geo_array.as_geometry_collection_opt() {
        let Some(gc) = gc_arr.get(row)? else {
            return Ok(None);
        };
        return reconstruct_geometry_collection(&gc, &nodes).map(Some);
    }
    if let Some(geom_arr) = geo_array.as_geometry_opt() {
        let Some(geom) = geom_arr.get(row)? else {
            return Ok(None);
        };
        if let geo_traits::GeometryType::GeometryCollection(gc) = geom.as_type() {
            return reconstruct_geometry_collection(gc, &nodes).map(Some);
        }
    }
    Ok(None)
}

fn has_encoded_nested_collections(geo_array: &dyn GeoArrowArray) -> GeoArrowResult<bool> {
    if NESTED_COLLECTION_COUNT.load(Ordering::Acquire) == 0 {
        return Ok(false);
    }
    if geo_array.as_geometry_collection_opt().is_none() && geo_array.as_geometry_opt().is_none() {
        return Ok(false);
    }
    for row in 0..geo_array.len() {
        if lookup_nested_tree(geo_array, row).is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_encoded_polyhedral_surfaces(geo_array: &dyn GeoArrowArray, field: &Field) -> bool {
    if is_polyhedral_surface_field(field) {
        return true;
    }
    if POLYHEDRAL_COUNT.load(Ordering::Acquire) == 0 {
        return false;
    }
    if geo_array.as_multi_polygon_opt().is_none() && geo_array.as_geometry_opt().is_none() {
        return false;
    }
    (0..geo_array.len()).any(|row| is_polyhedral_surface_row(geo_array, field, row))
}

pub(crate) fn array_to_wkt(
    geo_array: &dyn GeoArrowArray,
    field: &Field,
) -> GeoArrowResult<WktArray> {
    let has_polyhedral = has_encoded_polyhedral_surfaces(geo_array, field);
    let has_nested_gc = has_encoded_nested_collections(geo_array)?;
    let wkt_arr = to_wkt::<i32>(geo_array)?;
    if !has_polyhedral && !has_nested_gc {
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
            if has_polyhedral && is_polyhedral_surface_row(geo_array, field, row) {
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
    let has_polyhedral = has_encoded_polyhedral_surfaces(geo_array, field);
    let has_nested_gc = has_encoded_nested_collections(geo_array)?;
    let wkb_arr = to_wkb::<i32>(geo_array)?;
    if !has_polyhedral && !has_nested_gc {
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
            if has_polyhedral && is_polyhedral_surface_row(geo_array, field, row) {
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
            let children = gc
                .geometries()
                .map(|g| geometry_to_wkt(&g))
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::GeometryCollection(GeometryCollection::new(children, dim))
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

fn dimension_order(dim: Dimension) -> i8 {
    match dim {
        Dimension::XY => 0,
        Dimension::XYZ => 1,
        Dimension::XYM => 2,
        Dimension::XYZM => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_nested_collection() -> GeometryCollection<f64> {
        match "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)))"
            .parse::<Wkt<f64>>()
            .unwrap()
        {
            Wkt::GeometryCollection(gc) => gc,
            _ => unreachable!(),
        }
    }

    #[test]
    fn sidecar_retains_live_nested_collections_past_4096_and_reclaims_dropped() {
        let nested = parse_nested_collection();
        let typ = GeometryCollectionType::new(Dimension::XY, Default::default());
        let count = 4097;
        let mut arrays = Vec::with_capacity(count);
        for _ in 0..count {
            let mut builder = GeometryCollectionArrayBuilder::new(typ.clone());
            builder.push_geometry_collection(Some(&nested)).unwrap();
            arrays.push(builder.finish());
        }

        let first_decoded = decoded_row_geometry_collection(&arrays[0], 0)
            .unwrap()
            .expect("first live array must retain nested collection sidecar entry");
        assert_eq!(first_decoded, nested);

        let last_decoded = decoded_row_geometry_collection(&arrays[count - 1], 0)
            .unwrap()
            .expect("last live array must retain nested collection sidecar entry");
        assert_eq!(last_decoded, nested);

        let first_ptr = extract_gc_array_identity(&arrays[0]).unwrap().2.as_ptr() as usize;
        assert!(
            NESTED_COLLECTION_SIDECAR
                .read()
                .unwrap()
                .entries
                .contains_key(&first_ptr)
        );

        drop(arrays);

        // Trigger a sweep by registering SIDECAR_SWEEP_INTERVAL transient arrays.
        for _ in 0..SIDECAR_SWEEP_INTERVAL {
            let mut builder = GeometryCollectionArrayBuilder::new(typ.clone());
            builder.push_geometry_collection(Some(&nested)).unwrap();
            let _transient = builder.finish();
        }

        assert!(
            !NESTED_COLLECTION_SIDECAR
                .read()
                .unwrap()
                .entries
                .contains_key(&first_ptr),
            "dropped array sidecar entry should be reclaimed after sweep"
        );
    }
}
