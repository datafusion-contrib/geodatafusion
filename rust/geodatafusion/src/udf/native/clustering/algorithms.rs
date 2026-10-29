//! Planar views and clustering algorithms shared by the PostGIS clustering functions.
//!
//! PostGIS implements these functions on top of 2D planar primitives: minimum distance,
//! intersection, centroid, and bounding-box queries. This module keeps the same split:
//! geometry from the input array is classified once into a planar view, and the clustering
//! algorithms operate on that view. Geometry identity (Z/M coordinates, geometry type,
//! emptiness) is not lost because output collections are rebuilt from the original geometry
//! views.

use datafusion::error::{DataFusionError, Result};
use geo::{Centroid, Distance, Euclidean, Intersects};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType as NativeGeometryType,
    LineStringTrait, LineTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait,
    PointTrait, PolygonTrait, RectTrait, TriangleTrait,
};
use geoarrow_array::GeoArrowArrayAccessor;
use wkt::Wkt;
use wkt::types::Dimension as WktDimension;

use crate::udf::native::accessors::is_empty::is_geometry_topologically_empty;
use crate::udf::native::bounding_box::util::bounds::{GeometryBounds, geometry_bounds};
use crate::udf::native::clustering::common::arrow_error;

/// A 2D bounding box used for the PostGIS-style envelope prefilter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct BoundingBox {
    pub minx: f64,
    pub miny: f64,
    pub maxx: f64,
    pub maxy: f64,
}

impl BoundingBox {
    /// Whether `self` expanded by `distance` intersects `other`.
    ///
    /// This mirrors the `GEOSSTRtree` query envelope used by PostGIS: the candidate set is
    /// larger than the exact `distance` set, and the exact predicate is applied afterwards.
    pub fn expanded_intersects(&self, other: &Self, distance: f64) -> bool {
        self.minx - distance <= other.maxx
            && self.maxx + distance >= other.minx
            && self.miny - distance <= other.maxy
            && self.maxy + distance >= other.miny
    }
}

/// One non-null input geometry prepared for planar clustering.
#[derive(Debug, Clone)]
pub(super) struct ClusteringRow {
    /// Topological emptiness (recursively, matching `ST_IsEmpty`).
    pub is_empty: bool,
    /// Finite 2D bounds; `None` for empty geometries.
    pub bounds: Option<BoundingBox>,
    /// 2D geometry used for predicates and distances.
    pub planar: Option<geo::Geometry<f64>>,
}

impl ClusteringRow {
    pub fn can_participate(&self) -> bool {
        !self.is_empty && self.planar.is_some() && self.bounds.is_some()
    }

    pub fn bounds(&self) -> BoundingBox {
        self.bounds
            .expect("participating clustering rows always have finite bounds")
    }

    pub fn planar(&self) -> &geo::Geometry<f64> {
        self.planar
            .as_ref()
            .expect("participating clustering rows always have a planar view")
    }
}

/// Planar rows for every input row; `None` marks a SQL NULL geometry.
pub(super) fn clustering_rows<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> Result<Vec<Option<ClusteringRow>>> {
    let mut rows = Vec::with_capacity(array.len());
    for geometry in array.iter() {
        match geometry {
            None => rows.push(None),
            Some(geometry) => rows.push(Some(clustering_row(&geometry.map_err(arrow_error)?)?)),
        }
    }
    Ok(rows)
}

fn clustering_row(geometry: &impl GeometryTrait<T = f64>) -> Result<ClusteringRow> {
    let is_empty = is_geometry_topologically_empty(geometry);
    let bounds = match geometry_bounds(geometry) {
        GeometryBounds::Empty => None,
        GeometryBounds::NonFinite => {
            return Err(DataFusionError::Execution(
                "clustering functions do not support non-finite coordinates".to_string(),
            ));
        }
        GeometryBounds::Finite(bounds) => Some(BoundingBox {
            minx: bounds.minx(),
            miny: bounds.miny(),
            maxx: bounds.maxx(),
            maxy: bounds.maxy(),
        }),
    };

    let planar = if is_empty {
        None
    } else {
        planar_geometry(geometry)?
    };

    Ok(ClusteringRow {
        is_empty,
        bounds,
        planar,
    })
}

/// Convert a `geo_traits` geometry into a 2D `geo` geometry.
///
/// Z and M values are intentionally dropped: the PostGIS clustering functions are documented
/// as 2D Cartesian algorithms. Empty children inside a collection are dropped, matching how
/// PostGIS treats empty components for predicates; an entirely empty geometry yields `None`.
pub(super) fn planar_geometry(
    geometry: &impl GeometryTrait<T = f64>,
) -> Result<Option<geo::Geometry<f64>>> {
    use NativeGeometryType::*;

    let geometry = match geometry.as_type() {
        Point(point) => point
            .coord()
            .map(|coord| geo::Geometry::Point(geo::Point::new(coord.x(), coord.y()))),
        LineString(line_string) => Some(geo::Geometry::LineString(to_geo_line_string(line_string))),
        Polygon(polygon) => Some(geo::Geometry::Polygon(to_geo_polygon(polygon))),
        MultiPoint(multi_point) => {
            let points = multi_point
                .points()
                .filter_map(|point| {
                    point
                        .coord()
                        .map(|coord| geo::Point::new(coord.x(), coord.y()))
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                None
            } else {
                Some(geo::Geometry::MultiPoint(geo::MultiPoint::new(points)))
            }
        }
        MultiLineString(multi_line_string) => {
            Some(geo::Geometry::MultiLineString(geo::MultiLineString::new(
                multi_line_string
                    .line_strings()
                    .map(|line_string| to_geo_line_string(&line_string))
                    .collect(),
            )))
        }
        MultiPolygon(multi_polygon) => Some(geo::Geometry::MultiPolygon(geo::MultiPolygon::new(
            multi_polygon
                .polygons()
                .map(|polygon| to_geo_polygon(&polygon))
                .collect(),
        ))),
        GeometryCollection(collection) => {
            let children = collection
                .geometries()
                .map(|child| planar_geometry(&child))
                .collect::<Result<Vec<_>>>()?;
            let children = children.into_iter().flatten().collect::<Vec<_>>();
            if children.is_empty() {
                None
            } else {
                Some(geo::Geometry::GeometryCollection(
                    geo::GeometryCollection::new_from(children),
                ))
            }
        }
        Rect(rect) => Some(geo::Geometry::Rect(geo::Rect::new(
            (rect.min().x(), rect.min().y()),
            (rect.max().x(), rect.max().y()),
        ))),
        Triangle(triangle) => Some(geo::Geometry::Triangle(geo::Triangle::new(
            (triangle.first().x(), triangle.first().y()).into(),
            (triangle.second().x(), triangle.second().y()).into(),
            (triangle.third().x(), triangle.third().y()).into(),
        ))),
        Line(line) => Some(geo::Geometry::Line(geo::Line::new(
            geo::Coord {
                x: line.start().x(),
                y: line.start().y(),
            },
            geo::Coord {
                x: line.end().x(),
                y: line.end().y(),
            },
        ))),
    };

    Ok(geometry)
}

fn to_geo_line_string(line_string: &impl LineStringTrait<T = f64>) -> geo::LineString<f64> {
    geo::LineString::new(
        line_string
            .coords()
            .map(|coord| geo::Coord {
                x: coord.x(),
                y: coord.y(),
            })
            .collect(),
    )
}

fn to_geo_polygon(polygon: &impl PolygonTrait<T = f64>) -> geo::Polygon<f64> {
    let exterior = polygon
        .exterior()
        .map(|ring| to_geo_line_string(&ring))
        .unwrap_or_else(|| geo::LineString::new(Vec::new()));
    let interiors = polygon
        .interiors()
        .map(|ring| to_geo_line_string(&ring))
        .collect();
    geo::Polygon::new(exterior, interiors)
}

/// Exact 2D Cartesian distance between two non-empty planar geometries.
pub(super) fn planar_distance(left: &geo::Geometry<f64>, right: &geo::Geometry<f64>) -> f64 {
    Euclidean.distance(left, right)
}

/// Exact 2D intersection predicate between two non-empty planar geometries.
pub(super) fn planar_intersects(left: &geo::Geometry<f64>, right: &geo::Geometry<f64>) -> bool {
    left.intersects(right)
}

/// Centroid of a 2D geometry, or `None` if the geometry has no centroid.
pub(super) fn planar_centroid(geometry: &geo::Geometry<f64>) -> Option<geo::Point<f64>> {
    geometry.centroid()
}

/// A representative point for the K-means algorithm.
///
/// The distance used by K-means is the 3D Cartesian distance, and the `m` field is the
/// per-object weight (always 1 unless a `POINT` input carries an M value).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct KMeansObject {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub m: f64,
}

impl KMeansObject {
    pub fn squared_distance(&self, other: &Self) -> f64 {
        let dx = other.x - self.x;
        let dy = other.y - self.y;
        let dz = other.z - self.z;
        dx * dx + dy * dy + dz * dz
    }
}

/// K-means representative objects for every input row.
pub(super) fn kmeans_objects<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> Result<Vec<Option<KMeansObject>>> {
    let mut objects = Vec::with_capacity(array.len());
    for geometry in array.iter() {
        match geometry {
            None => objects.push(None),
            Some(geometry) => {
                let geometry = geometry.map_err(arrow_error)?;
                if is_geometry_topologically_empty(&geometry) {
                    objects.push(None);
                } else {
                    objects.push(kmeans_object(&geometry)?);
                }
            }
        }
    }
    Ok(objects)
}

/// Build the representative object for a non-null, non-empty geometry.
///
/// This follows the documented reference behavior:
/// - `POINT`: the point coordinates; an M value is used as a weight and must be positive;
/// - non-point without Z: the 2D centroid;
/// - non-point with Z: the center of the 3D bounding box.
pub(super) fn kmeans_object(
    geometry: &impl GeometryTrait<T = f64>,
) -> Result<Option<KMeansObject>> {
    use NativeGeometryType::*;

    match geometry.as_type() {
        Point(point) => {
            let Some(coord) = point.coord() else {
                return Ok(None);
            };
            let z = coord_z(&coord).unwrap_or(0.0);
            let m = coord_m(&coord).unwrap_or(1.0);
            if m <= 0.0 {
                return Err(DataFusionError::Execution(
                    "lwgeom_cluster_kmeans has an input point geometry with weight in M less or equal to 0"
                        .to_string(),
                ));
            }
            Ok(Some(KMeansObject {
                x: coord.x(),
                y: coord.y(),
                z,
                m,
            }))
        }
        _ if has_z(geometry) => {
            let GeometryBounds::Finite(bounds) = geometry_bounds(geometry) else {
                return Ok(None);
            };
            let Some((minz, maxz)) = z_range(geometry) else {
                return Ok(None);
            };
            Ok(Some(KMeansObject {
                x: (bounds.minx() + bounds.maxx()) / 2.0,
                y: (bounds.miny() + bounds.maxy()) / 2.0,
                z: (minz + maxz) / 2.0,
                m: 1.0,
            }))
        }
        _ => {
            let Some(planar) = planar_geometry(geometry)? else {
                return Ok(None);
            };
            let Some(centroid) = planar_centroid(&planar) else {
                return Ok(None);
            };
            Ok(Some(KMeansObject {
                x: centroid.x(),
                y: centroid.y(),
                z: 0.0,
                m: 1.0,
            }))
        }
    }
}

/// Whether the geometry's coordinates include Z.
pub(super) fn has_z(geometry: &impl GeometryTrait<T = f64>) -> bool {
    matches!(
        geometry.dim(),
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
    )
}

/// Minimum and maximum Z values over every coordinate in the geometry.
fn z_range(geometry: &impl GeometryTrait<T = f64>) -> Option<(f64, f64)> {
    let mut range: Option<(f64, f64)> = None;
    fn add_coord(range: &mut Option<(f64, f64)>, coord: &impl CoordTrait<T = f64>) {
        if let Some(z) = coord_z(coord) {
            *range = Some(match range {
                Some((min, max)) => (min.min(z), max.max(z)),
                None => (z, z),
            });
        }
    }

    fn visit(range: &mut Option<(f64, f64)>, geometry: &impl GeometryTrait<T = f64>) {
        use NativeGeometryType::*;
        match geometry.as_type() {
            Point(point) => {
                if let Some(coord) = point.coord() {
                    add_coord(range, &coord);
                }
            }
            LineString(line_string) => {
                for coord in line_string.coords() {
                    add_coord(range, &coord);
                }
            }
            Polygon(polygon) => {
                if let Some(exterior) = polygon.exterior() {
                    for coord in exterior.coords() {
                        add_coord(range, &coord);
                    }
                }
                for interior in polygon.interiors() {
                    for coord in interior.coords() {
                        add_coord(range, &coord);
                    }
                }
            }
            MultiPoint(multi_point) => {
                for point in multi_point.points() {
                    if let Some(coord) = point.coord() {
                        add_coord(range, &coord);
                    }
                }
            }
            MultiLineString(multi_line_string) => {
                for line_string in multi_line_string.line_strings() {
                    for coord in line_string.coords() {
                        add_coord(range, &coord);
                    }
                }
            }
            MultiPolygon(multi_polygon) => {
                for polygon in multi_polygon.polygons() {
                    if let Some(exterior) = polygon.exterior() {
                        for coord in exterior.coords() {
                            add_coord(range, &coord);
                        }
                    }
                    for interior in polygon.interiors() {
                        for coord in interior.coords() {
                            add_coord(range, &coord);
                        }
                    }
                }
            }
            GeometryCollection(collection) => {
                for child in collection.geometries() {
                    visit(range, &child);
                }
            }
            Rect(rect) => {
                add_coord(range, &rect.min());
                add_coord(range, &rect.max());
            }
            Triangle(triangle) => {
                add_coord(range, &triangle.first());
                add_coord(range, &triangle.second());
                add_coord(range, &triangle.third());
            }
            Line(line) => {
                add_coord(range, &line.start());
                add_coord(range, &line.end());
            }
        }
    }

    visit(&mut range, geometry);
    range
}

/// Convert a geometry to an owned WKT value that keeps Z and M coordinates.
///
/// This is used to rebuild output `GeometryCollection`s while preserving the exact
/// coordinates of the input geometries.
pub(super) fn owned_wkt_geometry(geometry: &impl GeometryTrait<T = f64>) -> Result<Wkt<f64>> {
    let dimension = wkt_dimension(geometry.dim())?;
    let owned = match geometry.as_type() {
        NativeGeometryType::Point(point) => Wkt::Point(wkt::types::Point::new(
            point.coord().map(|coord| owned_coord(&coord)),
            dimension,
        )),
        NativeGeometryType::LineString(line_string) => {
            Wkt::LineString(wkt::types::LineString::new(
                line_string
                    .coords()
                    .map(|coord| owned_coord(&coord))
                    .collect(),
                dimension,
            ))
        }
        NativeGeometryType::Polygon(polygon) => {
            Wkt::Polygon(wkt::types::Polygon::new(owned_rings(polygon), dimension))
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            Wkt::MultiPoint(wkt::types::MultiPoint::new(
                multi_point
                    .points()
                    .map(|point| {
                        wkt::types::Point::new(
                            point.coord().map(|coord| owned_coord(&coord)),
                            dimension,
                        )
                    })
                    .collect(),
                dimension,
            ))
        }
        NativeGeometryType::MultiLineString(multi_line_string) => {
            Wkt::MultiLineString(wkt::types::MultiLineString::new(
                multi_line_string
                    .line_strings()
                    .map(|line_string| {
                        wkt::types::LineString::new(
                            line_string
                                .coords()
                                .map(|coord| owned_coord(&coord))
                                .collect(),
                            dimension,
                        )
                    })
                    .collect(),
                dimension,
            ))
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            Wkt::MultiPolygon(wkt::types::MultiPolygon::new(
                multi_polygon
                    .polygons()
                    .map(|polygon| wkt::types::Polygon::new(owned_rings(&polygon), dimension))
                    .collect(),
                dimension,
            ))
        }
        NativeGeometryType::GeometryCollection(collection) => {
            Wkt::GeometryCollection(wkt::types::GeometryCollection::new(
                collection
                    .geometries()
                    .map(|child| owned_wkt_geometry(&child))
                    .collect::<Result<Vec<_>>>()?,
                dimension,
            ))
        }
        // `geoarrow` does not expose Rect/Triangle/Line through its builder APIs, but
        // geometry collections can contain them when they come from other libraries.
        NativeGeometryType::Rect(rect) => Wkt::Polygon(wkt::types::Polygon::new(
            vec![wkt::types::LineString::new(
                vec![
                    owned_coord(&rect.min()),
                    coord_xy(rect.min().x(), rect.max().y()),
                    owned_coord(&rect.max()),
                    coord_xy(rect.max().x(), rect.min().y()),
                    owned_coord(&rect.min()),
                ],
                dimension,
            )],
            dimension,
        )),
        NativeGeometryType::Triangle(triangle) => Wkt::Polygon(wkt::types::Polygon::new(
            vec![wkt::types::LineString::new(
                vec![
                    owned_coord(&triangle.first()),
                    owned_coord(&triangle.second()),
                    owned_coord(&triangle.third()),
                    owned_coord(&triangle.first()),
                ],
                dimension,
            )],
            dimension,
        )),
        NativeGeometryType::Line(line) => Wkt::LineString(wkt::types::LineString::new(
            vec![owned_coord(&line.start()), owned_coord(&line.end())],
            dimension,
        )),
    };
    Ok(owned)
}

fn owned_rings(polygon: &impl PolygonTrait<T = f64>) -> Vec<wkt::types::LineString<f64>> {
    let dimension = wkt_dimension(polygon.dim()).expect("polygon dimensions are always known");
    let mut rings = Vec::new();
    if let Some(exterior) = polygon.exterior() {
        rings.push(wkt::types::LineString::new(
            exterior.coords().map(|coord| owned_coord(&coord)).collect(),
            dimension,
        ));
    }
    for interior in polygon.interiors() {
        rings.push(wkt::types::LineString::new(
            interior.coords().map(|coord| owned_coord(&coord)).collect(),
            dimension,
        ));
    }
    rings
}

fn owned_coord(coord: &impl CoordTrait<T = f64>) -> wkt::types::Coord<f64> {
    wkt::types::Coord {
        x: coord.x(),
        y: coord.y(),
        z: coord_z(coord),
        m: coord_m(coord),
    }
}

/// The Z value of a coordinate, considering its actual dimension.
fn coord_z(coord: &impl CoordTrait<T = f64>) -> Option<f64> {
    match coord.dim() {
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm => coord.nth(2),
        _ => None,
    }
}

/// The M value of a coordinate, considering its actual dimension.
fn coord_m(coord: &impl CoordTrait<T = f64>) -> Option<f64> {
    match coord.dim() {
        geo_traits::Dimensions::Xym => coord.nth(2),
        geo_traits::Dimensions::Xyzm => coord.nth(3),
        _ => None,
    }
}

fn coord_xy(x: f64, y: f64) -> wkt::types::Coord<f64> {
    wkt::types::Coord {
        x,
        y,
        z: None,
        m: None,
    }
}

pub(super) fn wkt_dimension(dimensions: geo_traits::Dimensions) -> Result<WktDimension> {
    match dimensions {
        geo_traits::Dimensions::Xy => Ok(WktDimension::XY),
        geo_traits::Dimensions::Xyz => Ok(WktDimension::XYZ),
        geo_traits::Dimensions::Xym => Ok(WktDimension::XYM),
        geo_traits::Dimensions::Xyzm => Ok(WktDimension::XYZM),
        geo_traits::Dimensions::Unknown(value) => Err(DataFusionError::Execution(format!(
            "clustering functions do not support geometry dimension {value}"
        ))),
    }
}

/// A union-find with the same union rule as the PostGIS clustering implementation:
/// the larger component wins, and equal-size components keep the smaller root.
#[derive(Debug)]
pub(super) struct UnionFind {
    parent: Vec<u32>,
    size: Vec<u32>,
}

impl UnionFind {
    pub fn new(len: usize) -> Self {
        Self {
            parent: (0..len as u32).collect(),
            size: vec![1; len],
        }
    }

    pub fn find(&mut self, mut index: u32) -> u32 {
        let mut root = index;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while index != root {
            let next = self.parent[index as usize];
            self.parent[index as usize] = root;
            index = next;
        }
        root
    }

    pub fn union(&mut self, left: u32, right: u32) {
        let mut left_root = self.find(left);
        let mut right_root = self.find(right);
        if left_root == right_root {
            return;
        }

        if self.size[left_root as usize] < self.size[right_root as usize]
            || (self.size[left_root as usize] == self.size[right_root as usize]
                && left_root > right_root)
        {
            std::mem::swap(&mut left_root, &mut right_root);
        }
        self.parent[right_root as usize] = left_root;
        self.size[left_root as usize] += self.size[right_root as usize];
        self.size[right_root as usize] = 0;
    }

    /// Assign consecutive cluster ids in root order, mirroring
    /// `UF_get_collapsed_cluster_ids`.
    ///
    /// `is_in_cluster` is `None` when every index participates, or `Some` to leave
    /// non-participating rows without an id (used by DBSCAN for noise and empty inputs).
    pub fn collapsed_ids(&mut self, is_in_cluster: Option<&[bool]>) -> Vec<Option<u32>> {
        let mut ordered = (0..self.parent.len() as u32)
            .map(|index| (self.find(index), index))
            .collect::<Vec<_>>();
        ordered.sort_unstable();

        let mut ids = vec![None; self.parent.len()];
        let mut current_id = 0;
        let mut last_root = None;
        for (root, index) in ordered {
            if is_in_cluster.is_some_and(|flags| !flags[index as usize]) {
                continue;
            }
            if last_root.is_some_and(|last| last != root) {
                current_id += 1;
            }
            ids[index as usize] = Some(current_id);
            last_root = Some(root);
        }

        ids
    }
}

/// DBSCAN with `minpoints <= 1`.
///
/// Every non-empty geometry starts in its own cluster and clusters are merged whenever the
/// exact planar distance is at most `eps`. Empty geometries stay isolated, and SQL NULL rows
/// reserve a union-find slot (this reproduces the reference id gaps).
pub(super) fn dbscan_minpoints_one(rows: &[Option<ClusteringRow>], eps: f64) -> Vec<Option<u32>> {
    let mut union_find = UnionFind::new(rows.len());
    for p in 0..rows.len() {
        let Some(row) = rows[p].as_ref().filter(|row| row.can_participate()) else {
            continue;
        };
        for (q, candidate) in rows.iter().enumerate() {
            if p == q || union_find.find(p as u32) == union_find.find(q as u32) {
                continue;
            }
            let Some(candidate) = candidate.as_ref().filter(|row| row.can_participate()) else {
                continue;
            };
            if !row.bounds().expanded_intersects(&candidate.bounds(), eps) {
                continue;
            }
            if planar_distance(row.planar(), candidate.planar()) <= eps {
                union_find.union(p as u32, q as u32);
            }
        }
    }
    union_find.collapsed_ids(None)
}

/// General DBSCAN for `minpoints > 1`.
pub(super) fn dbscan(
    rows: &[Option<ClusteringRow>],
    eps: f64,
    minpoints: usize,
) -> Vec<Option<u32>> {
    if rows.len() < minpoints {
        return vec![None; rows.len()];
    }

    let mut union_find = UnionFind::new(rows.len());
    let mut in_cluster = vec![false; rows.len()];
    let mut is_core = vec![false; rows.len()];

    for p in 0..rows.len() {
        let Some(row) = rows[p].as_ref().filter(|row| row.can_participate()) else {
            continue;
        };

        // PostGIS determines core candidacy from the number of envelope candidates,
        // including the geometry itself, before computing exact distances.
        let candidates = (0..rows.len())
            .filter(|&q| {
                rows[q]
                    .as_ref()
                    .filter(|candidate| candidate.can_participate())
                    .is_some_and(|candidate| {
                        row.bounds().expanded_intersects(&candidate.bounds(), eps)
                    })
            })
            .collect::<Vec<_>>();
        if candidates.len() < minpoints {
            continue;
        }

        let mut neighbors = Vec::with_capacity(minpoints);
        for q in candidates {
            let is_core_point = neighbors.len() >= minpoints;
            if is_core_point {
                let p_root = union_find.find(p as u32);
                let q_root = union_find.find(q as u32);
                if p_root == q_root {
                    continue;
                }
                // A border point that already belongs to another cluster cannot merge
                // clusters; only core points can.
                if in_cluster[q] && !is_core[q] {
                    continue;
                }
            }

            let candidate = rows[q]
                .as_ref()
                .expect("envelope candidates always have a row");
            // `p == q` (and any other overlapping geometry) has distance 0, so the
            // self-count required by DBSCAN falls out of the exact predicate.
            if planar_distance(row.planar(), candidate.planar()) > eps {
                continue;
            }

            if !is_core_point {
                neighbors.push(q);
                if neighbors.len() == minpoints {
                    is_core[p] = true;
                    in_cluster[p] = true;
                    for &neighbor in &neighbors {
                        union_if_available(
                            &mut union_find,
                            p as u32,
                            neighbor as u32,
                            &is_core,
                            &mut in_cluster,
                        );
                    }
                }
            } else {
                union_if_available(
                    &mut union_find,
                    p as u32,
                    q as u32,
                    &is_core,
                    &mut in_cluster,
                );
            }
        }
    }

    union_find.collapsed_ids(Some(&in_cluster))
}

fn union_if_available(
    union_find: &mut UnionFind,
    p: u32,
    q: u32,
    is_core: &[bool],
    in_cluster: &mut [bool],
) {
    if in_cluster[q as usize] {
        if is_core[q as usize] {
            union_find.union(p, q);
        }
    } else {
        union_find.union(p, q);
        in_cluster[q as usize] = true;
    }
}

/// Connected components where a pair of geometries is connected when they intersect.
pub(super) fn intersecting_clusters(rows: &[Option<ClusteringRow>]) -> Vec<Option<u32>> {
    let mut union_find = UnionFind::new(rows.len());
    for p in 0..rows.len() {
        let Some(row) = rows[p].as_ref().filter(|row| row.can_participate()) else {
            continue;
        };
        for (q, candidate) in rows.iter().enumerate().skip(p + 1) {
            if union_find.find(p as u32) == union_find.find(q as u32) {
                continue;
            }
            let Some(candidate) = candidate.as_ref().filter(|row| row.can_participate()) else {
                continue;
            };
            if !bounds_intersect(&row.bounds(), &candidate.bounds()) {
                continue;
            }
            if planar_intersects(row.planar(), candidate.planar()) {
                union_find.union(p as u32, q as u32);
            }
        }
    }
    union_find.collapsed_ids(None)
}

fn bounds_intersect(left: &BoundingBox, right: &BoundingBox) -> bool {
    left.minx <= right.maxx
        && left.maxx >= right.minx
        && left.miny <= right.maxy
        && left.maxy >= right.miny
}
