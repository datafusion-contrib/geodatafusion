#![cfg_attr(not(feature = "geos-3_11"), allow(dead_code))]

use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    LineTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
    RectTrait, TriangleTrait, UnimplementedLine, UnimplementedRect, UnimplementedTriangle,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct OwnedCoord {
    dim: Dimensions,
    x: f64,
    y: f64,
    z: f64,
    m: f64,
}

impl OwnedCoord {
    fn with_xy(&self, x: f64, y: f64) -> Self {
        Self { x, y, ..*self }
    }

    fn from_coord(coord: &impl CoordTrait<T = f64>) -> Self {
        let dim = coord.dim();
        let z = matches!(dim, Dimensions::Xyz | Dimensions::Xyzm).then(|| coord.nth(2).unwrap());
        let m = match dim {
            Dimensions::Xym => Some(coord.nth(2).unwrap()),
            Dimensions::Xyzm => Some(coord.nth(3).unwrap()),
            _ => None,
        };
        Self {
            dim,
            x: coord.x(),
            y: coord.y(),
            z: z.unwrap_or(0.0),
            m: m.unwrap_or(0.0),
        }
    }

    fn rotate_xy(&mut self) {
        self.x = if self.x == 0.0 { 0.0 } else { self.x.round() };
        self.y = if self.y == 0.0 { 0.0 } else { self.y.round() };
    }
}

impl OwnedCoord {
    pub(crate) fn new(dim: Dimensions, x: f64, y: f64, z: f64, m: f64) -> Self {
        Self { dim, x, y, z, m }
    }
}
impl CoordTrait for OwnedCoord {
    type T = f64;

    fn dim(&self) -> Dimensions {
        self.dim
    }

    fn x(&self) -> Self::T {
        self.x
    }

    fn y(&self) -> Self::T {
        self.y
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        match (self.dim, n) {
            (_, 0) => self.x,
            (_, 1) => self.y,
            (Dimensions::Xyz | Dimensions::Xyzm, 2) => self.z,
            (Dimensions::Xym, 2) | (Dimensions::Xyzm, 3) => self.m,
            _ => panic!("coordinate index {n} is outside {:?}", self.dim),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedPoint {
    dim: Dimensions,
    coord: Option<OwnedCoord>,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedLineString {
    dim: Dimensions,
    coords: Vec<OwnedCoord>,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedPolygon {
    dim: Dimensions,
    exterior: Option<OwnedLineString>,
    interiors: Vec<OwnedLineString>,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedMultiPoint {
    dim: Dimensions,
    points: Vec<OwnedPoint>,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedMultiLineString {
    dim: Dimensions,
    line_strings: Vec<OwnedLineString>,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedMultiPolygon {
    dim: Dimensions,
    polygons: Vec<OwnedPolygon>,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedGeometryCollection {
    dim: Dimensions,
    geometries: Vec<OwnedGeometry>,
}

#[derive(Debug, Clone)]
pub(crate) enum OwnedGeometry {
    Point(OwnedPoint),
    LineString(OwnedLineString),
    Polygon(OwnedPolygon),
    MultiPoint(OwnedMultiPoint),
    MultiLineString(OwnedMultiLineString),
    MultiPolygon(OwnedMultiPolygon),
    GeometryCollection(OwnedGeometryCollection),
}

type OwnedGeometryType<'a> = GeometryType<
    'a,
    OwnedPoint,
    OwnedLineString,
    OwnedPolygon,
    OwnedMultiPoint,
    OwnedMultiLineString,
    OwnedMultiPolygon,
    OwnedGeometryCollection,
    UnimplementedRect<f64>,
    UnimplementedTriangle<f64>,
    UnimplementedLine<f64>,
>;

pub(crate) trait CoordTransform {
    fn apply(&mut self, coord: &impl CoordTrait<T = f64>) -> OwnedCoord;
}

pub(crate) struct IdentityCoordTransform;

impl CoordTransform for IdentityCoordTransform {
    fn apply(&mut self, coord: &impl CoordTrait<T = f64>) -> OwnedCoord {
        let dim = coord.dim();
        let z = matches!(dim, Dimensions::Xyz | Dimensions::Xyzm)
            .then(|| coord.nth(2).unwrap())
            .unwrap_or(0.0);
        let m = match dim {
            Dimensions::Xym => coord.nth(2).unwrap(),
            Dimensions::Xyzm => coord.nth(3).unwrap(),
            _ => 0.0,
        };
        OwnedCoord::new(dim, coord.x(), coord.y(), z, m)
    }
}

impl OwnedGeometry {
    pub(crate) fn from_geometry(
        geometry: &impl GeometryTrait<T = f64>,
        transform: &mut impl CoordTransform,
    ) -> Self {
        match geometry.as_type() {
            GeometryType::Point(point) => Self::Point(OwnedPoint {
                dim: geometry.dim(),
                coord: point.coord().map(|coord| transform.apply(&coord)),
            }),
            GeometryType::LineString(line_string) => Self::LineString(OwnedLineString {
                dim: geometry.dim(),
                coords: line_string
                    .coords()
                    .map(|coord| transform.apply(&coord))
                    .collect(),
            }),
            GeometryType::Polygon(polygon) => {
                Self::Polygon(transform_polygon(polygon, transform, false))
            }
            GeometryType::MultiPoint(multi_point) => Self::MultiPoint(OwnedMultiPoint {
                dim: geometry.dim(),
                points: multi_point
                    .points()
                    .map(|point| OwnedPoint {
                        dim: geometry.dim(),
                        coord: point.coord().map(|coord| transform.apply(&coord)),
                    })
                    .collect(),
            }),
            GeometryType::MultiLineString(multi_line_string) => {
                Self::MultiLineString(OwnedMultiLineString {
                    dim: geometry.dim(),
                    line_strings: multi_line_string
                        .line_strings()
                        .map(|line_string| OwnedLineString {
                            dim: geometry.dim(),
                            coords: line_string
                                .coords()
                                .map(|coord| transform.apply(&coord))
                                .collect(),
                        })
                        .collect(),
                })
            }
            GeometryType::MultiPolygon(multi_polygon) => Self::MultiPolygon(OwnedMultiPolygon {
                dim: geometry.dim(),
                polygons: multi_polygon
                    .polygons()
                    .map(|polygon| transform_polygon(&polygon, transform, true))
                    .collect(),
            }),
            GeometryType::GeometryCollection(collection) => {
                Self::GeometryCollection(OwnedGeometryCollection {
                    dim: geometry.dim(),
                    geometries: collection
                        .geometries()
                        .map(|geometry| Self::from_geometry(&geometry, transform))
                        .collect(),
                })
            }
            GeometryType::Rect(rect) => {
                let min = transform.apply(&rect.min());
                let max = transform.apply(&rect.max());
                let ring = vec![
                    min,
                    min.with_xy(max.x, min.y),
                    max,
                    min.with_xy(min.x, max.y),
                    min,
                ];
                Self::Polygon(OwnedPolygon {
                    dim: geometry.dim(),
                    exterior: Some(OwnedLineString {
                        dim: geometry.dim(),
                        coords: ring,
                    }),
                    interiors: Vec::new(),
                })
            }
            GeometryType::Triangle(triangle) => {
                let mut coords = triangle
                    .coords()
                    .iter()
                    .map(|coord| transform.apply(coord))
                    .collect::<Vec<_>>();
                coords.push(coords[0]);
                Self::Polygon(OwnedPolygon {
                    dim: geometry.dim(),
                    exterior: Some(OwnedLineString {
                        dim: geometry.dim(),
                        coords,
                    }),
                    interiors: Vec::new(),
                })
            }
            GeometryType::Line(line) => Self::LineString(OwnedLineString {
                dim: geometry.dim(),
                coords: line
                    .coords()
                    .iter()
                    .map(|coord| transform.apply(coord))
                    .collect(),
            }),
        }
    }

    pub fn as_point(&self) -> Option<Option<OwnedCoord>> {
        match self {
            Self::Point(point) => Some(point.coord),
            _ => None,
        }
    }

    fn dim(&self) -> Dimensions {
        match self {
            Self::Point(geometry) => geometry.dim,
            Self::LineString(geometry) => geometry.dim,
            Self::Polygon(geometry) => geometry.dim,
            Self::MultiPoint(geometry) => geometry.dim,
            Self::MultiLineString(geometry) => geometry.dim,
            Self::MultiPolygon(geometry) => geometry.dim,
            Self::GeometryCollection(geometry) => geometry.dim,
        }
    }

    fn as_type(&self) -> OwnedGeometryType<'_> {
        match self {
            Self::Point(geometry) => GeometryType::Point(geometry),
            Self::LineString(geometry) => GeometryType::LineString(geometry),
            Self::Polygon(geometry) => GeometryType::Polygon(geometry),
            Self::MultiPoint(geometry) => GeometryType::MultiPoint(geometry),
            Self::MultiLineString(geometry) => GeometryType::MultiLineString(geometry),
            Self::MultiPolygon(geometry) => GeometryType::MultiPolygon(geometry),
            Self::GeometryCollection(geometry) => GeometryType::GeometryCollection(geometry),
        }
    }
}

fn transform_polygon(
    polygon: &impl PolygonTrait<T = f64>,
    transform: &mut impl CoordTransform,
    normalize_empty: bool,
) -> OwnedPolygon {
    let exterior = polygon.exterior().map(|ring| OwnedLineString {
        dim: polygon.dim(),
        coords: ring.coords().map(|coord| transform.apply(&coord)).collect(),
    });
    let exterior = match (exterior, normalize_empty) {
        (None, true) => Some(OwnedLineString {
            dim: polygon.dim(),
            coords: Vec::new(),
        }),
        (exterior, _) => exterior,
    };
    OwnedPolygon {
        dim: polygon.dim(),
        exterior,
        interiors: polygon
            .interiors()
            .map(|ring| OwnedLineString {
                dim: polygon.dim(),
                coords: ring.coords().map(|coord| transform.apply(&coord)).collect(),
            })
            .collect(),
    }
}

/// Returns an identity-copied geometry when GeoArrow builders need an empty MultiPolygon
/// member represented as a polygon with an empty exterior ring instead of no exterior ring.
///
/// `MultiPolygonBuilder::push_multi_polygon` unwraps the exterior of every member, so a legal
/// `MULTIPOLYGON(EMPTY, ...)` must reach it with an empty ring rather than `None`.
pub(crate) fn normalize_empty_multipolygon_members(
    geometry: &impl GeometryTrait<T = f64>,
) -> Option<OwnedGeometry> {
    has_empty_multipolygon_member(geometry)
        .then(|| OwnedGeometry::from_geometry(geometry, &mut IdentityCoordTransform))
}

fn has_empty_multipolygon_member(geometry: &impl GeometryTrait<T = f64>) -> bool {
    match geometry.as_type() {
        GeometryType::MultiPolygon(multi_polygon) => multi_polygon
            .polygons()
            .any(|polygon| polygon.exterior().is_none()),
        GeometryType::GeometryCollection(collection) => collection
            .geometries()
            .any(|child| has_empty_multipolygon_member(&child)),
        _ => false,
    }
}

impl OwnedGeometry {
    /// Flatten a geometry to the single MVT geometry family PostGIS encodes.
    ///
    /// MVT features have one geometry type, so mixed collections are reduced
    /// to polygons, then line strings, then points, matching PostGIS.
    pub(crate) fn mvt_flatten(geometry: &impl GeometryTrait<T = f64>) -> Option<Self> {
        let mut polygons = Vec::new();
        collect_polygons(geometry, &mut polygons);
        if !polygons.is_empty() {
            return Some(if polygons.len() == 1 {
                Self::Polygon(polygons.remove(0))
            } else {
                Self::MultiPolygon(OwnedMultiPolygon {
                    dim: geometry.dim(),
                    polygons,
                })
            });
        }

        let mut line_strings = Vec::new();
        collect_line_strings(geometry, &mut line_strings);
        if !line_strings.is_empty() {
            return Some(if line_strings.len() == 1 {
                Self::LineString(line_strings.remove(0))
            } else {
                Self::MultiLineString(OwnedMultiLineString {
                    dim: geometry.dim(),
                    line_strings,
                })
            });
        }

        let mut points = Vec::new();
        collect_points(geometry, &mut points);
        if points.is_empty() {
            None
        } else if points.len() == 1 {
            Some(Self::Point(points.remove(0)))
        } else {
            Some(Self::MultiPoint(OwnedMultiPoint {
                dim: geometry.dim(),
                points,
            }))
        }
    }

    /// Keep only points inside an inclusive XY rectangle.
    pub(crate) fn mvt_filter_points(&self, bounds: [f64; 4]) -> Option<Self> {
        let mut points = Vec::new();
        collect_owned_points(self, bounds, &mut points);
        if points.is_empty() {
            None
        } else if points.len() == 1 {
            Some(Self::Point(points.remove(0)))
        } else {
            Some(Self::MultiPoint(OwnedMultiPoint {
                dim: self.dim(),
                points,
            }))
        }
    }

    /// Return finite XY bounds, or `None` when the geometry has no coordinates.
    pub(crate) fn mvt_bounds(&self) -> Option<[f64; 4]> {
        let mut bounds: Option<[f64; 4]> = None;
        update_bounds(self, &mut bounds);
        bounds
    }

    pub(crate) fn round_xy(&mut self) {
        match self {
            Self::Point(point) => {
                if let Some(coord) = &mut point.coord {
                    coord.rotate_xy();
                }
            }
            Self::LineString(line_string) => {
                for coord in &mut line_string.coords {
                    coord.rotate_xy();
                }
            }
            Self::Polygon(polygon) => {
                if let Some(exterior) = &mut polygon.exterior {
                    for coord in &mut exterior.coords {
                        coord.rotate_xy();
                    }
                }
                for interior in &mut polygon.interiors {
                    for coord in &mut interior.coords {
                        coord.rotate_xy();
                    }
                }
            }
            Self::MultiPoint(multi_point) => {
                for point in &mut multi_point.points {
                    if let Some(coord) = &mut point.coord {
                        coord.rotate_xy();
                    }
                }
            }
            Self::MultiLineString(multi_line_string) => {
                for line_string in &mut multi_line_string.line_strings {
                    for coord in &mut line_string.coords {
                        coord.rotate_xy();
                    }
                }
            }
            Self::MultiPolygon(multi_polygon) => {
                for polygon in &mut multi_polygon.polygons {
                    if let Some(exterior) = &mut polygon.exterior {
                        for coord in &mut exterior.coords {
                            coord.rotate_xy();
                        }
                    }
                    for interior in &mut polygon.interiors {
                        for coord in &mut interior.coords {
                            coord.rotate_xy();
                        }
                    }
                }
            }
            Self::GeometryCollection(collection) => {
                for geometry in &mut collection.geometries {
                    geometry.round_xy();
                }
            }
        }
    }
}

fn collect_polygons(geometry: &impl GeometryTrait<T = f64>, out: &mut Vec<OwnedPolygon>) {
    match geometry.as_type() {
        GeometryType::Polygon(polygon) => {
            if polygon_has_coordinates(polygon) {
                out.push(owned_polygon(polygon));
            }
        }
        GeometryType::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon.polygons() {
                if polygon_has_coordinates(&polygon) {
                    out.push(owned_polygon(&polygon));
                }
            }
        }
        GeometryType::Rect(rect) => {
            if let OwnedGeometry::Polygon(polygon) =
                OwnedGeometry::from_geometry(rect, &mut IdentityCoordTransform)
            {
                out.push(polygon);
            }
        }
        GeometryType::Triangle(triangle) => {
            if let OwnedGeometry::Polygon(polygon) =
                OwnedGeometry::from_geometry(triangle, &mut IdentityCoordTransform)
            {
                out.push(polygon);
            }
        }
        GeometryType::GeometryCollection(collection) => {
            for child in collection.geometries() {
                collect_polygons(&child, out);
            }
        }
        _ => {}
    }
}

fn collect_line_strings(geometry: &impl GeometryTrait<T = f64>, out: &mut Vec<OwnedLineString>) {
    match geometry.as_type() {
        GeometryType::LineString(line_string) => {
            if line_string.num_coords() > 0 {
                out.push(owned_line_string(line_string));
            }
        }
        GeometryType::MultiLineString(multi_line_string) => {
            for line_string in multi_line_string.line_strings() {
                if line_string.num_coords() > 0 {
                    out.push(owned_line_string(&line_string));
                }
            }
        }
        GeometryType::Line(line) => {
            if let OwnedGeometry::LineString(line_string) =
                OwnedGeometry::from_geometry(line, &mut IdentityCoordTransform)
            {
                out.push(line_string);
            }
        }
        GeometryType::GeometryCollection(collection) => {
            for child in collection.geometries() {
                collect_line_strings(&child, out);
            }
        }
        _ => {}
    }
}

fn collect_points(geometry: &impl GeometryTrait<T = f64>, out: &mut Vec<OwnedPoint>) {
    match geometry.as_type() {
        GeometryType::Point(point) => {
            if let Some(coord) = point.coord() {
                out.push(OwnedPoint {
                    dim: point.dim(),
                    coord: Some(OwnedCoord::from_coord(&coord)),
                });
            }
        }
        GeometryType::MultiPoint(multi_point) => {
            for point in multi_point.points() {
                if let Some(coord) = point.coord() {
                    out.push(OwnedPoint {
                        dim: point.dim(),
                        coord: Some(OwnedCoord::from_coord(&coord)),
                    });
                }
            }
        }
        GeometryType::GeometryCollection(collection) => {
            for child in collection.geometries() {
                collect_points(&child, out);
            }
        }
        _ => {}
    }
}

fn collect_owned_points(geometry: &OwnedGeometry, bounds: [f64; 4], out: &mut Vec<OwnedPoint>) {
    match geometry {
        OwnedGeometry::Point(point) => {
            if let Some(coord) = point.coord
                && coord.x >= bounds[0]
                && coord.y >= bounds[1]
                && coord.x <= bounds[2]
                && coord.y <= bounds[3]
            {
                out.push(point.clone());
            }
        }
        OwnedGeometry::MultiPoint(multi_point) => {
            for point in &multi_point.points {
                if let Some(coord) = point.coord
                    && coord.x >= bounds[0]
                    && coord.y >= bounds[1]
                    && coord.x <= bounds[2]
                    && coord.y <= bounds[3]
                {
                    out.push(point.clone());
                }
            }
        }
        OwnedGeometry::GeometryCollection(collection) => {
            for child in &collection.geometries {
                collect_owned_points(child, bounds, out);
            }
        }
        _ => {}
    }
}

fn update_bounds(geometry: &OwnedGeometry, bounds: &mut Option<[f64; 4]>) {
    match geometry {
        OwnedGeometry::Point(point) => {
            if let Some(coord) = point.coord {
                update_bounds_with_coord(bounds, &coord);
            }
        }
        OwnedGeometry::LineString(line_string) => {
            for coord in &line_string.coords {
                update_bounds_with_coord(bounds, coord);
            }
        }
        OwnedGeometry::Polygon(polygon) => {
            if let Some(exterior) = &polygon.exterior {
                for coord in &exterior.coords {
                    update_bounds_with_coord(bounds, coord);
                }
            }
            for interior in &polygon.interiors {
                for coord in &interior.coords {
                    update_bounds_with_coord(bounds, coord);
                }
            }
        }
        OwnedGeometry::MultiPoint(multi_point) => {
            for point in &multi_point.points {
                if let Some(coord) = point.coord {
                    update_bounds_with_coord(bounds, &coord);
                }
            }
        }
        OwnedGeometry::MultiLineString(multi_line_string) => {
            for line_string in &multi_line_string.line_strings {
                for coord in &line_string.coords {
                    update_bounds_with_coord(bounds, coord);
                }
            }
        }
        OwnedGeometry::MultiPolygon(multi_polygon) => {
            for polygon in &multi_polygon.polygons {
                if let Some(exterior) = &polygon.exterior {
                    for coord in &exterior.coords {
                        update_bounds_with_coord(bounds, coord);
                    }
                }
                for interior in &polygon.interiors {
                    for coord in &interior.coords {
                        update_bounds_with_coord(bounds, coord);
                    }
                }
            }
        }
        OwnedGeometry::GeometryCollection(collection) => {
            for child in &collection.geometries {
                update_bounds(child, bounds);
            }
        }
    }
}

fn update_bounds_with_coord(bounds: &mut Option<[f64; 4]>, coord: &OwnedCoord) {
    if !coord.x.is_finite() || !coord.y.is_finite() {
        return;
    }
    match bounds {
        Some(current) => {
            current[0] = current[0].min(coord.x);
            current[1] = current[1].min(coord.y);
            current[2] = current[2].max(coord.x);
            current[3] = current[3].max(coord.y);
        }
        None => *bounds = Some([coord.x, coord.y, coord.x, coord.y]),
    }
}

fn owned_line_string(line_string: &impl LineStringTrait<T = f64>) -> OwnedLineString {
    OwnedLineString {
        dim: line_string.dim(),
        coords: line_string
            .coords()
            .map(|coord| OwnedCoord::from_coord(&coord))
            .collect(),
    }
}

fn owned_polygon(polygon: &impl PolygonTrait<T = f64>) -> OwnedPolygon {
    OwnedPolygon {
        dim: polygon.dim(),
        exterior: polygon.exterior().map(|ring| owned_line_string(&ring)),
        interiors: polygon
            .interiors()
            .map(|ring| owned_line_string(&ring))
            .collect(),
    }
}

fn polygon_has_coordinates(polygon: &impl PolygonTrait<T = f64>) -> bool {
    polygon.exterior().is_some_and(|ring| ring.num_coords() > 0)
}

macro_rules! impl_owned_geometry {
    ($ty:ty, $variant:ident) => {
        impl GeometryTrait for $ty {
            type T = f64;
            type PointType<'a>
                = OwnedPoint
            where
                Self: 'a;
            type LineStringType<'a>
                = OwnedLineString
            where
                Self: 'a;
            type PolygonType<'a>
                = OwnedPolygon
            where
                Self: 'a;
            type MultiPointType<'a>
                = OwnedMultiPoint
            where
                Self: 'a;
            type MultiLineStringType<'a>
                = OwnedMultiLineString
            where
                Self: 'a;
            type MultiPolygonType<'a>
                = OwnedMultiPolygon
            where
                Self: 'a;
            type GeometryCollectionType<'a>
                = OwnedGeometryCollection
            where
                Self: 'a;
            type RectType<'a>
                = UnimplementedRect<f64>
            where
                Self: 'a;
            type TriangleType<'a>
                = UnimplementedTriangle<f64>
            where
                Self: 'a;
            type LineType<'a>
                = UnimplementedLine<f64>
            where
                Self: 'a;

            fn dim(&self) -> Dimensions {
                self.dim
            }

            fn as_type(&self) -> OwnedGeometryType<'_> {
                GeometryType::$variant(self)
            }
        }
    };
}

impl_owned_geometry!(OwnedPoint, Point);
impl_owned_geometry!(OwnedLineString, LineString);
impl_owned_geometry!(OwnedPolygon, Polygon);
impl_owned_geometry!(OwnedMultiPoint, MultiPoint);
impl_owned_geometry!(OwnedMultiLineString, MultiLineString);
impl_owned_geometry!(OwnedMultiPolygon, MultiPolygon);
impl_owned_geometry!(OwnedGeometryCollection, GeometryCollection);

impl GeometryTrait for OwnedGeometry {
    type T = f64;
    type PointType<'a>
        = OwnedPoint
    where
        Self: 'a;
    type LineStringType<'a>
        = OwnedLineString
    where
        Self: 'a;
    type PolygonType<'a>
        = OwnedPolygon
    where
        Self: 'a;
    type MultiPointType<'a>
        = OwnedMultiPoint
    where
        Self: 'a;
    type MultiLineStringType<'a>
        = OwnedMultiLineString
    where
        Self: 'a;
    type MultiPolygonType<'a>
        = OwnedMultiPolygon
    where
        Self: 'a;
    type GeometryCollectionType<'a>
        = OwnedGeometryCollection
    where
        Self: 'a;
    type RectType<'a>
        = UnimplementedRect<f64>
    where
        Self: 'a;
    type TriangleType<'a>
        = UnimplementedTriangle<f64>
    where
        Self: 'a;
    type LineType<'a>
        = UnimplementedLine<f64>
    where
        Self: 'a;

    fn dim(&self) -> Dimensions {
        self.dim()
    }

    fn as_type(&self) -> OwnedGeometryType<'_> {
        self.as_type()
    }
}

impl PointTrait for OwnedPoint {
    type CoordType<'a>
        = OwnedCoord
    where
        Self: 'a;

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        self.coord
    }
}

impl LineStringTrait for OwnedLineString {
    type CoordType<'a>
        = OwnedCoord
    where
        Self: 'a;

    fn num_coords(&self) -> usize {
        self.coords.len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        unsafe { *self.coords.get_unchecked(i) }
    }
}

impl PolygonTrait for OwnedPolygon {
    type RingType<'a>
        = OwnedLineString
    where
        Self: 'a;

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        self.exterior.clone()
    }

    fn num_interiors(&self) -> usize {
        self.interiors.len()
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        unsafe { self.interiors.get_unchecked(i).clone() }
    }
}

impl MultiPointTrait for OwnedMultiPoint {
    type InnerPointType<'a>
        = OwnedPoint
    where
        Self: 'a;

    fn num_points(&self) -> usize {
        self.points.len()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::InnerPointType<'_> {
        unsafe { self.points.get_unchecked(i).clone() }
    }
}

impl MultiLineStringTrait for OwnedMultiLineString {
    type InnerLineStringType<'a>
        = OwnedLineString
    where
        Self: 'a;

    fn num_line_strings(&self) -> usize {
        self.line_strings.len()
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::InnerLineStringType<'_> {
        unsafe { self.line_strings.get_unchecked(i).clone() }
    }
}

impl MultiPolygonTrait for OwnedMultiPolygon {
    type InnerPolygonType<'a>
        = OwnedPolygon
    where
        Self: 'a;

    fn num_polygons(&self) -> usize {
        self.polygons.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::InnerPolygonType<'_> {
        unsafe { self.polygons.get_unchecked(i).clone() }
    }
}

impl GeometryCollectionTrait for OwnedGeometryCollection {
    type GeometryType<'a>
        = OwnedGeometry
    where
        Self: 'a;

    fn num_geometries(&self) -> usize {
        self.geometries.len()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        unsafe { self.geometries.get_unchecked(i).clone() }
    }
}
