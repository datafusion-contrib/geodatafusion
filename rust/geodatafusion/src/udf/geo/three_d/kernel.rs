//! Small explicit 3D distance/intersection kernels.
//!
//! The kernel works on copied point primitives instead of introducing a new
//! geometry runtime. Polygons are treated as planar surfaces bounded by their
//! rings; when a ring is non-planar the best-fit plane of the exterior ring is
//! used and boundary distances are still computed on the true 3D segments.

use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    LineTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
    RectTrait, TriangleTrait,
};
use robust::{Coord as RobustCoord, Coord3D as RobustCoord3D, orient2d, orient3d};

use crate::error::{GeoDataFusionError, GeoDataFusionResult};

/// A copy of one coordinate with an explicit Z flag.
///
/// `has_z` distinguishes a real Z ordinate from a missing one. XYM input has
/// no Z and is treated exactly like XY by every 3D function.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Point3 {
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) z: f64,
    pub(crate) has_z: bool,
}

impl Point3 {
    fn with_xy(&self, x: f64, y: f64) -> Self {
        Self { x, y, ..*self }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Metric {
    Full,
    Xy,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Atom {
    Point(Point3),
    Segment { a: Point3, b: Point3 },
    Face { rings: Vec<Vec<Point3>> },
}

#[derive(Debug, Clone)]
pub(crate) struct GeometryModel {
    pub(crate) atoms: Vec<Atom>,
    /// Maximum-distance candidates. Polygons contribute only their exterior
    /// ring, matching PostGIS's vertex-pair definition.
    pub(crate) vertices: Vec<Point3>,
    pub(crate) has_z: bool,
    pub(crate) non_finite: bool,
}

impl GeometryModel {
    pub(crate) fn is_empty(&self) -> bool {
        self.atoms.is_empty()
    }

    pub(crate) fn z_range(&self) -> Option<(f64, f64)> {
        if !self.has_z {
            return None;
        }
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for point in self.all_points() {
            if point.has_z {
                min = min.min(point.z);
                max = max.max(point.z);
            }
        }
        if min.is_finite() && max.is_finite() {
            Some((min, max))
        } else {
            None
        }
    }

    fn all_points(&self) -> impl Iterator<Item = Point3> + '_ {
        self.atoms.iter().flat_map(|atom| match atom {
            Atom::Point(point) => vec![*point],
            Atom::Segment { a, b } => vec![*a, *b],
            Atom::Face { rings } => rings.iter().flatten().copied().collect(),
        })
    }
}

#[derive(Debug)]
struct Flattener {
    atoms: Vec<Atom>,
    vertices: Vec<Point3>,
    saw_z: bool,
    saw_xy: bool,
    non_finite: bool,
}

impl Flattener {
    fn new() -> Self {
        Self {
            atoms: Vec::new(),
            vertices: Vec::new(),
            saw_z: false,
            saw_xy: false,
            non_finite: false,
        }
    }

    fn point(&mut self, coord: &impl CoordTrait<T = f64>) -> GeoDataFusionResult<Point3> {
        let (z, has_z) = match coord.dim() {
            Dimensions::Xy | Dimensions::Xym => {
                self.saw_xy = true;
                (0.0, false)
            }
            Dimensions::Xyz | Dimensions::Xyzm => {
                self.saw_z = true;
                (coord.nth(2).unwrap_or(0.0), true)
            }
            Dimensions::Unknown(_) => {
                return Err(GeoDataFusionError::DataFusion(
                    datafusion::error::DataFusionError::Execution(
                        "ST_3D* functions require known coordinate dimensions".to_string(),
                    ),
                ));
            }
        };
        let x = coord.x();
        let y = coord.y();
        self.non_finite |= !x.is_finite() || !y.is_finite() || (has_z && !z.is_finite());
        Ok(Point3 { x, y, z, has_z })
    }

    fn add(&mut self, geometry: &impl GeometryTrait<T = f64>) -> GeoDataFusionResult<()> {
        match geometry.as_type() {
            GeometryType::Point(point) => {
                let Some(coord) = point.coord() else {
                    return Ok(());
                };
                let point = self.point(&coord)?;
                self.atoms.push(Atom::Point(point));
                self.vertices.push(point);
            }
            GeometryType::LineString(line_string) => {
                let points = line_string
                    .coords()
                    .map(|coord| self.point(&coord))
                    .collect::<GeoDataFusionResult<Vec<_>>>()?;
                self.vertices.extend(points.iter().copied());
                for pair in points.windows(2) {
                    self.atoms.push(Atom::Segment {
                        a: pair[0],
                        b: pair[1],
                    });
                }
            }
            GeometryType::Polygon(polygon) => {
                let mut rings = Vec::new();
                if let Some(exterior) = polygon.exterior() {
                    let ring = exterior
                        .coords()
                        .map(|coord| self.point(&coord))
                        .collect::<GeoDataFusionResult<Vec<_>>>()?;
                    self.vertices.extend(ring.iter().copied());
                    rings.push(ring);
                }
                for interior in polygon.interiors() {
                    rings.push(
                        interior
                            .coords()
                            .map(|coord| self.point(&coord))
                            .collect::<GeoDataFusionResult<Vec<_>>>()?,
                    );
                }
                if !rings.is_empty() {
                    self.atoms.push(Atom::Face { rings });
                }
            }
            GeometryType::MultiPoint(multi_point) => {
                for point in multi_point.points() {
                    self.add(&point)?;
                }
            }
            GeometryType::MultiLineString(multi_line_string) => {
                for line_string in multi_line_string.line_strings() {
                    self.add(&line_string)?;
                }
            }
            GeometryType::MultiPolygon(multi_polygon) => {
                for polygon in multi_polygon.polygons() {
                    self.add(&polygon)?;
                }
            }
            GeometryType::GeometryCollection(collection) => {
                for geometry in collection.geometries() {
                    self.add(&geometry)?;
                }
            }
            GeometryType::Rect(rect) => {
                let min = self.point(&rect.min())?;
                let max = self.point(&rect.max())?;
                let ring = vec![
                    min,
                    min.with_xy(max.x, min.y),
                    max,
                    min.with_xy(min.x, max.y),
                    min,
                ];
                self.vertices.extend(ring.iter().copied());
                self.atoms.push(Atom::Face { rings: vec![ring] });
            }
            GeometryType::Triangle(triangle) => {
                let mut ring = triangle
                    .coords()
                    .iter()
                    .map(|coord| self.point(coord))
                    .collect::<GeoDataFusionResult<Vec<_>>>()?;
                if let Some(first) = ring.first().copied() {
                    ring.push(first);
                }
                if !ring.is_empty() {
                    self.vertices.extend(ring.iter().copied());
                    self.atoms.push(Atom::Face { rings: vec![ring] });
                }
            }
            GeometryType::Line(line) => {
                let points = line
                    .coords()
                    .iter()
                    .map(|coord| self.point(coord))
                    .collect::<GeoDataFusionResult<Vec<_>>>()?;
                self.vertices.extend(points.iter().copied());
                if points.len() == 2 {
                    self.atoms.push(Atom::Segment {
                        a: points[0],
                        b: points[1],
                    });
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> GeoDataFusionResult<GeometryModel> {
        if self.saw_z && self.saw_xy {
            return Err(GeoDataFusionError::DataFusion(
                datafusion::error::DataFusionError::Execution(
                    "ST_3D* functions require consistent coordinate dimensions inside one geometry"
                        .to_string(),
                ),
            ));
        }
        Ok(GeometryModel {
            atoms: self.atoms,
            vertices: self.vertices,
            has_z: self.saw_z,
            non_finite: self.non_finite,
        })
    }
}

pub(crate) fn flatten(
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoDataFusionResult<GeometryModel> {
    let mut flattener = Flattener::new();
    flattener.add(geometry)?;
    flattener.finish()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Candidate {
    pub(crate) p1: Point3,
    pub(crate) p2: Point3,
    pub(crate) distance: f64,
}

impl Candidate {
    fn swapped(self) -> Self {
        Self {
            p1: self.p2,
            p2: self.p1,
            ..self
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sweep {
    Min,
    Max,
}

impl Sweep {
    fn is_better(self, current: f64, candidate: f64) -> bool {
        match self {
            Self::Min => candidate < current,
            Self::Max => candidate > current,
        }
    }
}

fn update(best: &mut Option<Candidate>, candidate: Candidate, sweep: Sweep) {
    if !candidate.distance.is_finite() {
        return;
    }
    match best {
        Some(current) if !sweep.is_better(current.distance, candidate.distance) => {}
        _ => *best = Some(candidate),
    }
}

fn distance(a: Point3, b: Point3, metric: Metric) -> f64 {
    let dx = b.x - a.x;
    let dy = b.y - a.y;
    if metric == Metric::Xy {
        (dx * dx + dy * dy).sqrt()
    } else {
        let dz = b.z - a.z;
        (dx * dx + dy * dy + dz * dz).sqrt()
    }
}

fn lerp(a: Point3, b: Point3, t: f64) -> Point3 {
    Point3 {
        x: a.x + t * (b.x - a.x),
        y: a.y + t * (b.y - a.y),
        z: a.z + t * (b.z - a.z),
        has_z: a.has_z || b.has_z,
    }
}

fn point_segment(point: Point3, a: Point3, b: Point3, metric: Metric) -> Candidate {
    if a.x == b.x && a.y == b.y && a.z == b.z {
        return Candidate {
            p1: point,
            p2: a,
            distance: distance(point, a, metric),
        };
    }
    let dx = b.x - a.x;
    let dy = b.y - a.y;
    let dz = b.z - a.z;
    let denominator = if metric == Metric::Xy {
        dx * dx + dy * dy
    } else {
        dx * dx + dy * dy + dz * dz
    };
    if denominator == 0.0 {
        return Candidate {
            p1: point,
            p2: a,
            distance: distance(point, a, metric),
        };
    }
    let t = if metric == Metric::Xy {
        ((point.x - a.x) * dx + (point.y - a.y) * dy) / denominator
    } else {
        ((point.x - a.x) * dx + (point.y - a.y) * dy + (point.z - a.z) * dz) / denominator
    };
    let closest = lerp(a, b, t.clamp(0.0, 1.0));
    Candidate {
        p1: point,
        p2: closest,
        distance: distance(point, closest, metric),
    }
}

fn segment_segment(a: Point3, b: Point3, c: Point3, d: Point3, metric: Metric) -> Candidate {
    let z_da = if metric == Metric::Xy { 0.0 } else { b.z - a.z };
    let z_db = if metric == Metric::Xy { 0.0 } else { d.z - c.z };
    let z_r = if metric == Metric::Xy { 0.0 } else { a.z - c.z };
    let da = [b.x - a.x, b.y - a.y, z_da];
    let db = [d.x - c.x, d.y - c.y, z_db];
    let r = [a.x - c.x, a.y - c.y, z_r];

    let a_len = da[0] * da[0] + da[1] * da[1] + da[2] * da[2];
    let b_dot = da[0] * db[0] + da[1] * db[1] + da[2] * db[2];
    let c_len = r[0] * da[0] + r[1] * da[1] + r[2] * da[2];
    let d_len = db[0] * db[0] + db[1] * db[1] + db[2] * db[2];
    let e_dot = r[0] * db[0] + r[1] * db[1] + r[2] * db[2];
    let denom = a_len * d_len - b_dot * b_dot;

    if a_len == 0.0 && d_len == 0.0 {
        return Candidate {
            p1: a,
            p2: c,
            distance: distance(a, c, metric),
        };
    }
    if a_len == 0.0 {
        return point_segment(a, c, d, metric);
    }
    if d_len == 0.0 {
        return point_segment(c, a, b, metric);
    }

    if denom == 0.0 {
        // Parallel segments: the closest pair is found through the four
        // endpoint-to-segment combinations, which also covers collinear
        // overlaps.
        let mut best = Some(point_segment(a, c, d, metric));
        update(&mut best, point_segment(b, c, d, metric), Sweep::Min);
        update(
            &mut best,
            point_segment(c, a, b, metric).swapped(),
            Sweep::Min,
        );
        update(
            &mut best,
            point_segment(d, a, b, metric).swapped(),
            Sweep::Min,
        );
        return best.expect("endpoint-segment combinations always produce a candidate");
    }

    let mut s = (b_dot * e_dot - c_len * d_len) / denom;
    let mut t = if d_len == 0.0 {
        0.0
    } else {
        (b_dot * s + e_dot) / d_len
    };

    if t < 0.0 {
        t = 0.0;
        s = (-c_len / a_len).clamp(0.0, 1.0);
    } else if t > 1.0 {
        t = 1.0;
        s = ((b_dot - c_len) / a_len).clamp(0.0, 1.0);
    }
    s = s.clamp(0.0, 1.0);

    let p1 = lerp(a, b, s);
    let p2 = lerp(c, d, t);
    Candidate {
        p1,
        p2,
        distance: distance(p1, p2, metric),
    }
}

#[derive(Debug, Clone, Copy)]
struct Plane {
    normal: [f64; 3],
    offset: f64,
}

impl Plane {
    fn signed_distance(self, point: Point3) -> f64 {
        self.normal[0] * point.x + self.normal[1] * point.y + self.normal[2] * point.z + self.offset
    }

    fn project(self, point: Point3) -> Point3 {
        let value = self.signed_distance(point);
        Point3 {
            x: point.x - self.normal[0] * value,
            y: point.y - self.normal[1] * value,
            z: point.z - self.normal[2] * value,
            has_z: point.has_z,
        }
    }
}

fn face_plane(face: &Atom) -> Option<Plane> {
    let Atom::Face { rings } = face else {
        return None;
    };
    let ring = rings.first()?;
    if ring.len() < 3 {
        return None;
    }

    // Newell's method gives a stable normal for a ring, including non-planar
    // input for which a single best-fit plane is still the documented limit.
    let mut normal = [0.0_f64; 3];
    let mut count = 0.0;
    let mut centroid = Point3 {
        x: 0.0,
        y: 0.0,
        z: 0.0,
        has_z: false,
    };
    for index in 0..ring.len() {
        let current = ring[index];
        let next = ring[(index + 1) % ring.len()];
        normal[0] += (current.y - next.y) * (current.z + next.z);
        normal[1] += (current.z - next.z) * (current.x + next.x);
        normal[2] += (current.x - next.x) * (current.y + next.y);
        centroid.x += current.x;
        centroid.y += current.y;
        centroid.z += current.z;
        count += 1.0;
    }
    let length_squared = normal[0] * normal[0] + normal[1] * normal[1] + normal[2] * normal[2];
    if length_squared == 0.0 || !length_squared.is_finite() {
        return None;
    }
    let length = length_squared.sqrt();
    let normal = [normal[0] / length, normal[1] / length, normal[2] / length];
    let centroid_x = centroid.x / count;
    let centroid_y = centroid.y / count;
    let centroid_z = centroid.z / count;
    let offset = -(normal[0] * centroid_x + normal[1] * centroid_y + normal[2] * centroid_z);
    Some(Plane { normal, offset })
}

fn local_tolerance(points: impl IntoIterator<Item = Point3>) -> f64 {
    let mut scale = 1.0_f64;
    for point in points {
        scale = scale
            .max(point.x.abs())
            .max(point.y.abs())
            .max(point.z.abs());
    }
    1e-12 * scale
}

fn ray_crossing(point: (f64, f64), ring: &[(f64, f64)], tolerance: f64) -> Option<bool> {
    let mut inside = false;
    for index in 0..ring.len() {
        let a = ring[index];
        let b = ring[(index + 1) % ring.len()];
        let cross = (b.0 - a.0) * (point.1 - a.1) - (b.1 - a.1) * (point.0 - a.0);
        if cross.abs() <= tolerance
            && point.0 >= a.0.min(b.0) - tolerance
            && point.0 <= a.0.max(b.0) + tolerance
            && point.1 >= a.1.min(b.1) - tolerance
            && point.1 <= a.1.max(b.1) + tolerance
        {
            return None;
        }
        if (a.1 > point.1) != (b.1 > point.1) {
            let x_intersection = a.0 + (point.1 - a.1) * (b.0 - a.0) / (b.1 - a.1);
            if point.0 < x_intersection {
                inside = !inside;
            }
        }
    }
    Some(inside)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaceLocation {
    Inside,
    Boundary,
    Outside,
    Hole(usize),
}

fn face_location(point: Point3, face: &Atom, metric: Metric, tolerance: f64) -> FaceLocation {
    let Atom::Face { rings } = face else {
        return FaceLocation::Outside;
    };
    let Some(exterior) = rings.first() else {
        return FaceLocation::Outside;
    };

    let project = |value: Point3| -> (f64, f64) {
        if metric == Metric::Xy {
            (value.x, value.y)
        } else if let Some(plane) = face_plane(face) {
            let projected = plane.project(value);
            if plane.normal[2].abs() >= plane.normal[0].abs()
                && plane.normal[2].abs() >= plane.normal[1].abs()
            {
                (projected.x, projected.y)
            } else if plane.normal[0].abs() >= plane.normal[1].abs() {
                (projected.y, projected.z)
            } else {
                (projected.x, projected.z)
            }
        } else {
            (value.x, value.y)
        }
    };

    let screen = project(point);
    let exterior_screen = exterior.iter().copied().map(project).collect::<Vec<_>>();
    match ray_crossing(screen, &exterior_screen, tolerance) {
        None => return FaceLocation::Boundary,
        Some(false) => return FaceLocation::Outside,
        Some(true) => {}
    }

    for (index, hole) in rings.iter().enumerate().skip(1) {
        let hole_screen = hole.iter().copied().map(project).collect::<Vec<_>>();
        match ray_crossing(screen, &hole_screen, tolerance) {
            None => return FaceLocation::Boundary,
            Some(true) => return FaceLocation::Hole(index - 1),
            Some(false) => {}
        }
    }
    FaceLocation::Inside
}

fn point_to_ring(point: Point3, ring: &[Point3], metric: Metric) -> Option<Candidate> {
    let mut best = None;
    let mut previous = ring.first().copied()?;
    for current in ring.iter().skip(1) {
        update(
            &mut best,
            point_segment(point, previous, *current, metric),
            Sweep::Min,
        );
        previous = *current;
    }
    best
}

fn point_face(point: Point3, face: &Atom, metric: Metric) -> Option<Candidate> {
    let Atom::Face { rings } = face else {
        return None;
    };
    let exterior = rings.first()?;

    let plane = match (metric == Metric::Full).then(|| face_plane(face)).flatten() {
        Some(plane) => plane,
        None => {
            let tolerance = local_tolerance(
                rings
                    .iter()
                    .flatten()
                    .copied()
                    .chain(std::iter::once(point)),
            );
            return match face_location(point, face, Metric::Xy, tolerance) {
                FaceLocation::Inside | FaceLocation::Boundary => Some(Candidate {
                    p1: point,
                    p2: Point3 {
                        x: point.x,
                        y: point.y,
                        z: 0.0,
                        has_z: false,
                    },
                    distance: 0.0,
                }),
                FaceLocation::Outside => point_to_ring(point, exterior, Metric::Xy),
                FaceLocation::Hole(index) => point_to_ring(point, &rings[index + 1], Metric::Xy),
            };
        }
    };

    let tolerance = local_tolerance(
        rings
            .iter()
            .flatten()
            .copied()
            .chain(std::iter::once(point)),
    );
    let projected = plane.project(point);
    match face_location(point, face, Metric::Full, tolerance) {
        FaceLocation::Inside => Some(Candidate {
            p1: point,
            p2: projected,
            distance: distance(point, projected, Metric::Full),
        }),
        FaceLocation::Boundary => Some(Candidate {
            p1: point,
            p2: projected,
            distance: distance(point, projected, Metric::Full),
        }),
        FaceLocation::Outside => point_to_ring(point, exterior, Metric::Full),
        FaceLocation::Hole(index) => point_to_ring(point, &rings[index + 1], Metric::Full),
    }
}

fn point_in_face_check(point: Point3, face: &Atom, metric: Metric, tolerance: f64) -> bool {
    matches!(
        face_location(point, face, metric, tolerance),
        FaceLocation::Inside | FaceLocation::Boundary
    )
}

fn segment_face(a: Point3, b: Point3, face: &Atom, metric: Metric) -> Option<Candidate> {
    let Atom::Face { rings } = face else {
        return None;
    };
    let mut best = None;
    update(&mut best, point_face(a, face, metric)?, Sweep::Min);
    update(&mut best, point_face(b, face, metric)?, Sweep::Min);

    let tolerance = local_tolerance(rings.iter().flatten().copied().chain([a, b]));
    if metric == Metric::Full {
        if let Some(plane) = face_plane(face) {
            let s1 = plane.signed_distance(a);
            let s2 = plane.signed_distance(b);
            if s1 == 0.0 && point_in_face_check(a, face, Metric::Full, tolerance) {
                update(
                    &mut best,
                    Candidate {
                        p1: a,
                        p2: a,
                        distance: 0.0,
                    },
                    Sweep::Min,
                );
            }
            if s2 == 0.0 && point_in_face_check(b, face, Metric::Full, tolerance) {
                update(
                    &mut best,
                    Candidate {
                        p1: b,
                        p2: b,
                        distance: 0.0,
                    },
                    Sweep::Min,
                );
            }
            if (s1 < 0.0 && s2 > 0.0) || (s1 > 0.0 && s2 < 0.0) {
                let t = s1 / (s1 - s2);
                let intersection = lerp(a, b, t);
                if point_in_face_check(intersection, face, Metric::Full, tolerance) {
                    update(
                        &mut best,
                        Candidate {
                            p1: intersection,
                            p2: intersection,
                            distance: 0.0,
                        },
                        Sweep::Min,
                    );
                }
            }
        }
    } else if point_in_face_check(a, face, Metric::Xy, tolerance)
        || point_in_face_check(b, face, Metric::Xy, tolerance)
    {
        update(
            &mut best,
            Candidate {
                p1: a,
                p2: a,
                distance: 0.0,
            },
            Sweep::Min,
        );
    }

    for ring in rings {
        let mut previous = ring.first().copied()?;
        for current in ring.iter().skip(1) {
            update(
                &mut best,
                segment_segment(a, b, previous, *current, metric),
                Sweep::Min,
            );
            previous = *current;
        }
    }
    best
}

fn face_face(left: &Atom, right: &Atom, metric: Metric) -> Option<Candidate> {
    let Atom::Face { rings: left_rings } = left else {
        return None;
    };
    let Atom::Face { rings: right_rings } = right else {
        return None;
    };
    let mut best = None;

    for left_ring in left_rings {
        for (&left_previous, &left_current) in left_ring.iter().zip(left_ring.iter().skip(1)) {
            for right_ring in right_rings {
                for (&right_previous, &right_current) in
                    right_ring.iter().zip(right_ring.iter().skip(1))
                {
                    update(
                        &mut best,
                        segment_segment(
                            left_previous,
                            left_current,
                            right_previous,
                            right_current,
                            metric,
                        ),
                        Sweep::Min,
                    );
                }
            }
        }
    }

    // A boundary edge can cross the other filled polygon's plane at an
    // interior point without touching the other boundary. Test every edge
    // against the full face, mirroring PostGIS's pointarray-polygon pass.
    for left_ring in left_rings {
        for (&previous, &current) in left_ring.iter().zip(left_ring.iter().skip(1)) {
            if let Some(candidate) = segment_face(previous, current, right, metric) {
                update(&mut best, candidate, Sweep::Min);
            }
        }
    }
    for right_ring in right_rings {
        for (&previous, &current) in right_ring.iter().zip(right_ring.iter().skip(1)) {
            if let Some(candidate) = segment_face(previous, current, left, metric) {
                update(&mut best, candidate.swapped(), Sweep::Min);
            }
        }
    }

    // PostGIS checks exterior-ring vertices against the other filled polygon
    // before falling back to boundary segments.
    if let Some(left_exterior) = left_rings.first() {
        for vertex in left_exterior {
            if let Some(candidate) = point_face(*vertex, right, metric) {
                update(&mut best, candidate, Sweep::Min);
            }
        }
    }
    if let Some(right_exterior) = right_rings.first() {
        for vertex in right_exterior {
            if let Some(candidate) = point_face(*vertex, left, metric) {
                update(&mut best, candidate.swapped(), Sweep::Min);
            }
        }
    }
    best
}

fn pair(left: &Atom, right: &Atom, metric: Metric) -> Option<Candidate> {
    match (left, right) {
        (Atom::Point(a), Atom::Point(b)) => Some(Candidate {
            p1: *a,
            p2: *b,
            distance: distance(*a, *b, metric),
        }),
        (Atom::Point(a), Atom::Segment { a: b, b: c })
        | (Atom::Segment { a: b, b: c }, Atom::Point(a)) => {
            let candidate = point_segment(*a, *b, *c, metric);
            let candidate = if matches!(left, Atom::Point(_)) {
                candidate
            } else {
                candidate.swapped()
            };
            Some(candidate)
        }
        (Atom::Point(a), Atom::Face { .. }) => point_face(*a, right, metric),
        (Atom::Face { .. }, Atom::Point(a)) => point_face(*a, left, metric).map(Candidate::swapped),
        (Atom::Segment { a, b }, Atom::Segment { a: c, b: d }) => {
            Some(segment_segment(*a, *b, *c, *d, metric))
        }
        (Atom::Segment { a, b }, Atom::Face { .. }) => segment_face(*a, *b, right, metric),
        (Atom::Face { .. }, Atom::Segment { a, b }) => {
            segment_face(*a, *b, left, metric).map(Candidate::swapped)
        }
        (Atom::Face { .. }, Atom::Face { .. }) => face_face(left, right, metric),
    }
}

fn sweep_models(
    left: &GeometryModel,
    right: &GeometryModel,
    metric: Metric,
    sweep: Sweep,
) -> Option<Candidate> {
    let mut best = None;
    if sweep == Sweep::Max {
        for p1 in &left.vertices {
            for p2 in &right.vertices {
                let candidate = Candidate {
                    p1: *p1,
                    p2: *p2,
                    distance: distance(*p1, *p2, metric),
                };
                update(&mut best, candidate, Sweep::Max);
            }
        }
        return best;
    }

    for left_atom in &left.atoms {
        for right_atom in &right.atoms {
            let Some(candidate) = pair(left_atom, right_atom, metric) else {
                continue;
            };
            update(&mut best, candidate, Sweep::Min);
        }
    }
    best
}

pub(crate) fn min_pair(
    left: &GeometryModel,
    right: &GeometryModel,
    metric: Metric,
) -> Option<Candidate> {
    sweep_models(left, right, metric, Sweep::Min)
}

pub(crate) fn max_pair(
    left: &GeometryModel,
    right: &GeometryModel,
    metric: Metric,
) -> Option<Candidate> {
    sweep_models(left, right, metric, Sweep::Max)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Projection {
    Xy,
    Xz,
    Yz,
}

impl Projection {
    fn coordinate(self, point: Point3) -> RobustCoord<f64> {
        match self {
            Self::Xy => RobustCoord {
                x: point.x,
                y: point.y,
            },
            Self::Xz => RobustCoord {
                x: point.x,
                y: point.z,
            },
            Self::Yz => RobustCoord {
                x: point.y,
                y: point.z,
            },
        }
    }

    fn values(self, point: Point3) -> (f64, f64) {
        let coordinate = self.coordinate(point);
        (coordinate.x, coordinate.y)
    }
}

const PROJECTIONS: [Projection; 3] = [Projection::Xy, Projection::Xz, Projection::Yz];

fn robust_3d(point: Point3) -> RobustCoord3D<f64> {
    RobustCoord3D {
        x: point.x,
        y: point.y,
        z: point.z,
    }
}

fn point_equal(left: Point3, right: Point3, metric: Metric) -> bool {
    left.x == right.x && left.y == right.y && (metric == Metric::Xy || left.z == right.z)
}

fn point_on_segment_2d(point: Point3, a: Point3, b: Point3, projection: Projection) -> bool {
    if point_equal(point, a, Metric::Xy) || point_equal(point, b, Metric::Xy) {
        return true;
    }
    if point_equal(a, b, Metric::Xy) {
        return false;
    }
    if orient2d(
        projection.coordinate(a),
        projection.coordinate(b),
        projection.coordinate(point),
    ) != 0.0
    {
        return false;
    }
    let (px, py) = projection.values(point);
    let (ax, ay) = projection.values(a);
    let (bx, by) = projection.values(b);
    px >= ax.min(bx) && px <= ax.max(bx) && py >= ay.min(by) && py <= ay.max(by)
}

fn points_collinear_3d(a: Point3, b: Point3, c: Point3) -> bool {
    PROJECTIONS.into_iter().all(|projection| {
        orient2d(
            projection.coordinate(a),
            projection.coordinate(b),
            projection.coordinate(c),
        ) == 0.0
    })
}

fn point_on_segment_3d(point: Point3, a: Point3, b: Point3) -> bool {
    if point_equal(point, a, Metric::Full) || point_equal(point, b, Metric::Full) {
        return true;
    }
    if point_equal(a, b, Metric::Full) || !points_collinear_3d(a, b, point) {
        return false;
    }
    point.x >= a.x.min(b.x)
        && point.x <= a.x.max(b.x)
        && point.y >= a.y.min(b.y)
        && point.y <= a.y.max(b.y)
        && point.z >= a.z.min(b.z)
        && point.z <= a.z.max(b.z)
}

fn point_on_segment(point: Point3, a: Point3, b: Point3, metric: Metric) -> bool {
    match metric {
        Metric::Xy => point_on_segment_2d(point, a, b, Projection::Xy),
        Metric::Full => point_on_segment_3d(point, a, b),
    }
}

fn segments_intersect_2d(
    a: Point3,
    b: Point3,
    c: Point3,
    d: Point3,
    projection: Projection,
) -> bool {
    let o1 = orient2d(
        projection.coordinate(a),
        projection.coordinate(b),
        projection.coordinate(c),
    );
    let o2 = orient2d(
        projection.coordinate(a),
        projection.coordinate(b),
        projection.coordinate(d),
    );
    let o3 = orient2d(
        projection.coordinate(c),
        projection.coordinate(d),
        projection.coordinate(a),
    );
    let o4 = orient2d(
        projection.coordinate(c),
        projection.coordinate(d),
        projection.coordinate(b),
    );

    if ((o1 > 0.0 && o2 < 0.0) || (o1 < 0.0 && o2 > 0.0))
        && ((o3 > 0.0 && o4 < 0.0) || (o3 < 0.0 && o4 > 0.0))
    {
        return true;
    }

    (o1 == 0.0 && point_on_segment_2d(c, a, b, projection))
        || (o2 == 0.0 && point_on_segment_2d(d, a, b, projection))
        || (o3 == 0.0 && point_on_segment_2d(a, c, d, projection))
        || (o4 == 0.0 && point_on_segment_2d(b, c, d, projection))
}

fn direction_has_nonzero_projection(
    a: Point3,
    b: Point3,
    c: Point3,
    d: Point3,
    projection: Projection,
) -> bool {
    let da = projection.coordinate(b);
    let a_projected = projection.coordinate(a);
    let dc = projection.coordinate(d);
    let c_projected = projection.coordinate(c);
    let dax = da.x - a_projected.x;
    let day = da.y - a_projected.y;
    let dcx = dc.x - c_projected.x;
    let dcy = dc.y - c_projected.y;
    orient2d(
        RobustCoord { x: 0.0, y: 0.0 },
        RobustCoord { x: dax, y: day },
        RobustCoord { x: dcx, y: dcy },
    ) != 0.0
}

fn segments_intersect_3d(a: Point3, b: Point3, c: Point3, d: Point3) -> bool {
    if point_equal(a, b, Metric::Full) {
        return point_on_segment_3d(a, c, d);
    }
    if point_equal(c, d, Metric::Full) {
        return point_on_segment_3d(c, a, b);
    }
    if orient3d(robust_3d(a), robust_3d(b), robust_3d(c), robust_3d(d)) != 0.0 {
        return false;
    }
    if point_on_segment_3d(a, c, d)
        || point_on_segment_3d(b, c, d)
        || point_on_segment_3d(c, a, b)
        || point_on_segment_3d(d, a, b)
    {
        return true;
    }

    for projection in PROJECTIONS {
        if !direction_has_nonzero_projection(a, b, c, d, projection) {
            continue;
        }
        return segments_intersect_2d(a, b, c, d, projection);
    }
    false
}

fn face_basis(face: &Atom) -> Option<[Point3; 3]> {
    let Atom::Face { rings } = face else {
        return None;
    };
    let ring = rings.first()?;
    let a = *ring.first()?;
    let b = ring.iter().copied().find(|point| *point != a)?;
    let c = ring
        .iter()
        .copied()
        .find(|point| !points_collinear_3d(a, b, *point))?;
    Some([a, b, c])
}

fn face_projection(face: &Atom) -> Option<Projection> {
    let [a, b, c] = face_basis(face)?;
    PROJECTIONS.into_iter().find(|projection| {
        let da = projection.coordinate(b);
        let a_projected = projection.coordinate(a);
        let dc = projection.coordinate(c);
        let dax = da.x - a_projected.x;
        let day = da.y - a_projected.y;
        let dcx = dc.x - a_projected.x;
        let dcy = dc.y - a_projected.y;
        orient2d(
            RobustCoord { x: 0.0, y: 0.0 },
            RobustCoord { x: dax, y: day },
            RobustCoord { x: dcx, y: dcy },
        ) != 0.0
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RingPosition {
    Inside,
    Outside,
    Boundary,
}

fn ring_position(point: Point3, ring: &[Point3], projection: Projection) -> RingPosition {
    let point_projected = projection.values(point);
    let mut inside = false;
    for (&a, &b) in ring.iter().zip(ring.iter().skip(1)) {
        let a_projected = projection.values(a);
        let b_projected = projection.values(b);
        let orientation = orient2d(
            projection.coordinate(a),
            projection.coordinate(b),
            projection.coordinate(point),
        );
        if orientation == 0.0
            && point_projected.0 >= a_projected.0.min(b_projected.0)
            && point_projected.0 <= a_projected.0.max(b_projected.0)
            && point_projected.1 >= a_projected.1.min(b_projected.1)
            && point_projected.1 <= a_projected.1.max(b_projected.1)
        {
            return RingPosition::Boundary;
        }
        if (a_projected.1 > point_projected.1) != (b_projected.1 > point_projected.1) {
            let x_intersection = a_projected.0
                + (point_projected.1 - a_projected.1) * (b_projected.0 - a_projected.0)
                    / (b_projected.1 - a_projected.1);
            if point_projected.0 < x_intersection {
                inside = !inside;
            }
        }
    }
    if inside {
        RingPosition::Inside
    } else {
        RingPosition::Outside
    }
}

fn face_position(point: Point3, face: &Atom, projection: Projection) -> RingPosition {
    let Atom::Face { rings } = face else {
        return RingPosition::Outside;
    };
    let Some(exterior) = rings.first() else {
        return RingPosition::Outside;
    };
    match ring_position(point, exterior, projection) {
        RingPosition::Outside => return RingPosition::Outside,
        RingPosition::Boundary => return RingPosition::Boundary,
        RingPosition::Inside => {}
    }
    for hole in rings.iter().skip(1) {
        match ring_position(point, hole, projection) {
            RingPosition::Inside => return RingPosition::Outside,
            RingPosition::Boundary => return RingPosition::Boundary,
            RingPosition::Outside => {}
        }
    }
    RingPosition::Inside
}

fn point_on_face(point: Point3, face: &Atom, metric: Metric) -> bool {
    if metric == Metric::Xy {
        return face_position(point, face, Projection::Xy) != RingPosition::Outside;
    }
    let Some([a, b, c]) = face_basis(face) else {
        return false;
    };
    if orient3d(robust_3d(a), robust_3d(b), robust_3d(c), robust_3d(point)) != 0.0 {
        return false;
    }
    face_projection(face)
        .is_some_and(|projection| face_position(point, face, projection) != RingPosition::Outside)
}

fn segment_face_intersects(a: Point3, b: Point3, face: &Atom, metric: Metric) -> bool {
    if point_on_face(a, face, metric) || point_on_face(b, face, metric) {
        return true;
    }
    let Atom::Face { rings } = face else {
        return false;
    };
    for ring in rings {
        for (&c, &d) in ring.iter().zip(ring.iter().skip(1)) {
            let intersects = if metric == Metric::Xy {
                segments_intersect_2d(a, b, c, d, Projection::Xy)
            } else {
                segments_intersect_3d(a, b, c, d)
            };
            if intersects {
                return true;
            }
        }
    }
    if metric == Metric::Xy {
        return false;
    }
    let Some([p, q, r]) = face_basis(face) else {
        return false;
    };
    let side_a = orient3d(robust_3d(p), robust_3d(q), robust_3d(r), robust_3d(a));
    let side_b = orient3d(robust_3d(p), robust_3d(q), robust_3d(r), robust_3d(b));
    if (side_a < 0.0 && side_b > 0.0) || (side_a > 0.0 && side_b < 0.0) {
        let t = side_a / (side_a - side_b);
        let intersection = lerp(a, b, t);
        return point_on_face(intersection, face, Metric::Full);
    }
    false
}

fn face_face_intersects(left: &Atom, right: &Atom, metric: Metric) -> bool {
    let Atom::Face { rings: left_rings } = left else {
        return false;
    };
    let Atom::Face { rings: right_rings } = right else {
        return false;
    };
    for left_ring in left_rings {
        for (&a, &b) in left_ring.iter().zip(left_ring.iter().skip(1)) {
            for right_ring in right_rings {
                for (&c, &d) in right_ring.iter().zip(right_ring.iter().skip(1)) {
                    let intersects = if metric == Metric::Xy {
                        segments_intersect_2d(a, b, c, d, Projection::Xy)
                    } else {
                        segments_intersect_3d(a, b, c, d)
                    };
                    if intersects {
                        return true;
                    }
                }
            }
            if segment_face_intersects(a, b, right, metric) {
                return true;
            }
        }
    }
    for right_ring in right_rings {
        for (&a, &b) in right_ring.iter().zip(right_ring.iter().skip(1)) {
            if segment_face_intersects(a, b, left, metric) {
                return true;
            }
        }
    }
    for ring in left_rings {
        for &vertex in ring {
            if point_on_face(vertex, right, metric) {
                return true;
            }
        }
    }
    for ring in right_rings {
        for &vertex in ring {
            if point_on_face(vertex, left, metric) {
                return true;
            }
        }
    }
    false
}

fn atom_intersects(left: &Atom, right: &Atom, metric: Metric) -> bool {
    match (left, right) {
        (Atom::Point(a), Atom::Point(b)) => point_equal(*a, *b, metric),
        (Atom::Point(point), Atom::Segment { a, b })
        | (Atom::Segment { a, b }, Atom::Point(point)) => point_on_segment(*point, *a, *b, metric),
        (Atom::Point(point), Atom::Face { .. }) => point_on_face(*point, right, metric),
        (Atom::Face { .. }, Atom::Point(point)) => point_on_face(*point, left, metric),
        (Atom::Segment { a, b }, Atom::Segment { a: c, b: d }) => {
            if metric == Metric::Xy {
                segments_intersect_2d(*a, *b, *c, *d, Projection::Xy)
            } else {
                segments_intersect_3d(*a, *b, *c, *d)
            }
        }
        (Atom::Segment { a, b }, Atom::Face { .. }) => {
            segment_face_intersects(*a, *b, right, metric)
        }
        (Atom::Face { .. }, Atom::Segment { a, b }) => {
            segment_face_intersects(*a, *b, left, metric)
        }
        (Atom::Face { .. }, Atom::Face { .. }) => face_face_intersects(left, right, metric),
    }
}

pub(crate) fn intersects(left: &GeometryModel, right: &GeometryModel, metric: Metric) -> bool {
    left.atoms.iter().any(|left_atom| {
        right
            .atoms
            .iter()
            .any(|right_atom| atom_intersects(left_atom, right_atom, metric))
    })
}

fn vertical_model(x: f64, y: f64, z_min: f64, z_max: f64) -> GeometryModel {
    GeometryModel {
        atoms: vec![Atom::Segment {
            a: Point3 {
                x,
                y,
                z: z_min,
                has_z: true,
            },
            b: Point3 {
                x,
                y,
                z: z_max,
                has_z: true,
            },
        }],
        vertices: vec![
            Point3 {
                x,
                y,
                z: z_min,
                has_z: true,
            },
            Point3 {
                x,
                y,
                z: z_max,
                has_z: true,
            },
        ],
        has_z: true,
        non_finite: false,
    }
}

pub(crate) fn min_with_vertical(
    target: &GeometryModel,
    x: f64,
    y: f64,
    z_min: f64,
    z_max: f64,
    target_is_first: bool,
) -> Option<Candidate> {
    let vertical = vertical_model(x, y, z_min, z_max);
    if target_is_first {
        min_pair(target, &vertical, Metric::Full)
    } else {
        min_pair(&vertical, target, Metric::Full)
    }
}

pub(crate) fn max_with_vertical(
    target: &GeometryModel,
    x: f64,
    y: f64,
    z_min: f64,
    z_max: f64,
    target_is_first: bool,
) -> Option<Candidate> {
    let vertical = vertical_model(x, y, z_min, z_max);
    if target_is_first {
        max_pair(target, &vertical, Metric::Full)
    } else {
        max_pair(&vertical, target, Metric::Full)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use wkt::Wkt;

    use super::*;

    fn model(wkt_text: &str) -> GeometryModel {
        let wkt = wkt_text.parse::<Wkt<f64>>().expect("valid WKT");
        flatten(&wkt).expect("flatten succeeds")
    }

    #[test]
    fn point_to_segment_min() {
        let point = model("POINT Z (2 0 1)");
        let line = model("LINESTRING Z (0 0 0, 10 0 0)");
        let candidate = min_pair(&point, &line, Metric::Full).expect("candidate");
        assert_eq!(candidate.p1.x, 2.0);
        assert_eq!(candidate.p1.y, 0.0);
        assert_eq!(candidate.p1.z, 1.0);
        assert_eq!(candidate.p2.x, 2.0);
        assert_eq!(candidate.p2.z, 0.0);
        assert_relative_eq!(candidate.distance, 1.0);
    }

    #[test]
    fn skew_segments_min() {
        let first = model("LINESTRING Z (0 0 0, 10 0 0)");
        let second = model("LINESTRING Z (10 4 3, 0 4 3)");
        let candidate = min_pair(&first, &second, Metric::Full).expect("candidate");
        assert_eq!(candidate.p1.z, 0.0);
        assert_eq!(candidate.p2.z, 3.0);
        assert_relative_eq!(candidate.distance, 5.0);
    }

    #[test]
    fn point_to_polygon_hole() {
        let point = model("POINT Z (5 5 1)");
        let polygon = model(
            "POLYGON Z ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0), \
             (2 2 0, 2 8 0, 8 8 0, 8 2 0, 2 2 0))",
        );
        let candidate = min_pair(&point, &polygon, Metric::Full).expect("candidate");
        assert_relative_eq!(candidate.distance, 10.0_f64.sqrt());
    }

    #[test]
    fn max_is_vertex_based() {
        let first = model("LINESTRING Z (0 0 0, 10 0 0)");
        let second = model("LINESTRING Z (5 -5 5, 5 5 5)");
        let candidate = max_pair(&first, &second, Metric::Full).expect("candidate");
        assert_relative_eq!(candidate.distance, 75.0_f64.sqrt(), epsilon = 1e-12);
    }

    #[test]
    fn mixed_min_uses_vertical_ray() {
        let line = model("LINESTRING Z (0 0 0, 10 0 10)");
        let point = model("POINT (2 0)");
        let metric_candidate = min_pair(&line, &point, Metric::Xy).expect("2D candidate");
        let candidate = min_with_vertical(
            &line,
            metric_candidate.p2.x,
            metric_candidate.p2.y,
            line.z_range().unwrap().0,
            line.z_range().unwrap().1,
            true,
        )
        .expect("vertical candidate");
        assert_eq!(candidate.p1.x, 2.0);
        assert_eq!(candidate.p1.z, 2.0);
        assert_eq!(candidate.p2.x, 2.0);
        assert_eq!(candidate.p2.z, 2.0);
        assert_relative_eq!(candidate.distance, 0.0);
    }

    #[test]
    fn line_crosses_planar_polygon() {
        let line = model("LINESTRING Z (5 5 -5, 5 5 5)");
        let polygon = model("POLYGON Z ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))");
        let candidate = min_pair(&line, &polygon, Metric::Full).expect("candidate");
        assert_relative_eq!(candidate.distance, 0.0);
    }

    #[test]
    fn polygon_crosses_skew_plane() {
        let first = model("POLYGON Z ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))");
        let second = model("POLYGON Z ((2 2 -1, 12 2 1, 12 12 1, 2 12 -1, 2 2 -1))");
        let candidate = min_pair(&first, &second, Metric::Full).expect("candidate");
        assert_relative_eq!(candidate.distance, 0.0, epsilon = 1e-12);
    }

    #[test]
    fn boundary_edges_cross_on_polygon_plane() {
        let cand = segment_segment(
            Point3 {
                x: 0.0,
                y: 2.0,
                z: 0.0,
                has_z: true,
            },
            Point3 {
                x: 10.0,
                y: 2.0,
                z: 0.0,
                has_z: true,
            },
            Point3 {
                x: 2.0,
                y: 2.0,
                z: -1.0,
                has_z: true,
            },
            Point3 {
                x: 12.0,
                y: 2.0,
                z: 1.0,
                has_z: true,
            },
            Metric::Full,
        );
        assert_relative_eq!(cand.distance, 0.0, epsilon = 1e-12);
    }

    #[test]
    fn intersection_rejects_a_representable_separation() {
        let first = model("POINT Z (0 0 0)");
        let second = model("POINT Z (0 0 0.0000000000001)");
        assert!(!intersects(&first, &second, Metric::Full));

        let line = model("LINESTRING Z (0 0 0, 1 0 0)");
        let near = model("POINT Z (0.5 0 0.0000000000001)");
        assert!(!intersects(&line, &near, Metric::Full));
        let exact = model("POINT Z (0.5 0 0)");
        assert!(intersects(&line, &exact, Metric::Full));
    }

    #[test]
    fn intersection_uses_robust_collinearity() {
        let line = model("LINESTRING Z (0 0 0, 1 0 0)");
        let point = model("POINT Z (0.3 0 0)");
        assert!(intersects(&line, &point, Metric::Full));
    }
}
