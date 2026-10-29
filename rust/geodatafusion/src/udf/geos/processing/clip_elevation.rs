//! Z restoration for `ST_ClipByBox2D` on native GEOS that drops Z.
//!
//! GEOS 3.14 and newer interpolate Z ordinates along the input linework and
//! fill the remaining clip vertices - the box corners and interior points of
//! holes - from a three-by-three cell elevation model over the input extent.
//! GEOS 3.11's `ClipByRect` drops Z instead. This module ports both rules
//! (`operation/intersection/RectangleIntersection.cpp` and
//! `operation/overlayng/ElevationModel.cpp`) so the result does not depend on
//! the native version.

use std::collections::HashMap;

use datafusion::error::DataFusionError;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use wkb::reader::Wkb;

use crate::error::GeoDataFusionResult;

/// GEOS divides the input extent into this many cells per axis.
const CELLS_PER_AXIS: usize = 3;

/// Relative tolerance for matching a clip vertex against the input linework.
///
/// Vertices produced by the native clip are a few floating-point steps away
/// from the exact intersection with the input linework; the tolerance is large
/// enough for that rounding and far below any meaningful coordinate distance.
const SEGMENT_TOLERANCE: f64 = 1e-12;

/// Z ordinate source for a rectangle-intersection result.
#[derive(Debug)]
pub(super) struct ClipElevation {
    grid: ElevationGrid,
    vertices: HashMap<(u64, u64), f64>,
    segments: Vec<Segment>,
}

impl ClipElevation {
    /// Reads the Z-bearing linework and extent of the original input geometry.
    ///
    /// Only linework that can reach the clipping box is kept, so the lookup
    /// cost follows the clipped part of the geometry rather than its full size.
    pub(super) fn new(
        wkb: &[u8],
        x_min: f64,
        y_min: f64,
        x_max: f64,
        y_max: f64,
    ) -> GeoDataFusionResult<Self> {
        let parsed = Wkb::try_new(wkb).map_err(|error| {
            DataFusionError::Execution(format!("GEOS input WKB is invalid: {error}"))
        })?;
        let mut vertices = Vec::new();
        let mut segments = Vec::new();
        collect(&parsed, &mut vertices, &mut segments)?;
        segments.retain(|segment| segment.reaches(x_min, y_min, x_max, y_max));
        let grid = ElevationGrid::build(&vertices);
        let vertices = vertices
            .into_iter()
            .filter_map(|(x, y, z)| z.map(|z| (coordinate_key(x, y), z)))
            .collect();
        Ok(Self {
            grid,
            vertices,
            segments,
        })
    }

    /// Z ordinate for a clip output vertex.
    ///
    /// Vertices on the input linework keep the linearly interpolated Z value;
    /// the remaining vertices take the elevation of their model cell.
    pub(super) fn z_at(&self, x: f64, y: f64) -> f64 {
        if let Some(z) = self.vertices.get(&coordinate_key(x, y)) {
            return *z;
        }
        self.interpolated_z(x, y)
            .unwrap_or_else(|| self.grid.z_at(x, y))
    }

    fn interpolated_z(&self, x: f64, y: f64) -> Option<f64> {
        let mut best: Option<(f64, f64)> = None;
        for segment in &self.segments {
            let (distance, tolerance, z) = segment.elevation(x, y);
            if distance <= tolerance && best.is_none_or(|(current, _)| distance < current) {
                best = Some((distance, z));
            }
        }
        best.map(|(_, z)| z)
    }
}

/// A segment of the input linework with its interpolated Z values.
#[derive(Debug)]
struct Segment {
    start_x: f64,
    start_y: f64,
    start_z: f64,
    end_x: f64,
    end_y: f64,
    end_z: f64,
}

impl Segment {
    /// Whether the segment can contain a point inside the clipping box.
    fn reaches(&self, x_min: f64, y_min: f64, x_max: f64, y_max: f64) -> bool {
        let (segment_x_min, segment_x_max) = ordered(self.start_x, self.end_x);
        let (segment_y_min, segment_y_max) = ordered(self.start_y, self.end_y);
        segment_x_max >= x_min
            && segment_x_min <= x_max
            && segment_y_max >= y_min
            && segment_y_min <= y_max
    }

    /// Returns the distance to `(x, y)`, the match tolerance, and the Z value
    /// that the native clip would interpolate at the closest point.
    fn elevation(&self, x: f64, y: f64) -> (f64, f64, f64) {
        let scale = x
            .abs()
            .max(y.abs())
            .max(self.start_x.abs())
            .max(self.start_y.abs())
            .max(self.end_x.abs())
            .max(self.end_y.abs())
            .max(1.0);
        let tolerance = SEGMENT_TOLERANCE * scale;

        let delta_x = self.end_x - self.start_x;
        let delta_y = self.end_y - self.start_y;
        let length_squared = delta_x * delta_x + delta_y * delta_y;
        if length_squared == 0.0 {
            let distance = ((x - self.start_x).powi(2) + (y - self.start_y).powi(2)).sqrt();
            return (distance, tolerance, self.start_z);
        }

        let parameter =
            ((x - self.start_x) * delta_x + (y - self.start_y) * delta_y) / length_squared;
        let parameter = parameter.clamp(0.0, 1.0);
        let projected_x = self.start_x + parameter * delta_x;
        let projected_y = self.start_y + parameter * delta_y;
        let distance = ((x - projected_x).powi(2) + (y - projected_y).powi(2)).sqrt();
        let z = self.start_z + parameter * (self.end_z - self.start_z);
        (distance, tolerance, z)
    }
}

fn ordered(left: f64, right: f64) -> (f64, f64) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

/// Key for exact input-vertex lookups.
///
/// Both zeros share one key so a `-0.0` output ordinate still finds the input
/// vertex that defines it.
fn coordinate_key(x: f64, y: f64) -> (u64, u64) {
    (canonical_bits(x), canonical_bits(y))
}

fn canonical_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    }
}

/// GEOS's three-by-three cell elevation model.
#[derive(Debug)]
struct ElevationGrid {
    min_x: f64,
    min_y: f64,
    cell_width: f64,
    cell_height: f64,
    columns: usize,
    rows: usize,
    sums: Vec<f64>,
    counts: Vec<usize>,
    average: Option<f64>,
}

impl ElevationGrid {
    fn build(vertices: &[(f64, f64, Option<f64>)]) -> Self {
        let (mut min_x, mut min_y) = (f64::INFINITY, f64::INFINITY);
        let (mut max_x, mut max_y) = (f64::NEG_INFINITY, f64::NEG_INFINITY);
        for (x, y, _) in vertices {
            min_x = min_x.min(*x);
            min_y = min_y.min(*y);
            max_x = max_x.max(*x);
            max_y = max_y.max(*y);
        }
        if !min_x.is_finite() || !min_y.is_finite() {
            min_x = 0.0;
            min_y = 0.0;
            max_x = 0.0;
            max_y = 0.0;
        }

        let cell_width = (max_x - min_x) / CELLS_PER_AXIS as f64;
        let cell_height = (max_y - min_y) / CELLS_PER_AXIS as f64;
        let columns = if cell_width > 0.0 { CELLS_PER_AXIS } else { 1 };
        let rows = if cell_height > 0.0 { CELLS_PER_AXIS } else { 1 };

        let mut grid = Self {
            min_x,
            min_y,
            cell_width,
            cell_height,
            columns,
            rows,
            sums: vec![0.0; columns * rows],
            counts: vec![0; columns * rows],
            average: None,
        };
        for (x, y, z) in vertices {
            let Some(z) = z.filter(|z| !z.is_nan()) else {
                continue;
            };
            let index = grid.cell_index(*x, *y);
            grid.sums[index] += z;
            grid.counts[index] += 1;
        }

        let mut total = 0.0;
        let mut filled = 0;
        for index in 0..grid.counts.len() {
            if grid.counts[index] > 0 {
                total += grid.sums[index] / grid.counts[index] as f64;
                filled += 1;
            }
        }
        grid.average = (filled > 0).then(|| total / filled as f64);
        grid
    }

    fn z_at(&self, x: f64, y: f64) -> f64 {
        let index = self.cell_index(x, y);
        if self.counts[index] > 0 {
            self.sums[index] / self.counts[index] as f64
        } else {
            self.average.unwrap_or(f64::NAN)
        }
    }

    fn cell_index(&self, x: f64, y: f64) -> usize {
        let column = self.axis_index(x, self.min_x, self.cell_width, self.columns);
        let row = self.axis_index(y, self.min_y, self.cell_height, self.rows);
        row * self.columns + column
    }

    fn axis_index(&self, value: f64, minimum: f64, cell_size: f64, cells: usize) -> usize {
        if cells == 1 {
            return 0;
        }
        let position = (value - minimum) / cell_size;
        let index = if position.is_finite() {
            position as i64
        } else {
            0
        };
        index.clamp(0, cells as i64 - 1) as usize
    }
}

/// Collects the input vertices and Z-bearing segments in traversal order.
fn collect(
    geometry: &impl GeometryTrait<T = f64>,
    vertices: &mut Vec<(f64, f64, Option<f64>)>,
    segments: &mut Vec<Segment>,
) -> GeoDataFusionResult<()> {
    match geometry.as_type() {
        GeometryType::Point(point) => {
            if let Some(coord) = point.coord() {
                vertices.push(coordinate(&coord));
            }
        }
        GeometryType::MultiPoint(points) => {
            for point in points.points() {
                if let Some(coord) = point.coord() {
                    vertices.push(coordinate(&coord));
                }
            }
        }
        GeometryType::LineString(line) => collect_line(line, vertices, segments),
        GeometryType::MultiLineString(lines) => {
            for line in lines.line_strings() {
                collect_line(&line, vertices, segments);
            }
        }
        GeometryType::Polygon(polygon) => {
            for ring in polygon.exterior().into_iter().chain(polygon.interiors()) {
                collect_line(&ring, vertices, segments);
            }
        }
        GeometryType::MultiPolygon(polygons) => {
            for polygon in polygons.polygons() {
                for ring in polygon.exterior().into_iter().chain(polygon.interiors()) {
                    collect_line(&ring, vertices, segments);
                }
            }
        }
        GeometryType::GeometryCollection(collection) => {
            for geometry in collection.geometries() {
                collect(&geometry, vertices, segments)?;
            }
        }
        GeometryType::Rect(_) | GeometryType::Triangle(_) | GeometryType::Line(_) => {
            return Err(DataFusionError::Execution(
                "ST_ClipByBox2D cannot restore Z ordinates for this geometry type".to_string(),
            )
            .into());
        }
    }
    Ok(())
}

fn collect_line(
    line: &impl LineStringTrait<T = f64>,
    vertices: &mut Vec<(f64, f64, Option<f64>)>,
    segments: &mut Vec<Segment>,
) {
    let mut previous: Option<(f64, f64, Option<f64>)> = None;
    for coord in line.coords() {
        let current = coordinate(&coord);
        vertices.push(current);
        if let Some((start_x, start_y, Some(start_z))) = previous
            && let (end_x, end_y, Some(end_z)) = current
        {
            segments.push(Segment {
                start_x,
                start_y,
                start_z,
                end_x,
                end_y,
                end_z,
            });
        }
        previous = Some(current);
    }
}

fn coordinate(coord: &dyn CoordTrait<T = f64>) -> (f64, f64, Option<f64>) {
    let z = has_z(coord.dim()).then(|| coord.nth_or_panic(2));
    (coord.x(), coord.y(), z)
}

fn has_z(dimension: Dimensions) -> bool {
    matches!(
        dimension,
        Dimensions::Xyz | Dimensions::Xyzm | Dimensions::Unknown(3) | Dimensions::Unknown(4)
    )
}
