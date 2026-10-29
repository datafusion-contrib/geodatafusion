use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use wkt::Wkt;
use wkt::types::{Coord, Dimension, MultiPoint, Point, Polygon};

use super::util;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeneratePoints {
    signature: Signature,
}

impl GeneratePoints {
    pub fn new() -> Self {
        Self {
            signature: util::mutable_signature(),
        }
    }
}

impl Default for GeneratePoints {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeneratePoints {
    fn name(&self) -> &str {
        "st_generatepoints"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        util::check_arg_count(arg_types, &[2, 3], self.name())?;
        util::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        coerced[1] = util::coerce_int64(&arg_types[1], 1, self.name())?;
        if arg_types.len() > 2 {
            coerced[2] = util::coerce_int64(&arg_types[2], 2, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_GeneratePoints requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        util::geometry_return_field(&args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(generate_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Generates a MultiPoint whose points lie inside a Polygon or MultiPolygon. Positive npoints are required; zero, negative, and empty inputs return NULL. An explicit seed must be greater than zero and makes the point sequence reproducible. The default uses process-local entropy.",
                "ST_GeneratePoints(area, npoints, seed)",
            )
            .with_argument("area", "polygon or multipolygon")
            .with_argument("npoints", "number of points")
            .with_argument("seed", "optional positive deterministic seed")
            .build()
        }))
    }
}

fn generate_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays[0].len();
    let geometries = util::input_geometry_values(args, "ST_GeneratePoints")?;
    let mut output = Vec::with_capacity(row_count);

    for (row, geometry) in geometries.iter().enumerate() {
        let Some(geometry) = geometry.clone() else {
            output.push(None);
            continue;
        };
        let Some(npoints) = util::int64_value(&arrays, 1, row, "ST_GeneratePoints")? else {
            output.push(None);
            continue;
        };
        if npoints < 0 {
            output.push(None);
            continue;
        }
        let npoints = usize::try_from(npoints).map_err(|_| {
            DataFusionError::Execution("ST_GeneratePoints: npoints is too large".to_string())
        })?;

        let explicit_seed = if arrays.len() > 2 {
            Some(util::int64_value(&arrays, 2, row, "ST_GeneratePoints")?)
        } else {
            None
        };
        let seed = match explicit_seed {
            Some(Some(seed)) if seed < 1 => {
                return Err(DataFusionError::Execution(
                    "ST_GeneratePoints: seed must be greater than zero".to_string(),
                )
                .into());
            }
            Some(Some(seed)) => seed as u64,
            Some(None) => {
                output.push(None);
                continue;
            }
            None => random_seed(),
        };

        if npoints == 0 {
            output.push(None);
            continue;
        }

        let polygons = polygons_from_geometry(geometry)?;
        if polygons.is_empty() || polygons.iter().all(|polygon| polygon.area() == 0.0) {
            output.push(None);
            continue;
        }

        let mut rng = SplitMix64::new(seed);
        let points = generate_points(&polygons, npoints, &mut rng)?;
        output.push(Some(Wkt::MultiPoint(MultiPoint::new(
            points,
            Dimension::XY,
        ))));
    }

    util::geometry_columnar(args, output)
}

#[derive(Debug)]
struct Polygon2D {
    rings: Vec<Vec<(f64, f64)>>,
    ymin: f64,
    ymax: f64,
}

impl Polygon2D {
    fn from_wkt(polygon: Polygon<f64>) -> Option<Self> {
        let (rings, _) = polygon.into_inner();
        if rings.is_empty() {
            return None;
        }
        let mut output = Vec::with_capacity(rings.len());
        for ring in rings {
            let (coords, _) = ring.into_inner();
            if coords.is_empty() {
                continue;
            }
            output.push(
                coords
                    .into_iter()
                    .map(|coord| (coord.x, coord.y))
                    .collect::<Vec<_>>(),
            );
        }
        if output.is_empty() || output[0].len() < 3 {
            return None;
        }
        let (mut ymin, mut ymax) = (f64::INFINITY, f64::NEG_INFINITY);
        for (_, y) in &output[0] {
            ymin = ymin.min(*y);
            ymax = ymax.max(*y);
        }
        Some(Self {
            rings: output,
            ymin,
            ymax,
        })
    }

    fn area(&self) -> f64 {
        let mut area = ring_area(&self.rings[0]);
        for ring in self.rings.iter().skip(1) {
            area -= ring_area(ring).abs();
        }
        area.abs()
    }

    fn scanline_intervals(&self, y: f64) -> Vec<(f64, f64)> {
        let mut crossings = Vec::new();
        for ring in &self.rings {
            for edge in ring.windows(2) {
                let (x1, y1) = edge[0];
                let (x2, y2) = edge[1];
                if (y1 > y) != (y2 > y) {
                    crossings.push(x1 + (y - y1) * (x2 - x1) / (y2 - y1));
                }
            }
        }
        crossings.sort_by(f64::total_cmp);
        crossings
            .as_chunks::<2>()
            .0
            .iter()
            .filter_map(|pair| {
                let width = pair[1] - pair[0];
                (width > 0.0).then_some((pair[0], pair[1]))
            })
            .collect()
    }
}

fn polygons_from_geometry(geometry: Wkt<f64>) -> GeoDataFusionResult<Vec<Polygon2D>> {
    match geometry {
        Wkt::Polygon(polygon) => Ok(Polygon2D::from_wkt(polygon).into_iter().collect()),
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, _) = multi_polygon.into_inner();
            Ok(polygons
                .into_iter()
                .filter_map(Polygon2D::from_wkt)
                .collect())
        }
        other => Err(DataFusionError::Execution(format!(
            "lwgeom_to_points: unsupported geometry type '{}'",
            geometry_type_name(&other)
        ))
        .into()),
    }
}

fn geometry_type_name(geometry: &Wkt<f64>) -> &'static str {
    match geometry {
        Wkt::Point(_) => "Point",
        Wkt::LineString(_) => "LineString",
        Wkt::Polygon(_) => "Polygon",
        Wkt::MultiPoint(_) => "MultiPoint",
        Wkt::MultiLineString(_) => "MultiLineString",
        Wkt::MultiPolygon(_) => "MultiPolygon",
        Wkt::GeometryCollection(_) => "GeometryCollection",
    }
}

fn generate_points(
    polygons: &[Polygon2D],
    npoints: usize,
    rng: &mut SplitMix64,
) -> GeoDataFusionResult<Vec<Point<f64>>> {
    let areas = polygons.iter().map(Polygon2D::area).collect::<Vec<_>>();
    let total_area: f64 = areas.iter().sum();
    if total_area == 0.0 {
        return Err(DataFusionError::Execution(
            "ST_GeneratePoints: zero area input polygon".to_string(),
        )
        .into());
    }

    let mut allocations = vec![0usize; polygons.len()];
    let mut assigned = 0usize;
    let mut fractions = Vec::with_capacity(polygons.len());
    for (index, area) in areas.iter().enumerate() {
        let exact = npoints as f64 * *area / total_area;
        let count = exact.floor() as usize;
        allocations[index] = count;
        assigned += count;
        fractions.push((index, exact - count as f64));
    }
    fractions.sort_by(|left, right| right.1.total_cmp(&left.1));
    for (index, _) in fractions {
        if assigned == npoints {
            break;
        }
        allocations[index] += 1;
        assigned += 1;
    }

    let mut points = Vec::with_capacity(npoints);
    for (polygon, count) in polygons.iter().zip(allocations) {
        for _ in 0..count {
            points.push(random_point_in_polygon(polygon, rng)?);
        }
    }
    if points.len() != npoints {
        return Err(DataFusionError::Internal(format!(
            "ST_GeneratePoints allocated {} points for a request of {npoints}",
            points.len()
        ))
        .into());
    }
    Ok(points)
}

fn random_point_in_polygon(
    polygon: &Polygon2D,
    rng: &mut SplitMix64,
) -> GeoDataFusionResult<Point<f64>> {
    for _ in 0..1024 {
        let y = polygon.ymin + (polygon.ymax - polygon.ymin) * rng.next_f64();
        let intervals = polygon.scanline_intervals(y);
        if intervals.is_empty() {
            continue;
        }
        let total_width: f64 = intervals.iter().map(|(start, end)| end - start).sum();
        if total_width <= 0.0 {
            continue;
        }
        let mut pick = rng.next_f64() * total_width;
        for (start, end) in intervals {
            let width = end - start;
            if pick < width {
                let x = start + pick;
                return Ok(Point::new(
                    Some(Coord {
                        x,
                        y,
                        z: None,
                        m: None,
                    }),
                    Dimension::XY,
                ));
            }
            pick -= width;
        }
    }

    Err(DataFusionError::Execution(
        "ST_GeneratePoints: failed to generate a point inside the input polygon".to_string(),
    )
    .into())
}

fn ring_area(ring: &[(f64, f64)]) -> f64 {
    let mut sum = 0.0;
    for edge in ring.windows(2) {
        sum += edge[0].0 * edge[1].1 - edge[1].0 * edge[0].1;
    }
    if ring.len() > 2 {
        let first = ring[0];
        let last = ring[ring.len() - 1];
        sum += last.0 * first.1 - first.0 * last.1;
    }
    sum / 2.0
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn random_seed() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    time ^ (u64::from(std::process::id()) << 32)
        ^ COUNTER.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed)
}
