use std::sync::OnceLock;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use wkt::Wkt;
use wkt::types::{Coord, Dimension, Point};

use super::util;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeometricMedian {
    signature: Signature,
}

impl GeometricMedian {
    pub fn new() -> Self {
        Self {
            signature: util::signature(),
        }
    }
}

impl Default for GeometricMedian {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeometricMedian {
    fn name(&self) -> &str {
        "st_geometricmedian"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        util::check_arg_count(arg_types, &[1, 2, 3, 4], self.name())?;
        util::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() > 1 {
            coerced[1] = util::coerce_float64(&arg_types[1], 1, self.name())?;
        }
        if arg_types.len() > 2 {
            coerced[2] = util::coerce_int64(&arg_types[2], 2, self.name())?;
        }
        if arg_types.len() > 3 {
            coerced[3] = util::coerce_boolean(&arg_types[3], 3, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_GeometricMedian requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        util::geometry_return_field(&args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(median_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the geometric median of a Point or MultiPoint with the weighted 3D Weiszfeld algorithm. M values are positive relative weights. A Point is returned unchanged. The output is 3D when the input has Z and does not carry M.",
                "ST_GeometricMedian(geometry, tolerance, max_iter, fail_if_not_converged)",
            )
            .with_argument("geometry", "Point or MultiPoint")
            .with_argument("tolerance", "optional positive convergence tolerance")
            .with_argument("max_iter", "maximum iterations")
            .with_argument(
                "fail_if_not_converged",
                "whether to fail instead of returning the last iterate",
            )
            .build()
        }))
    }
}

fn median_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays[0].len();
    let geometries = util::input_geometry_values(args, "ST_GeometricMedian")?;
    let mut output = Vec::with_capacity(row_count);

    for (row, geometry) in geometries.iter().enumerate() {
        let Some(geometry) = geometry.clone() else {
            output.push(None);
            continue;
        };

        match geometry {
            Wkt::Point(point) => {
                validate_median_arguments(&arrays, row)?;
                output.push(Some(Wkt::Point(point)));
                continue;
            }
            Wkt::MultiPoint(multi_point) => {
                let (points, dimension) = multi_point.into_inner();
                let total_points = points.len();
                let has_z = matches!(dimension, Dimension::XYZ | Dimension::XYZM);
                let has_m = matches!(dimension, Dimension::XYM | Dimension::XYZM);
                let mut values = Vec::with_capacity(points.len());
                for (point_index, point) in points.into_iter().enumerate() {
                    let (coord, _) = point.into_inner();
                    let Some(coord) = coord else {
                        continue;
                    };
                    let weight = if has_m {
                        let weight = coord.m.unwrap_or(0.0);
                        if weight < 0.0 {
                            return Err(DataFusionError::Execution(format!(
                                "Geometric median input contains points with negative weights (POINT({} {} {} {}), number {} of {} in input). Implementation can't guarantee global minimum convergence.",
                                coord.x,
                                coord.y,
                                coord.z.unwrap_or(0.0),
                                weight,
                                point_index,
                                total_points
                            ))
                            .into());
                        }
                        if weight <= f64::EPSILON {
                            continue;
                        }
                        weight
                    } else {
                        1.0
                    };
                    values.push(WeightedPoint {
                        x: coord.x,
                        y: coord.y,
                        z: coord.z.unwrap_or(0.0),
                        weight,
                    });
                }

                if values.is_empty() {
                    if total_points == 0 {
                        // Empty MultiPoint input produces an empty Point.
                        output.push(Some(Wkt::Point(Point::new(None, Dimension::XY))));
                        continue;
                    }
                    return Err(DataFusionError::Execution(
                        "Median failed to find non-empty input points with positive weight."
                            .to_string(),
                    )
                    .into());
                }

                let explicit_tolerance = if arrays.len() > 1 {
                    util::float64_value(&arrays, 1, row, "ST_GeometricMedian")?
                } else {
                    None
                };
                let tolerance = match explicit_tolerance {
                    Some(tolerance) => {
                        if tolerance < 0.0 {
                            return Err(DataFusionError::Execution(
                                "Tolerance must be positive.".to_string(),
                            )
                            .into());
                        }
                        tolerance
                    }
                    None => default_tolerance(&values, has_z),
                };
                let max_iter = if arrays.len() > 2 {
                    match util::int64_value(&arrays, 2, row, "ST_GeometricMedian")? {
                        Some(value) if value < 0 => {
                            return Err(DataFusionError::Execution(
                                "Maximum iterations must be positive.".to_string(),
                            )
                            .into());
                        }
                        Some(value) => usize::try_from(value).map_err(|_| {
                            DataFusionError::Execution(
                                "Maximum iterations must be positive.".to_string(),
                            )
                        })?,
                        None => {
                            return Err(DataFusionError::Execution(
                                "Maximum iterations must be positive.".to_string(),
                            )
                            .into());
                        }
                    }
                } else {
                    10_000
                };
                let fail_if_not_converged = if arrays.len() > 3 {
                    util::boolean_value(&arrays, 3, row, "ST_GeometricMedian")?.unwrap_or(false)
                } else {
                    false
                };

                let median = weiszfeld(&values, tolerance, max_iter, fail_if_not_converged)?;
                output.push(Some(Wkt::Point(Point::new(
                    Some(Coord {
                        x: median.0,
                        y: median.1,
                        z: has_z.then_some(median.2),
                        m: None,
                    }),
                    if has_z { Dimension::XYZ } else { Dimension::XY },
                ))));
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "lwgeom_median: Unsupported geometry type: {}",
                    geometry_type_name(&other)
                ))
                .into());
            }
        }
    }

    util::geometry_columnar(args, output)
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

#[derive(Debug, Clone, Copy)]
struct WeightedPoint {
    x: f64,
    y: f64,
    z: f64,
    weight: f64,
}

fn validate_median_arguments(arrays: &[ArrayRef], row: usize) -> GeoDataFusionResult<()> {
    if arrays.len() > 1
        && let Some(tolerance) = util::float64_value(arrays, 1, row, "ST_GeometricMedian")?
        && tolerance < 0.0
    {
        return Err(DataFusionError::Execution("Tolerance must be positive.".to_string()).into());
    }
    if arrays.len() > 2 {
        match util::int64_value(arrays, 2, row, "ST_GeometricMedian")? {
            Some(value) if value >= 0 => {}
            _ => {
                return Err(DataFusionError::Execution(
                    "Maximum iterations must be positive.".to_string(),
                )
                .into());
            }
        }
    }
    Ok(())
}

fn default_tolerance(points: &[WeightedPoint], has_z: bool) -> f64 {
    let mut xmin = f64::INFINITY;
    let mut xmax = f64::NEG_INFINITY;
    let mut ymin = f64::INFINITY;
    let mut ymax = f64::NEG_INFINITY;
    let mut zmin = f64::INFINITY;
    let mut zmax = f64::NEG_INFINITY;
    for point in points {
        xmin = xmin.min(point.x);
        xmax = xmax.max(point.x);
        ymin = ymin.min(point.y);
        ymax = ymax.max(point.y);
        zmin = zmin.min(point.z);
        zmax = zmax.max(point.z);
    }
    let mut min_dim = (xmax - xmin).min(ymax - ymin);
    if has_z {
        min_dim = min_dim.min(zmax - zmin);
    }
    (1e-6 * min_dim).max(1e-8)
}

fn weiszfeld(
    points: &[WeightedPoint],
    tolerance: f64,
    max_iter: usize,
    fail_if_not_converged: bool,
) -> GeoDataFusionResult<(f64, f64, f64)> {
    let mut mass = 0.0;
    let mut current = (0.0, 0.0, 0.0);
    for point in points {
        current.0 += point.x * point.weight;
        current.1 += point.y * point.weight;
        current.2 += point.z * point.weight;
        mass += point.weight;
    }
    if mass <= 0.0 {
        return Err(DataFusionError::Execution(
            "Median failed to find non-empty input points with positive weight.".to_string(),
        )
        .into());
    }
    current.0 /= mass;
    current.1 /= mass;
    current.2 /= mass;

    let mut distances = vec![0.0; points.len()];
    let mut sum_current = weighted_distances(current, points, &mut distances);
    let mut converged = false;

    for _ in 0..max_iter {
        let mut next = (0.0, 0.0, 0.0);
        let mut denom = 0.0;
        let mut hit = false;
        for (point, distance) in points.iter().zip(&distances) {
            if *distance > f64::EPSILON {
                next.0 += point.x / *distance;
                next.1 += point.y / *distance;
                next.2 += point.z / *distance;
                denom += 1.0 / *distance;
            } else {
                hit = true;
            }
        }
        if denom < f64::EPSILON {
            converged = true;
            break;
        }
        next.0 /= denom;
        next.1 /= denom;
        next.2 /= denom;

        if hit {
            let mut dx = 0.0;
            let mut dy = 0.0;
            let mut dz = 0.0;
            for (point, distance) in points.iter().zip(&distances) {
                if *distance > f64::EPSILON {
                    dx += (point.x - current.0) / *distance;
                    dy += (point.y - current.1) / *distance;
                    dz += (point.z - current.2) / *distance;
                }
            }
            let d_sqr = (dx * dx + dy * dy + dz * dz).sqrt();
            if d_sqr > f64::EPSILON {
                let r_inv = 0.0f64.max(1.0 / d_sqr);
                next.0 = (1.0 - r_inv) * next.0 + r_inv * current.0;
                next.1 = (1.0 - r_inv) * next.1 + r_inv * current.1;
                next.2 = (1.0 - r_inv) * next.2 + r_inv * current.2;
            }
        }

        let sum_next = weighted_distances(next, points, &mut distances);
        if sum_current - sum_next < tolerance {
            converged = true;
            break;
        }
        current = next;
        sum_current = sum_next;
    }

    if fail_if_not_converged && !converged {
        return Err(DataFusionError::Execution(format!(
            "Median failed to converge within {tolerance} after {max_iter} iterations."
        ))
        .into());
    }
    Ok(current)
}

fn weighted_distances(
    current: (f64, f64, f64),
    points: &[WeightedPoint],
    distances: &mut [f64],
) -> f64 {
    let mut weight = 0.0;
    for (index, point) in points.iter().enumerate() {
        let dx = current.0 - point.x;
        let dy = current.1 - point.y;
        let dz = current.2 - point.z;
        let distance = (dx * dx + dy * dy + dz * dz).sqrt();
        distances[index] = distance / point.weight;
        weight += distance * point.weight;
    }
    weight
}
