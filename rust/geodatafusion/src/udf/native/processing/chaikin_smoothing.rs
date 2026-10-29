use std::sync::OnceLock;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use wkt::Wkt;
use wkt::types::{Coord, GeometryCollection, LineString, MultiLineString, MultiPolygon, Polygon};

use super::util;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ChaikinSmoothing {
    signature: Signature,
}

impl ChaikinSmoothing {
    pub fn new() -> Self {
        Self {
            signature: util::signature(),
        }
    }
}

impl Default for ChaikinSmoothing {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ChaikinSmoothing {
    fn name(&self) -> &str {
        "st_chaikinsmoothing"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        util::check_arg_count(arg_types, &[1, 2, 3], self.name())?;
        util::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() > 1 {
            coerced[1] = util::coerce_int64(&arg_types[1], 1, self.name())?;
        }
        if arg_types.len() > 2 {
            coerced[2] = util::coerce_boolean(&arg_types[2], 2, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_ChaikinSmoothing requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        util::geometry_return_field(&args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(chaikin_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Smooths linear and polygonal geometry with Chaikin's algorithm. Iterations must be between 1 and 5. Points pass through unchanged. Z coordinates are interpolated. M is interpolated for XYZM input; PostGIS-compatible M-only input introduces zero M values.",
                "ST_ChaikinSmoothing(geometry, nIterations, preserveEndPoints)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("nIterations", "integer between 1 and 5")
            .with_argument(
                "preserveEndPoints",
                "whether polygon ring endpoints are preserved",
            )
            .build()
        }))
    }
}

fn chaikin_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let mut output = Vec::with_capacity(arrays[0].len());
    let geometries = util::input_geometry_values(args, "ST_ChaikinSmoothing")?;

    for (row, geometry) in geometries.iter().enumerate() {
        let Some(geometry) = geometry.clone() else {
            output.push(None);
            continue;
        };

        let Some(iterations) = argument_iterations(&arrays, row)? else {
            output.push(None);
            continue;
        };
        let Some(preserve) = argument_preserve(&arrays, row)? else {
            output.push(None);
            continue;
        };

        if matches!(&geometry, Wkt::Point(_) | Wkt::MultiPoint(_)) {
            output.push(Some(geometry));
            continue;
        }

        let iterations = usize::try_from(iterations).map_err(|_| {
            DataFusionError::Execution(
                "Number of iterations must be between 1 and 5 : LWGEOM_ChaikinSmoothing"
                    .to_string(),
            )
        })?;
        if !(1..=5).contains(&iterations) {
            return Err(DataFusionError::Execution(
                "Number of iterations must be between 1 and 5 : LWGEOM_ChaikinSmoothing"
                    .to_string(),
            )
            .into());
        }
        output.push(Some(smooth_geometry(geometry, iterations, preserve)?));
    }

    util::geometry_columnar(args, output)
}

fn argument_iterations(arrays: &[ArrayRef], row: usize) -> GeoDataFusionResult<Option<i64>> {
    if arrays.len() > 1 {
        match util::int64_value(arrays, 1, row, "ST_ChaikinSmoothing")? {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    } else {
        Ok(Some(1))
    }
}

fn argument_preserve(arrays: &[ArrayRef], row: usize) -> GeoDataFusionResult<Option<bool>> {
    if arrays.len() > 2 {
        match util::boolean_value(arrays, 2, row, "ST_ChaikinSmoothing")? {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    } else {
        Ok(Some(false))
    }
}

fn smooth_geometry(
    geometry: Wkt<f64>,
    iterations: usize,
    preserve: bool,
) -> GeoDataFusionResult<Wkt<f64>> {
    Ok(match geometry {
        other @ (Wkt::Point(_) | Wkt::MultiPoint(_)) => other,
        Wkt::LineString(line) => {
            let (coords, dimension) = line.into_inner();
            Wkt::LineString(LineString::new(
                smooth_coords(coords, dimension, iterations, true, false),
                dimension,
            ))
        }
        Wkt::Polygon(polygon) => {
            let (rings, dimension) = polygon.into_inner();
            let mut output = Vec::with_capacity(rings.len());
            for ring in rings {
                let (coords, ring_dimension) = ring.into_inner();
                let smoothed = smooth_coords(coords, ring_dimension, iterations, preserve, true);
                if !smoothed.is_empty() {
                    output.push(LineString::new(smoothed, ring_dimension));
                }
            }
            Wkt::Polygon(Polygon::new(output, dimension))
        }
        Wkt::MultiLineString(multi_line) => {
            let (lines, dimension) = multi_line.into_inner();
            let output = lines
                .into_iter()
                .map(|line| {
                    let (coords, line_dimension) = line.into_inner();
                    LineString::new(
                        smooth_coords(coords, line_dimension, iterations, true, false),
                        line_dimension,
                    )
                })
                .collect();
            Wkt::MultiLineString(MultiLineString::new(output, dimension))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            let output = polygons
                .into_iter()
                .map(|polygon| smooth_polygon(polygon, iterations, preserve))
                .collect::<GeoDataFusionResult<Vec<_>>>()?;
            Wkt::MultiPolygon(MultiPolygon::new(output, dimension))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            let geometries = geometries
                .into_iter()
                .map(|geometry| smooth_geometry(geometry, iterations, preserve))
                .collect::<GeoDataFusionResult<Vec<_>>>()?;
            Wkt::GeometryCollection(GeometryCollection::new(geometries, dimension))
        }
    })
}

fn smooth_polygon(
    polygon: Polygon<f64>,
    iterations: usize,
    preserve: bool,
) -> GeoDataFusionResult<Polygon<f64>> {
    let (rings, dimension) = polygon.into_inner();
    let mut output = Vec::with_capacity(rings.len());
    for ring in rings {
        let (coords, ring_dimension) = ring.into_inner();
        let smoothed = smooth_coords(coords, ring_dimension, iterations, preserve, true);
        if !smoothed.is_empty() {
            output.push(LineString::new(smoothed, ring_dimension));
        }
    }
    Ok(Polygon::new(output, dimension))
}

fn smooth_coords(
    input: Vec<Coord<f64>>,
    dimension: wkt::types::Dimension,
    iterations: usize,
    preserve_endpoint: bool,
    is_closed: bool,
) -> Vec<Coord<f64>> {
    let mut current = input;
    for _ in 0..iterations {
        current = smooth_once(&current, dimension, preserve_endpoint, is_closed);
    }
    current
}

fn smooth_once(
    input: &[Coord<f64>],
    dimension: wkt::types::Dimension,
    preserve_endpoint: bool,
    is_closed: bool,
) -> Vec<Coord<f64>> {
    if input.is_empty() {
        return Vec::new();
    }
    let has_z = matches!(
        dimension,
        wkt::types::Dimension::XYZ | wkt::types::Dimension::XYZM
    );
    let has_m = matches!(
        dimension,
        wkt::types::Dimension::XYM | wkt::types::Dimension::XYZM
    );

    let mut output = Vec::with_capacity(input.len() * 2);
    if preserve_endpoint {
        output.push(input[0]);
    }

    let mut previous = input[0];
    for (index, current) in input.iter().enumerate().skip(1) {
        let next = previous;
        previous = *current;

        let mut first = None;
        let mut second = None;
        if !preserve_endpoint || index > 1 {
            first = Some(interpolate(&previous, &next, 0.25, 0.75, has_z, has_m));
        }
        if !preserve_endpoint || index < input.len() - 1 {
            second = Some(interpolate(&previous, &next, 0.75, 0.25, has_z, has_m));
        }
        if let Some(coord) = first {
            output.push(coord);
        }
        if let Some(coord) = second {
            output.push(coord);
        }
    }

    if preserve_endpoint {
        if let Some(last) = input.last() {
            output.push(*last);
        }
    } else if is_closed && !output.is_empty() {
        output.push(output[0]);
    }
    output
}

fn interpolate(
    current: &Coord<f64>,
    previous: &Coord<f64>,
    current_weight: f64,
    previous_weight: f64,
    has_z: bool,
    has_m: bool,
) -> Coord<f64> {
    Coord {
        x: current.x * current_weight + previous.x * previous_weight,
        y: current.y * current_weight + previous.y * previous_weight,
        z: has_z.then(|| {
            current.z.unwrap_or(0.0) * current_weight + previous.z.unwrap_or(0.0) * previous_weight
        }),
        // PostGIS 3.6's native Chaikin path interpolates M for XYZM input
        // but emits zero for newly introduced M-only vertices.
        m: has_m.then(|| {
            if has_z {
                current.m.unwrap_or(0.0) * current_weight
                    + previous.m.unwrap_or(0.0) * previous_weight
            } else {
                0.0
            }
        }),
    }
}
