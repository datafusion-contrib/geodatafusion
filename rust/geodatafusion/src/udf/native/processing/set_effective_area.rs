use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{Dimensions, GeometryTrait};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension, GeometryCollection, LineString, MultiLineString, MultiPolygon, Polygon,
};

use super::util;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SetEffectiveArea {
    signature: Signature,
}

impl SetEffectiveArea {
    pub fn new() -> Self {
        Self {
            signature: util::signature(),
        }
    }
}

impl Default for SetEffectiveArea {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SetEffectiveArea {
    fn name(&self) -> &str {
        "st_seteffectivearea"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        util::check_arg_count(arg_types, &[1, 2, 3], self.name())?;
        util::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() > 1 {
            coerced[1] = util::coerce_float64(&arg_types[1], 1, self.name())?;
        }
        if arg_types.len() > 2 {
            coerced[2] = util::coerce_int64(&arg_types[2], 2, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_SetEffectiveArea requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        util::geometry_return_field(&args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(set_effective_area_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Sets each vertex M value to its Visvalingam-Whyatt effective area. set_area=0 instead uses the computed area only for filtering and preserves the input M dimension. Vertices below threshold are removed; line endpoints always have FLT_MAX effective area.",
                "ST_SetEffectiveArea(geometry, threshold, set_area)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("threshold", "minimum effective area to retain")
            .with_argument("set_area", "nonzero writes effective area to M")
            .build()
        }))
    }
}

fn set_effective_area_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays[0].len();
    let geometries = util::input_geometry_values(args, "ST_SetEffectiveArea")?;
    let mut output = Vec::with_capacity(row_count);

    for (row, geometry) in geometries.iter().enumerate() {
        let Some(geometry) = geometry.clone() else {
            output.push(None);
            continue;
        };

        let threshold = if arrays.len() > 1 {
            match util::float64_value(&arrays, 1, row, "ST_SetEffectiveArea")? {
                Some(value) => value,
                None => {
                    output.push(None);
                    continue;
                }
            }
        } else {
            -1.0
        };
        let set_area = if arrays.len() > 2 {
            match util::int64_value(&arrays, 2, row, "ST_SetEffectiveArea")? {
                Some(value) => value != 0,
                None => {
                    output.push(None);
                    continue;
                }
            }
        } else {
            1 != 0
        };

        output.push(apply_geometry(geometry, threshold, set_area)?);
    }

    util::geometry_columnar(args, output)
}

fn apply_geometry(
    geometry: Wkt<f64>,
    threshold: f64,
    set_area: bool,
) -> GeoDataFusionResult<Option<Wkt<f64>>> {
    Ok(match geometry {
        Wkt::Point(point) => Some(Wkt::Point(point)),
        Wkt::MultiPoint(multi_point) => Some(Wkt::MultiPoint(multi_point)),
        Wkt::LineString(line) => {
            let (coords, dimension) = line.into_inner();
            if coords.len() < 3 {
                Some(Wkt::LineString(LineString::new(coords, dimension)))
            } else {
                Some(Wkt::LineString(LineString::new(
                    apply_line(coords, dimension, threshold, set_area, 2),
                    output_dimension(dimension, set_area),
                )))
            }
        }
        Wkt::Polygon(polygon) => {
            let (rings, dimension) = polygon.into_inner();
            if rings.is_empty() {
                return Ok(Some(Wkt::Polygon(Polygon::new(
                    Vec::new(),
                    output_dimension(dimension, set_area),
                ))));
            }
            let mut output = Vec::with_capacity(rings.len());
            for (index, ring) in rings.into_iter().enumerate() {
                let (coords, ring_dimension) = ring.into_inner();
                if coords.len() < 4 {
                    continue;
                }
                let avoid_collapse = if index == 0 { 4 } else { 0 };
                let coords =
                    apply_line(coords, ring_dimension, threshold, set_area, avoid_collapse);
                if coords.len() >= 4 {
                    output.push(LineString::new(
                        coords,
                        output_dimension(ring_dimension, set_area),
                    ));
                }
            }
            if output.is_empty() {
                None
            } else {
                Some(Wkt::Polygon(Polygon::new(
                    output,
                    output_dimension(dimension, set_area),
                )))
            }
        }
        Wkt::MultiLineString(multi_line) => {
            let (lines, dimension) = multi_line.into_inner();
            let mut output = Vec::with_capacity(lines.len());
            for line in lines {
                let (coords, line_dimension) = line.into_inner();
                output.push(LineString::new(
                    apply_line(coords, line_dimension, threshold, set_area, 2),
                    output_dimension(line_dimension, set_area),
                ));
            }
            let output_dimension = output_dimension(dimension, set_area);
            ensure_collection_dimensions(
                output.iter().map(|line| to_dimension(line.dim())),
                output_dimension,
            )?;
            Some(Wkt::MultiLineString(MultiLineString::new(
                output,
                output_dimension,
            )))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            let mut output = Vec::with_capacity(polygons.len());
            for polygon in polygons {
                if let Some(Wkt::Polygon(polygon)) =
                    apply_geometry(Wkt::Polygon(polygon), threshold, set_area)?
                {
                    output.push(polygon);
                }
            }
            let output_dimension = output_dimension(dimension, set_area);
            ensure_collection_dimensions(
                output.iter().map(|polygon| to_dimension(polygon.dim())),
                output_dimension,
            )?;
            Some(Wkt::MultiPolygon(MultiPolygon::new(
                output,
                output_dimension,
            )))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            let mut output = Vec::with_capacity(geometries.len());
            for geometry in geometries {
                if let Some(geometry) = apply_geometry(geometry, threshold, set_area)? {
                    output.push(geometry);
                }
            }
            let output_dimension = output_dimension(dimension, set_area);
            ensure_collection_dimensions(
                output.iter().map(|geometry| to_dimension(geometry.dim())),
                output_dimension,
            )?;
            Some(Wkt::GeometryCollection(GeometryCollection::new(
                output,
                output_dimension,
            )))
        }
    })
}

fn ensure_collection_dimensions(
    dimensions: impl Iterator<Item = Dimension>,
    expected: Dimension,
) -> GeoDataFusionResult<()> {
    for dimension in dimensions {
        if dimension != expected {
            return Err(DataFusionError::Execution(
                "Dimensions mismatch in lwcollection".to_string(),
            )
            .into());
        }
    }
    Ok(())
}

fn output_dimension(dimension: Dimension, set_area: bool) -> Dimension {
    if set_area {
        match dimension {
            Dimension::XY => Dimension::XYM,
            Dimension::XYZ => Dimension::XYZM,
            Dimension::XYM => Dimension::XYM,
            Dimension::XYZM => Dimension::XYZM,
        }
    } else {
        dimension
    }
}

fn to_dimension(dimension: Dimensions) -> Dimension {
    match dimension {
        Dimensions::Xy | Dimensions::Unknown(2) => Dimension::XY,
        Dimensions::Xyz | Dimensions::Unknown(3) => Dimension::XYZ,
        Dimensions::Xym => Dimension::XYM,
        Dimensions::Xyzm | Dimensions::Unknown(4) => Dimension::XYZM,
        Dimensions::Unknown(_) => Dimension::XY,
    }
}

fn apply_line(
    coords: Vec<Coord<f64>>,
    dimension: Dimension,
    threshold: f64,
    set_area: bool,
    avoid_collapse: usize,
) -> Vec<Coord<f64>> {
    if coords.len() < 3 {
        return coords;
    }

    let areas = effective_areas(&coords, dimension, avoid_collapse, set_area, threshold);
    coords
        .into_iter()
        .zip(areas)
        .filter_map(|(mut coord, area)| {
            if area
                .partial_cmp(&threshold)
                .is_none_or(|ordering| ordering == Ordering::Less)
            {
                return None;
            }
            if set_area {
                coord.m = Some(area);
            }
            Some(coord)
        })
        .collect()
}

fn effective_areas(
    coords: &[Coord<f64>],
    dimension: Dimension,
    avoid_collapse: usize,
    set_area: bool,
    threshold: f64,
) -> Vec<f64> {
    let count = coords.len();
    let mut areas = vec![0.0; count];
    let mut result = vec![f64::from(f32::MAX); count];
    let mut previous: Vec<usize> = (0..count).map(|index| index.saturating_sub(1)).collect();
    let mut next: Vec<usize> = (0..count).map(|index| (index + 1).min(count - 1)).collect();
    let is_3d = matches!(dimension, Dimension::XYZ | Dimension::XYZM);

    areas[0] = f64::from(f32::MAX);
    areas[count - 1] = f64::from(f32::MAX);
    for index in 1..count - 1 {
        areas[index] = triangle_area(coords[index - 1], coords[index], coords[index + 1], is_3d);
    }

    // Keep the active-vertex links aligned with PostGIS's doubly linked list.
    previous[0] = 0;
    next[count - 1] = count - 1;

    let mut heap = BinaryHeap::with_capacity(count);
    for (index, area) in areas.iter().enumerate() {
        heap.push(HeapEntry { area: *area, index });
    }

    let mut popped = 0usize;
    while let Some(entry) = heap.pop() {
        let current = entry.index;
        if entry.area != areas[current] {
            continue;
        }
        let current_area = if popped < count - avoid_collapse {
            areas[current]
        } else {
            f64::from(f32::MAX)
        };
        result[current] = current_area;
        popped += 1;

        let before = previous[current];
        let after = next[current];

        if before > 0 {
            let area = triangle_area(
                coords[previous[before]],
                coords[before],
                coords[after],
                is_3d,
            )
            .max(current_area);
            if area != areas[before] {
                areas[before] = area;
                heap.push(HeapEntry {
                    area,
                    index: before,
                });
            }
        }
        if after < count - 1 {
            let area = triangle_area(coords[before], coords[after], coords[next[after]], is_3d)
                .max(current_area);
            if area != areas[after] {
                areas[after] = area;
                heap.push(HeapEntry { area, index: after });
            }
        }

        next[before] = after;
        previous[after] = before;

        if (!set_area && result[current] >= threshold) || next[0] == count - 1 {
            break;
        }
    }

    result
}

fn triangle_area(p1: Coord<f64>, p2: Coord<f64>, p3: Coord<f64>, is_3d: bool) -> f64 {
    if is_3d {
        let (ax, ay, az) = (
            p1.x - p2.x,
            p1.y - p2.y,
            p1.z.unwrap_or(0.0) - p2.z.unwrap_or(0.0),
        );
        let (bx, by, bz) = (
            p3.x - p2.x,
            p3.y - p2.y,
            p3.z.unwrap_or(0.0) - p2.z.unwrap_or(0.0),
        );
        let cx = ay * bz - az * by;
        let cy = az * bx - ax * bz;
        let cz = ax * by - ay * bx;
        (0.5 * (cx * cx + cy * cy + cz * cz).sqrt()).abs()
    } else {
        (0.5 * ((p1.x - p2.x) * (p3.y - p2.y) - (p1.y - p2.y) * (p3.x - p2.x))).abs()
    }
}

#[derive(Debug, Clone, Copy)]
struct HeapEntry {
    area: f64,
    index: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.area.total_cmp(&other.area) == Ordering::Equal && self.index == other.index
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .area
            .total_cmp(&self.area)
            .then_with(|| other.index.cmp(&self.index))
    }
}
