//! Native geometry editors with grid, view, and small-part semantics.

use std::sync::{LazyLock, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{CoordTrait, RectTrait};
use geoarrow_array::array::RectArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{BoxType, Dimension};
use wkt::Wkt;
use wkt::types::{
    Coord, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point,
    Polygon,
};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::bounds::{GeometryBounds, geometry_bounds};
use crate::udf::native::editors::transform::{
    invoke_with_wkt_edits, output_kind_for_input, return_field_for, wkt_values,
};
use crate::udf::native::geometry::is_null_geometry_input;

static BOX_TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| {
    [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ]
    .map(|dimension| BoxType::new(dimension, Default::default()).data_type())
    .to_vec()
});

#[derive(Debug, Clone, Copy)]
struct GridSpec {
    ipx: f64,
    ipy: f64,
    ipz: f64,
    ipm: f64,
    xsize: f64,
    ysize: f64,
    zsize: f64,
    msize: f64,
    xscale: f64,
    yscale: f64,
}

impl GridSpec {
    fn new(
        (ipx, ipy, ipz, ipm): (f64, f64, f64, f64),
        (xsize, ysize, zsize, msize): (f64, f64, f64, f64),
    ) -> Option<Self> {
        if xsize == 0.0 && ysize == 0.0 && zsize == 0.0 && msize == 0.0 {
            return None;
        }

        let mut spec = Self {
            ipx,
            ipy,
            ipz,
            ipm,
            xsize,
            ysize,
            zsize,
            msize,
            xscale: 0.0,
            yscale: 0.0,
        };

        if spec.xsize > 0.0 {
            if spec.xsize < 1.0 {
                spec.xscale = snap_to_int(1.0 / spec.xsize);
            } else {
                spec.xsize = snap_to_int(spec.xsize);
            }
        }
        if spec.ysize > 0.0 {
            if spec.ysize < 1.0 {
                spec.yscale = snap_to_int(1.0 / spec.ysize);
            } else {
                spec.ysize = snap_to_int(spec.ysize);
            }
        }

        Some(spec)
    }

    fn snap_coord(self, coord: &Coord<f64>) -> Coord<f64> {
        let mut output = *coord;
        if self.xsize > 0.0 {
            output.x = if self.xsize < 1.0 {
                round_by_scale(output.x, self.ipx, self.xscale)
            } else {
                round_to_grid(output.x, self.ipx, self.xsize)
            };
        }
        if self.ysize > 0.0 {
            output.y = if self.ysize < 1.0 {
                round_by_scale(output.y, self.ipy, self.yscale)
            } else {
                round_to_grid(output.y, self.ipy, self.ysize)
            };
        }
        if let Some(z) = output.z.as_mut()
            && self.zsize > 0.0
        {
            *z = round_to_grid(*z, self.ipz, self.zsize);
        }
        if let Some(m) = output.m.as_mut()
            && self.msize > 0.0
        {
            *m = round_to_grid(*m, self.ipm, self.msize);
        }
        output
    }
}

fn snap_to_int(value: f64) -> f64 {
    let rounded = value.round_ties_even();
    if (value - rounded).abs() < 1e-6 {
        rounded
    } else {
        value
    }
}

fn round_to_grid(value: f64, origin: f64, size: f64) -> f64 {
    ((value - origin) / size).round_ties_even() * size + origin
}

fn round_by_scale(value: f64, origin: f64, scale: f64) -> f64 {
    ((value - origin) * scale).round_ties_even() / scale + origin
}

fn is_empty_wkt(geometry: &Wkt<f64>) -> bool {
    match geometry {
        Wkt::Point(point) => point.coord().is_none(),
        Wkt::LineString(line_string) => line_string.coords().is_empty(),
        Wkt::Polygon(polygon) => polygon
            .rings()
            .first()
            .is_none_or(|ring| ring.coords().is_empty()),
        Wkt::MultiPoint(multi_point) => multi_point
            .points()
            .iter()
            .all(|point| point.coord().is_none()),
        Wkt::MultiLineString(multi_line_string) => multi_line_string
            .line_strings()
            .iter()
            .all(|line_string| line_string.coords().is_empty()),
        Wkt::MultiPolygon(multi_polygon) => multi_polygon.polygons().iter().all(|polygon| {
            polygon
                .rings()
                .first()
                .is_none_or(|ring| ring.coords().is_empty())
        }),
        Wkt::GeometryCollection(collection) => {
            collection.geometries().is_empty() || collection.geometries().iter().all(is_empty_wkt)
        }
    }
}

fn snap_to_grid(geometry: &Wkt<f64>, spec: GridSpec) -> Wkt<f64> {
    if is_empty_wkt(geometry) {
        return geometry.clone();
    }

    match geometry {
        Wkt::Point(point) => {
            let (coord, dimension) = point.clone().into_inner();
            Wkt::Point(Point::new(
                coord.map(|coord| spec.snap_coord(&coord)),
                dimension,
            ))
        }
        Wkt::LineString(line_string) => snap_line_string(line_string, spec),
        Wkt::Polygon(polygon) => snap_polygon(polygon, spec),
        Wkt::MultiPoint(multi_point) => {
            let points = multi_point
                .points()
                .iter()
                .filter_map(|point| {
                    point
                        .coord()
                        .map(|coord| Point::new(Some(spec.snap_coord(coord)), point.dimension()))
                })
                .collect();
            Wkt::MultiPoint(MultiPoint::new(points, multi_point.dimension()))
        }
        Wkt::MultiLineString(multi_line_string) => {
            let lines = multi_line_string
                .line_strings()
                .iter()
                .filter_map(|line_string| match snap_line_string(line_string, spec) {
                    Wkt::LineString(line_string) if !line_string.coords().is_empty() => {
                        Some(line_string)
                    }
                    _ => None,
                })
                .collect();
            Wkt::MultiLineString(MultiLineString::new(lines, multi_line_string.dimension()))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let polygons = multi_polygon
                .polygons()
                .iter()
                .filter_map(|polygon| match snap_polygon(polygon, spec) {
                    Wkt::Polygon(polygon) if !is_empty_wkt(&Wkt::Polygon(polygon.clone())) => {
                        Some(polygon)
                    }
                    _ => None,
                })
                .collect();
            Wkt::MultiPolygon(MultiPolygon::new(polygons, multi_polygon.dimension()))
        }
        Wkt::GeometryCollection(collection) => {
            let geometries = collection
                .geometries()
                .iter()
                .map(|geometry| snap_to_grid(geometry, spec))
                .filter(|geometry| !is_empty_wkt(geometry))
                .collect();
            Wkt::GeometryCollection(GeometryCollection::new(geometries, collection.dimension()))
        }
    }
}

fn snap_line_string(line_string: &LineString<f64>, spec: GridSpec) -> Wkt<f64> {
    let mut coords = Vec::with_capacity(line_string.coords().len());
    for coord in line_string.coords() {
        let coord = spec.snap_coord(coord);
        if coords.last() != Some(&coord) {
            coords.push(coord);
        }
    }
    if coords.len() < 2 {
        Wkt::LineString(LineString::empty(line_string.dimension()))
    } else {
        Wkt::LineString(LineString::new(coords, line_string.dimension()))
    }
}

fn snap_polygon(polygon: &Polygon<f64>, spec: GridSpec) -> Wkt<f64> {
    let mut rings = Vec::with_capacity(polygon.rings().len());
    for ring in polygon.rings() {
        let Wkt::LineString(ring) = snap_line_string(ring, spec) else {
            unreachable!("line string snapping returns a line string");
        };
        if rings.is_empty() && ring.coords().len() < 4 {
            return Wkt::Polygon(Polygon::empty(polygon.dimension()));
        }
        if ring.coords().len() >= 4 {
            rings.push(ring);
        }
    }
    Wkt::Polygon(Polygon::new(rings, polygon.dimension()))
}

fn part_is_small(coords: &[Coord<f64>], min_x: f64, min_y: f64) -> bool {
    let mut xmin = 0.0;
    let mut ymin = 0.0;
    let mut xmax = 0.0;
    let mut ymax = 0.0;

    for (index, coord) in coords.iter().enumerate() {
        if min_x > 0.0 {
            if index == 0 || coord.x < xmin {
                xmin = coord.x;
            }
            if index == 0 || coord.x > xmax {
                xmax = coord.x;
            }
        }
        if min_y > 0.0 {
            if index == 0 || coord.y < ymin {
                ymin = coord.y;
            }
            if index == 0 || coord.y > ymax {
                ymax = coord.y;
            }
        }
    }

    (min_x > 0.0 && xmax - xmin < min_x) || (min_y > 0.0 && ymax - ymin < min_y)
}

fn remove_small_parts(geometry: &Wkt<f64>, min_x: f64, min_y: f64) -> Wkt<f64> {
    if min_x <= 0.0 && min_y <= 0.0 {
        return geometry.clone();
    }

    match geometry {
        Wkt::LineString(line_string) => {
            if part_is_small(line_string.coords(), min_x, min_y) {
                Wkt::LineString(LineString::empty(line_string.dimension()))
            } else {
                geometry.clone()
            }
        }
        Wkt::Polygon(polygon) => remove_small_parts_polygon(polygon, min_x, min_y),
        Wkt::MultiLineString(multi_line_string) => {
            let lines = multi_line_string
                .line_strings()
                .iter()
                .filter(|line_string| !part_is_small(line_string.coords(), min_x, min_y))
                .cloned()
                .collect();
            Wkt::MultiLineString(MultiLineString::new(lines, multi_line_string.dimension()))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let polygons = multi_polygon
                .polygons()
                .iter()
                .filter_map(
                    |polygon| match remove_small_parts_polygon(polygon, min_x, min_y) {
                        Wkt::Polygon(polygon) if !is_empty_wkt(&Wkt::Polygon(polygon.clone())) => {
                            Some(polygon)
                        }
                        _ => None,
                    },
                )
                .collect();
            Wkt::MultiPolygon(MultiPolygon::new(polygons, multi_polygon.dimension()))
        }
        _ => geometry.clone(),
    }
}

fn remove_small_parts_polygon(polygon: &Polygon<f64>, min_x: f64, min_y: f64) -> Wkt<f64> {
    let mut rings = Vec::with_capacity(polygon.rings().len());
    for (index, ring) in polygon.rings().iter().enumerate() {
        if part_is_small(ring.coords(), min_x, min_y) {
            if index == 0 {
                return Wkt::Polygon(Polygon::empty(polygon.dimension()));
            }
        } else {
            rings.push(ring.clone());
        }
    }
    Wkt::Polygon(Polygon::new(rings, polygon.dimension()))
}

#[derive(Debug, Clone, Copy)]
struct ViewBounds {
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}

fn remove_irrelevant_points(
    geometry: &Wkt<f64>,
    bounds: ViewBounds,
    cartesian_hint: bool,
) -> Wkt<f64> {
    if is_empty_wkt(geometry) {
        return geometry.clone();
    }

    let GeometryBounds::Finite(geometry_bounds) = geometry_bounds(geometry) else {
        return geometry.clone();
    };
    if bounds.xmin <= geometry_bounds.minx()
        && bounds.ymin <= geometry_bounds.miny()
        && bounds.xmax >= geometry_bounds.maxx()
        && bounds.ymax >= geometry_bounds.maxy()
    {
        return geometry.clone();
    }

    match geometry {
        Wkt::LineString(line_string) => remove_irrelevant_line(line_string, bounds, cartesian_hint),
        Wkt::Polygon(polygon) => remove_irrelevant_polygon(polygon, bounds, cartesian_hint),
        Wkt::MultiLineString(multi_line_string) => {
            let lines = multi_line_string
                .line_strings()
                .iter()
                .map(|line_string| {
                    match remove_irrelevant_line(line_string, bounds, cartesian_hint) {
                        Wkt::LineString(line_string) => line_string,
                        _ => unreachable!("line removal returns a line string"),
                    }
                })
                .filter(|line_string| !line_string.coords().is_empty())
                .collect();
            Wkt::MultiLineString(MultiLineString::new(lines, multi_line_string.dimension()))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let polygons = multi_polygon
                .polygons()
                .iter()
                .filter_map(|polygon| {
                    match remove_irrelevant_polygon(polygon, bounds, cartesian_hint) {
                        Wkt::Polygon(polygon) if !is_empty_wkt(&Wkt::Polygon(polygon.clone())) => {
                            Some(polygon)
                        }
                        _ => None,
                    }
                })
                .collect();
            Wkt::MultiPolygon(MultiPolygon::new(polygons, multi_polygon.dimension()))
        }
        _ => geometry.clone(),
    }
}

fn remove_irrelevant_line(
    line_string: &LineString<f64>,
    bounds: ViewBounds,
    cartesian_hint: bool,
) -> Wkt<f64> {
    let mut coords = line_string.coords().to_vec();
    remove_points(&mut coords, bounds, false, cartesian_hint);
    if coords.is_empty() {
        Wkt::LineString(LineString::empty(line_string.dimension()))
    } else {
        Wkt::LineString(LineString::new(coords, line_string.dimension()))
    }
}

fn remove_irrelevant_polygon(
    polygon: &Polygon<f64>,
    bounds: ViewBounds,
    cartesian_hint: bool,
) -> Wkt<f64> {
    let mut rings = Vec::with_capacity(polygon.rings().len());
    for (index, ring) in polygon.rings().iter().enumerate() {
        let mut coords = ring.coords().to_vec();
        remove_points(&mut coords, bounds, true, cartesian_hint);
        if coords.is_empty() {
            if index == 0 {
                return Wkt::Polygon(Polygon::empty(polygon.dimension()));
            }
        } else {
            rings.push(LineString::new(coords, ring.dimension()));
        }
    }
    Wkt::Polygon(Polygon::new(rings, polygon.dimension()))
}

fn encode_to_bits(value: f64, min: f64, max: f64) -> i32 {
    if value < min {
        0x1
    } else if value <= max {
        0x2
    } else {
        0x4
    }
}

fn encode_to_bits_straight(
    xa: f64,
    ya: f64,
    xb: f64,
    yb: f64,
    bounds: ViewBounds,
    straight_position: i32,
) -> i32 {
    if straight_position == 1 || straight_position == 2 {
        if ya == yb {
            return 0;
        }
        let y = if straight_position == 2 {
            bounds.ymax
        } else {
            bounds.ymin
        };
        if ya < y && yb < y {
            return 0;
        }
        if ya > y && yb > y {
            return 0;
        }

        let dx = xb - xa;
        let dy = yb - ya;
        let mut d = dy;
        let mut c = dx * (y - ya);
        if dy < 0.0 {
            d = -d;
            c = -c;
        }
        return if c < d * (bounds.xmin - xa) {
            0x1
        } else if c < d * (bounds.xmax - xa) {
            0x2
        } else {
            0x4
        };
    }

    if straight_position == 3 || straight_position == 4 {
        if xa == xb {
            return 0;
        }
        let x = if straight_position == 4 {
            bounds.xmax
        } else {
            bounds.xmin
        };
        if xa < x && xb < x {
            return 0;
        }
        if xa > x && xb > x {
            return 0;
        }

        let dx = xb - xa;
        let dy = yb - ya;
        let mut d = dx;
        let mut c = dy * (x - xa);
        if dx < 0.0 {
            d = -d;
            c = -c;
        }
        return if c < d * (bounds.ymin - ya) {
            0x1
        } else if c < d * (bounds.ymax - ya) {
            0x2
        } else {
            0x4
        };
    }

    0
}

fn remove_points(
    points: &mut Vec<Coord<f64>>,
    bounds: ViewBounds,
    closed: bool,
    cartesian_hint: bool,
) {
    let minpoints = if closed { 4 } else { 2 };
    if points.len() < minpoints {
        points.clear();
        return;
    }

    let mut npoints = points.len();
    let mut previous = if closed {
        let first = &points[0];
        let last = &points[npoints - 1];
        if first.x != last.x || first.y != last.y {
            return;
        }
        npoints -= 1;
        points[npoints - 1]
    } else {
        points[0]
    };

    let mut previous_vx = encode_to_bits(previous.x, bounds.xmin, bounds.xmax);
    let mut previous_vy = encode_to_bits(previous.y, bounds.ymin, bounds.ymax);
    let mut output = Vec::with_capacity(npoints);
    let mut all_vx = 0;
    let mut all_vy = 0;
    let mut inside_all = false;

    for index in 0..npoints {
        let point = &points[index];
        let vx = encode_to_bits(point.x, bounds.xmin, bounds.xmax);
        let vy = encode_to_bits(point.y, bounds.ymin, bounds.ymax);
        let next = if index + 1 == npoints {
            if closed { 0 } else { index }
        } else {
            index + 1
        };
        let next_point = &points[next];
        let next_vx = encode_to_bits(next_point.x, bounds.xmin, bounds.xmax);
        let next_vy = encode_to_bits(next_point.y, bounds.ymin, bounds.ymax);

        let same_x = vx == next_vx && vx == previous_vx;
        let same_y = vy == next_vy && vy == previous_vy;
        let same = same_x && same_y;
        let inside_x = vx == 0x2;
        let inside_y = vy == 0x2;
        let inside = inside_x && inside_y;
        let mut skip = same_x && same_y && !inside;
        skip |= same_x && !inside_x;
        skip |= same_y && !inside_y;

        if cartesian_hint
            && !skip
            && !same
            && !inside
            && (previous_vx | previous_vy) != 0x2
            && (next_vx | next_vy) != 0x2
        {
            let mut vvx = 0;
            let mut vvy = 0;
            for side in 1..=2 {
                vvx |=
                    encode_to_bits_straight(previous.x, previous.y, point.x, point.y, bounds, side);
                vvx |= encode_to_bits_straight(
                    point.x,
                    point.y,
                    next_point.x,
                    next_point.y,
                    bounds,
                    side,
                );
                vvx |= encode_to_bits_straight(
                    previous.x,
                    previous.y,
                    next_point.x,
                    next_point.y,
                    bounds,
                    side,
                );
                if (vvx & 0x2) != 0 {
                    break;
                }

                vvy |= encode_to_bits_straight(
                    previous.x,
                    previous.y,
                    point.x,
                    point.y,
                    bounds,
                    side + 2,
                );
                vvy |= encode_to_bits_straight(
                    point.x,
                    point.y,
                    next_point.x,
                    next_point.y,
                    bounds,
                    side + 2,
                );
                vvy |= encode_to_bits_straight(
                    previous.x,
                    previous.y,
                    next_point.x,
                    next_point.y,
                    bounds,
                    side + 2,
                );
                if (vvy & 0x2) != 0 {
                    break;
                }
            }
            if ((vvx | vvy) & 0x2) == 0 {
                skip |= vvx == 0x1;
                skip |= vvx == 0x4;
                skip |= vvy == 0x1;
                skip |= vvy == 0x4;
            }
        }

        if skip {
            continue;
        }

        output.push(*point);
        previous = *point;
        previous_vx = vx;
        previous_vy = vy;
        all_vx |= vx;
        all_vy |= vy;
        inside_all |= inside;
    }

    if closed && !output.is_empty() {
        output.push(output[0]);
    }

    let mut clear = output.len() < minpoints;
    clear |= all_vx == 0x1;
    clear |= all_vx == 0x4;
    clear |= all_vy == 0x1;
    clear |= all_vy == 0x4;

    if cartesian_hint && !clear && !inside_all && !output.is_empty() {
        let mut cutting = false;
        for index in 0..output.len() - 1 {
            let first = &output[index];
            let second = &output[index + 1];
            for side in 1..=4 {
                cutting |=
                    encode_to_bits_straight(first.x, first.y, second.x, second.y, bounds, side)
                        == 0x2;
            }
        }

        if !cutting && closed {
            let mut crossings = 0;
            for index in 0..output.len() - 1 {
                let first = &output[index];
                let second = &output[index + 1];
                if encode_to_bits_straight(first.x, first.y, second.x, second.y, bounds, 1) == 0x1 {
                    crossings += 1;
                }
            }
            clear |= crossings % 2 == 0;
        }
    }

    if clear {
        points.clear();
    } else {
        *points = output;
    }
}

fn coerce_geometry(data_type: &DataType, position: usize, name: &str) -> Result<DataType> {
    if *data_type == DataType::Null || any_geometry_type().contains(data_type) {
        Ok(data_type.clone())
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} argument {} must be a geometry, got {data_type}",
            position + 1
        )))
    }
}

fn coerce_box(data_type: &DataType, position: usize, name: &str) -> Result<DataType> {
    if *data_type == DataType::Null || BOX_TYPES.contains(data_type) {
        Ok(data_type.clone())
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} argument {} must be a box2d, got {data_type}",
            position + 1
        )))
    }
}

fn coerce_float(data_type: &DataType, position: usize, name: &str) -> Result<DataType> {
    if *data_type == DataType::Null || data_type.is_numeric() {
        Ok(DataType::Float64)
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} argument {} must be numeric, got {data_type}",
            position + 1
        )))
    }
}

fn coerce_boolean(data_type: &DataType, position: usize, name: &str) -> Result<DataType> {
    if *data_type == DataType::Null || *data_type == DataType::Boolean {
        Ok(DataType::Boolean)
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} argument {} must be boolean, got {data_type}",
            position + 1
        )))
    }
}

fn float64_value(
    array: &ArrayRef,
    index: usize,
    name: &str,
    position: usize,
) -> GeoDataFusionResult<Option<f64>> {
    match array.data_type() {
        DataType::Float64 => {
            let values = array.as_primitive::<Float64Type>();
            Ok((!values.is_null(index)).then(|| values.value(index)))
        }
        DataType::Null => Ok(None),
        data_type => Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Float64: {data_type}",
            position + 1
        ))
        .into()),
    }
}

fn boolean_value(
    array: &ArrayRef,
    index: usize,
    name: &str,
    position: usize,
) -> GeoDataFusionResult<Option<bool>> {
    match array.data_type() {
        DataType::Boolean => {
            let values = array.as_boolean();
            Ok((!values.is_null(index)).then(|| values.value(index)))
        }
        DataType::Null => Ok(None),
        data_type => Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Boolean: {data_type}",
            position + 1
        ))
        .into()),
    }
}

fn view_bounds_values(
    array: &ArrayRef,
    field: &Field,
    name: &str,
) -> GeoDataFusionResult<Vec<Option<ViewBounds>>> {
    if is_null_geometry_input(field, array.as_ref()) {
        return Ok(vec![None; array.len()]);
    }

    let rects = RectArray::try_from((array.as_ref(), field))?;
    (0..array.len())
        .map(|index| {
            if rects.is_null(index) {
                return Ok(None);
            }
            let rect = rects.value(index)?;
            let min = rect.min();
            let max = rect.max();
            Ok(Some(ViewBounds {
                xmin: min.x(),
                ymin: min.y(),
                xmax: max.x(),
                ymax: max.y(),
            }))
        })
        .collect::<GeoDataFusionResult<_>>()
        .map_err(|error| {
            DataFusionError::Execution(format!("{name} could not read its box2d argument: {error}"))
                .into()
        })
}

fn advanced_return_field(args: ReturnFieldArgs) -> Result<FieldRef> {
    let output_kind = output_kind_for_input(args.arg_fields[0].as_ref())?;
    Ok(return_field_for(args, output_kind)?)
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SnapToGrid {
    signature: Signature,
}

impl SnapToGrid {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SnapToGrid {
    fn default() -> Self {
        Self::new()
    }
}

static SNAP_TO_GRID_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SnapToGrid {
    fn name(&self) -> &str {
        "st_snaptogrid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let roles: &[bool] = match arg_types.len() {
            2 => &[true, false],
            3 => &[true, false, false],
            5 => &[true, false, false, false, false],
            6 => &[true, true, false, false, false, false],
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "ST_SnapToGrid does not accept {} arguments",
                    arg_types.len()
                )));
            }
        };

        arg_types
            .iter()
            .zip(roles)
            .enumerate()
            .map(|(position, (data_type, is_geometry))| {
                if *is_geometry {
                    coerce_geometry(data_type, position, "ST_SnapToGrid")
                } else {
                    coerce_float(data_type, position, "ST_SnapToGrid")
                }
            })
            .collect()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_SnapToGrid requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        advanced_return_field(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(snap_to_grid_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SNAP_TO_GRID_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Snaps all points of a geometry to a regular grid, optionally using a point origin and per-dimension cell sizes.",
                "ST_SnapToGrid(geometry, size | sizeX, sizeY | originX, originY, sizeX, sizeY | pointOrigin, sizeX, sizeY, sizeZ, sizeM)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("grid", "origin and cell-size arguments")
            .build()
        }))
    }
}

fn snap_to_grid_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let has_point_origin = args.args.len() == 6;
    let origin_values = if has_point_origin && args.arg_fields[0].data_type() != &DataType::Null {
        if args.arg_fields[1].data_type() == &DataType::Null {
            vec![None; args.number_rows]
        } else {
            let arrays = ColumnarValue::values_to_arrays(&args.args)?;
            if is_null_geometry_input(args.arg_fields[1].as_ref(), arrays[1].as_ref()) {
                vec![None; args.number_rows]
            } else {
                wkt_values(&arrays[1], &args.arg_fields[1])?
            }
        }
    } else {
        Vec::new()
    };

    let output_kind = output_kind_for_input(args.arg_fields[0].as_ref())?;
    invoke_with_wkt_edits(args, output_kind, move |index, arrays, geometry| {
        let (origin, sizes) = match arrays.len() {
            2 => {
                let Some(size) = float64_value(&arrays[1], index, "ST_SnapToGrid", 1)? else {
                    return Ok(None);
                };
                ((0.0, 0.0, 0.0, 0.0), (size, size, 0.0, 0.0))
            }
            3 => {
                let Some(size_x) = float64_value(&arrays[1], index, "ST_SnapToGrid", 1)? else {
                    return Ok(None);
                };
                let Some(size_y) = float64_value(&arrays[2], index, "ST_SnapToGrid", 2)? else {
                    return Ok(None);
                };
                ((0.0, 0.0, 0.0, 0.0), (size_x, size_y, 0.0, 0.0))
            }
            5 => {
                let Some(origin_x) = float64_value(&arrays[1], index, "ST_SnapToGrid", 1)? else {
                    return Ok(None);
                };
                let Some(origin_y) = float64_value(&arrays[2], index, "ST_SnapToGrid", 2)? else {
                    return Ok(None);
                };
                let Some(size_x) = float64_value(&arrays[3], index, "ST_SnapToGrid", 3)? else {
                    return Ok(None);
                };
                let Some(size_y) = float64_value(&arrays[4], index, "ST_SnapToGrid", 4)? else {
                    return Ok(None);
                };
                ((origin_x, origin_y, 0.0, 0.0), (size_x, size_y, 0.0, 0.0))
            }
            6 => {
                let Some(origin_value) = &origin_values[index] else {
                    return Ok(None);
                };
                let origin = match origin_value {
                    Ok(Wkt::Point(point)) => point.coord().ok_or_else(|| {
                        DataFusionError::Execution(
                            "ST_SnapToGrid: Offset geometry must be a point".to_string(),
                        )
                    })?,
                    Ok(_) => {
                        return Err(DataFusionError::Execution(
                            "ST_SnapToGrid: Offset geometry must be a point".to_string(),
                        )
                        .into());
                    }
                    Err(error) => return Err(DataFusionError::Execution(error.clone()).into()),
                };
                let Some(size_x) = float64_value(&arrays[2], index, "ST_SnapToGrid", 2)? else {
                    return Ok(None);
                };
                let Some(size_y) = float64_value(&arrays[3], index, "ST_SnapToGrid", 3)? else {
                    return Ok(None);
                };
                let Some(size_z) = float64_value(&arrays[4], index, "ST_SnapToGrid", 4)? else {
                    return Ok(None);
                };
                let Some(size_m) = float64_value(&arrays[5], index, "ST_SnapToGrid", 5)? else {
                    return Ok(None);
                };
                (
                    (
                        origin.x,
                        origin.y,
                        origin.z.unwrap_or(0.0),
                        origin.m.unwrap_or(0.0),
                    ),
                    (size_x, size_y, size_z, size_m),
                )
            }
            _ => unreachable!("coerce_types validates argument count"),
        };

        let Some(spec) = GridSpec::new(origin, sizes) else {
            return Ok(Some(geometry.clone()));
        };
        Ok(Some(snap_to_grid(geometry, spec)))
    })
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct RemoveSmallParts {
    signature: Signature,
}

impl RemoveSmallParts {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RemoveSmallParts {
    fn default() -> Self {
        Self::new()
    }
}

static REMOVE_SMALL_PARTS_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for RemoveSmallParts {
    fn name(&self) -> &str {
        "st_removesmallparts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return Err(DataFusionError::Plan(format!(
                "ST_RemoveSmallParts does not accept {} arguments",
                arg_types.len()
            )));
        }
        Ok(vec![
            coerce_geometry(&arg_types[0], 0, "ST_RemoveSmallParts")?,
            coerce_float(&arg_types[1], 1, "ST_RemoveSmallParts")?,
            coerce_float(&arg_types[2], 2, "ST_RemoveSmallParts")?,
        ])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_RemoveSmallParts requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        advanced_return_field(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let output_kind = output_kind_for_input(args.arg_fields[0].as_ref())?;
        Ok(invoke_with_wkt_edits(
            args,
            output_kind,
            |index, arrays, geometry| {
                let Some(min_x) = float64_value(&arrays[1], index, "ST_RemoveSmallParts", 1)?
                else {
                    return Ok(None);
                };
                let Some(min_y) = float64_value(&arrays[2], index, "ST_RemoveSmallParts", 2)?
                else {
                    return Ok(None);
                };
                Ok(Some(remove_small_parts(geometry, min_x, min_y)))
            },
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(REMOVE_SMALL_PARTS_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Removes polygon rings and line strings whose width or height is smaller than the requested minimum.",
                "ST_RemoveSmallParts(geometry, minSizeX, minSizeY)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("minSizeX", "minimum width, or 0 to disable")
            .with_argument("minSizeY", "minimum height, or 0 to disable")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct RemoveIrrelevantPointsForView {
    signature: Signature,
}

impl RemoveIrrelevantPointsForView {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RemoveIrrelevantPointsForView {
    fn default() -> Self {
        Self::new()
    }
}

static REMOVE_IRRELEVANT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for RemoveIrrelevantPointsForView {
    fn name(&self) -> &str {
        "st_removeirrelevantpointsforview"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            2 => Ok(vec![
                coerce_geometry(&arg_types[0], 0, "ST_RemoveIrrelevantPointsForView")?,
                coerce_box(&arg_types[1], 1, "ST_RemoveIrrelevantPointsForView")?,
            ]),
            3 => Ok(vec![
                coerce_geometry(&arg_types[0], 0, "ST_RemoveIrrelevantPointsForView")?,
                coerce_box(&arg_types[1], 1, "ST_RemoveIrrelevantPointsForView")?,
                coerce_boolean(&arg_types[2], 2, "ST_RemoveIrrelevantPointsForView")?,
            ]),
            _ => Err(DataFusionError::Plan(format!(
                "ST_RemoveIrrelevantPointsForView does not accept {} arguments",
                arg_types.len()
            ))),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_RemoveIrrelevantPointsForView requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        advanced_return_field(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let bounds = if args.arg_fields[0].data_type() == &DataType::Null {
            vec![None; args.number_rows]
        } else {
            view_bounds_values(
                &arrays[1],
                args.arg_fields[1].as_ref(),
                "ST_RemoveIrrelevantPointsForView",
            )?
        };
        let output_kind = output_kind_for_input(args.arg_fields[0].as_ref())?;
        Ok(invoke_with_wkt_edits(
            args,
            output_kind,
            move |index, arrays, geometry| {
                let Some(bounds) = bounds[index] else {
                    return Ok(None);
                };
                let cartesian_hint = if arrays.len() == 3 {
                    let Some(cartesian_hint) =
                        boolean_value(&arrays[2], index, "ST_RemoveIrrelevantPointsForView", 2)?
                    else {
                        return Ok(None);
                    };
                    cartesian_hint
                } else {
                    false
                };
                Ok(Some(remove_irrelevant_points(
                    geometry,
                    bounds,
                    cartesian_hint,
                )))
            },
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(REMOVE_IRRELEVANT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Removes vertices that are irrelevant for rendering a geometry within a rectangular view.",
                "ST_RemoveIrrelevantPointsForView(geometry, bounds[, cartesian_hint])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("bounds", "box2d view bounds")
            .with_argument("cartesian_hint", "optional Cartesian rendering hint")
            .build()
        }))
    }
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(SnapToGrid::new().into());
    session_context.register_udf(RemoveSmallParts::new().into());
    session_context.register_udf(RemoveIrrelevantPointsForView::new().into());
}
