use std::collections::HashMap;
use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{Dimensions, GeometryTrait};
use serde_json::Value;
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
};

use super::hershey_data::GLYPHS;
use super::util;
use crate::error::GeoDataFusionResult;
use crate::udf::native::io::twkb::decode_twkb;

const DEFAULT_TEXT_HEIGHT: f64 = 100.0;
// Hershey's capital letters span roughly 12 baseline units. This keeps the
// default cap height near PostGIS's default-font proportions.
// JHF uses y-down coordinates with the baseline at R+9; 28.5 units span the
// default font's descender-to-capital range used by ST_Letters.
const DEFAULT_UNITS_TO_TEXT_HEIGHT: f64 = DEFAULT_TEXT_HEIGHT / 28.5;
const CUSTOM_FONT_HEIGHT: f64 = 1000.0;
const STROKE_HALF_WIDTH: f64 = 0.30;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Letters {
    signature: Signature,
}

impl Letters {
    pub fn new() -> Self {
        Self {
            signature: util::signature(),
        }
    }
}

impl Default for Letters {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Letters {
    fn name(&self) -> &str {
        "st_letters"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        util::check_arg_count(arg_types, &[1, 2], self.name())?;
        let mut coerced = arg_types.to_vec();
        coerced[0] = util::coerce_text(&arg_types[0], 0, self.name())?;
        if arg_types.len() > 1 {
            coerced[1] = util::coerce_text(&arg_types[1], 1, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Letters requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        util::geometry_return_field(&args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(letters_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Renders text as a MultiPolygon. The default font is an independently distributed public-domain Hershey Simplex vector font with a nominal text height of 100 and glyphs laid out from x = 0 (stroke width may extend slightly below the baseline). Its glyph geometry, member/point counts, and advances differ from PostGIS's embedded default font; use a custom font when PostGIS-exact glyph output is required. Empty or glyphless text returns NULL. The optional font is a JSON object mapping characters to base64-encoded XY/XYZ/XYM/XYZM TWKB glyphs with a height of 1000; it must also provide an 'm' glyph used for spacing.",
                "ST_Letters(letters, font)",
            )
            .with_argument("letters", "text to render")
            .with_argument("font", "optional JSON font map")
            .build()
        }))
    }
}

fn letters_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays[0].len();
    let mut output = Vec::with_capacity(row_count);

    for row in 0..row_count {
        let Some(letters) = util::text_value(&arrays, 0, row, "ST_Letters")? else {
            output.push(None);
            continue;
        };
        let font = if arrays.len() > 1 {
            util::text_value(&arrays, 1, row, "ST_Letters")?
        } else {
            None
        };
        output.push(render_letters(&letters, font.as_deref())?);
    }

    util::geometry_columnar(args, output)
}

fn render_letters(letters: &str, font: Option<&str>) -> GeoDataFusionResult<Option<Wkt<f64>>> {
    let mut rendered = Vec::new();
    let mut position = 0.0;
    let (spacing, glyphs) = match font {
        None => {
            let m_width = default_glyph_width('m')?;
            (m_width / 12.0, HashMap::new())
        }
        Some(font) => {
            let mut glyphs = HashMap::new();
            let value: Value = serde_json::from_str(font).map_err(|error| {
                DataFusionError::Execution(format!("ST_Letters font is not valid JSON: {error}"))
            })?;
            let Value::Object(object) = value else {
                return Err(DataFusionError::Execution(
                    "ST_Letters font must be a JSON object".to_string(),
                )
                .into());
            };
            let Some(m_value) = object.get("m").and_then(Value::as_str) else {
                return Ok(None);
            };
            let m_geometry = decode_font_geometry(m_value)?;
            let m_width = geometry_width(&m_geometry).ok_or_else(|| {
                DataFusionError::Execution(
                    "ST_Letters font 'm' glyph must be a non-empty geometry".to_string(),
                )
            })?;
            for (key, value) in object {
                let Some(value) = value.as_str() else {
                    return Err(DataFusionError::Execution(format!(
                        "ST_Letters font glyph `{key}` must be a base64 string"
                    ))
                    .into());
                };
                let Some(character) = key.chars().next() else {
                    continue;
                };
                if key.chars().count() != 1 {
                    continue;
                }
                glyphs.insert(character, decode_font_geometry(value)?);
            }
            (m_width / 12.0, glyphs)
        }
    };

    for character in letters.chars() {
        let geometry = if let Some(custom) = glyphs.get(&character) {
            Some(custom.clone())
        } else if font.is_none() {
            default_glyph(character)?
        } else {
            None
        };

        let (width, translated) = match geometry {
            Some(geometry) => {
                let width = geometry_width(&geometry);
                let Some((xmin, _xmax)) = geometry_x_bounds(&geometry) else {
                    position += spacing;
                    continue;
                };
                let translated = translate_x(geometry, -xmin);
                let width = width.ok_or_else(|| {
                    DataFusionError::Execution(
                        "ST_Letters glyph must be a non-empty geometry".to_string(),
                    )
                })?;
                (width, Some(translated))
            }
            None => (spacing * 3.5, None),
        };

        if let Some(translated) = translated {
            let offset = position;
            let mut translated = translated;
            translated = translate_x(translated, offset);
            let scale = if font.is_none() {
                DEFAULT_UNITS_TO_TEXT_HEIGHT
            } else {
                DEFAULT_TEXT_HEIGHT / CUSTOM_FONT_HEIGHT
            };
            rendered.push(scale_geometry(translated, scale));
        }
        position += width + spacing;
    }

    combine_letters(rendered)
}

fn combine_letters(geometries: Vec<Wkt<f64>>) -> GeoDataFusionResult<Option<Wkt<f64>>> {
    if geometries.is_empty() {
        return Ok(None);
    }

    let dimension = to_wkt_dimension(geometries[0].dim());
    for geometry in geometries.iter().skip(1) {
        if to_wkt_dimension(geometry.dim()) != dimension {
            return Err(DataFusionError::Execution(
                "ST_Letters font glyphs have mixed coordinate dimensions".to_string(),
            )
            .into());
        }
    }

    if geometries
        .iter()
        .all(|geometry| matches!(geometry, Wkt::Polygon(_) | Wkt::MultiPolygon(_)))
    {
        let mut polygons = Vec::new();
        for geometry in geometries {
            match geometry {
                Wkt::Polygon(polygon) => polygons.push(polygon),
                Wkt::MultiPolygon(multi_polygon) => {
                    let (members, _) = multi_polygon.into_inner();
                    polygons.extend(members);
                }
                _ => unreachable!(),
            }
        }
        return Ok(Some(Wkt::MultiPolygon(MultiPolygon::new(
            polygons, dimension,
        ))));
    }

    Ok(Some(Wkt::GeometryCollection(
        wkt::types::GeometryCollection::new(geometries, dimension),
    )))
}

fn to_wkt_dimension(dimension: Dimensions) -> Dimension {
    match dimension {
        Dimensions::Xy | Dimensions::Unknown(2) => Dimension::XY,
        Dimensions::Xyz | Dimensions::Unknown(3) => Dimension::XYZ,
        Dimensions::Xym => Dimension::XYM,
        Dimensions::Xyzm | Dimensions::Unknown(4) => Dimension::XYZM,
        Dimensions::Unknown(_) => Dimension::XY,
    }
}

fn default_glyph(character: char) -> GeoDataFusionResult<Option<Wkt<f64>>> {
    let Some((_, record)) = GLYPHS.iter().find(|(key, _)| *key == character) else {
        return Ok(None);
    };
    let (strokes, _advance) = parse_hershey(record).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "ST_Letters built-in glyph `{character}` is invalid"
        ))
    })?;
    let polygons = strokes
        .into_iter()
        .filter_map(|stroke| stroke_to_polygon(&stroke, STROKE_HALF_WIDTH))
        .map(|ring| {
            let ring = ring
                .into_iter()
                .map(|(x, y)| Coord {
                    x,
                    y,
                    z: None,
                    m: None,
                })
                .collect();
            Polygon::new(vec![LineString::new(ring, Dimension::XY)], Dimension::XY)
        })
        .collect::<Vec<_>>();
    if polygons.is_empty() {
        return Ok(None);
    }
    Ok(Some(Wkt::MultiPolygon(MultiPolygon::new(
        polygons,
        Dimension::XY,
    ))))
}

fn default_glyph_width(character: char) -> GeoDataFusionResult<f64> {
    let Some((_, record)) = GLYPHS.iter().find(|(key, _)| *key == character) else {
        return Ok(0.0);
    };
    let (_, advance) = parse_hershey(record).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "ST_Letters built-in glyph `{character}` is invalid"
        ))
    })?;
    Ok(advance)
}

type HersheyGlyph = (Vec<Vec<(f64, f64)>>, f64);

fn parse_hershey(record: &str) -> Option<HersheyGlyph> {
    let bytes = record.as_bytes();
    if bytes.len() < 10 {
        return None;
    }
    let left = i32::from(bytes[8]) - i32::from(b'R');
    let right = i32::from(bytes[9]) - i32::from(b'R');
    let mut strokes = Vec::new();
    let mut current = Vec::new();
    let mut iterator = bytes[10..].iter().copied();
    let mut previous = None;
    while let (Some(x), Some(y)) = (iterator.next(), iterator.next()) {
        if x == b' ' && y == b'R' {
            if !current.is_empty() {
                strokes.push(std::mem::take(&mut current));
            }
            previous = None;
            continue;
        }
        let point = (
            f64::from(i32::from(x) - i32::from(b'R') - left),
            f64::from(9 - (i32::from(y) - i32::from(b'R'))),
        );
        if previous != Some(point) {
            current.push(point);
            previous = Some(point);
        }
    }
    if !current.is_empty() {
        strokes.push(current);
    }
    Some((strokes, f64::from(right - left)))
}

fn stroke_to_polygon(points: &[(f64, f64)], half_width: f64) -> Option<Vec<(f64, f64)>> {
    if points.len() < 2 {
        return None;
    }
    let mut left = Vec::with_capacity(points.len());
    let mut right = Vec::with_capacity(points.len());
    for index in 0..points.len() {
        let normal = vertex_normal(points, index)?;
        let (x, y) = points[index];
        left.push((x + normal.0 * half_width, y + normal.1 * half_width));
        right.push((x - normal.0 * half_width, y - normal.1 * half_width));
    }
    right.reverse();
    let mut ring = left;
    ring.extend(right);
    if let Some(first) = ring.first().copied() {
        ring.push(first);
    }
    (ring.len() >= 4).then_some(ring)
}

fn vertex_normal(points: &[(f64, f64)], index: usize) -> Option<(f64, f64)> {
    let mut normal = (0.0, 0.0);
    if index > 0 {
        normal = add_normal(normal, segment_normal(points[index - 1], points[index])?);
    }
    if index + 1 < points.len() {
        normal = add_normal(normal, segment_normal(points[index], points[index + 1])?);
    }
    let length = (normal.0 * normal.0 + normal.1 * normal.1).sqrt();
    if length <= f64::EPSILON {
        if index + 1 < points.len() {
            return segment_normal(points[index], points[index + 1]);
        }
        return segment_normal(points[index - 1], points[index]);
    }
    Some((normal.0 / length, normal.1 / length))
}

fn add_normal(left: (f64, f64), right: (f64, f64)) -> (f64, f64) {
    (left.0 + right.0, left.1 + right.1)
}

fn segment_normal(start: (f64, f64), end: (f64, f64)) -> Option<(f64, f64)> {
    let dx = end.0 - start.0;
    let dy = end.1 - start.1;
    let length = (dx * dx + dy * dy).sqrt();
    (length > f64::EPSILON).then_some((-dy / length, dx / length))
}

fn decode_font_geometry(encoded: &str) -> GeoDataFusionResult<Wkt<f64>> {
    let bytes = STANDARD.decode(encoded.trim()).map_err(|error| {
        DataFusionError::Execution(format!("ST_Letters font contains invalid base64: {error}"))
    })?;
    decode_twkb(&bytes)
}

fn geometry_width(geometry: &Wkt<f64>) -> Option<f64> {
    geometry_x_bounds(geometry).map(|(min, max)| max - min)
}

fn geometry_x_bounds(geometry: &Wkt<f64>) -> Option<(f64, f64)> {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    geometry_x_bounds_inner(geometry, &mut min, &mut max);
    (min <= max).then_some((min, max))
}

fn geometry_x_bounds_inner(geometry: &Wkt<f64>, min: &mut f64, max: &mut f64) {
    match geometry {
        Wkt::Point(point) => {
            if let (Some(coord), _) = point.clone().into_inner() {
                *min = min.min(coord.x);
                *max = max.max(coord.x);
            }
        }
        Wkt::LineString(line) => {
            let (coords, _) = line.clone().into_inner();
            for coord in coords {
                *min = min.min(coord.x);
                *max = max.max(coord.x);
            }
        }
        Wkt::Polygon(polygon) => {
            let (rings, _) = polygon.clone().into_inner();
            for ring in rings {
                let (coords, _) = ring.into_inner();
                for coord in coords {
                    *min = min.min(coord.x);
                    *max = max.max(coord.x);
                }
            }
        }
        Wkt::MultiPoint(multi_point) => {
            let (points, _) = multi_point.clone().into_inner();
            for point in points {
                if let (Some(coord), _) = point.into_inner() {
                    *min = min.min(coord.x);
                    *max = max.max(coord.x);
                }
            }
        }
        Wkt::MultiLineString(multi_line) => {
            let (lines, _) = multi_line.clone().into_inner();
            for line in lines {
                let (coords, _) = line.into_inner();
                for coord in coords {
                    *min = min.min(coord.x);
                    *max = max.max(coord.x);
                }
            }
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, _) = multi_polygon.clone().into_inner();
            for polygon in polygons {
                let (rings, _) = polygon.into_inner();
                for ring in rings {
                    let (coords, _) = ring.into_inner();
                    for coord in coords {
                        *min = min.min(coord.x);
                        *max = max.max(coord.x);
                    }
                }
            }
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, _) = collection.clone().into_inner();
            for geometry in geometries {
                geometry_x_bounds_inner(&geometry, min, max);
            }
        }
    }
}

fn translate_x(geometry: Wkt<f64>, offset: f64) -> Wkt<f64> {
    map_coords(geometry, |mut coord| {
        coord.x += offset;
        coord
    })
}

fn scale_geometry(geometry: Wkt<f64>, scale: f64) -> Wkt<f64> {
    map_coords(geometry, |mut coord| {
        coord.x *= scale;
        coord.y *= scale;
        coord
    })
}

fn map_coords(geometry: Wkt<f64>, map: impl Fn(Coord<f64>) -> Coord<f64> + Copy) -> Wkt<f64> {
    match geometry {
        Wkt::Point(point) => {
            let (coord, dimension) = point.into_inner();
            Wkt::Point(Point::new(coord.map(map), dimension))
        }
        Wkt::LineString(line) => {
            let (coords, dimension) = line.into_inner();
            Wkt::LineString(LineString::new(
                coords.into_iter().map(map).collect(),
                dimension,
            ))
        }
        Wkt::Polygon(polygon) => {
            let (rings, dimension) = polygon.into_inner();
            let rings = rings
                .into_iter()
                .map(|ring| {
                    let (coords, ring_dimension) = ring.into_inner();
                    LineString::new(coords.into_iter().map(map).collect(), ring_dimension)
                })
                .collect();
            Wkt::Polygon(Polygon::new(rings, dimension))
        }
        Wkt::MultiPoint(multi_point) => {
            let (points, dimension) = multi_point.into_inner();
            let points = points
                .into_iter()
                .map(|point| {
                    let (coord, point_dimension) = point.into_inner();
                    Point::new(coord.map(map), point_dimension)
                })
                .collect();
            Wkt::MultiPoint(MultiPoint::new(points, dimension))
        }
        Wkt::MultiLineString(multi_line) => {
            let (lines, dimension) = multi_line.into_inner();
            let lines = lines
                .into_iter()
                .map(|line| {
                    let (coords, line_dimension) = line.into_inner();
                    LineString::new(coords.into_iter().map(map).collect(), line_dimension)
                })
                .collect();
            Wkt::MultiLineString(MultiLineString::new(lines, dimension))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            let polygons = polygons
                .into_iter()
                .map(|polygon| {
                    let (rings, polygon_dimension) = polygon.into_inner();
                    let rings = rings
                        .into_iter()
                        .map(|ring| {
                            let (coords, ring_dimension) = ring.into_inner();
                            LineString::new(coords.into_iter().map(map).collect(), ring_dimension)
                        })
                        .collect();
                    Polygon::new(rings, polygon_dimension)
                })
                .collect();
            Wkt::MultiPolygon(MultiPolygon::new(polygons, dimension))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            let geometries = geometries
                .into_iter()
                .map(|geometry| map_coords(geometry, map))
                .collect();
            Wkt::GeometryCollection(wkt::types::GeometryCollection::new(geometries, dimension))
        }
    }
}
