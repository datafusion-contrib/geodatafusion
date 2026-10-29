//! Minimal Mapbox Vector Tile protobuf encoder.
//!
//! The encoder is deliberately independent of the test decoder. It writes only
//! the fields needed by `ST_AsMVT` and preserves the input feature order.

use std::collections::HashMap;

use datafusion::error::DataFusionError;
use geo_traits::{
    CoordTrait, GeometryTrait, LineStringTrait, LineTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait, TriangleTrait,
};
use wkb::reader::Wkb;

use crate::error::GeoDataFusionResult;

const MOVE_TO: u64 = 1;
const LINE_TO: u64 = 2;
const CLOSE_PATH: u64 = 7;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum MvtValue {
    Int(i64),
    UInt(u64),
    Float32(u32),
    Float64(u64),
    Bool(bool),
    Str(String),
}

impl MvtValue {
    pub(crate) fn from_float32(value: f32) -> Self {
        Self::Float32(value.to_bits())
    }

    pub(crate) fn from_float64(value: f64) -> Self {
        Self::Float64(value.to_bits())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MvtFeature {
    pub(crate) geometry: Option<Vec<u8>>,
    pub(crate) id: Option<u64>,
    pub(crate) properties: Vec<(String, MvtValue)>,
}

pub(crate) fn encode_tile(
    layer_name: &str,
    extent: i64,
    features: &[MvtFeature],
) -> GeoDataFusionResult<Vec<u8>> {
    let mut layer = LayerBuilder::new(layer_name, extent);
    for feature in features {
        layer.add_feature(feature)?;
    }

    if layer.features.is_empty() {
        return Ok(Vec::new());
    }

    let mut tile = Vec::new();
    write_length_delimited(&mut tile, 3, &layer.finish());
    Ok(tile)
}

struct LayerBuilder {
    name: String,
    extent: i64,
    features: Vec<LayerFeature>,
    keys: Vec<String>,
    key_index: HashMap<String, usize>,
    values: Vec<MvtValue>,
    value_index: HashMap<MvtValue, usize>,
}

struct LayerFeature {
    id: Option<u64>,
    tags: Vec<u64>,
    geometry_type: u64,
    commands: Vec<u64>,
}

impl LayerBuilder {
    fn new(name: &str, extent: i64) -> Self {
        Self {
            name: name.to_string(),
            extent,
            features: Vec::new(),
            keys: Vec::new(),
            key_index: HashMap::new(),
            values: Vec::new(),
            value_index: HashMap::new(),
        }
    }

    fn add_feature(&mut self, feature: &MvtFeature) -> GeoDataFusionResult<()> {
        let Some(geometry) = &feature.geometry else {
            return Ok(());
        };
        let (geometry_type, commands) = geometry_commands(geometry)?;

        let mut tags = Vec::with_capacity(feature.properties.len() * 2);
        for (name, value) in &feature.properties {
            let key_index = match self.key_index.get(name) {
                Some(index) => *index,
                None => {
                    let index = self.keys.len();
                    self.keys.push(name.clone());
                    self.key_index.insert(name.clone(), index);
                    index
                }
            };
            let value_index = match self.value_index.get(value) {
                Some(index) => *index,
                None => {
                    let index = self.values.len();
                    self.values.push(value.clone());
                    self.value_index.insert(value.clone(), index);
                    index
                }
            };
            tags.push(key_index as u64);
            tags.push(value_index as u64);
        }

        self.features.push(LayerFeature {
            id: feature.id,
            tags,
            geometry_type,
            commands,
        });
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        let mut layer = Vec::new();
        write_string(&mut layer, 1, &self.name);

        for feature in &self.features {
            let mut encoded = Vec::new();
            if let Some(id) = feature.id {
                write_varint_field(&mut encoded, 1, id);
            }
            if !feature.tags.is_empty() {
                write_packed_varints(&mut encoded, 2, &feature.tags);
            }
            write_varint_field(&mut encoded, 3, feature.geometry_type);
            if !feature.commands.is_empty() {
                write_packed_varints(&mut encoded, 4, &feature.commands);
            }
            write_length_delimited(&mut layer, 2, &encoded);
        }

        for key in &self.keys {
            write_string(&mut layer, 3, key);
        }
        for value in &self.values {
            let mut encoded = Vec::new();
            match value {
                MvtValue::Str(value) => write_string(&mut encoded, 1, value),
                MvtValue::Float32(bits) => {
                    write_tag(&mut encoded, 2, 5);
                    encoded.extend_from_slice(&bits.to_le_bytes());
                }
                MvtValue::Float64(bits) => {
                    write_tag(&mut encoded, 3, 1);
                    encoded.extend_from_slice(&bits.to_le_bytes());
                }
                MvtValue::Int(value) if *value >= 0 => {
                    write_varint_field(&mut encoded, 5, *value as u64);
                }
                MvtValue::Int(value) => {
                    write_varint_field(&mut encoded, 6, zigzag_i64(*value) as u64);
                }
                MvtValue::UInt(value) => write_varint_field(&mut encoded, 5, *value),
                MvtValue::Bool(value) => write_varint_field(&mut encoded, 7, u64::from(*value)),
            }
            write_length_delimited(&mut layer, 4, &encoded);
        }

        write_varint_field(
            &mut layer,
            5,
            i32::try_from(self.extent).expect("ST_AsMVT extent is validated before encoding") as u32
                as u64,
        );
        write_varint_field(&mut layer, 15, 2);
        layer
    }
}

fn geometry_commands(wkb: &[u8]) -> GeoDataFusionResult<(u64, Vec<u64>)> {
    let geometry = Wkb::try_new(wkb).map_err(|error| {
        DataFusionError::Execution(format!("ST_AsMVT: invalid geometry WKB: {error}"))
    })?;
    let mut builder = CommandBuilder::default();

    match geometry.as_type() {
        geo_traits::GeometryType::Point(point) => {
            let Some(coord) = point.coord() else {
                // PostGIS emits a zero-count MoveTo for an empty point.
                return Ok((1, vec![1, 0, 0]));
            };
            builder.move_to(&[(coord.x(), coord.y())])?;
            Ok((1, builder.finish()))
        }
        geo_traits::GeometryType::LineString(line_string) => {
            let coords = line_string_coords(&line_string)?;
            if coords.is_empty() {
                return Ok((2, Vec::new()));
            }
            builder.line_string(&coords)?;
            Ok((2, builder.finish()))
        }
        geo_traits::GeometryType::Polygon(polygon) => {
            if !builder.polygon(&polygon)? {
                return Ok((3, Vec::new()));
            }
            Ok((3, builder.finish()))
        }
        geo_traits::GeometryType::MultiPoint(multi_point) => {
            let coords = multi_point
                .points()
                .filter_map(|point| point.coord().map(|coord| (coord.x(), coord.y())))
                .collect::<Vec<_>>();
            if coords.is_empty() {
                return Ok((1, Vec::new()));
            }
            builder.move_to(&coords)?;
            Ok((1, builder.finish()))
        }
        geo_traits::GeometryType::MultiLineString(multi_line_string) => {
            let mut found = false;
            for line_string in multi_line_string.line_strings() {
                let coords = line_string_coords(&line_string)?;
                if coords.is_empty() {
                    continue;
                }
                builder.line_string(&coords)?;
                found = true;
            }
            if !found {
                return Ok((2, Vec::new()));
            }
            Ok((2, builder.finish()))
        }
        geo_traits::GeometryType::MultiPolygon(multi_polygon) => {
            let mut found = false;
            for polygon in multi_polygon.polygons() {
                found |= builder.polygon(&polygon)?;
            }
            if !found {
                return Ok((3, Vec::new()));
            }
            Ok((3, builder.finish()))
        }
        geo_traits::GeometryType::Rect(rect) => {
            let min = rect.min();
            let max = rect.max();
            builder.move_to(&[(min.x(), min.y())])?;
            builder.line_to(&[(min.x(), max.y()), (max.x(), max.y()), (max.x(), min.y())])?;
            builder.close_path();
            Ok((3, builder.finish()))
        }
        geo_traits::GeometryType::Triangle(triangle) => {
            let coords = triangle
                .coords()
                .iter()
                .map(|coord| (coord.x(), coord.y()))
                .collect::<Vec<_>>();
            if coords.is_empty() {
                return Ok((3, Vec::new()));
            }
            builder.move_to(&coords[..1])?;
            if coords.len() > 1 {
                builder.line_to(&coords[1..])?;
            }
            builder.close_path();
            Ok((3, builder.finish()))
        }
        geo_traits::GeometryType::Line(line) => {
            let coords = line
                .coords()
                .iter()
                .map(|coord| (coord.x(), coord.y()))
                .collect::<Vec<_>>();
            if coords.is_empty() {
                return Ok((2, Vec::new()));
            }
            builder.line_string(&coords)?;
            Ok((2, builder.finish()))
        }
        geo_traits::GeometryType::GeometryCollection(_) => Err(DataFusionError::Execution(
            "ST_AsMVT: 'GeometryCollection' geometry type not supported".to_string(),
        )
        .into()),
    }
}

fn line_string_coords(
    line_string: &impl LineStringTrait<T = f64>,
) -> GeoDataFusionResult<Vec<(f64, f64)>> {
    Ok(line_string
        .coords()
        .map(|coord| (coord.x(), coord.y()))
        .collect())
}

#[derive(Default)]
struct CommandBuilder {
    commands: Vec<u64>,
    cursor_x: i64,
    cursor_y: i64,
}

impl CommandBuilder {
    fn finish(self) -> Vec<u64> {
        self.commands
    }

    fn move_to(&mut self, coords: &[(f64, f64)]) -> GeoDataFusionResult<()> {
        self.commands.push((coords.len() as u64) << 3 | MOVE_TO);
        self.coordinates(coords)
    }

    fn line_to(&mut self, coords: &[(f64, f64)]) -> GeoDataFusionResult<()> {
        if coords.is_empty() {
            return Ok(());
        }
        self.commands.push((coords.len() as u64) << 3 | LINE_TO);
        self.coordinates(coords)
    }

    fn close_path(&mut self) {
        self.commands.push((1 << 3) | CLOSE_PATH);
    }

    fn coordinates(&mut self, coords: &[(f64, f64)]) -> GeoDataFusionResult<()> {
        for &(x, y) in coords {
            let x = coordinate_to_i32(x, "x")? as i64;
            let y = coordinate_to_i32(y, "y")? as i64;
            let dx = x - self.cursor_x;
            let dy = y - self.cursor_y;
            self.cursor_x = x;
            self.cursor_y = y;
            self.commands.push(zigzag_i32(dx, "x")?);
            self.commands.push(zigzag_i32(dy, "y")?);
        }
        Ok(())
    }

    fn line_string(&mut self, coords: &[(f64, f64)]) -> GeoDataFusionResult<()> {
        self.move_to(&coords[..1])?;
        self.line_to(&coords[1..])
    }

    fn polygon(&mut self, polygon: &impl PolygonTrait<T = f64>) -> GeoDataFusionResult<bool> {
        let Some(exterior) = polygon.exterior() else {
            return Ok(false);
        };
        let exterior = ring_coords(&exterior)?;
        if exterior.is_empty() {
            return Ok(false);
        }
        self.ring(&exterior)?;

        for interior in polygon.interiors() {
            let interior = ring_coords(&interior)?;
            if !interior.is_empty() {
                self.ring(&interior)?;
            }
        }
        Ok(true)
    }

    fn ring(&mut self, coords: &[(f64, f64)]) -> GeoDataFusionResult<()> {
        let coords = if coords.len() > 1 && coords[0] == coords[coords.len() - 1] {
            &coords[..coords.len() - 1]
        } else {
            coords
        };
        if coords.is_empty() {
            return Ok(());
        }
        self.move_to(&coords[..1])?;
        if coords.len() > 1 {
            self.line_to(&coords[1..])?;
        }
        self.close_path();
        Ok(())
    }
}

fn ring_coords(ring: &impl LineStringTrait<T = f64>) -> GeoDataFusionResult<Vec<(f64, f64)>> {
    Ok(ring.coords().map(|coord| (coord.x(), coord.y())).collect())
}

fn coordinate_to_i32(value: f64, axis: &str) -> GeoDataFusionResult<i32> {
    if !value.is_finite() || value < i32::MIN as f64 || value > i32::MAX as f64 {
        return Err(DataFusionError::Execution(format!(
            "ST_AsMVT: coordinate {axis} value {value} is outside the int32 range"
        ))
        .into());
    }
    Ok(value.trunc() as i32)
}

fn zigzag_i32(value: i64, axis: &str) -> GeoDataFusionResult<u64> {
    let value = i32::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!(
            "ST_AsMVT: coordinate delta for {axis} is outside the int32 range"
        ))
    })?;
    Ok(((value << 1) ^ (value >> 31)) as u32 as u64)
}

fn zigzag_i64(value: i64) -> i64 {
    (value << 1) ^ (value >> 63)
}

fn write_tag(buffer: &mut Vec<u8>, field: u64, wire_type: u64) {
    write_varint(buffer, (field << 3) | wire_type);
}

fn write_varint(buffer: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buffer.push((value as u8) | 0x80);
        value >>= 7;
    }
    buffer.push(value as u8);
}

fn write_varint_field(buffer: &mut Vec<u8>, field: u64, value: u64) {
    write_tag(buffer, field, 0);
    write_varint(buffer, value);
}

fn write_string(buffer: &mut Vec<u8>, field: u64, value: &str) {
    write_length_delimited(buffer, field, value.as_bytes());
}

fn write_length_delimited(buffer: &mut Vec<u8>, field: u64, value: &[u8]) {
    write_tag(buffer, field, 2);
    write_varint(buffer, value.len() as u64);
    buffer.extend_from_slice(value);
}

fn write_packed_varints(buffer: &mut Vec<u8>, field: u64, values: &[u64]) {
    let mut packed = Vec::new();
    for value in values {
        write_varint(&mut packed, *value);
    }
    write_length_delimited(buffer, field, &packed);
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn empty_tile_has_no_layer() {
        assert!(encode_tile("layer", 4096, &[]).unwrap().is_empty());
    }
}
