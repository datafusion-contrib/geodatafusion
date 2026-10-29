//! Independent Mapbox Vector Tile decoder for tests and reviewed baselines.
//!
//! This parser does not use the production encoder. It canonicalizes the
//! semantic content of a tile so reviewed PostGIS bytes can be compared with
//! DataFusion bytes even when protobuf field ordering differs.

#![allow(dead_code)]

use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, String>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DecodedTile {
    pub layers: Vec<DecodedLayer>,
}

impl DecodedTile {
    pub fn only_layer(&self) -> Option<&DecodedLayer> {
        (self.layers.len() == 1).then(|| &self.layers[0])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DecodedLayer {
    pub name: String,
    pub extent: u64,
    pub version: u64,
    pub features: Vec<DecodedFeature>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DecodedFeature {
    pub id: Option<u64>,
    pub geometry_type: u64,
    pub geometry: Option<DecodedGeometry>,
    pub properties: BTreeMap<String, DecodedValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DecodedGeometry {
    Points(Vec<(i64, i64)>),
    Lines(Vec<Vec<(i64, i64)>>),
    Polygons(Vec<Vec<Vec<(i64, i64)>>>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DecodedValue {
    String(String),
    Float32(u32),
    Float64(u64),
    Int(i64),
    UInt(u64),
    SInt(i64),
    Bool(bool),
}

pub fn decode_tile(bytes: &[u8]) -> Result<DecodedTile> {
    let mut tile = DecodedTile { layers: Vec::new() };
    let mut fields = Fields::new(bytes);
    while let Some((field, wire, value)) = fields.next_field()? {
        if field != 3 || wire != 2 {
            continue;
        }
        tile.layers.push(decode_layer(value)?);
    }
    for layer in &mut tile.layers {
        layer.features.sort();
    }
    tile.layers.sort();
    Ok(tile)
}

pub fn decode_hex(hex: &str) -> Result<DecodedTile> {
    let bytes = hex::decode(hex).map_err(|error| error.to_string())?;
    decode_tile(&bytes)
}

fn decode_layer(bytes: &[u8]) -> Result<DecodedLayer> {
    let mut name = None;
    let mut extent = 4096;
    let mut version = None;
    let mut feature_bytes = Vec::new();
    let mut keys = Vec::new();
    let mut value_bytes = Vec::new();
    let mut fields = Fields::new(bytes);

    while let Some((field, wire, value)) = fields.next_field()? {
        match (field, wire) {
            (1, 2) => name = Some(decode_string(value)?),
            (2, 2) => feature_bytes.push(value),
            (3, 2) => keys.push(decode_string(value)?),
            (4, 2) => value_bytes.push(value),
            (5, 0) => extent = decode_varint(value)?,
            (15, 0) => version = Some(decode_varint(value)?),
            _ => {}
        }
    }

    let values = value_bytes
        .iter()
        .map(|bytes| decode_value(bytes))
        .collect::<Result<Vec<_>>>()?;
    let mut features = Vec::with_capacity(feature_bytes.len());
    for feature in feature_bytes {
        features.push(decode_feature(feature, &keys, &values)?);
    }

    Ok(DecodedLayer {
        name: name.ok_or_else(|| "MVT layer is missing a name".to_string())?,
        extent,
        version: version.ok_or_else(|| "MVT layer is missing a version".to_string())?,
        features,
    })
}

fn decode_feature(
    bytes: &[u8],
    keys: &[String],
    values: &[DecodedValue],
) -> Result<DecodedFeature> {
    let mut id = None;
    let mut tags = Vec::new();
    let mut geometry_type = None;
    let mut geometry_commands = None;
    let mut fields = Fields::new(bytes);

    while let Some((field, wire, value)) = fields.next_field()? {
        match (field, wire) {
            (1, 0) => id = Some(decode_varint(value)?),
            (2, 0) => tags.push(decode_varint(value)?),
            (2, 2) => tags.extend(decode_packed_varints(value)?),
            (3, 0) => geometry_type = Some(decode_varint(value)?),
            (4, 0) => geometry_commands = Some(vec![decode_varint(value)?]),
            (4, 2) => geometry_commands = Some(decode_packed_varints(value)?),
            _ => {}
        }
    }

    if tags.len() % 2 != 0 {
        return Err("MVT feature has an odd tag count".to_string());
    }
    let mut properties = BTreeMap::new();
    for pair in tags.as_chunks::<2>().0 {
        let key = keys
            .get(pair[0] as usize)
            .ok_or_else(|| format!("MVT key index {} is out of range", pair[0]))?;
        let value = values
            .get(pair[1] as usize)
            .ok_or_else(|| format!("MVT value index {} is out of range", pair[1]))?;
        properties.insert(key.clone(), value.clone());
    }

    let geometry_type = geometry_type.ok_or_else(|| "MVT feature has no type".to_string())?;
    let geometry = geometry_commands
        .map(|commands| decode_geometry(geometry_type, &commands))
        .transpose()?;

    Ok(DecodedFeature {
        id,
        geometry_type,
        geometry,
        properties,
    })
}

fn decode_geometry(geometry_type: u64, commands: &[u64]) -> Result<DecodedGeometry> {
    let mut reader = CommandReader::new(commands);
    let geometry = match geometry_type {
        1 => {
            let mut points = Vec::new();
            while let Some((command, count)) = reader.next_command()? {
                match command {
                    1 if count == 0 => {
                        // PostGIS emits a zero-count MoveTo followed by two
                        // coordinate slots for an empty point. Treat the
                        // remaining slots as opaque and canonicalize it to an
                        // empty point.
                        reader.skip(2)?;
                        break;
                    }
                    1 => points.extend(reader.coordinates(count)?),
                    0 => {}
                    _ => return Err(format!("MVT point geometry has command {command}")),
                }
            }
            DecodedGeometry::Points(points)
        }
        2 => {
            let mut lines = Vec::new();
            while let Some((command, count)) = reader.next_command()? {
                match command {
                    1 => lines.push(reader.coordinates(count)?),
                    2 => {
                        let target = lines
                            .last_mut()
                            .ok_or_else(|| "MVT line has LineTo before MoveTo".to_string())?;
                        target.extend(reader.coordinates(count)?);
                    }
                    _ => return Err(format!("MVT line geometry has command {command}")),
                }
            }
            DecodedGeometry::Lines(lines)
        }
        3 => {
            let mut rings: Vec<Vec<(i64, i64)>> = Vec::new();
            while let Some((command, count)) = reader.next_command()? {
                match command {
                    1 => rings.push(reader.coordinates(count)?),
                    2 => {
                        let ring = rings
                            .last_mut()
                            .ok_or_else(|| "MVT polygon has LineTo before MoveTo".to_string())?;
                        ring.extend(reader.coordinates(count)?);
                    }
                    7 => {
                        if count != 1 {
                            return Err(format!("MVT ClosePath count is {count}"));
                        }
                    }
                    _ => return Err(format!("MVT polygon geometry has command {command}")),
                }
            }
            let polygons = rings
                .into_iter()
                .map(|ring| vec![canonical_ring(ring)])
                .collect();
            DecodedGeometry::Polygons(polygons)
        }
        other => return Err(format!("unsupported MVT geometry type {other}")),
    };
    Ok(geometry)
}

fn canonical_ring(mut ring: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    if ring.len() > 1 && ring[0] == ring[ring.len() - 1] {
        ring.pop();
    }
    if ring.len() <= 1 {
        return ring;
    }

    let reversed = ring.iter().rev().copied().collect::<Vec<_>>();
    let rotations = |source: &[(i64, i64)]| {
        (0..source.len())
            .map(|offset| {
                let mut candidate = Vec::with_capacity(source.len());
                candidate.extend_from_slice(&source[offset..]);
                candidate.extend_from_slice(&source[..offset]);
                candidate
            })
            .collect::<Vec<_>>()
    };
    rotations(&ring)
        .into_iter()
        .chain(rotations(&reversed))
        .min()
        .expect("ring has coordinates")
}

fn decode_value(bytes: &[u8]) -> Result<DecodedValue> {
    let mut fields = Fields::new(bytes);
    while let Some((field, wire, value)) = fields.next_field()? {
        let decoded = match (field, wire) {
            (1, 2) => DecodedValue::String(decode_string(value)?),
            (2, 5) => DecodedValue::Float32(u32::from_le_bytes(
                value
                    .try_into()
                    .map_err(|_| "invalid float32".to_string())?,
            )),
            (3, 1) => DecodedValue::Float64(u64::from_le_bytes(
                value
                    .try_into()
                    .map_err(|_| "invalid float64".to_string())?,
            )),
            (4, 0) => {
                let value = decode_varint(value)?;
                if value <= i64::MAX as u64 {
                    DecodedValue::UInt(value)
                } else {
                    DecodedValue::Int(value as i64)
                }
            }
            (5, 0) => DecodedValue::UInt(decode_varint(value)?),
            (6, 0) => DecodedValue::SInt(zigzag(decode_varint(value)?)),
            (7, 0) => DecodedValue::Bool(decode_varint(value)? != 0),
            _ => continue,
        };
        return Ok(decoded);
    }
    Err("MVT value has no supported field".to_string())
}

fn decode_string(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|error| error.to_string())
}

fn decode_varint(bytes: &[u8]) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    for byte in bytes {
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err("MVT varint is too long".to_string());
        }
    }
    Err("MVT varint is truncated".to_string())
}

fn decode_packed_varints(bytes: &[u8]) -> Result<Vec<u64>> {
    let mut values = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        let start = position;
        while position < bytes.len() && bytes[position] & 0x80 != 0 {
            position += 1;
        }
        if position >= bytes.len() {
            return Err("MVT packed varint is truncated".to_string());
        }
        position += 1;
        values.push(decode_varint(&bytes[start..position])?);
    }
    Ok(values)
}

fn zigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

struct CommandReader<'a> {
    commands: &'a [u64],
    position: usize,
    x: i64,
    y: i64,
}

impl<'a> CommandReader<'a> {
    fn new(commands: &'a [u64]) -> Self {
        Self {
            commands,
            position: 0,
            x: 0,
            y: 0,
        }
    }

    fn next_command(&mut self) -> Result<Option<(u64, u64)>> {
        let Some(&command) = self.commands.get(self.position) else {
            return Ok(None);
        };
        self.position += 1;
        Ok(Some((command & 0x7, command >> 3)))
    }

    fn coordinates(&mut self, count: u64) -> Result<Vec<(i64, i64)>> {
        let mut coords = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let dx = zigzag(
                *self
                    .commands
                    .get(self.position)
                    .ok_or_else(|| "MVT command is missing an X delta".to_string())?,
            );
            let dy = zigzag(
                *self
                    .commands
                    .get(self.position + 1)
                    .ok_or_else(|| "MVT command is missing a Y delta".to_string())?,
            );
            self.position += 2;
            self.x += dx;
            self.y += dy;
            coords.push((self.x, self.y));
        }
        Ok(coords)
    }

    fn skip(&mut self, count: usize) -> Result<()> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| "MVT command position overflow".to_string())?;
        if end > self.commands.len() {
            return Err("MVT command payload is truncated".to_string());
        }
        self.position = end;
        Ok(())
    }
}

struct Fields<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Fields<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn next_field(&mut self) -> Result<Option<(u64, u64, &'a [u8])>> {
        if self.position == self.bytes.len() {
            return Ok(None);
        }
        let key = self.varint()?;
        let field = key >> 3;
        let wire = key & 0x7;
        let value = match wire {
            0 => {
                let start = self.position;
                self.varint()?;
                &self.bytes[start..self.position]
            }
            1 => self.take(8)?,
            2 => {
                let len = self.varint()? as usize;
                self.take(len)?
            }
            5 => self.take(4)?,
            _ => return Err(format!("unsupported MVT wire type {wire}")),
        };
        Ok(Some((field, wire, value)))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| "MVT field length overflow".to_string())?;
        if end > self.bytes.len() {
            return Err("MVT field is truncated".to_string());
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let byte = *self
                .bytes
                .get(self.position)
                .ok_or_else(|| "MVT varint is truncated".to_string())?;
            self.position += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift >= 64 {
                return Err("MVT varint is too long".to_string());
            }
        }
    }
}
