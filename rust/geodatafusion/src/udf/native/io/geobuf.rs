use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, BinaryArray, StructArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use datafusion::scalar::ScalarValue;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType as NativeGeometryType,
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait,
    PolygonTrait,
};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use serde::{Deserialize, Serialize};

use super::common::geometry_field_from_physical;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

const GEOFUB_MAX_PRECISION: u32 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum GbDimension {
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

impl GbDimension {
    fn from_geo_traits(dim: geo_traits::Dimensions) -> GeoDataFusionResult<Self> {
        match dim {
            geo_traits::Dimensions::Xy => Ok(Self::Xy),
            geo_traits::Dimensions::Xyz => Ok(Self::Xyz),
            geo_traits::Dimensions::Xym => Ok(Self::Xym),
            geo_traits::Dimensions::Xyzm => Ok(Self::Xyzm),
            geo_traits::Dimensions::Unknown(dim) => Err(DataFusionError::NotImplemented(format!(
                "ST_AsGeobuf does not support {dim}-dimensional coordinates"
            ))
            .into()),
        }
    }

    fn num_ordinates(self) -> usize {
        match self {
            Self::Xy => 2,
            Self::Xyz | Self::Xym => 3,
            Self::Xyzm => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct GbCoord {
    values: [f64; 4],
}

impl GbCoord {
    fn from_coord<C: CoordTrait<T = f64>>(coord: &C) -> Self {
        let mut values = [0.0; 4];
        for (index, value) in values.iter_mut().enumerate() {
            if index < coord.dim().size() {
                *value = coord.nth_or_panic(index);
            }
        }
        Self { values }
    }

    fn ordinate(self, index: usize, source: GbDimension, target: GbDimension) -> f64 {
        if index >= target.num_ordinates() || index >= source.num_ordinates() {
            0.0
        } else {
            self.values[index]
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum GbGeometry {
    Point(GbDimension, Option<GbCoord>),
    LineString(GbDimension, Vec<GbCoord>),
    Polygon(GbDimension, Vec<Vec<GbCoord>>),
    MultiPoint(GbDimension, Vec<Option<GbCoord>>),
    MultiLineString(GbDimension, Vec<Vec<GbCoord>>),
    MultiPolygon(GbDimension, Vec<Vec<Vec<GbCoord>>>),
    GeometryCollection(GbDimension, Vec<GbGeometry>),
}

impl GbGeometry {
    fn dimension(&self) -> GbDimension {
        match self {
            Self::Point(dimension, _)
            | Self::LineString(dimension, _)
            | Self::Polygon(dimension, _)
            | Self::MultiPoint(dimension, _)
            | Self::MultiLineString(dimension, _)
            | Self::MultiPolygon(dimension, _)
            | Self::GeometryCollection(dimension, _) => *dimension,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum GbValue {
    String(String),
    DoubleBits(u64),
    PositiveInt(u64),
    NegativeInt(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GbFeature {
    geometry: GbGeometry,
    properties: Vec<(u32, GbValue)>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GbState {
    saw_input: bool,
    features: Vec<GbFeature>,
}

fn ensure_finite_coord(coord: &GbCoord, dimension: GbDimension) -> GeoDataFusionResult<()> {
    if coord.values[..dimension.num_ordinates()]
        .iter()
        .all(|value| value.is_finite())
    {
        Ok(())
    } else {
        Err(DataFusionError::Execution(
            "ST_AsGeobuf does not support non-finite coordinates".to_string(),
        )
        .into())
    }
}

fn convert_ring<L: LineStringTrait<T = f64>>(
    ring: &L,
    dimension: GbDimension,
) -> GeoDataFusionResult<Vec<GbCoord>> {
    let mut coords = Vec::with_capacity(ring.num_coords());
    for index in 0..ring.num_coords() {
        let coord = ring.coord(index).ok_or_else(|| {
            DataFusionError::Internal("line-string coordinate index out of range".to_string())
        })?;
        let coord = GbCoord::from_coord(&coord);
        ensure_finite_coord(&coord, dimension)?;
        coords.push(coord);
    }
    Ok(coords)
}

fn convert_polygon<P: PolygonTrait<T = f64>>(
    polygon: &P,
    dimension: GbDimension,
) -> GeoDataFusionResult<Vec<Vec<GbCoord>>> {
    let mut rings = Vec::new();
    if let Some(exterior) = polygon.exterior() {
        let ring = convert_ring(&exterior, dimension)?;
        if ring.is_empty() {
            return Err(DataFusionError::Execution(
                "ST_AsGeobuf does not support empty polygon rings".to_string(),
            )
            .into());
        }
        rings.push(ring);
    }
    for interior in polygon.interiors() {
        let ring = convert_ring(&interior, dimension)?;
        if ring.is_empty() {
            return Err(DataFusionError::Execution(
                "ST_AsGeobuf does not support empty polygon rings".to_string(),
            )
            .into());
        }
        rings.push(ring);
    }
    Ok(rings)
}

fn convert_geometry<G: GeometryTrait<T = f64>>(geometry: &G) -> GeoDataFusionResult<GbGeometry> {
    let dimension = GbDimension::from_geo_traits(geometry.dim())?;

    let geometry = match geometry.as_type() {
        NativeGeometryType::Point(point) => {
            let coord = point
                .coord()
                .map(|coord| -> GeoDataFusionResult<GbCoord> {
                    let coord = GbCoord::from_coord(&coord);
                    ensure_finite_coord(&coord, dimension)?;
                    Ok(coord)
                })
                .transpose()?;
            GbGeometry::Point(dimension, coord)
        }
        NativeGeometryType::LineString(line) => {
            GbGeometry::LineString(dimension, convert_ring(line, dimension)?)
        }
        NativeGeometryType::Polygon(polygon) => {
            GbGeometry::Polygon(dimension, convert_polygon(polygon, dimension)?)
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            let mut points = Vec::with_capacity(multi_point.num_points());
            for index in 0..multi_point.num_points() {
                let point = multi_point.point(index).ok_or_else(|| {
                    DataFusionError::Internal("multi-point index out of range".to_string())
                })?;
                let point = if let Some(coord) = point.coord() {
                    let coord = GbCoord::from_coord(&coord);
                    ensure_finite_coord(&coord, dimension)?;
                    Some(coord)
                } else {
                    return Err(DataFusionError::Execution(
                        "ST_AsGeobuf does not support empty MULTIPOINT members".to_string(),
                    )
                    .into());
                };
                points.push(point);
            }
            GbGeometry::MultiPoint(dimension, points)
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            let mut lines = Vec::with_capacity(multi_line.num_line_strings());
            for index in 0..multi_line.num_line_strings() {
                let line = multi_line.line_string(index).ok_or_else(|| {
                    DataFusionError::Internal("multi-line-string index out of range".to_string())
                })?;
                lines.push(convert_ring(&line, dimension)?);
            }
            GbGeometry::MultiLineString(dimension, lines)
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            let mut polygons = Vec::with_capacity(multi_polygon.num_polygons());
            for index in 0..multi_polygon.num_polygons() {
                let polygon = multi_polygon.polygon(index).ok_or_else(|| {
                    DataFusionError::Internal("multi-polygon index out of range".to_string())
                })?;
                polygons.push(convert_polygon(&polygon, dimension)?);
            }
            GbGeometry::MultiPolygon(dimension, polygons)
        }
        NativeGeometryType::GeometryCollection(collection) => {
            let mut geometries = Vec::with_capacity(collection.num_geometries());
            for index in 0..collection.num_geometries() {
                let geometry = collection.geometry(index).ok_or_else(|| {
                    DataFusionError::Internal("geometry-collection index out of range".to_string())
                })?;
                geometries.push(convert_geometry(&geometry)?);
            }
            GbGeometry::GeometryCollection(dimension, geometries)
        }
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => {
            return Err(DataFusionError::NotImplemented(
                "ST_AsGeobuf supports only GeoArrow geometry types".to_string(),
            )
            .into());
        }
    };
    Ok(geometry)
}

fn convert_geometry_column<'a, A: GeoArrowArrayAccessor<'a>>(
    array: &'a A,
) -> GeoDataFusionResult<Vec<Option<GbGeometry>>> {
    let mut geometries = Vec::with_capacity(array.len());
    for value in array.iter() {
        match value {
            None => geometries.push(None),
            Some(Ok(geometry)) => geometries.push(Some(convert_geometry(&geometry)?)),
            Some(Err(error)) => return Err(error.into()),
        }
    }
    Ok(geometries)
}

fn integer_value(value: i64) -> GbValue {
    if value >= 0 {
        GbValue::PositiveInt(value as u64)
    } else {
        GbValue::NegativeInt(value.unsigned_abs())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn property_value(array: &dyn Array, row: usize) -> GeoDataFusionResult<Option<GbValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Int8 => integer_value(i64::from(array.as_primitive::<Int8Type>().value(row))),
        DataType::Int16 => integer_value(i64::from(array.as_primitive::<Int16Type>().value(row))),
        DataType::Int32 => integer_value(i64::from(array.as_primitive::<Int32Type>().value(row))),
        DataType::Int64 => integer_value(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => {
            GbValue::PositiveInt(u64::from(array.as_primitive::<UInt8Type>().value(row)))
        }
        DataType::UInt16 => {
            GbValue::PositiveInt(u64::from(array.as_primitive::<UInt16Type>().value(row)))
        }
        DataType::UInt32 => {
            GbValue::PositiveInt(u64::from(array.as_primitive::<UInt32Type>().value(row)))
        }
        DataType::UInt64 => GbValue::PositiveInt(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float16 => GbValue::DoubleBits(
            array
                .as_primitive::<Float16Type>()
                .value(row)
                .to_f64()
                .to_bits(),
        ),
        DataType::Float32 => {
            GbValue::DoubleBits(f64::from(array.as_primitive::<Float32Type>().value(row)).to_bits())
        }
        DataType::Float64 => {
            GbValue::DoubleBits(array.as_primitive::<Float64Type>().value(row).to_bits())
        }
        // PostGIS emits PostgreSQL's boolean text representation rather than
        // the Geobuf boolean value, so preserve that observable contract.
        DataType::Boolean => GbValue::String(
            if array.as_boolean().value(row) {
                "t"
            } else {
                "f"
            }
            .to_string(),
        ),
        DataType::Utf8 => GbValue::String(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => GbValue::String(array.as_string::<i64>().value(row).to_string()),
        DataType::Utf8View => GbValue::String(array.as_string_view().value(row).to_string()),
        DataType::Binary => GbValue::String(format!(
            "\\x{}",
            hex_encode(array.as_binary::<i32>().value(row))
        )),
        DataType::LargeBinary => GbValue::String(format!(
            "\\x{}",
            hex_encode(array.as_binary::<i64>().value(row))
        )),
        DataType::BinaryView => GbValue::String(format!(
            "\\x{}",
            hex_encode(array.as_binary_view().value(row))
        )),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "ST_AsGeobuf does not support property type {other}; supported types are integers, floats, booleans, text, and binary"
            ))
            .into());
        }
    };
    Ok(Some(value))
}

fn round_half_up(value: f64) -> f64 {
    let floor = value.floor();
    if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }
}

fn required_precision(value: f64) -> u32 {
    let mut factor = 1.0;
    for precision in 0..=GEOFUB_MAX_PRECISION {
        if round_half_up(value * factor) / factor == value {
            return precision;
        }
        factor *= 10.0;
    }
    GEOFUB_MAX_PRECISION
}

fn feature_precision(feature: &GbFeature, dimension: GbDimension) -> u32 {
    fn visit(geometry: &GbGeometry, dimension: GbDimension, precision: &mut u32) {
        let mut update = |coord: &GbCoord, source: GbDimension| {
            for index in 0..dimension.num_ordinates() {
                *precision =
                    (*precision).max(required_precision(coord.ordinate(index, source, dimension)));
            }
        };
        match geometry {
            GbGeometry::Point(source, coord) => {
                if let Some(coord) = coord {
                    update(coord, *source);
                }
            }
            GbGeometry::LineString(source, coords) => {
                for coord in coords {
                    update(coord, *source);
                }
            }
            GbGeometry::Polygon(source, rings) => {
                for ring in rings {
                    for coord in ring {
                        update(coord, *source);
                    }
                }
            }
            GbGeometry::MultiPoint(source, points) => {
                for coord in points.iter().flatten() {
                    update(coord, *source);
                }
            }
            GbGeometry::MultiLineString(source, lines) => {
                for line in lines {
                    for coord in line {
                        update(coord, *source);
                    }
                }
            }
            GbGeometry::MultiPolygon(source, polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        for coord in ring {
                            update(coord, *source);
                        }
                    }
                }
            }
            GbGeometry::GeometryCollection(_, geometries) => {
                for geometry in geometries {
                    visit(geometry, dimension, precision);
                }
            }
        }
    }

    let mut precision = 0;
    visit(&feature.geometry, dimension, &mut precision);
    precision.min(GEOFUB_MAX_PRECISION)
}

fn write_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn write_tag(output: &mut Vec<u8>, field: u32, wire_type: u32) {
    write_varint(output, (u64::from(field) << 3) | u64::from(wire_type));
}

fn write_varint_field(output: &mut Vec<u8>, field: u32, value: u64) {
    write_tag(output, field, 0);
    write_varint(output, value);
}

fn write_length_field(output: &mut Vec<u8>, field: u32, body: &[u8]) {
    write_tag(output, field, 2);
    write_varint(output, body.len() as u64);
    output.extend_from_slice(body);
}

fn write_fixed64_field(output: &mut Vec<u8>, field: u32, value: u64) {
    write_tag(output, field, 1);
    output.extend_from_slice(&value.to_le_bytes());
}

fn zigzag64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn write_packed_u32(output: &mut Vec<u8>, field: u32, values: &[u32]) {
    if values.is_empty() {
        return;
    }
    let mut body = Vec::new();
    for value in values {
        write_varint(&mut body, u64::from(*value));
    }
    write_length_field(output, field, &body);
}

fn write_packed_sint64(output: &mut Vec<u8>, field: u32, values: &[i64]) {
    if values.is_empty() {
        return;
    }
    let mut body = Vec::new();
    for value in values {
        write_varint(&mut body, zigzag64(*value));
    }
    write_length_field(output, field, &body);
}

fn encode_value(value: &GbValue) -> Vec<u8> {
    let mut body = Vec::new();
    match value {
        GbValue::String(value) => write_length_field(&mut body, 1, value.as_bytes()),
        GbValue::DoubleBits(bits) => write_fixed64_field(&mut body, 2, *bits),
        GbValue::PositiveInt(value) => write_varint_field(&mut body, 3, *value),
        GbValue::NegativeInt(value) => write_varint_field(&mut body, 4, *value),
    }
    body
}

fn write_coords(
    output: &mut Vec<i64>,
    coords: &[GbCoord],
    source: GbDimension,
    target: GbDimension,
    factor: f64,
    sums: &mut [i64; 4],
) {
    for coord in coords {
        for (index, sum) in sums.iter_mut().enumerate().take(target.num_ordinates()) {
            let value = round_half_up(coord.ordinate(index, source, target) * factor) as i64;
            let delta = value - *sum;
            *sum = value;
            output.push(delta);
        }
    }
}

fn encode_geometry(
    geometry: &GbGeometry,
    target: GbDimension,
    factor: f64,
) -> GeoDataFusionResult<Vec<u8>> {
    let mut body = Vec::new();
    let (type_code, source) = match geometry {
        GbGeometry::Point(source, _) => (0u64, *source),
        GbGeometry::MultiPoint(source, _) => (1, *source),
        GbGeometry::LineString(source, _) => (2, *source),
        GbGeometry::MultiLineString(source, _) => (3, *source),
        GbGeometry::Polygon(source, _) => (4, *source),
        GbGeometry::MultiPolygon(source, _) => (5, *source),
        GbGeometry::GeometryCollection(source, _) => (6, *source),
    };
    write_varint_field(&mut body, 1, type_code);

    let mut coords = Vec::new();
    let mut lengths = Vec::new();
    match geometry {
        GbGeometry::Point(_, coord) => {
            if let Some(coord) = coord {
                let mut sums = [0i64; 4];
                write_coords(
                    &mut coords,
                    std::slice::from_ref(coord),
                    source,
                    target,
                    factor,
                    &mut sums,
                );
            }
        }
        GbGeometry::LineString(_, line) => {
            let mut sums = [0i64; 4];
            write_coords(&mut coords, line, source, target, factor, &mut sums);
        }
        GbGeometry::Polygon(_, rings) => {
            for ring in rings {
                let stored = if ring.is_empty() {
                    &[][..]
                } else {
                    &ring[..ring.len() - 1]
                };
                lengths.push(stored.len() as u32);
                let mut sums = [0i64; 4];
                write_coords(&mut coords, stored, source, target, factor, &mut sums);
            }
            if lengths.len() <= 1 {
                lengths.clear();
            }
        }
        GbGeometry::MultiPoint(_, points) => {
            for point in points.iter().flatten() {
                let mut sums = [0i64; 4];
                write_coords(
                    &mut coords,
                    std::slice::from_ref(point),
                    source,
                    target,
                    factor,
                    &mut sums,
                );
            }
        }
        GbGeometry::MultiLineString(_, lines) => {
            for line in lines {
                lengths.push(line.len() as u32);
                let mut sums = [0i64; 4];
                write_coords(&mut coords, line, source, target, factor, &mut sums);
            }
            if lengths.len() <= 1 {
                lengths.clear();
            }
        }
        GbGeometry::MultiPolygon(_, polygons) => {
            lengths.push(polygons.len() as u32);
            for polygon in polygons {
                lengths.push(polygon.len() as u32);
                for ring in polygon {
                    let stored = if ring.is_empty() {
                        &[][..]
                    } else {
                        &ring[..ring.len() - 1]
                    };
                    lengths.push(stored.len() as u32);
                    let mut sums = [0i64; 4];
                    write_coords(&mut coords, stored, source, target, factor, &mut sums);
                }
            }
            if lengths.len() <= 1 {
                lengths.clear();
            }
        }
        GbGeometry::GeometryCollection(_, geometries) => {
            for geometry in geometries {
                let child = encode_geometry(geometry, target, factor)?;
                write_length_field(&mut body, 4, &child);
            }
            return Ok(body);
        }
    }

    write_packed_u32(&mut body, 2, &lengths);
    write_packed_sint64(&mut body, 3, &coords);
    Ok(body)
}

fn encode_feature(
    feature: &GbFeature,
    target: GbDimension,
    factor: f64,
) -> GeoDataFusionResult<Vec<u8>> {
    let mut body = Vec::new();
    let geometry = encode_geometry(&feature.geometry, target, factor)?;
    write_length_field(&mut body, 1, &geometry);

    let mut properties = Vec::new();
    let mut values = Vec::new();
    for (key, value) in &feature.properties {
        properties.push(*key);
        properties.push(values.len() as u32);
        values.push(encode_value(value));
    }
    for value in &values {
        write_length_field(&mut body, 13, value);
    }
    write_packed_u32(&mut body, 14, &properties);
    Ok(body)
}

fn encode_geobuf(keys: &[String], features: &[GbFeature]) -> GeoDataFusionResult<Vec<u8>> {
    let dimension = features
        .first()
        .map(|feature| feature.geometry.dimension())
        .unwrap_or(GbDimension::Xy);
    let precision = features
        .iter()
        .map(|feature| feature_precision(feature, dimension))
        .max()
        .unwrap_or(0);
    let factor = 10f64.powi(precision as i32);

    let mut data = Vec::new();
    for key in keys {
        write_length_field(&mut data, 1, key.as_bytes());
    }
    if dimension != GbDimension::Xy {
        write_varint_field(&mut data, 2, dimension.num_ordinates() as u64);
    }
    if precision != GEOFUB_MAX_PRECISION {
        write_varint_field(&mut data, 3, u64::from(precision));
    }

    let mut feature_collection = Vec::new();
    for feature in features {
        let body = encode_feature(feature, dimension, factor)?;
        write_length_field(&mut feature_collection, 1, &body);
    }
    write_length_field(&mut data, 4, &feature_collection);
    Ok(data)
}

#[derive(Debug)]
struct GeobufAccumulator {
    fields: Vec<FieldRef>,
    geometry_index: usize,
    property_indices: Vec<usize>,
    geometry_field: FieldRef,
    saw_input: bool,
    features: Vec<GbFeature>,
}

impl GeobufAccumulator {
    fn new(struct_field: &Field, geom_name: Option<&str>) -> Result<Self> {
        let DataType::Struct(fields) = struct_field.data_type() else {
            return Err(DataFusionError::Execution(format!(
                "ST_AsGeobuf expects a struct row, got {}",
                struct_field.data_type()
            )));
        };
        let (geometry_index, geometry_field) = match geom_name {
            Some(name) => {
                let index = fields
                    .iter()
                    .position(|field| field.name() == name)
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "ST_AsGeobuf geometry column `{name}` was not found"
                        ))
                    })?;
                let field = geometry_field_from_physical(&fields[index]).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "ST_AsGeobuf field `{name}` is not a GeoArrow geometry"
                    ))
                })?;
                (index, field)
            }
            None => {
                let mut found = None;
                for (index, field) in fields.iter().enumerate() {
                    if let Some(geometry_field) = geometry_field_from_physical(field) {
                        found = Some((index, geometry_field));
                        break;
                    }
                }
                found.ok_or_else(|| {
                    DataFusionError::Execution("ST_AsGeobuf found no geometry column".to_string())
                })?
            }
        };
        let property_indices = (0..fields.len())
            .filter(|index| *index != geometry_index)
            .collect();
        Ok(Self {
            fields: fields.iter().cloned().collect(),
            geometry_index,
            property_indices,
            geometry_field,
            saw_input: false,
            features: Vec::new(),
        })
    }
}

impl Accumulator for GeobufAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let structs = values
            .first()
            .ok_or_else(|| {
                DataFusionError::Internal("ST_AsGeobuf received no input batch".to_string())
            })?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("ST_AsGeobuf input is not a struct array".to_string())
            })?;
        if structs.is_empty() {
            return Ok(());
        }
        self.saw_input = true;

        let geometry_array = structs.column(self.geometry_index);
        let geometry_array = geometry_array_from_arrow(geometry_array, &self.geometry_field)?;
        let geometry_array_ref = geometry_array.as_ref();
        let geometries = downcast_geoarrow_array!(geometry_array_ref, convert_geometry_column)?;

        for (row, geometry) in geometries.iter().enumerate() {
            if structs.is_null(row) {
                continue;
            }
            let Some(geometry) = geometry.clone() else {
                continue;
            };
            let mut properties = Vec::new();
            for (position, field_index) in self.property_indices.iter().enumerate() {
                let column = structs.column(*field_index);
                if let Some(value) = property_value(column.as_ref(), row)? {
                    properties.push((position as u32, value));
                }
            }
            self.features.push(GbFeature {
                geometry,
                properties,
            });
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states = states.first().ok_or_else(|| {
            DataFusionError::Internal("ST_AsGeobuf received no state batch".to_string())
        })?;
        let states = states
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("ST_AsGeobuf expected a Binary state array".to_string())
            })?;
        for index in 0..states.len() {
            if states.is_null(index) {
                continue;
            }
            let state: GbState = serde_json::from_slice(states.value(index)).map_err(|error| {
                DataFusionError::Execution(format!(
                    "ST_AsGeobuf could not decode partial state: {error}"
                ))
            })?;
            self.saw_input |= state.saw_input;
            self.features.extend(state.features);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let state = GbState {
            saw_input: self.saw_input,
            features: self.features.clone(),
        };
        Ok(vec![ScalarValue::Binary(Some(
            serde_json::to_vec(&state).map_err(|error| {
                DataFusionError::Execution(format!(
                    "ST_AsGeobuf could not encode partial state: {error}"
                ))
            })?,
        ))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if !self.saw_input {
            return Ok(ScalarValue::Binary(None));
        }
        let keys = self
            .fields
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != self.geometry_index)
            .map(|(_, field)| field.name().clone())
            .collect::<Vec<_>>();
        match encode_geobuf(&keys, &self.features) {
            Ok(bytes) => Ok(ScalarValue::Binary(Some(bytes))),
            Err(error) => Err(DataFusionError::Execution(error.to_string())),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.features.len() * std::mem::size_of::<GbFeature>()
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsGeobuf {
    signature: Signature,
}

impl AsGeobuf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for AsGeobuf {
    fn default() -> Self {
        Self::new()
    }
}

static AS_GEOBUF_DOC: OnceLock<Documentation> = OnceLock::new();

impl AggregateUDFImpl for AsGeobuf {
    fn name(&self) -> &str {
        "st_asgeobuf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(1..=2).contains(&arg_types.len()) {
            return Err(DataFusionError::Execution(format!(
                "ST_AsGeobuf expects one struct argument and an optional geometry column name, got {} arguments",
                arg_types.len()
            )));
        }
        if !matches!(arg_types[0], DataType::Struct(_)) {
            return Err(DataFusionError::Execution(format!(
                "ST_AsGeobuf first argument must be a struct row, got {}",
                arg_types[0]
            )));
        }
        if arg_types.len() == 2
            && !matches!(
                arg_types[1],
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            )
        {
            return Err(DataFusionError::Execution(format!(
                "ST_AsGeobuf second argument must be a text geometry column name, got {}",
                arg_types[1]
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field(&self, _arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("", DataType::Binary, true)))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let struct_field = acc_args.expr_fields[0].as_ref();
        let geom_name = match acc_args.exprs.get(1) {
            None => None,
            Some(expression) => {
                let literal = (expression.as_ref() as &dyn std::any::Any)
                    .downcast_ref::<Literal>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "ST_AsGeobuf geometry column name must be a literal".to_string(),
                        )
                    })?;
                match literal.value() {
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::LargeUtf8(Some(value))
                    | ScalarValue::Utf8View(Some(value)) => Some(value.as_str()),
                    ScalarValue::Utf8(None)
                    | ScalarValue::LargeUtf8(None)
                    | ScalarValue::Utf8View(None)
                    | ScalarValue::Null => None,
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "ST_AsGeobuf geometry column name must be text, got {other:?}"
                        )));
                    }
                }
            }
        };
        Ok(Box::new(GeobufAccumulator::new(struct_field, geom_name)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "state"),
            DataType::Binary,
            true,
        ))])
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_GEOBUF_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Geobuf representation of a set of rows.",
                "ST_AsGeobuf(row[, geom_name])",
            )
            .with_argument(
                "row",
                "record containing one geometry column and properties",
            )
            .build()
        }))
    }
}
