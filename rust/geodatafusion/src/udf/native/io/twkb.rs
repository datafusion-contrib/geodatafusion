use std::sync::{Arc, OnceLock};

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, LargeListArray, ListArray};
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType as NativeGeometryType,
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait,
    PolygonTrait,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType, Metadata};
use serde_json::Value;
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
};

use super::common::geometry_field_from_physical;
use crate::data_types::any_geometry_type;
use crate::error::{GeoDataFusionError, GeoDataFusionResult};
use crate::udf::native::crs::{CrsKind, classify_metadata};
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata, geometry_array_from_arrow,
};

const TWKB_TYPE_POINT: u8 = 1;
const TWKB_TYPE_LINESTRING: u8 = 2;
const TWKB_TYPE_POLYGON: u8 = 3;
const TWKB_TYPE_MULTIPOINT: u8 = 4;
const TWKB_TYPE_MULTILINESTRING: u8 = 5;
const TWKB_TYPE_MULTIPOLYGON: u8 = 6;
const TWKB_TYPE_GEOMETRYCOLLECTION: u8 = 7;

const TWKB_METADATA_BBOX: u8 = 0x01;
const TWKB_METADATA_SIZE: u8 = 0x02;
const TWKB_METADATA_IDLIST: u8 = 0x04;
const TWKB_METADATA_EXTENDED: u8 = 0x08;
const TWKB_METADATA_EMPTY: u8 = 0x10;

const TWKB_MAX_PRECISION: i32 = 7;
/// Bounds nested geometry collections before they can exhaust the stack.
const TWKB_MAX_DEPTH: usize = 64;

#[derive(Debug, Clone, Copy)]
struct TwkbOptions {
    precision_xy: i32,
    precision_z: i32,
    precision_m: i32,
    with_sizes: bool,
    with_boxes: bool,
}

impl TwkbOptions {
    fn from_columns(
        columns: &[ArrayRef],
        row: usize,
        offset: usize,
        metadata: &Metadata,
    ) -> Result<Self> {
        let precision_at = |index: usize| -> Result<Option<i32>> {
            let Some(argument) = columns.get(offset + index) else {
                return Ok(None);
            };
            if argument.is_null(row) {
                return Ok(None);
            }
            match argument.data_type() {
                DataType::Int32 => Ok(Some(argument.as_primitive::<Int32Type>().value(row))),
                DataType::Int64 => {
                    let value = argument.as_primitive::<Int64Type>().value(row);
                    i32::try_from(value).map(Some).map_err(|_| {
                        DataFusionError::Execution(format!(
                            "ST_AsTWKB precision {value} is outside the integer range"
                        ))
                    })
                }
                other => Err(DataFusionError::Internal(format!(
                    "ST_AsTWKB precision column has unsupported type {other}"
                ))),
            }
        };
        let boolean_at = |index: usize| -> Result<Option<bool>> {
            let Some(argument) = columns.get(offset + index) else {
                return Ok(None);
            };
            if argument.is_null(row) {
                return Ok(None);
            }
            match argument.data_type() {
                DataType::Boolean => Ok(Some(argument.as_boolean().value(row))),
                other => Err(DataFusionError::Internal(format!(
                    "ST_AsTWKB boolean option column has unsupported type {other}"
                ))),
            }
        };

        let options = Self {
            precision_xy: match precision_at(0)? {
                Some(precision) => precision,
                None => default_xy_precision(metadata)?,
            },
            precision_z: precision_at(1)?.unwrap_or(0),
            precision_m: precision_at(2)?.unwrap_or(0),
            with_sizes: boolean_at(3)?.unwrap_or(false),
            with_boxes: boolean_at(4)?.unwrap_or(false),
        };
        options.validate()?;
        Ok(options)
    }

    fn validate(&self) -> Result<()> {
        if !(-TWKB_MAX_PRECISION..=TWKB_MAX_PRECISION).contains(&self.precision_xy) {
            return Err(DataFusionError::Execution(format!(
                "ST_AsTWKB X/Y precision must be between -{TWKB_MAX_PRECISION} and {TWKB_MAX_PRECISION}"
            )));
        }
        for (axis, precision) in [("Z", self.precision_z), ("M", self.precision_m)] {
            if !(0..=TWKB_MAX_PRECISION).contains(&precision) {
                return Err(DataFusionError::Execution(format!(
                    "ST_AsTWKB {axis} precision must be between 0 and {TWKB_MAX_PRECISION}"
                )));
            }
        }
        Ok(())
    }

    fn precision_for(&self, index: usize) -> i32 {
        match index {
            0 | 1 => self.precision_xy,
            2 => self.precision_z,
            3 => self.precision_m,
            _ => unreachable!("TWKB has at most four ordinates"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TwkbDimension {
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

impl TwkbDimension {
    fn from_geometry<G: GeometryTrait<T = f64>>(geometry: &G) -> GeoDataFusionResult<Self> {
        match geometry.dim() {
            geo_traits::Dimensions::Xy => Ok(Self::Xy),
            geo_traits::Dimensions::Xyz => Ok(Self::Xyz),
            geo_traits::Dimensions::Xym => Ok(Self::Xym),
            geo_traits::Dimensions::Xyzm => Ok(Self::Xyzm),
            geo_traits::Dimensions::Unknown(dimensions) => Err(DataFusionError::NotImplemented(
                format!("ST_AsTWKB does not support {dimensions}-dimensional coordinates"),
            )
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

    fn has_z(self) -> bool {
        matches!(self, Self::Xyz | Self::Xyzm)
    }

    fn has_m(self) -> bool {
        matches!(self, Self::Xym | Self::Xyzm)
    }

    fn wkt_dimension(self) -> Dimension {
        match self {
            Self::Xy => Dimension::XY,
            Self::Xyz => Dimension::XYZ,
            Self::Xym => Dimension::XYM,
            Self::Xyzm => Dimension::XYZM,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Bbox {
    min: [i64; 4],
    max: [i64; 4],
    initialized: bool,
}

impl Bbox {
    fn update(&mut self, ordinates: &[i64; 4], dimensions: TwkbDimension) {
        for (index, value) in ordinates
            .iter()
            .take(dimensions.num_ordinates())
            .enumerate()
        {
            if !self.initialized {
                self.min[index] = *value;
                self.max[index] = *value;
            } else {
                self.min[index] = self.min[index].min(*value);
                self.max[index] = self.max[index].max(*value);
            }
        }
        self.initialized = true;
    }

    fn write(&self, output: &mut Vec<u8>, dimensions: TwkbDimension) {
        for index in 0..dimensions.num_ordinates() {
            write_svarint(output, self.min[index]);
            write_svarint(output, self.max[index] - self.min[index]);
        }
    }

    fn encoded_size(&self, dimensions: TwkbDimension) -> usize {
        (0..dimensions.num_ordinates())
            .map(|index| {
                svarint_size(self.min[index]) + svarint_size(self.max[index] - self.min[index])
            })
            .sum()
    }

    fn union(&mut self, other: &Bbox, dimensions: TwkbDimension) {
        if !other.initialized {
            return;
        }
        if !self.initialized {
            *self = *other;
            return;
        }
        for index in 0..dimensions.num_ordinates() {
            self.min[index] = self.min[index].min(other.min[index]);
            self.max[index] = self.max[index].max(other.max[index]);
        }
    }
}

fn svarint_size(value: i64) -> usize {
    let mut buffer = [0u8; 10];
    svarint_bytes(value, &mut buffer)
}

fn svarint_bytes(value: i64, buffer: &mut [u8; 10]) -> usize {
    let mut encoded = zigzag64(value);
    let mut length = 0;
    loop {
        let byte = (encoded & 0x7f) as u8;
        encoded >>= 7;
        if encoded == 0 {
            buffer[length] = byte;
            length += 1;
            return length;
        }
        buffer[length] = byte | 0x80;
        length += 1;
    }
}

fn write_svarint(output: &mut Vec<u8>, value: i64) {
    write_uvarint(output, zigzag64(value));
}

fn write_uvarint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn zigzag64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn zigzag8(value: i8) -> u8 {
    ((value << 1) ^ (value >> 7)) as u8
}

fn unzigzag8(value: u8) -> i8 {
    ((value >> 1) as i8) ^ -((value & 1) as i8)
}

fn default_xy_precision(metadata: &Metadata) -> Result<i32> {
    match classify_metadata(metadata) {
        CrsKind::Geographic => Ok(5),
        CrsKind::NonGeographic => Ok(0),
        CrsKind::Unknown => Err(DataFusionError::Execution(format!(
            "ST_AsTWKB cannot determine whether CRS {} is geographic; pass an explicit X/Y precision",
            metadata
                .crs()
                .crs_value()
                .map_or_else(|| "<missing>".to_string(), Value::to_string)
        ))),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Ordinate {
    values: [f64; 4],
}

impl Ordinate {
    fn from_coord<C: CoordTrait<T = f64>>(coord: &C) -> Self {
        let mut values = [0.0; 4];
        for (index, value) in values.iter_mut().enumerate() {
            if index < coord.dim().size() {
                *value = coord.nth_or_panic(index);
            }
        }
        Self { values }
    }

    fn write(
        &self,
        output: &mut Vec<u8>,
        dimensions: TwkbDimension,
        options: &TwkbOptions,
        sums: &mut [i64; 4],
    ) {
        for (index, value) in self
            .values
            .iter()
            .take(dimensions.num_ordinates())
            .enumerate()
        {
            let factor = 10f64.powi(options.precision_for(index));
            let scaled = (value * factor).round() as i64;
            let delta = scaled - sums[index];
            sums[index] = scaled;
            write_svarint(output, delta);
        }
    }
}

fn collect_line<L: LineStringTrait<T = f64>>(line: &L) -> Vec<Ordinate> {
    (0..line.num_coords())
        .filter_map(|index| line.coord(index))
        .map(|coord| Ordinate::from_coord(&coord))
        .collect()
}

fn collect_polygon_rings<P: PolygonTrait<T = f64>>(polygon: &P) -> Vec<Vec<Ordinate>> {
    let mut rings = Vec::new();
    if let Some(exterior) = polygon.exterior() {
        rings.push(collect_line(&exterior));
    }
    for interior in polygon.interiors() {
        rings.push(collect_line(&interior));
    }
    rings
}

fn geometry_is_empty<G: GeometryTrait<T = f64>>(geometry: &G) -> bool {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => point.coord().is_none(),
        NativeGeometryType::LineString(line) => line.num_coords() == 0,
        NativeGeometryType::Polygon(polygon) => {
            polygon.exterior().is_none_or(|ring| ring.num_coords() == 0)
        }
        NativeGeometryType::MultiPoint(multi_point) => (0..multi_point.num_points())
            .filter_map(|index| multi_point.point(index))
            .all(|point| point.coord().is_none()),
        NativeGeometryType::MultiLineString(multi_line) => (0..multi_line.num_line_strings())
            .filter_map(|index| multi_line.line_string(index))
            .all(|line| line.num_coords() == 0),
        NativeGeometryType::MultiPolygon(multi_polygon) => (0..multi_polygon.num_polygons())
            .filter_map(|index| multi_polygon.polygon(index))
            .all(|polygon| polygon.exterior().is_none_or(|ring| ring.num_coords() == 0)),
        NativeGeometryType::GeometryCollection(collection) => (0..collection.num_geometries())
            .filter_map(|index| collection.geometry(index))
            .all(|geometry| geometry_is_empty(&geometry)),
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => false,
    }
}

fn write_point_array_values(
    points: &[Ordinate],
    output: &mut Vec<u8>,
    dimensions: TwkbDimension,
    options: &TwkbOptions,
    min_points: usize,
    sums: &mut [i64; 4],
) {
    let mut encoded = Vec::new();
    let mut max_points_left = points.len();
    let mut written = 0usize;
    for (index, point) in points.iter().enumerate() {
        let duplicate = index > 0 && point == &points[index - 1];
        if duplicate && max_points_left > min_points {
            max_points_left -= 1;
            continue;
        }
        written += 1;
        point.write(&mut encoded, dimensions, options, sums);
    }
    write_uvarint(output, written as u64);
    output.extend_from_slice(&encoded);
}

fn write_line_body(
    line: &impl LineStringTrait<T = f64>,
    output: &mut Vec<u8>,
    dimensions: TwkbDimension,
    options: &TwkbOptions,
    sums: &mut [i64; 4],
) {
    let points = collect_line(line);
    write_point_array_values(&points, output, dimensions, options, 2, sums);
}

fn write_polygon_body<P: PolygonTrait<T = f64>>(
    polygon: &P,
    output: &mut Vec<u8>,
    dimensions: TwkbDimension,
    options: &TwkbOptions,
    sums: &mut [i64; 4],
) {
    let rings = collect_polygon_rings(polygon);
    write_uvarint(output, rings.len() as u64);
    for ring in &rings {
        write_point_array_values(ring, output, dimensions, options, 4, sums);
    }
}

fn write_geometry_body<G: GeometryTrait<T = f64>>(
    geometry: &G,
    output: &mut Vec<u8>,
    dimensions: TwkbDimension,
    options: &TwkbOptions,
    idlist: Option<&[i64]>,
) -> GeoDataFusionResult<()> {
    write_geometry_body_with_sums(
        geometry,
        output,
        dimensions,
        options,
        idlist,
        &mut [0i64; 4],
    )
}

fn write_geometry_body_with_sums<G: GeometryTrait<T = f64>>(
    geometry: &G,
    output: &mut Vec<u8>,
    dimensions: TwkbDimension,
    options: &TwkbOptions,
    idlist: Option<&[i64]>,
    sums: &mut [i64; 4],
) -> GeoDataFusionResult<()> {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => {
            let Some(coord) = point.coord() else {
                return Ok(());
            };
            Ordinate::from_coord(&coord).write(output, dimensions, options, sums);
        }
        NativeGeometryType::LineString(line) => {
            write_line_body(line, output, dimensions, options, sums);
        }
        NativeGeometryType::Polygon(polygon) => {
            write_polygon_body(polygon, output, dimensions, options, sums);
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            let points = (0..multi_point.num_points())
                .filter_map(|index| multi_point.point(index))
                .filter(|point| point.coord().is_some())
                .collect::<Vec<_>>();
            write_uvarint(output, points.len() as u64);
            if let Some(idlist) = idlist {
                let all_points = (0..multi_point.num_points())
                    .filter_map(|index| multi_point.point(index))
                    .collect::<Vec<_>>();
                if idlist.len() != all_points.len() {
                    return Err(DataFusionError::Execution(format!(
                        "ST_AsTWKB id array has {} entries but geometry array has {} entries",
                        idlist.len(),
                        all_points.len()
                    ))
                    .into());
                }
                for (point, id) in all_points.iter().zip(idlist) {
                    if point.coord().is_some() {
                        write_svarint(output, *id);
                    }
                }
            }
            for point in points {
                let coord = point.coord().ok_or_else(|| {
                    DataFusionError::Execution("empty point in MULTIPOINT".to_string())
                })?;
                Ordinate::from_coord(&coord).write(output, dimensions, options, sums);
            }
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            let lines = (0..multi_line.num_line_strings())
                .filter_map(|index| multi_line.line_string(index))
                .collect::<Vec<_>>();
            write_uvarint(output, lines.len() as u64);
            if let Some(idlist) = idlist {
                if idlist.len() != lines.len() {
                    return Err(DataFusionError::Execution(format!(
                        "ST_AsTWKB id array has {} entries but geometry array has {} non-null entries",
                        idlist.len(),
                        lines.len()
                    ))
                    .into());
                }
                for id in idlist {
                    write_svarint(output, *id);
                }
            }
            for line in lines {
                write_line_body(&line, output, dimensions, options, sums);
            }
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            let polygons = (0..multi_polygon.num_polygons())
                .filter_map(|index| multi_polygon.polygon(index))
                .collect::<Vec<_>>();
            write_uvarint(output, polygons.len() as u64);
            if let Some(idlist) = idlist {
                if idlist.len() != polygons.len() {
                    return Err(DataFusionError::Execution(format!(
                        "ST_AsTWKB id array has {} entries but geometry array has {} non-null entries",
                        idlist.len(),
                        polygons.len()
                    ))
                    .into());
                }
                for id in idlist {
                    write_svarint(output, *id);
                }
            }
            for polygon in polygons {
                write_polygon_body(&polygon, output, dimensions, options, sums);
            }
        }
        NativeGeometryType::GeometryCollection(collection) => {
            let geometries = (0..collection.num_geometries())
                .filter_map(|index| collection.geometry(index))
                .collect::<Vec<_>>();
            write_uvarint(output, geometries.len() as u64);
            if let Some(idlist) = idlist {
                if idlist.len() != geometries.len() {
                    return Err(DataFusionError::Execution(format!(
                        "ST_AsTWKB id array has {} entries but geometry array has {} non-null entries",
                        idlist.len(),
                        geometries.len()
                    ))
                    .into());
                }
                for id in idlist {
                    write_svarint(output, *id);
                }
            }
            for child in geometries {
                let child = encode_geometry(&child, options, None)?;
                output.extend_from_slice(&child);
            }
        }
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => {
            return Err(DataFusionError::NotImplemented(
                "ST_AsTWKB supports only GeoArrow geometry types".to_string(),
            )
            .into());
        }
    }
    Ok(())
}

fn compute_bbox<G: GeometryTrait<T = f64>>(
    geometry: &G,
    dimensions: TwkbDimension,
    options: &TwkbOptions,
) -> GeoDataFusionResult<Bbox> {
    fn visit<G: GeometryTrait<T = f64>>(
        geometry: &G,
        dimensions: TwkbDimension,
        options: &TwkbOptions,
        bbox: &mut Bbox,
    ) -> GeoDataFusionResult<()> {
        let mut update = |ordinate: &Ordinate| {
            let mut scaled = [0i64; 4];
            for (index, value) in ordinate
                .values
                .iter()
                .take(dimensions.num_ordinates())
                .enumerate()
            {
                let factor = 10f64.powi(options.precision_for(index));
                scaled[index] = (value * factor).round() as i64;
            }
            bbox.update(&scaled, dimensions);
        };
        match geometry.as_type() {
            NativeGeometryType::Point(point) => {
                if let Some(coord) = point.coord() {
                    update(&Ordinate::from_coord(&coord));
                }
            }
            NativeGeometryType::LineString(line) => {
                for coord in (0..line.num_coords()).filter_map(|index| line.coord(index)) {
                    update(&Ordinate::from_coord(&coord));
                }
            }
            NativeGeometryType::Polygon(polygon) => {
                for ring in collect_polygon_rings(polygon) {
                    for coord in ring {
                        update(&coord);
                    }
                }
            }
            NativeGeometryType::MultiPoint(multi_point) => {
                for point in
                    (0..multi_point.num_points()).filter_map(|index| multi_point.point(index))
                {
                    if let Some(coord) = point.coord() {
                        update(&Ordinate::from_coord(&coord));
                    }
                }
            }
            NativeGeometryType::MultiLineString(multi_line) => {
                for line in (0..multi_line.num_line_strings())
                    .filter_map(|index| multi_line.line_string(index))
                {
                    for coord in (0..line.num_coords()).filter_map(|index| line.coord(index)) {
                        update(&Ordinate::from_coord(&coord));
                    }
                }
            }
            NativeGeometryType::MultiPolygon(multi_polygon) => {
                for polygon in (0..multi_polygon.num_polygons())
                    .filter_map(|index| multi_polygon.polygon(index))
                {
                    for ring in collect_polygon_rings(&polygon) {
                        for coord in ring {
                            update(&coord);
                        }
                    }
                }
            }
            NativeGeometryType::GeometryCollection(collection) => {
                for child in
                    (0..collection.num_geometries()).filter_map(|index| collection.geometry(index))
                {
                    visit(&child, dimensions, options, bbox)?;
                }
            }
            NativeGeometryType::Rect(_)
            | NativeGeometryType::Triangle(_)
            | NativeGeometryType::Line(_) => {
                return Err(DataFusionError::NotImplemented(
                    "ST_AsTWKB supports only GeoArrow geometry types".to_string(),
                )
                .into());
            }
        }
        Ok(())
    }

    let mut bbox = Bbox::default();
    visit(geometry, dimensions, options, &mut bbox)?;
    Ok(bbox)
}

fn encode_geometry<G: GeometryTrait<T = f64>>(
    geometry: &G,
    options: &TwkbOptions,
    idlist: Option<&[i64]>,
) -> GeoDataFusionResult<Vec<u8>> {
    let dimensions = TwkbDimension::from_geometry(geometry)?;
    let empty = geometry_is_empty(geometry);
    let type_code = match geometry.as_type() {
        NativeGeometryType::Point(_) => TWKB_TYPE_POINT,
        NativeGeometryType::LineString(_) => TWKB_TYPE_LINESTRING,
        NativeGeometryType::Polygon(_) => TWKB_TYPE_POLYGON,
        NativeGeometryType::MultiPoint(_) => TWKB_TYPE_MULTIPOINT,
        NativeGeometryType::MultiLineString(_) => TWKB_TYPE_MULTILINESTRING,
        NativeGeometryType::MultiPolygon(_) => TWKB_TYPE_MULTIPOLYGON,
        NativeGeometryType::GeometryCollection(_) => TWKB_TYPE_GEOMETRYCOLLECTION,
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => {
            return Err(DataFusionError::NotImplemented(
                "ST_AsTWKB supports only GeoArrow geometry types".to_string(),
            )
            .into());
        }
    };

    let mut body = Vec::new();
    if !empty {
        if idlist.is_some()
            && !matches!(
                geometry.as_type(),
                NativeGeometryType::MultiPoint(_)
                    | NativeGeometryType::MultiLineString(_)
                    | NativeGeometryType::MultiPolygon(_)
                    | NativeGeometryType::GeometryCollection(_)
            )
        {
            return Err(DataFusionError::Execution(
                "ST_AsTWKB id lists require a collection geometry".to_string(),
            )
            .into());
        }
        write_geometry_body(geometry, &mut body, dimensions, options, idlist)?;
    }

    let bbox = if options.with_boxes && !empty {
        Some(compute_bbox(geometry, dimensions, options)?)
    } else {
        None
    };

    let mut header = Vec::new();
    header.push((type_code & 0x0f) | ((zigzag8(options.precision_xy as i8) & 0x0f) << 4));
    let mut metadata = 0u8;
    if bbox.is_some() {
        metadata |= TWKB_METADATA_BBOX;
    }
    if options.with_sizes {
        metadata |= TWKB_METADATA_SIZE;
    }
    if idlist.is_some() && !empty {
        metadata |= TWKB_METADATA_IDLIST;
    }
    if dimensions.has_z() || dimensions.has_m() {
        metadata |= TWKB_METADATA_EXTENDED;
    }
    if empty {
        metadata |= TWKB_METADATA_EMPTY;
    }
    header.push(metadata);

    if dimensions.has_z() || dimensions.has_m() {
        let mut extended = 0u8;
        if dimensions.has_z() {
            extended |= 0x01;
        }
        if dimensions.has_m() {
            extended |= 0x02;
        }
        extended |= ((options.precision_z as u8) & 0x07) << 2;
        extended |= ((options.precision_m as u8) & 0x07) << 5;
        header.push(extended);
    }

    if options.with_sizes {
        let size = body.len() + bbox.map_or(0, |bbox| bbox.encoded_size(dimensions));
        write_uvarint(&mut header, size as u64);
    }
    if let Some(bbox) = bbox {
        bbox.write(&mut header, dimensions);
    }
    header.extend_from_slice(&body);
    Ok(header)
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeomFromTWKB {
    coord_type: CoordType,
}

impl GeomFromTWKB {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for GeomFromTWKB {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOM_FROM_TWKB_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromTWKB {
    fn name(&self) -> &str {
        "st_geomfromtwkb"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| {
            Signature::uniform(
                1,
                vec![
                    DataType::Binary,
                    DataType::LargeBinary,
                    DataType::BinaryView,
                ],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = field_metadata(input_field)?;
        let geometry_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(geometry_type
            .to_field(input_field.name(), input_field.is_nullable())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
        let geometry_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        let array = arrays[0].as_ref();
        let mut builder = StrictGeometryBuilder::new(geometry_type);
        let mut push = |bytes: Option<&[u8]>| -> GeoDataFusionResult<()> {
            match bytes {
                None => builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?,
                Some(bytes) => {
                    let geometry = decode_twkb(bytes)?;
                    if let Wkt::MultiPolygon(multi_polygon) = &geometry
                        && multi_polygon
                            .polygons()
                            .iter()
                            .any(|polygon| polygon.exterior().is_none())
                    {
                        return Err(DataFusionError::Execution(
                            "MULTIPOLYGON with an empty polygon member is not supported"
                                .to_string(),
                        )
                        .into());
                    }
                    builder.push_geometry(Some(&geometry))?;
                }
            }
            Ok(())
        };

        match array.data_type() {
            DataType::Null => {
                for _ in 0..array.len() {
                    push(None)?;
                }
            }
            DataType::Binary => {
                for value in array
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .unwrap()
                    .iter()
                {
                    push(value)?;
                }
            }
            DataType::LargeBinary => {
                for value in array
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .unwrap()
                    .iter()
                {
                    push(value)?;
                }
            }
            DataType::BinaryView => {
                for value in array
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryViewArray>()
                    .unwrap()
                    .iter()
                {
                    push(value)?;
                }
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "ST_GeomFromTWKB received unsupported input type {other}"
                )));
            }
        }
        Ok(ColumnarValue::Array(
            Arc::new(builder.finish()).to_array_ref(),
        ))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_TWKB_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a geometry from a Tiny Well-Known Binary (TWKB) representation.",
                "ST_GeomFromTWKB(bytea)",
            )
            .with_argument("twkb", "TWKB bytes")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsTWKB {
    signature: Signature,
}

impl AsTWKB {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for AsTWKB {
    fn default() -> Self {
        Self::new()
    }
}

static AS_TWKB_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsTWKB {
    fn name(&self) -> &str {
        "st_astwkb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 7 {
            return Err(DataFusionError::Execution(format!(
                "ST_AsTWKB expects between 1 and 7 arguments, got {}",
                arg_types.len()
            )));
        }
        let first = &arg_types[0];
        let array_form = matches!(first, DataType::List(_) | DataType::LargeList(_));
        let valid_first = if array_form {
            is_geometry_list(first)
        } else {
            *first == DataType::Null || any_geometry_type().contains(first)
        };
        if !valid_first {
            return Err(DataFusionError::Execution(format!(
                "ST_AsTWKB first argument must be a geometry or geometry array, got {first}"
            )));
        }

        let options_offset = usize::from(array_form);
        if array_form && (arg_types.len() < 2 || !is_int64_list(&arg_types[1])) {
            return Err(DataFusionError::Execution(
                "ST_AsTWKB array form expects a bigint[] id array as its second argument"
                    .to_string(),
            ));
        }
        for (index, data_type) in arg_types.iter().enumerate().skip(1 + options_offset) {
            let relative = index - 1 - options_offset;
            let valid = match relative {
                0..=2 => {
                    matches!(
                        data_type,
                        DataType::Int32 | DataType::Int64 | DataType::Null
                    )
                }
                3..=4 => matches!(data_type, DataType::Boolean | DataType::Null),
                _ => false,
            };
            if !valid {
                return Err(DataFusionError::Execution(format!(
                    "ST_AsTWKB argument {} has unsupported type {data_type}",
                    index + 1
                )));
            }
        }
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            input_field.name(),
            DataType::Binary,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let first_type = args.arg_fields[0].data_type();
        if matches!(first_type, DataType::List(_) | DataType::LargeList(_)) {
            return self.invoke_array_form(&args);
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if first_type == &DataType::Null {
            let mut builder = BinaryBuilder::new();
            for _ in 0..arrays[0].len() {
                builder.append_null();
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }
        let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
        let metadata = field_metadata(&args.arg_fields[0])?;
        let geo_array = geo_array.as_ref();
        let result =
            downcast_geoarrow_array!(geo_array, encode_twkb_array, &arrays, 1, metadata.as_ref())?;
        Ok(ColumnarValue::Array(result))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_TWKB_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the geometry as Tiny Well-Known Binary (TWKB). When the CRS metadata \
                 cannot be classified as geographic or projected, an explicit X/Y precision is \
                 required.",
                "ST_AsTWKB(geometry[, precision_xy, precision_z, precision_m, with_sizes, with_boxes])",
            )
            .with_argument("g1", "geometry or geometry array")
            .build()
        }))
    }
}

impl AsTWKB {
    fn invoke_array_form(&self, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geometry_list = arrays[0].as_ref();
        let id_list = arrays[1].as_ref();
        let item_field = list_item_field(&args.arg_fields[0])?;
        let geometry_field = geometry_field_from_physical(&item_field).ok_or_else(|| {
            DataFusionError::Execution(
                "ST_AsTWKB geometry array elements are not GeoArrow geometries".to_string(),
            )
        })?;
        let metadata = field_metadata(&geometry_field)?;
        let mut builder = BinaryBuilder::new();
        if geometry_list.len() != id_list.len() {
            return Err(DataFusionError::Execution(format!(
                "ST_AsTWKB size of geometry[] and bigint[] arrays must match, got {} and {}",
                geometry_list.len(),
                id_list.len()
            )));
        }

        for row in 0..geometry_list.len() {
            if geometry_list.is_null(row) || id_list.is_null(row) {
                builder.append_null();
                continue;
            }
            let geometries = list_values(geometry_list, row)?;
            let ids = list_values(id_list, row)?;
            if geometries.len() != ids.len() {
                return Err(DataFusionError::Execution(format!(
                    "ST_AsTWKB size of geometry[] and bigint[] arrays must match, got {} and {}",
                    geometries.len(),
                    ids.len()
                )));
            }
            let geo_array = geometry_array_from_arrow(geometries.as_ref(), &geometry_field)?;
            let ids = ids
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "ST_AsTWKB id array did not contain Int64 values".to_string(),
                    )
                })?;
            let geo_array = geo_array.as_ref();
            let encoded = downcast_geoarrow_array!(
                geo_array,
                encode_twkb_idlist_row,
                row,
                ids,
                &arrays,
                2,
                metadata.as_ref()
            )?;
            match encoded {
                Some(encoded) => builder.append_value(encoded),
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn encode_twkb_idlist_row<'a, A: GeoArrowArrayAccessor<'a>>(
    array: &'a A,
    row: usize,
    ids: &arrow_array::Int64Array,
    columns: &[ArrayRef],
    offset: usize,
    metadata: &Metadata,
) -> GeoDataFusionResult<Option<Vec<u8>>> {
    let entries = (0..array.len())
        .filter(|index| !array.is_null(*index) && !ids.is_null(*index))
        .map(|index| (index, ids.value(index)))
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Ok(None);
    }
    let options = TwkbOptions::from_columns(columns, row, offset, metadata)?;

    let first = array
        .value(entries[0].0)
        .map_err(GeoDataFusionError::GeoArrow)?;
    let dimensions = TwkbDimension::from_geometry(&first)?;
    let type_code = geometry_type_code(&first)?;
    let mut homogeneous = true;
    for (index, _) in entries.iter().skip(1) {
        let geometry = array.value(*index).map_err(GeoDataFusionError::GeoArrow)?;
        if TwkbDimension::from_geometry(&geometry)? != dimensions {
            return Err(DataFusionError::Execution(
                "ST_AsTWKB geometries have different dimensionality".to_string(),
            )
            .into());
        }
        if geometry_type_code(&geometry)? != type_code {
            homogeneous = false;
        }
    }

    let root_type = if homogeneous {
        match type_code {
            TWKB_TYPE_POINT => TWKB_TYPE_MULTIPOINT,
            TWKB_TYPE_LINESTRING => TWKB_TYPE_MULTILINESTRING,
            TWKB_TYPE_POLYGON => TWKB_TYPE_MULTIPOLYGON,
            other => other,
        }
    } else {
        TWKB_TYPE_GEOMETRYCOLLECTION
    };

    let written_entries = if root_type == TWKB_TYPE_MULTIPOINT {
        let mut written = Vec::with_capacity(entries.len());
        for (index, id) in &entries {
            let geometry = array.value(*index).map_err(GeoDataFusionError::GeoArrow)?;
            if !geometry_is_empty(&geometry) {
                written.push((*index, *id));
            }
        }
        written
    } else {
        entries.clone()
    };

    let mut body = Vec::new();
    write_uvarint(&mut body, written_entries.len() as u64);
    for (_, id) in &written_entries {
        write_svarint(&mut body, *id);
    }
    let mut sums = [0i64; 4];
    for (index, _) in &written_entries {
        let geometry = array.value(*index).map_err(GeoDataFusionError::GeoArrow)?;
        if homogeneous {
            write_geometry_body_with_sums(
                &geometry, &mut body, dimensions, &options, None, &mut sums,
            )?;
        } else {
            body.extend_from_slice(&encode_geometry(&geometry, &options, None)?);
        }
    }

    let bbox = if options.with_boxes {
        let mut bbox = Bbox::default();
        for (index, _) in &entries {
            let geometry = array.value(*index).map_err(GeoDataFusionError::GeoArrow)?;
            let child = compute_bbox(&geometry, dimensions, &options)?;
            bbox.union(&child, dimensions);
        }
        Some(bbox)
    } else {
        None
    };

    let mut header = vec![(root_type & 0x0f) | ((zigzag8(options.precision_xy as i8) & 0x0f) << 4)];
    let mut metadata = TWKB_METADATA_IDLIST;
    if bbox.is_some() {
        metadata |= TWKB_METADATA_BBOX;
    }
    if options.with_sizes {
        metadata |= TWKB_METADATA_SIZE;
    }
    if dimensions.has_z() || dimensions.has_m() {
        metadata |= TWKB_METADATA_EXTENDED;
    }
    header.push(metadata);
    if dimensions.has_z() || dimensions.has_m() {
        let mut extended = 0u8;
        if dimensions.has_z() {
            extended |= 0x01;
        }
        if dimensions.has_m() {
            extended |= 0x02;
        }
        extended |= ((options.precision_z as u8) & 0x07) << 2;
        extended |= ((options.precision_m as u8) & 0x07) << 5;
        header.push(extended);
    }
    if options.with_sizes {
        let size = body.len() + bbox.map_or(0, |bbox| bbox.encoded_size(dimensions));
        write_uvarint(&mut header, size as u64);
    }
    if let Some(bbox) = bbox {
        bbox.write(&mut header, dimensions);
    }
    header.extend_from_slice(&body);
    Ok(Some(header))
}

fn geometry_type_code<G: GeometryTrait<T = f64>>(geometry: &G) -> GeoDataFusionResult<u8> {
    Ok(match geometry.as_type() {
        NativeGeometryType::Point(_) => TWKB_TYPE_POINT,
        NativeGeometryType::LineString(_) => TWKB_TYPE_LINESTRING,
        NativeGeometryType::Polygon(_) => TWKB_TYPE_POLYGON,
        NativeGeometryType::MultiPoint(_) => TWKB_TYPE_MULTIPOINT,
        NativeGeometryType::MultiLineString(_) => TWKB_TYPE_MULTILINESTRING,
        NativeGeometryType::MultiPolygon(_) => TWKB_TYPE_MULTIPOLYGON,
        NativeGeometryType::GeometryCollection(_) => TWKB_TYPE_GEOMETRYCOLLECTION,
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => {
            return Err(DataFusionError::NotImplemented(
                "ST_AsTWKB supports only GeoArrow geometry types".to_string(),
            )
            .into());
        }
    })
}

fn encode_twkb_array<'a, A: GeoArrowArrayAccessor<'a>>(
    array: &'a A,
    columns: &[ArrayRef],
    offset: usize,
    metadata: &Metadata,
) -> GeoDataFusionResult<ArrayRef> {
    let mut builder = BinaryBuilder::new();
    for (row, value) in array.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(Ok(geometry)) => {
                let options = TwkbOptions::from_columns(columns, row, offset, metadata)?;
                builder.append_value(encode_geometry(&geometry, &options, None)?);
            }
            Some(Err(error)) => return Err(error.into()),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn is_geometry_list(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            field.data_type() == &DataType::Null || geometry_field_from_physical(field).is_some()
        }
        _ => false,
    }
}

fn is_int64_list(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            matches!(field.data_type(), DataType::Int64 | DataType::Null)
        }
        _ => false,
    }
}

fn list_item_field(field: &Field) -> Result<Arc<Field>> {
    match field.data_type() {
        DataType::List(item) | DataType::LargeList(item) => Ok(Arc::clone(item)),
        other => Err(DataFusionError::Internal(format!(
            "expected list input, got {other}"
        ))),
    }
}

fn list_values(array: &dyn Array, row: usize) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => Ok(array
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(row)),
        DataType::LargeList(_) => Ok(array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap()
            .value(row)),
        other => Err(DataFusionError::Internal(format!(
            "expected list input, got {other}"
        ))),
    }
}

struct TwkbReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> TwkbReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn byte(&mut self) -> GeoDataFusionResult<u8> {
        let value = *self
            .bytes
            .get(self.position)
            .ok_or_else(|| DataFusionError::Execution("truncated TWKB buffer".to_string()))?;
        self.position += 1;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    /// Reads a count and rejects values that cannot fit in the remaining bytes.
    fn count(&mut self, min_bytes_per_item: usize, label: &str) -> GeoDataFusionResult<usize> {
        let count = self.uvarint()?;
        let count = usize::try_from(count).map_err(|_| {
            DataFusionError::Execution(format!(
                "TWKB {label} count {count} exceeds platform capacity"
            ))
        })?;
        let remaining = self.remaining();
        if min_bytes_per_item > 0 && count > remaining / min_bytes_per_item {
            return Err(DataFusionError::Execution(format!(
                "TWKB {label} count {count} exceeds remaining {remaining} bytes \
                 (minimum {min_bytes_per_item} bytes per item)"
            ))
            .into());
        }
        Ok(count)
    }

    fn uvarint(&mut self) -> GeoDataFusionResult<u64> {
        let mut value = 0u64;
        for index in 0..10 {
            let byte = self.byte()?;
            let payload = u64::from(byte & 0x7f);
            if index == 9 && payload > 1 {
                return Err(DataFusionError::Execution("TWKB varint overflow".to_string()).into());
            }
            value |= payload << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DataFusionError::Execution("TWKB varint is too long".to_string()).into())
    }

    fn svarint(&mut self) -> GeoDataFusionResult<i64> {
        let value = self.uvarint()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    fn skip_idlist(&mut self, count: usize) -> GeoDataFusionResult<()> {
        for _ in 0..count {
            self.svarint()?;
        }
        Ok(())
    }
}

fn read_coord(
    reader: &mut TwkbReader<'_>,
    dimensions: TwkbDimension,
    precision_xy: i32,
    precision_z: i32,
    precision_m: i32,
    sums: &mut [i64; 4],
) -> GeoDataFusionResult<Coord<f64>> {
    let factors = [
        10f64.powi(precision_xy),
        10f64.powi(precision_xy),
        10f64.powi(precision_z),
        10f64.powi(precision_m),
    ];
    let mut values = [0.0; 4];
    for (index, value) in values
        .iter_mut()
        .take(dimensions.num_ordinates())
        .enumerate()
    {
        sums[index] = sums[index]
            .checked_add(reader.svarint()?)
            .ok_or_else(|| DataFusionError::Execution("TWKB coordinate overflow".to_string()))?;
        *value = sums[index] as f64 / factors[index];
    }
    Ok(Coord {
        x: values[0],
        y: values[1],
        z: dimensions.has_z().then_some(values[2]),
        m: dimensions
            .has_m()
            .then_some(values[if dimensions.has_z() { 3 } else { 2 }]),
    })
}

/// Decodes a complete TWKB buffer into the shared WKT representation.
///
/// This is the single wire decoder for TWKB. Other I/O paths should reuse it
/// instead of implementing a second codec.
pub(crate) fn decode_twkb(bytes: &[u8]) -> GeoDataFusionResult<Wkt<f64>> {
    let mut reader = TwkbReader::new(bytes);
    let geometry = decode_twkb_geometry(&mut reader, 0)?;
    if reader.position != bytes.len() {
        return Err(DataFusionError::Execution(format!(
            "ST_GeomFromTWKB found {} trailing bytes",
            bytes.len() - reader.position
        ))
        .into());
    }
    Ok(geometry)
}

fn decode_twkb_geometry(
    reader: &mut TwkbReader<'_>,
    depth: usize,
) -> GeoDataFusionResult<Wkt<f64>> {
    if depth > TWKB_MAX_DEPTH {
        return Err(DataFusionError::Execution(format!(
            "TWKB geometry nesting exceeds the maximum depth of {TWKB_MAX_DEPTH}"
        ))
        .into());
    }
    let type_prec = reader.byte()?;
    let metadata = reader.byte()?;
    let type_code = type_prec & 0x0f;
    let precision_xy = i32::from(unzigzag8((type_prec & 0xf0) >> 4));
    let has_bbox = metadata & TWKB_METADATA_BBOX != 0;
    let has_size = metadata & TWKB_METADATA_SIZE != 0;
    let has_idlist = metadata & TWKB_METADATA_IDLIST != 0;
    let has_extended = metadata & TWKB_METADATA_EXTENDED != 0;
    let is_empty = metadata & TWKB_METADATA_EMPTY != 0;
    let (has_z, has_m, precision_z, precision_m) = if has_extended {
        let extended = reader.byte()?;
        (
            extended & 0x01 != 0,
            extended & 0x02 != 0,
            i32::from((extended & 0x1c) >> 2),
            i32::from((extended & 0xe0) >> 5),
        )
    } else {
        (false, false, 0, 0)
    };
    let dimensions = match (has_z, has_m) {
        (false, false) => TwkbDimension::Xy,
        (true, false) => TwkbDimension::Xyz,
        (false, true) => TwkbDimension::Xym,
        (true, true) => TwkbDimension::Xyzm,
    };
    if has_size {
        reader.uvarint()?;
    }
    if has_bbox {
        for _ in 0..dimensions.num_ordinates() {
            reader.svarint()?;
            reader.svarint()?;
        }
    }

    let mut state = TwkbDecodeState {
        dimensions,
        precision_xy,
        precision_z,
        precision_m,
        sums: [0; 4],
    };
    decode_twkb_body(reader, type_code, is_empty, has_idlist, &mut state, depth)
}

struct TwkbDecodeState {
    dimensions: TwkbDimension,
    precision_xy: i32,
    precision_z: i32,
    precision_m: i32,
    sums: [i64; 4],
}

impl TwkbDecodeState {
    fn read_coord(&mut self, reader: &mut TwkbReader<'_>) -> GeoDataFusionResult<Coord<f64>> {
        read_coord(
            reader,
            self.dimensions,
            self.precision_xy,
            self.precision_z,
            self.precision_m,
            &mut self.sums,
        )
    }

    fn read_point(&mut self, reader: &mut TwkbReader<'_>) -> GeoDataFusionResult<Point<f64>> {
        Ok(Point::from_coord(self.read_coord(reader)?))
    }

    fn read_line(&mut self, reader: &mut TwkbReader<'_>) -> GeoDataFusionResult<LineString<f64>> {
        let count = reader.count(self.dimensions.num_ordinates(), "LINESTRING point")?;
        if count == 0 {
            return Ok(LineString::empty(self.dimensions.wkt_dimension()));
        }
        if count < 2 {
            return Err(DataFusionError::Execution(
                "TWKB LINESTRING must have at least two points".to_string(),
            )
            .into());
        }
        let mut coords = Vec::with_capacity(count);
        for _ in 0..count {
            coords.push(self.read_coord(reader)?);
        }
        Ok(LineString::new(coords, self.dimensions.wkt_dimension()))
    }

    fn read_polygon(&mut self, reader: &mut TwkbReader<'_>) -> GeoDataFusionResult<Polygon<f64>> {
        let min_ring_bytes = 1 + 4 * self.dimensions.num_ordinates();
        let ring_count = reader.count(min_ring_bytes, "POLYGON ring")?;
        let mut rings = Vec::with_capacity(ring_count);
        for _ in 0..ring_count {
            let count = reader.count(self.dimensions.num_ordinates(), "POLYGON ring point")?;
            if count < 4 {
                return Err(DataFusionError::Execution(
                    "TWKB POLYGON rings must have at least four points".to_string(),
                )
                .into());
            }
            let mut coords = Vec::with_capacity(count);
            for _ in 0..count {
                coords.push(self.read_coord(reader)?);
            }
            if coords.first() != coords.last() {
                coords.push(coords[0]);
            }
            rings.push(LineString::new(coords, self.dimensions.wkt_dimension()));
        }
        Ok(Polygon::new(rings, self.dimensions.wkt_dimension()))
    }
}

fn decode_twkb_body(
    reader: &mut TwkbReader<'_>,
    type_code: u8,
    is_empty: bool,
    has_idlist: bool,
    state: &mut TwkbDecodeState,
    depth: usize,
) -> GeoDataFusionResult<Wkt<f64>> {
    let dimension = state.dimensions.wkt_dimension();
    match type_code {
        TWKB_TYPE_POINT => {
            if is_empty {
                Ok(Wkt::Point(Point::empty(dimension)))
            } else {
                Ok(Wkt::Point(state.read_point(reader)?))
            }
        }
        TWKB_TYPE_LINESTRING => {
            if is_empty {
                Ok(Wkt::LineString(LineString::empty(dimension)))
            } else {
                Ok(Wkt::LineString(state.read_line(reader)?))
            }
        }
        TWKB_TYPE_POLYGON => {
            if is_empty {
                Ok(Wkt::Polygon(Polygon::empty(dimension)))
            } else {
                Ok(Wkt::Polygon(state.read_polygon(reader)?))
            }
        }
        TWKB_TYPE_MULTIPOINT => {
            if is_empty {
                return Ok(Wkt::MultiPoint(MultiPoint::empty(dimension)));
            }
            let id_bytes = usize::from(has_idlist);
            let count = reader.count(
                state.dimensions.num_ordinates() + id_bytes,
                "MULTIPOINT point",
            )?;
            if has_idlist {
                reader.skip_idlist(count)?;
            }
            let mut points = Vec::with_capacity(count);
            for _ in 0..count {
                points.push(state.read_point(reader)?);
            }
            Ok(Wkt::MultiPoint(MultiPoint::new(points, dimension)))
        }
        TWKB_TYPE_MULTILINESTRING => {
            if is_empty {
                return Ok(Wkt::MultiLineString(MultiLineString::empty(dimension)));
            }
            let count = reader.count(1 + usize::from(has_idlist), "MULTILINESTRING line")?;
            if has_idlist {
                reader.skip_idlist(count)?;
            }
            let mut lines = Vec::with_capacity(count);
            for _ in 0..count {
                lines.push(state.read_line(reader)?);
            }
            Ok(Wkt::MultiLineString(MultiLineString::new(lines, dimension)))
        }
        TWKB_TYPE_MULTIPOLYGON => {
            if is_empty {
                return Ok(Wkt::MultiPolygon(MultiPolygon::empty(dimension)));
            }
            let count = reader.count(1 + usize::from(has_idlist), "MULTIPOLYGON polygon")?;
            if has_idlist {
                reader.skip_idlist(count)?;
            }
            let mut polygons = Vec::with_capacity(count);
            for _ in 0..count {
                polygons.push(state.read_polygon(reader)?);
            }
            Ok(Wkt::MultiPolygon(MultiPolygon::new(polygons, dimension)))
        }
        TWKB_TYPE_GEOMETRYCOLLECTION => {
            if is_empty {
                return Ok(Wkt::GeometryCollection(GeometryCollection::empty(
                    dimension,
                )));
            }
            let count = reader.count(2 + usize::from(has_idlist), "GEOMETRYCOLLECTION geometry")?;
            if has_idlist {
                reader.skip_idlist(count)?;
            }
            let mut geometries = Vec::with_capacity(count);
            for _ in 0..count {
                let mut inner = TwkbReader::new(&reader.bytes[reader.position..]);
                let geometry = decode_twkb_geometry(&mut inner, depth + 1)?;
                reader.position += inner.position;
                geometries.push(geometry);
            }
            Ok(Wkt::GeometryCollection(GeometryCollection::new(
                geometries, dimension,
            )))
        }
        other => Err(DataFusionError::Execution(format!(
            "ST_GeomFromTWKB encountered unknown TWKB type {other}"
        ))
        .into()),
    }
}

#[cfg(test)]
mod decode_tests {
    use super::{TWKB_MAX_DEPTH, decode_twkb};

    const MAX_UVARINT: [u8; 10] = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];

    fn with_count(header: [u8; 2], count: &[u8]) -> Vec<u8> {
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(count);
        bytes
    }

    #[test]
    fn valid_geometries_decode_and_every_truncation_is_an_error() {
        let cases: &[&[u8]] = &[
            &[0x01, 0x00, 0x02, 0x04],
            &[0x02, 0x00, 0x02, 0x02, 0x04, 0x04, 0x04],
            &[
                0x03, 0x00, 0x01, 0x04, 0x00, 0x00, 0x02, 0x00, 0x00, 0x02, 0x01, 0x00,
            ],
            &[0x04, 0x00, 0x02, 0x02, 0x04, 0x04, 0x04],
            &[0x05, 0x00, 0x01, 0x02, 0x02, 0x04, 0x04, 0x04],
            &[
                0x06, 0x00, 0x01, 0x01, 0x04, 0x00, 0x00, 0x02, 0x00, 0x00, 0x02, 0x01, 0x00,
            ],
            &[0x07, 0x00, 0x01, 0x01, 0x00, 0x02, 0x04],
            &[0x04, 0x04, 0x02, 0x14, 0x28, 0x02, 0x04, 0x04, 0x04],
        ];
        for bytes in cases {
            decode_twkb(bytes).unwrap_or_else(|error| panic!("valid TWKB failed: {error}"));
            for end in 0..bytes.len() {
                assert!(
                    decode_twkb(&bytes[..end]).is_err(),
                    "truncated TWKB unexpectedly decoded: {bytes:?}[..{end}]"
                );
            }
        }
    }

    #[test]
    fn huge_counts_fail_before_allocation() {
        let cases = [
            with_count([0x02, 0x00], &MAX_UVARINT),
            with_count([0x03, 0x00], &MAX_UVARINT),
            with_count(
                [0x03, 0x00],
                &[
                    0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
                ],
            ),
            with_count([0x04, 0x00], &MAX_UVARINT),
            with_count([0x05, 0x00], &MAX_UVARINT),
            with_count([0x06, 0x00], &MAX_UVARINT),
            with_count([0x07, 0x00], &MAX_UVARINT),
        ];
        for bytes in cases {
            let error = decode_twkb(&bytes)
                .expect_err("oversized count unexpectedly decoded")
                .to_string();
            assert!(error.contains("count"), "{error}");
        }
    }

    #[test]
    fn nested_geometry_collection_is_preserved() {
        let bytes = [0x07, 0x00, 0x01, 0x07, 0x00, 0x01, 0x01, 0x00, 0x02, 0x04];
        let decoded = decode_twkb(&bytes).unwrap().to_string();
        assert_eq!(
            decoded.matches("GEOMETRYCOLLECTION").count(),
            2,
            "{decoded}"
        );
    }

    #[test]
    fn varints_reject_overflow_and_overlong_encodings() {
        let mut overflow = with_count([0x02, 0x00], &[0xff; 9]);
        overflow.push(0x02);
        let error = decode_twkb(&overflow).unwrap_err().to_string();
        assert!(error.contains("varint overflow"), "{error}");

        let mut too_long = with_count([0x02, 0x00], &[0xff; 9]);
        too_long.push(0x80);
        let error = decode_twkb(&too_long).unwrap_err().to_string();
        assert!(error.contains("varint is too long"), "{error}");
    }

    #[test]
    fn geometry_collection_depth_is_bounded() {
        let mut valid = Vec::new();
        for _ in 0..TWKB_MAX_DEPTH {
            valid.extend_from_slice(&[0x07, 0x00, 0x01]);
        }
        valid.extend_from_slice(&[0x01, 0x00, 0x02, 0x04]);
        decode_twkb(&valid).expect("maximum supported nesting should decode");

        let mut too_deep = Vec::new();
        for _ in 0..=TWKB_MAX_DEPTH {
            too_deep.extend_from_slice(&[0x07, 0x00, 0x01]);
        }
        too_deep.extend_from_slice(&[0x01, 0x00, 0x02, 0x04]);
        let error = decode_twkb(&too_deep).unwrap_err().to_string();
        assert!(error.contains("maximum depth"), "{error}");
    }
}
