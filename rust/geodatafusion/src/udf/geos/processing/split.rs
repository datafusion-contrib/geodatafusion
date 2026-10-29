use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPolygonTrait, PolygonTrait,
};
use geos::{Geom, Geometry, GeometryTypes};
use wkb::reader::Wkb;
use wkb::writer::{WriteOptions, geometry_wkb_size, write_geometry};

use super::super::adapter;
use super::super::adapter::GeosWkbWriter;
use crate::error::GeoDataFusionResult;

const FP_TOLERANCE: f64 = 1e-12;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Split {
    signature: Signature,
}

impl Split {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for Split {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Split {
    fn name(&self) -> &str {
        "st_split"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        adapter::check_geometry_type(&arg_types[1], 1, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0, 1], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(split_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Splits a line or polygon by a blade geometry and returns a GeometryCollection of the pieces. Line-by-point splits preserve and interpolate Z and M; GEOS-backed line and polygon splits preserve Z and drop M. Unsupported blade and primary type combinations return controlled errors.",
                "ST_Split(geom, blade)",
            )
            .with_argument("geom", "line, polygon, or multi-geometry to split")
            .with_argument("blade", "geometry used to cut the input")
            .build()
        }))
    }
}

/// A WKB piece plus the coordinate dimensions it carries.
#[derive(Debug, Clone)]
struct Piece {
    wkb: Vec<u8>,
    dims: Dimensions,
}

#[derive(Debug)]
struct SplitOutcome {
    pieces: Vec<Piece>,
    /// Dimensions used when the result is an empty GeometryCollection.
    empty_dims: Dimensions,
}

impl SplitOutcome {
    fn with_pieces(pieces: Vec<Piece>) -> Self {
        Self {
            pieces,
            empty_dims: Dimensions::Xy,
        }
    }
}

impl Piece {
    fn new(wkb: Vec<u8>, dims: Dimensions) -> Self {
        Self { wkb, dims }
    }
}

#[derive(Debug, Clone, Copy)]
struct Coord4 {
    x: f64,
    y: f64,
    z: f64,
    m: f64,
}

#[derive(Debug)]
enum PointSplit {
    None,
    Boundary,
    Split(Vec<Coord4>, Vec<Coord4>),
}

fn split_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::binary_geos(
        args,
        |_row, _arrays, left_wkb, _left, _right_wkb, blade, writer| {
            let outcome = split_geometry(left_wkb, blade, writer)?;
            Ok(Some(wrap_collection(&outcome.pieces, outcome.empty_dims)))
        },
    )
}

fn split_geometry(
    wkb: &[u8],
    blade: &Geometry,
    writer: &mut GeosWkbWriter,
) -> GeoDataFusionResult<SplitOutcome> {
    let parsed = Wkb::try_new(wkb).map_err(|error| {
        DataFusionError::Execution(format!("ST_Split input WKB is invalid: {error}"))
    })?;
    match parsed.as_type() {
        GeometryType::LineString(line) => split_line(wkb, line, blade, writer),
        GeometryType::Polygon(polygon) => split_polygon(polygon, wkb, blade, writer),
        GeometryType::MultiLineString(_)
        | GeometryType::MultiPolygon(_)
        | GeometryType::GeometryCollection(_) => {
            let mut pieces = Vec::new();
            for child_wkb in collection_member_wkbs(&parsed)? {
                pieces.extend(split_geometry(&child_wkb, blade, writer)?.pieces);
            }
            Ok(SplitOutcome::with_pieces(pieces))
        }
        other => Err(DataFusionError::Execution(format!(
            "Splitting of {} geometries is unsupported",
            match other {
                GeometryType::Point(_) => "Point",
                GeometryType::LineString(_) => "LineString",
                GeometryType::Polygon(_) => "Polygon",
                GeometryType::MultiPoint(_) => "MultiPoint",
                GeometryType::MultiLineString(_) => "MultiLineString",
                GeometryType::MultiPolygon(_) => "MultiPolygon",
                GeometryType::GeometryCollection(_) => "GeometryCollection",
                GeometryType::Rect(_) | GeometryType::Triangle(_) | GeometryType::Line(_) => {
                    "GeometryCollection"
                }
            }
        ))
        .into()),
    }
}

fn split_line(
    wkb: &[u8],
    line: &impl LineStringTrait<T = f64>,
    blade: &Geometry,
    _writer: &mut GeosWkbWriter,
) -> GeoDataFusionResult<SplitOutcome> {
    match blade.geometry_type()? {
        GeometryTypes::Point => {
            let dims = line.dim();
            if line.num_coords() == 0 {
                return Ok(SplitOutcome::with_pieces(vec![Piece::new(
                    empty_line_wkb(dims),
                    dims,
                )]));
            }
            let (x, y) = point_coordinates(blade)?;
            let coords = line_coordinates(line)?;
            match split_at_point(&coords, x, y, None) {
                PointSplit::Split(prefix, suffix) => Ok(SplitOutcome::with_pieces(vec![
                    Piece::new(line_wkb(&prefix, dims), dims),
                    Piece::new(line_wkb(&suffix, dims), dims),
                ])),
                PointSplit::None | PointSplit::Boundary => {
                    Ok(SplitOutcome::with_pieces(vec![Piece::new(
                        wkb.to_vec(),
                        dims,
                    )]))
                }
            }
        }
        GeometryTypes::MultiPoint => {
            let dims = line.dim();
            if line.num_coords() == 0 {
                return Ok(SplitOutcome::with_pieces(vec![Piece::new(
                    empty_line_wkb(dims),
                    dims,
                )]));
            }
            let mut lines = vec![line_coordinates(line)?];
            for index in 0..blade.get_num_geometries()? {
                let point = blade.get_geometry_n(index)?;
                let (x, y) = point_coordinates(&point)?;
                let mut line_index = 0;
                while line_index < lines.len() {
                    if let PointSplit::Split(prefix, suffix) =
                        split_at_point(&lines[line_index], x, y, None)
                    {
                        lines[line_index] = suffix;
                        lines.push(prefix);
                    }
                    line_index += 1;
                }
            }
            Ok(SplitOutcome::with_pieces(
                lines
                    .into_iter()
                    .map(|coords| Piece::new(line_wkb(&coords, dims), dims))
                    .collect(),
            ))
        }
        GeometryTypes::LineString
        | GeometryTypes::LinearRing
        | GeometryTypes::MultiLineString
        | GeometryTypes::Polygon
        | GeometryTypes::MultiPolygon => split_line_by_line(wkb, blade),
        other => Err(DataFusionError::Execution(format!(
            "Splitting a Line by a {} is unsupported",
            postgis_type_name(other)
        ))
        .into()),
    }
}

fn split_line_by_line(wkb: &[u8], blade: &Geometry) -> GeoDataFusionResult<SplitOutcome> {
    let prepared = adapter::prepare_geos_wkb(wkb)?;
    let line = Geometry::new_from_wkb(prepared.as_ref())?;
    let boundary;
    let blade = match blade.geometry_type()? {
        GeometryTypes::Polygon | GeometryTypes::MultiPolygon => {
            boundary = blade.boundary()?;
            &boundary
        }
        _ => blade,
    };

    if line.relate_pattern(blade, "1********")? {
        return Err(DataFusionError::Execution(
            "Splitter line has linear intersection with input".to_string(),
        )
        .into());
    }

    // GEOS 3.15 no longer splits lines as a side effect of `difference`, so
    // split at the intersection points explicitly. PostGIS 3.14 produced the
    // same pieces from `GEOSDifference` ordered along the input line.
    let parsed = Wkb::try_new(wkb).map_err(|error| {
        DataFusionError::Execution(format!("ST_Split input WKB is invalid: {error}"))
    })?;
    let GeometryType::LineString(input) = parsed.as_type() else {
        return Err(DataFusionError::Internal(
            "ST_Split line dispatch lost its LineString".to_string(),
        )
        .into());
    };
    let input_dims = input.dim();
    let output_dims = match input_dims {
        Dimensions::Xyz | Dimensions::Xyzm => Dimensions::Xyz,
        _ => Dimensions::Xy,
    };
    let coords = line_coordinates(input)?;
    if coords.is_empty() {
        // GEOS reports 2D empties even when the input carried Z.
        return Ok(SplitOutcome::with_pieces(vec![Piece::new(
            empty_line_wkb(Dimensions::Xy),
            Dimensions::Xy,
        )]));
    }

    let intersection = line.intersection(blade)?;
    let mut points = intersection_points(&intersection)?;
    points.sort_by(|left, right| {
        position_along(&coords, left.0, left.1)
            .total_cmp(&position_along(&coords, right.0, right.1))
    });

    let mut pieces = vec![coords];
    for (x, y, z) in points {
        let mut index = 0;
        while index < pieces.len() {
            if let PointSplit::Split(prefix, suffix) = split_at_point(&pieces[index], x, y, z) {
                pieces[index] = prefix;
                pieces.insert(index + 1, suffix);
                break;
            }
            index += 1;
        }
    }

    Ok(SplitOutcome::with_pieces(
        pieces
            .into_iter()
            .map(|coords| Piece::new(line_wkb(&coords, output_dims), output_dims))
            .collect(),
    ))
}

fn split_polygon(
    polygon: &impl PolygonTrait<T = f64>,
    wkb: &[u8],
    blade: &Geometry,
    writer: &mut GeosWkbWriter,
) -> GeoDataFusionResult<SplitOutcome> {
    match blade.geometry_type()? {
        GeometryTypes::LineString | GeometryTypes::MultiLineString => {}
        other => {
            return Err(DataFusionError::Execution(format!(
                "Splitting a Polygon by a {} is unsupported",
                postgis_type_name(other)
            ))
            .into());
        }
    }

    // PostGIS drops M when it hands polygons to GEOS and preserves Z only.
    let empty_dims = match polygon.dim() {
        Dimensions::Xyz | Dimensions::Xyzm => Dimensions::Xyz,
        _ => Dimensions::Xy,
    };

    let prepared = adapter::prepare_geos_wkb(wkb)?;
    let polygon_geometry = Geometry::new_from_wkb(prepared.as_ref())?;
    let boundary = polygon_geometry.boundary()?;
    let union = boundary.union(blade)?;
    let polygonized = Geometry::polygonize(&[union])?;

    let mut pieces = Vec::new();
    for index in 0..polygonized.get_num_geometries()? {
        let candidate = polygonized.get_geometry_n(index)?;
        let point = candidate.point_on_surface()?;
        if polygon_geometry.contains(&point)? {
            pieces.push(Piece::new(
                writer.write(&candidate)?,
                coordinate_dimensions(&candidate)?,
            ));
        }
    }
    if pieces.is_empty() {
        return Ok(SplitOutcome {
            pieces: Vec::new(),
            empty_dims,
        });
    }
    Ok(SplitOutcome::with_pieces(pieces))
}

fn point_coordinates(point: &impl Geom) -> GeoDataFusionResult<(f64, f64)> {
    if point.is_empty()? {
        return Err(
            DataFusionError::Execution("ST_Split: the blade point is empty".to_string()).into(),
        );
    }
    Ok((point.get_x()?, point.get_y()?))
}

fn intersection_points(geometry: &impl Geom) -> GeoDataFusionResult<Vec<(f64, f64, Option<f64>)>> {
    let mut points = Vec::new();
    collect_intersection_points(geometry, &mut points)?;
    Ok(points)
}

fn collect_intersection_points(
    geometry: &impl Geom,
    points: &mut Vec<(f64, f64, Option<f64>)>,
) -> GeoDataFusionResult<()> {
    match geometry.geometry_type()? {
        GeometryTypes::Point => {
            if !geometry.is_empty()? {
                let z = match geometry.get_coordinate_dimension()? {
                    geos::CoordDimensions::ThreeD => Some(geometry.get_z()?),
                    geos::CoordDimensions::TwoD => None,
                    // The shared adapter strips M before GEOS, so a four-dimensional
                    // result cannot reach this path; keep Z if it ever does.
                    #[cfg(feature = "geos-3_12")]
                    geos::CoordDimensions::FourD => Some(geometry.get_z()?),
                };
                points.push((geometry.get_x()?, geometry.get_y()?, z));
            }
        }
        GeometryTypes::MultiPoint | GeometryTypes::GeometryCollection => {
            for index in 0..geometry.get_num_geometries()? {
                let child = geometry.get_geometry_n(index)?;
                collect_intersection_points(&child, points)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn position_along(coords: &[Coord4], px: f64, py: f64) -> f64 {
    let mut cumulative = 0.0;
    for index in 0..coords.len().saturating_sub(1) {
        let start = coords[index];
        let end = coords[index + 1];
        let length = ((end.x - start.x).powi(2) + (end.y - start.y).powi(2)).sqrt();
        if distance2d_sqr_pt_seg(px, py, start, end) == 0.0 {
            let segment = ((end.x - start.x).powi(2) + (end.y - start.y).powi(2)).sqrt();
            let fraction = if segment == 0.0 {
                0.0
            } else {
                (((px - start.x) * (end.x - start.x) + (py - start.y) * (end.y - start.y))
                    / (segment * segment))
                    .clamp(0.0, 1.0)
            };
            return cumulative + fraction * length;
        }
        cumulative += length;
    }
    cumulative
}

fn coordinate_dimensions(geometry: &impl Geom) -> GeoDataFusionResult<Dimensions> {
    Ok(match geometry.get_coordinate_dimension()? {
        geos::CoordDimensions::TwoD => Dimensions::Xy,
        geos::CoordDimensions::ThreeD => Dimensions::Xyz,
        #[cfg(feature = "geos-3_12")]
        geos::CoordDimensions::FourD => Dimensions::Xyz,
    })
}

fn line_coordinates(line: &impl LineStringTrait<T = f64>) -> GeoDataFusionResult<Vec<Coord4>> {
    let dims = line.dim();
    let mut coords = Vec::with_capacity(line.num_coords());
    for index in 0..line.num_coords() {
        let coord = line.coord(index).ok_or_else(|| {
            DataFusionError::Execution("ST_Split: line coordinate is missing".to_string())
        })?;
        coords.push(coord_from_trait(&coord, dims));
    }
    Ok(coords)
}

fn coord_from_trait(coord: &impl CoordTrait<T = f64>, dims: Dimensions) -> Coord4 {
    let (z, m) = match dims {
        Dimensions::Xy => (f64::NAN, f64::NAN),
        Dimensions::Xyz => (coord.nth(2).unwrap_or(f64::NAN), f64::NAN),
        Dimensions::Xym => (f64::NAN, coord.nth(2).unwrap_or(f64::NAN)),
        Dimensions::Xyzm => (
            coord.nth(2).unwrap_or(f64::NAN),
            coord.nth(3).unwrap_or(f64::NAN),
        ),
        Dimensions::Unknown(_) => (f64::NAN, f64::NAN),
    };
    Coord4 {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    }
}

fn split_at_point(coords: &[Coord4], px: f64, py: f64, z_override: Option<f64>) -> PointSplit {
    if coords.is_empty() {
        return PointSplit::None;
    }
    let segments = coords.len() - 1;
    if segments == 0 {
        return PointSplit::Boundary;
    }

    let mut mindist = f64::INFINITY;
    let mut closest = None;
    for index in 0..segments {
        let distance = distance2d_sqr_pt_seg(px, py, coords[index], coords[index + 1]);
        if index == 0 || distance < mindist {
            mindist = distance;
            closest = Some(index);
            if mindist == 0.0 {
                break;
            }
        }
    }

    if mindist > 0.0 {
        return PointSplit::None;
    }
    let Some(segment) = closest else {
        return PointSplit::Boundary;
    };

    let start = coords[segment];
    let end = coords[segment + 1];
    let mut projected = closest_point_on_segment(px, py, start, end);
    // PostGIS keeps the blade X/Y so interpolation cannot move the split point.
    projected.x = px;
    projected.y = py;
    if let Some(z) = z_override {
        projected.z = z;
    }

    if (segment == segments - 1 && projected.x == end.x && projected.y == end.y)
        || (segment == 0 && projected.x == start.x && projected.y == start.y)
    {
        return PointSplit::Boundary;
    }

    let mut prefix = coords[..=segment].to_vec();
    prefix.push(projected);
    let mut suffix = Vec::with_capacity(coords.len() - segment);
    suffix.push(projected);
    suffix.extend_from_slice(&coords[segment + 1..]);
    if prefix.is_empty() || suffix.is_empty() {
        return PointSplit::Boundary;
    }
    PointSplit::Split(prefix, suffix)
}

fn distance2d_sqr_pt_seg(px: f64, py: f64, start: Coord4, end: Coord4) -> f64 {
    let ba_x = end.x - start.x;
    let ba_y = end.y - start.y;
    let length_sqr = ba_x * ba_x + ba_y * ba_y;
    if start.x == end.x && start.y == end.y {
        return (px - start.x) * (px - start.x) + (py - start.y) * (py - start.y);
    }

    let ca_x = px - start.x;
    let ca_y = py - start.y;
    let dot = ca_x * ba_x + ca_y * ba_y;
    if dot <= 0.0 {
        return ca_x * ca_x + ca_y * ca_y;
    }
    if dot >= length_sqr {
        return (px - end.x) * (px - end.x) + (py - end.y) * (py - end.y);
    }
    let cross = ca_x * ba_y - ca_y * ba_x;
    cross * cross / length_sqr
}

fn closest_point_on_segment(px: f64, py: f64, start: Coord4, end: Coord4) -> Coord4 {
    if fp_equals(start.x, end.x) && fp_equals(start.y, end.y) {
        return start;
    }
    let r = ((px - start.x) * (end.x - start.x) + (py - start.y) * (end.y - start.y))
        / ((end.x - start.x) * (end.x - start.x) + (end.y - start.y) * (end.y - start.y));
    if r <= 0.0 {
        return start;
    }
    if r >= 1.0 {
        return end;
    }
    Coord4 {
        x: start.x + (end.x - start.x) * r,
        y: start.y + (end.y - start.y) * r,
        z: start.z + (end.z - start.z) * r,
        m: start.m + (end.m - start.m) * r,
    }
}

fn fp_equals(left: f64, right: f64) -> bool {
    (left - right).abs() <= FP_TOLERANCE
}

fn collection_member_wkbs(
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoDataFusionResult<Vec<Vec<u8>>> {
    Ok(match geometry.as_type() {
        GeometryType::MultiLineString(collection) => collection
            .line_strings()
            .map(|child| geometry_wkb(&child))
            .collect::<GeoDataFusionResult<Vec<_>>>()?,
        GeometryType::MultiPolygon(collection) => collection
            .polygons()
            .map(|child| geometry_wkb(&child))
            .collect::<GeoDataFusionResult<Vec<_>>>()?,
        GeometryType::GeometryCollection(collection) => collection
            .geometries()
            .map(|child| geometry_wkb(&child))
            .collect::<GeoDataFusionResult<Vec<_>>>()?,
        _ => Vec::new(),
    })
}

fn geometry_wkb(geometry: &impl GeometryTrait<T = f64>) -> GeoDataFusionResult<Vec<u8>> {
    let mut output = Vec::with_capacity(geometry_wkb_size(geometry));
    write_geometry(&mut output, geometry, &WriteOptions::default()).map_err(|error| {
        DataFusionError::Execution(format!("ST_Split could not encode geometry WKB: {error}"))
    })?;
    Ok(output)
}

fn line_wkb(coords: &[Coord4], dims: Dimensions) -> Vec<u8> {
    let type_code = match dims {
        Dimensions::Xy => 2u32,
        Dimensions::Xyz => 1002,
        Dimensions::Xym => 2002,
        Dimensions::Xyzm => 3002,
        Dimensions::Unknown(_) => 2,
    };
    let mut output = Vec::with_capacity(9 + coords.len() * 16 * dims.size());
    output.push(1);
    output.extend_from_slice(&type_code.to_le_bytes());
    output.extend_from_slice(&(coords.len() as u32).to_le_bytes());
    for coord in coords {
        output.extend_from_slice(&coord.x.to_le_bytes());
        output.extend_from_slice(&coord.y.to_le_bytes());
        if matches!(dims, Dimensions::Xyz | Dimensions::Xyzm) {
            output.extend_from_slice(&coord.z.to_le_bytes());
        }
        if matches!(dims, Dimensions::Xym | Dimensions::Xyzm) {
            output.extend_from_slice(&coord.m.to_le_bytes());
        }
    }
    output
}

fn empty_line_wkb(dims: Dimensions) -> Vec<u8> {
    line_wkb(&[], dims)
}

fn wrap_collection(pieces: &[Piece], empty_dims: Dimensions) -> Vec<u8> {
    let dims = pieces.first().map_or(empty_dims, |piece| piece.dims);
    let mut output = Vec::new();
    output.push(1);
    output.extend_from_slice(&collection_type_code(dims).to_le_bytes());
    output.extend_from_slice(&(pieces.len() as u32).to_le_bytes());
    for piece in pieces {
        output.extend_from_slice(&piece.wkb);
    }
    output
}

fn collection_type_code(dims: Dimensions) -> u32 {
    match dims {
        Dimensions::Xy => 7,
        Dimensions::Xyz => 1007,
        Dimensions::Xym => 2007,
        Dimensions::Xyzm => 3007,
        Dimensions::Unknown(_) => 7,
    }
}

fn postgis_type_name(geometry_type: GeometryTypes) -> &'static str {
    match geometry_type {
        GeometryTypes::Point => "Point",
        GeometryTypes::LineString => "LineString",
        GeometryTypes::LinearRing => "LinearRing",
        GeometryTypes::Polygon => "Polygon",
        GeometryTypes::MultiPoint => "MultiPoint",
        GeometryTypes::MultiLineString => "MultiLineString",
        GeometryTypes::MultiPolygon => "MultiPolygon",
        GeometryTypes::GeometryCollection => "GeometryCollection",
    }
}
