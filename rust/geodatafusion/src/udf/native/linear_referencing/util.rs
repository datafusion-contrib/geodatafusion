//! Internal linear-referencing math and GeoArrow construction helpers.
//!
//! Clipping and substring operations are expressed independently as interval
//! intersections over normalized ordinate or length domains. PostGIS behavior
//! is used as a black-box compatibility oracle; no PostGIS source is copied.

use std::sync::Arc;

use arrow_array::Float64Array;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;
use geo_traits::{
    CoordTrait, Dimensions as TraitDimensions, GeometryCollectionTrait, GeometryTrait,
    GeometryType, LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait,
    PointTrait, PolygonTrait,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{GeometryArray, LineStringArray, MultiPointArray, PointArray};
use geoarrow_array::builder::{
    LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder,
    PointBuilder, PolygonBuilder,
};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, GeometryType as GeometryDataType, LineStringType, Metadata,
    MultiPointType, PointType,
};
use wkt::Wkt;
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, GeometryCollection as WktGeometryCollection,
    LineString as WktLineString, MultiLineString as WktMultiLineString,
    MultiPoint as WktMultiPoint, MultiPolygon as WktMultiPolygon, Point as WktPoint,
    Polygon as WktPolygon,
};

use crate::error::{GeoDataFusionError, GeoDataFusionResult};
use crate::udf::native::geometry::{StrictGeometryBuilder, merge_metadata};

const FP_TOLERANCE: f64 = 1e-12;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Coord4 {
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) z: Option<f64>,
    pub(crate) m: Option<f64>,
}

impl Coord4 {
    pub(crate) fn from_coord(coord: &impl CoordTrait<T = f64>) -> GeoDataFusionResult<Self> {
        let z = match coord.dim() {
            TraitDimensions::Xy | TraitDimensions::Xym => None,
            TraitDimensions::Xyz | TraitDimensions::Xyzm => {
                Some(coord.nth(2).ok_or_else(|| {
                    datafusion_error("geometry coordinate is missing its Z ordinate")
                })?)
            }
            TraitDimensions::Unknown(size) => {
                return Err(datafusion_error(format!(
                    "unsupported coordinate dimension: {size}"
                )));
            }
        };
        let m = match coord.dim() {
            TraitDimensions::Xy | TraitDimensions::Xyz => None,
            TraitDimensions::Xym => coord.nth(2),
            TraitDimensions::Xyzm => coord.nth(3),
            TraitDimensions::Unknown(size) => {
                return Err(datafusion_error(format!(
                    "unsupported coordinate dimension: {size}"
                )));
            }
        };
        let value = Self {
            x: coord.x(),
            y: coord.y(),
            z,
            m,
        };
        value.ensure_finite_xy()?;
        Ok(value)
    }

    pub(crate) fn interpolate(&self, other: &Self, fraction: f64) -> Self {
        Self {
            x: self.x + (other.x - self.x) * fraction,
            y: self.y + (other.y - self.y) * fraction,
            z: match (self.z, other.z) {
                (Some(start), Some(end)) => Some(start + (end - start) * fraction),
                _ => None,
            },
            m: match (self.m, other.m) {
                (Some(start), Some(end)) => Some(start + (end - start) * fraction),
                _ => None,
            },
        }
    }

    pub(crate) fn ordinate(&self, ordinate: ClipOrdinate) -> f64 {
        match ordinate {
            ClipOrdinate::Z => self.z.unwrap_or(0.0),
            ClipOrdinate::M => self.m.unwrap_or(0.0),
        }
    }

    pub(crate) fn with_m(self, m: f64) -> Self {
        Self { m: Some(m), ..self }
    }

    pub(crate) fn to_wkt(self) -> WktCoord<f64> {
        WktCoord {
            x: self.x,
            y: self.y,
            z: self.z,
            m: self.m,
        }
    }

    fn ensure_finite_xy(&self) -> GeoDataFusionResult<()> {
        if !self.x.is_finite() || !self.y.is_finite() {
            return Err(datafusion_error(
                "linear referencing requires finite X and Y coordinates",
            ));
        }
        Ok(())
    }

    fn same_xy(&self, other: &Self) -> bool {
        (self.x - other.x).abs() <= FP_TOLERANCE && (self.y - other.y).abs() <= FP_TOLERANCE
    }

    pub(crate) fn same_all(&self, other: &Self) -> bool {
        (self.x - other.x).abs() <= FP_TOLERANCE
            && (self.y - other.y).abs() <= FP_TOLERANCE
            && optional_almost_equal(self.z, other.z)
            && optional_almost_equal(self.m, other.m)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClipOrdinate {
    Z,
    M,
}

#[derive(Clone, Debug)]
pub(crate) struct LineString4 {
    pub(crate) coords: Vec<Coord4>,
    pub(crate) dim: Dimension,
}

impl LineString4 {
    pub(crate) fn from_trait(line: &impl LineStringTrait<T = f64>) -> GeoDataFusionResult<Self> {
        let dim = Dimension::try_from(line.dim()).map_err(|_| {
            datafusion_error(format!(
                "unsupported LineString dimension: {:?}",
                line.dim()
            ))
        })?;
        let coords = line
            .coords()
            .map(|coord| Coord4::from_coord(&coord))
            .collect::<GeoDataFusionResult<Vec<_>>>()?;
        Ok(Self { coords, dim })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.coords.is_empty()
    }

    pub(crate) fn has_z(&self) -> bool {
        matches!(self.dim, Dimension::XYZ | Dimension::XYZM)
    }

    pub(crate) fn length_2d(&self) -> f64 {
        self.coords
            .windows(2)
            .map(|pair| distance_2d(&pair[0], &pair[1]))
            .sum()
    }

    pub(crate) fn length_3d(&self) -> f64 {
        if !self.has_z() {
            return self.length_2d();
        }
        self.coords
            .windows(2)
            .map(|pair| {
                let dx = pair[1].x - pair[0].x;
                let dy = pair[1].y - pair[0].y;
                let dz = pair[1].z.unwrap_or(0.0) - pair[0].z.unwrap_or(0.0);
                (dx * dx + dy * dy + dz * dz).sqrt()
            })
            .sum()
    }

    pub(crate) fn interpolate(
        &self,
        fraction: f64,
        use_3d_length: bool,
    ) -> GeoDataFusionResult<Coord4> {
        let first = self
            .coords
            .first()
            .copied()
            .ok_or_else(|| datafusion_error("cannot interpolate an empty LineString"))?;
        let last = self.coords.last().copied().unwrap_or(first);
        if fraction == 0.0 {
            return Ok(first);
        }
        if fraction == 1.0 {
            return Ok(last);
        }

        let total_length = if use_3d_length {
            self.length_3d()
        } else {
            self.length_2d()
        };
        if total_length == 0.0 {
            return Ok(last);
        }

        let mut consumed = 0.0;
        for pair in self.coords.windows(2) {
            let segment_length = if use_3d_length {
                let dx = pair[1].x - pair[0].x;
                let dy = pair[1].y - pair[0].y;
                let dz = pair[1].z.unwrap_or(0.0) - pair[0].z.unwrap_or(0.0);
                (dx * dx + dy * dy + dz * dz).sqrt()
            } else {
                distance_2d(&pair[0], &pair[1])
            };
            let segment_fraction_length = segment_length / total_length;
            if segment_length > 0.0 && fraction < consumed + segment_fraction_length {
                let segment_fraction = (fraction - consumed) / segment_fraction_length;
                return Ok(pair[0].interpolate(&pair[1], segment_fraction));
            }
            consumed += segment_fraction_length;
        }
        Ok(last)
    }

    pub(crate) fn locate_fraction_2d(&self, point: &Coord4) -> GeoDataFusionResult<f64> {
        if self.coords.len() <= 1 {
            return Ok(0.0);
        }

        let mut minimum_distance = f64::MAX;
        let mut segment = 0;
        for index in 1..self.coords.len() {
            let distance =
                distance_squared_to_segment(point, &self.coords[index - 1], &self.coords[index]);
            if distance < minimum_distance {
                minimum_distance = distance;
                segment = index - 1;
                if minimum_distance == 0.0 {
                    break;
                }
            }
        }

        let start = self.coords[segment];
        let end = self.coords[segment + 1];
        let projected = closest_point_on_segment(point, &start, &end);
        if segment >= self.coords.len() - 2 && projected.same_xy(&end) {
            return Ok(1.0);
        }

        let total_length = self.length_2d();
        if total_length == 0.0 {
            return Ok(0.0);
        }

        let mut traversed = 0.0;
        for index in 0..segment {
            traversed += distance_2d(&self.coords[index], &self.coords[index + 1]);
        }
        traversed += distance_2d(&projected, &start);
        Ok(traversed / total_length)
    }

    pub(crate) fn substring(&self, from: f64, to: f64) -> GeoDataFusionResult<Vec<Coord4>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let length = self.length_2d();
        if length == 0.0 {
            // A path with no 2D extent collapses to its final segment or
            // vertex regardless of the requested interval.
            if self.coords.len() > 2 {
                return Ok(self.coords[self.coords.len() - 2..].to_vec());
            }
            return Ok(vec![*self.coords.last().unwrap()]);
        }

        let from_distance = length * from;
        let to_distance = length * to;
        if from_distance == to_distance {
            return Ok(vec![self.sample_at_distance(from_distance)]);
        }

        let mut result = vec![self.sample_at_distance(from_distance)];
        let mut traversed = 0.0;
        for pair in self.coords.windows(2) {
            let segment_length = distance_2d(&pair[0], &pair[1]);
            if segment_length == 0.0 {
                continue;
            }
            traversed += segment_length;
            if traversed > from_distance && traversed < to_distance {
                push_unique_exact(&mut result, pair[1]);
            }
            if traversed >= to_distance {
                break;
            }
        }
        push_unique_exact(&mut result, self.sample_at_distance(to_distance));
        Ok(result)
    }

    fn sample_at_distance(&self, distance: f64) -> Coord4 {
        let first = self.coords[0];
        let last = self.coords[self.coords.len() - 1];
        if distance <= 0.0 {
            return first;
        }
        let total_length = self.length_2d();
        if distance >= total_length {
            return last;
        }

        let mut traversed = 0.0;
        for pair in self.coords.windows(2) {
            let segment_length = distance_2d(&pair[0], &pair[1]);
            if segment_length == 0.0 {
                continue;
            }
            let next = traversed + segment_length;
            if distance <= next {
                return pair[0].interpolate(&pair[1], (distance - traversed) / segment_length);
            }
            traversed = next;
        }
        last
    }

    pub(crate) fn add_measure(&self, start: f64, end: f64) -> GeoDataFusionResult<Self> {
        let dim = with_measure(self.dim);
        if self.coords.is_empty() {
            return Ok(Self {
                coords: Vec::new(),
                dim,
            });
        }

        let total_length = self.length_2d();
        let range = end - start;
        let mut traversed = 0.0;
        let coords = self
            .coords
            .iter()
            .enumerate()
            .map(|(index, coord)| {
                if index > 0 {
                    traversed += distance_2d(&self.coords[index - 1], coord);
                }
                let measure = if total_length > 0.0 {
                    start + range * traversed / total_length
                } else if self.coords.len() > 1 {
                    start + range * index as f64 / (self.coords.len() - 1) as f64
                } else {
                    0.0
                };
                coord.with_m(measure)
            })
            .collect();
        Ok(Self { coords, dim })
    }

    pub(crate) fn to_wkt(&self) -> Wkt<f64> {
        Wkt::LineString(WktLineString::new(
            self.coords.iter().copied().map(Coord4::to_wkt).collect(),
            wkt_dimension(self.dim),
        ))
    }
}

pub(crate) fn point_coord(point: &impl PointTrait<T = f64>) -> GeoDataFusionResult<Option<Coord4>> {
    point
        .coord()
        .map(|coord| Coord4::from_coord(&coord))
        .transpose()
}

pub(crate) fn point_wkt(coord: Option<Coord4>, dim: Dimension) -> Wkt<f64> {
    Wkt::Point(WktPoint::new(coord.map(Coord4::to_wkt), wkt_dimension(dim)))
}

pub(crate) fn multi_point_wkt(points: Vec<Coord4>, dim: Dimension) -> Wkt<f64> {
    Wkt::MultiPoint(WktMultiPoint::new(
        points
            .into_iter()
            .map(|coord| WktPoint::from_coord(coord.to_wkt()))
            .collect(),
        wkt_dimension(dim),
    ))
}

#[derive(Clone, Debug)]
enum ClipPart {
    Point(Coord4),
    Line(Vec<Coord4>),
    Polygon(Vec<Vec<Coord4>>),
}

pub(crate) fn clip_geometry_to_ordinate(
    geometry: &impl GeometryTrait<T = f64>,
    ordinate: ClipOrdinate,
    from: f64,
    to: f64,
) -> GeoDataFusionResult<Wkt<f64>> {
    let (from, to) = if to < from { (to, from) } else { (from, to) };
    let dimension = geometry_dimension(geometry)?;
    match geometry.as_type() {
        GeometryType::Point(point) => {
            let points = point_coord(point)?
                .filter(|coord| {
                    let value = coord.ordinate(ordinate);
                    from <= value && value <= to
                })
                .into_iter()
                .collect();
            Ok(multi_point_wkt(points, dimension))
        }
        GeometryType::MultiPoint(points) => {
            let mut selected = Vec::new();
            for point in points.points() {
                if let Some(coord) = point_coord(&point)? {
                    let value = coord.ordinate(ordinate);
                    if from <= value && value <= to {
                        selected.push(coord);
                    }
                }
            }
            Ok(multi_point_wkt(selected, dimension))
        }
        GeometryType::LineString(line) => {
            let line = LineString4::from_trait(line)?;
            let parts = clip_line(&line, ordinate, from, to);
            Ok(parts_to_line_collection(parts, dimension))
        }
        GeometryType::Polygon(polygon) => {
            let polygons = clip_polygon_to_ordinate(polygon, ordinate, from, to)?;
            Ok(multi_polygon_wkt(polygons, dimension))
        }
        GeometryType::MultiPolygon(multi_polygon) => {
            let mut polygons = Vec::new();
            for polygon in multi_polygon.polygons() {
                polygons.extend(clip_polygon_to_ordinate(&polygon, ordinate, from, to)?);
            }
            Ok(multi_polygon_wkt(polygons, dimension))
        }
        GeometryType::MultiLineString(multi_line) => {
            let lines = multi_line.line_strings().collect::<Vec<_>>();
            if lines.len() == 1 {
                return clip_geometry_to_ordinate(&lines[0], ordinate, from, to);
            }

            let mut parts = Vec::new();
            let mut force_collection = false;
            for line in lines {
                let clipped = clip_geometry_to_ordinate(&line, ordinate, from, to)?;
                if !matches!(clipped, Wkt::MultiLineString(_)) {
                    force_collection = true;
                }
                append_wkt_parts(&mut parts, clipped)?;
            }
            if force_collection {
                Ok(parts_to_geometry_collection(parts, dimension))
            } else {
                Ok(parts_to_line_collection(parts, dimension))
            }
        }
        GeometryType::GeometryCollection(collection) => {
            let geometries = collection.geometries().collect::<Vec<_>>();
            if geometries.len() == 1 {
                return clip_geometry_to_ordinate(&geometries[0], ordinate, from, to);
            }

            let mut parts = Vec::new();
            for geometry in geometries {
                let clipped = clip_geometry_to_ordinate(&geometry, ordinate, from, to)?;
                append_wkt_parts(&mut parts, clipped)?;
            }
            Ok(parts_to_geometry_collection(parts, dimension))
        }
        _ => Err(datafusion_error(format!(
            "This function does not accept {} geometries.",
            geometry_name(geometry)
        ))),
    }
}

pub(crate) fn clip_polygon_to_ordinate(
    polygon: &impl PolygonTrait<T = f64>,
    ordinate: ClipOrdinate,
    from: f64,
    to: f64,
) -> GeoDataFusionResult<Vec<Vec<Vec<Coord4>>>> {
    let Some(exterior) = polygon.exterior() else {
        return Ok(Vec::new());
    };
    let exterior = LineString4::from_trait(&exterior)?;
    let exterior = clamp_ring_to_ordinate(&exterior, ordinate, from, to);
    if exterior.len() < 4 {
        return Ok(Vec::new());
    }

    let mut rings = vec![exterior];
    for interior in polygon.interiors() {
        let interior = LineString4::from_trait(&interior)?;
        let interior = clamp_ring_to_ordinate(&interior, ordinate, from, to);
        if interior.len() >= 4 {
            rings.push(interior);
        }
    }
    Ok(vec![rings])
}

fn clamp_ring_to_ordinate(
    ring: &LineString4,
    ordinate: ClipOrdinate,
    from: f64,
    to: f64,
) -> Vec<Coord4> {
    let mut result = Vec::new();
    for pair in ring.coords.windows(2) {
        let Some((lower, upper)) = ordinate_interval(&pair[0], &pair[1], ordinate, from, to) else {
            continue;
        };
        push_unique_exact(&mut result, pair[0].interpolate(&pair[1], lower));
        if upper != lower {
            push_unique_exact(&mut result, pair[0].interpolate(&pair[1], upper));
        }
    }
    if result.len() > 2 {
        let first = result[0];
        push_unique_exact(&mut result, first);
    }
    result
}

fn ordinate_interval(
    start: &Coord4,
    end: &Coord4,
    ordinate: ClipOrdinate,
    from: f64,
    to: f64,
) -> Option<(f64, f64)> {
    let start_value = start.ordinate(ordinate);
    let end_value = end.ordinate(ordinate);
    if start_value == end_value {
        return (from <= start_value && start_value <= to).then_some((0.0, 1.0));
    }

    let first = (from - start_value) / (end_value - start_value);
    let second = (to - start_value) / (end_value - start_value);
    let lower = first.min(second).max(0.0);
    let upper = first.max(second).min(1.0);
    (lower <= upper).then_some((lower, upper))
}

pub(crate) fn multi_polygon_wkt(polygons: Vec<Vec<Vec<Coord4>>>, dimension: Dimension) -> Wkt<f64> {
    let polygons = polygons
        .into_iter()
        .map(|rings| {
            WktPolygon::new(
                rings
                    .into_iter()
                    .map(|ring| {
                        WktLineString::new(
                            ring.into_iter().map(Coord4::to_wkt).collect(),
                            wkt_dimension(dimension),
                        )
                    })
                    .collect(),
                wkt_dimension(dimension),
            )
        })
        .collect();
    Wkt::MultiPolygon(WktMultiPolygon::new(polygons, wkt_dimension(dimension)))
}

pub(crate) fn geometry_dimension(
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoDataFusionResult<Dimension> {
    Dimension::try_from(geometry.dim()).map_err(|_| {
        datafusion_error(format!(
            "unsupported geometry dimension: {:?}",
            geometry.dim()
        ))
    })
}

pub(crate) fn geometry_has_m(geometry: &impl GeometryTrait<T = f64>) -> bool {
    matches!(geometry.dim(), TraitDimensions::Xym | TraitDimensions::Xyzm)
}

pub(crate) fn geometry_has_z(geometry: &impl GeometryTrait<T = f64>) -> bool {
    matches!(geometry.dim(), TraitDimensions::Xyz | TraitDimensions::Xyzm)
}

pub(crate) fn geometry_coord_type(field: &FieldRef) -> GeoDataFusionResult<CoordType> {
    if field.data_type() == &DataType::Null {
        return Ok(CoordType::default());
    }
    Ok(GeoArrowType::from_arrow_field(field.as_ref())?
        .coord_type()
        .unwrap_or_default())
}

pub(crate) fn planar_metadata(
    field: &FieldRef,
    function_name: &str,
) -> GeoDataFusionResult<Arc<Metadata>> {
    let metadata = ensure_planar_metadata([field.as_ref()], function_name)?;
    Ok(Arc::new(Metadata::new(metadata.crs().clone(), None)))
}

pub(crate) fn ensure_planar_metadata<'a>(
    fields: impl IntoIterator<Item = &'a Field>,
    function_name: &str,
) -> GeoDataFusionResult<Arc<Metadata>> {
    let metadata = merge_metadata(fields, function_name)?;
    if metadata.edges().is_some() {
        return Err(DataFusionError::Plan(format!(
            "{function_name} supports only planar edge metadata"
        ))
        .into());
    }
    Ok(metadata)
}

pub(crate) fn generic_geometry_type(
    field: &FieldRef,
    function_name: &str,
) -> GeoDataFusionResult<GeometryDataType> {
    Ok(
        GeometryDataType::new(planar_metadata(field, function_name)?)
            .with_coord_type(geometry_coord_type(field)?),
    )
}

pub(crate) fn has_sql_null_argument(args: &ScalarFunctionArgs) -> bool {
    args.args.iter().any(|argument| match argument {
        ColumnarValue::Scalar(ScalarValue::Null) => true,
        ColumnarValue::Array(array) => array.data_type() == &DataType::Null,
        _ => false,
    })
}

pub(crate) fn null_float64_value(number_rows: usize) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(Float64Array::new_null(number_rows)))
}

pub(crate) fn null_geometry_value(
    return_field: &FieldRef,
    number_rows: usize,
) -> GeoDataFusionResult<ColumnarValue> {
    let output_type = GeoArrowType::from_arrow_field(return_field.as_ref())?;
    match output_type {
        GeoArrowType::Point(typ) => Ok(ColumnarValue::Array(
            build_point_array(std::iter::repeat_n(None, number_rows), typ)?.to_array_ref(),
        )),
        GeoArrowType::MultiPoint(typ) => Ok(ColumnarValue::Array(
            build_multi_point_array(std::iter::repeat_n(None, number_rows), typ)?.to_array_ref(),
        )),
        GeoArrowType::LineString(typ) => Ok(ColumnarValue::Array(
            build_line_string_array(std::iter::repeat_n(None, number_rows), typ)?.to_array_ref(),
        )),
        GeoArrowType::MultiLineString(typ) => {
            let mut builder = MultiLineStringBuilder::new(typ);
            for row in std::iter::repeat_n(None::<Wkt<f64>>, number_rows) {
                builder.push_geometry(row.as_ref())?;
            }
            Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
        }
        GeoArrowType::Polygon(typ) => {
            let mut builder = PolygonBuilder::new(typ);
            for row in std::iter::repeat_n(None::<Wkt<f64>>, number_rows) {
                builder.push_geometry(row.as_ref())?;
            }
            Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
        }
        GeoArrowType::MultiPolygon(typ) => {
            let mut builder = MultiPolygonBuilder::new(typ);
            for row in std::iter::repeat_n(None::<Wkt<f64>>, number_rows) {
                builder.push_geometry(row.as_ref())?;
            }
            Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
        }
        GeoArrowType::Geometry(typ) => Ok(ColumnarValue::Array(
            build_generic_geometry_array(std::iter::repeat_n(None, number_rows), typ)?
                .to_array_ref(),
        )),
        other => Err(datafusion_error(format!(
            "unsupported null result type: {other:?}"
        ))),
    }
}

pub(crate) fn build_point_array(
    rows: impl IntoIterator<Item = Option<Wkt<f64>>>,
    typ: PointType,
) -> GeoDataFusionResult<PointArray> {
    let mut builder = PointBuilder::new(typ);
    for row in rows {
        builder.push_geometry(row.as_ref())?;
    }
    Ok(builder.finish())
}

pub(crate) fn build_multi_point_array(
    rows: impl IntoIterator<Item = Option<Wkt<f64>>>,
    typ: MultiPointType,
) -> GeoDataFusionResult<MultiPointArray> {
    let mut builder = MultiPointBuilder::new(typ);
    for row in rows {
        builder.push_geometry(row.as_ref())?;
    }
    Ok(builder.finish())
}

pub(crate) fn build_line_string_array(
    rows: impl IntoIterator<Item = Option<Wkt<f64>>>,
    typ: LineStringType,
) -> GeoDataFusionResult<LineStringArray> {
    let mut builder = LineStringBuilder::new(typ);
    for row in rows {
        builder.push_geometry(row.as_ref())?;
    }
    Ok(builder.finish())
}

pub(crate) fn build_generic_geometry_array(
    rows: impl IntoIterator<Item = Option<Wkt<f64>>>,
    typ: GeometryDataType,
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = StrictGeometryBuilder::new(typ);
    for row in rows {
        builder.push_geometry(row.as_ref())?;
    }
    Ok(builder.finish())
}

pub(crate) fn with_measure(dimension: Dimension) -> Dimension {
    match dimension {
        Dimension::XY | Dimension::XYM => Dimension::XYM,
        Dimension::XYZ | Dimension::XYZM => Dimension::XYZM,
    }
}

pub(crate) fn wkt_dimension(dimension: Dimension) -> WktDimension {
    match dimension {
        Dimension::XY => WktDimension::XY,
        Dimension::XYZ => WktDimension::XYZ,
        Dimension::XYM => WktDimension::XYM,
        Dimension::XYZM => WktDimension::XYZM,
    }
}

pub(crate) fn datafusion_error(message: impl Into<String>) -> GeoDataFusionError {
    DataFusionError::Execution(message.into()).into()
}

pub(crate) fn fp_equal(left: f64, right: f64) -> bool {
    (left - right).abs() <= FP_TOLERANCE
}

pub(crate) fn finite_fraction(fraction: f64, ordinal: &str) -> GeoDataFusionResult<()> {
    if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
        return Err(datafusion_error(format!(
            "line_interpolate_point: {ordinal} arg isn't within [0,1]"
        )));
    }
    Ok(())
}

pub(crate) fn push_unique(points: &mut Vec<Coord4>, value: Coord4) {
    if points
        .last()
        .is_none_or(|previous| !previous.same_all(&value))
    {
        points.push(value);
    }
}

fn push_unique_exact(points: &mut Vec<Coord4>, value: Coord4) {
    if points
        .last()
        .is_none_or(|previous| !same_all_exact(previous, &value))
    {
        points.push(value);
    }
}

fn same_all_exact(left: &Coord4, right: &Coord4) -> bool {
    left.x == right.x && left.y == right.y && left.z == right.z && left.m == right.m
}

fn clip_line(line: &LineString4, ordinate: ClipOrdinate, from: f64, to: f64) -> Vec<ClipPart> {
    let mut parts = Vec::new();
    let mut run = Vec::new();
    let mut previous_ended_at_vertex = false;

    for pair in line.coords.windows(2) {
        if let Some((lower, upper)) = ordinate_interval(&pair[0], &pair[1], ordinate, from, to) {
            let continues = !run.is_empty() && previous_ended_at_vertex && lower == 0.0;
            if !continues {
                flush_clip_run(&mut run, &mut parts);
            }
            push_unique_exact(&mut run, pair[0].interpolate(&pair[1], lower));
            if upper != lower {
                push_unique_exact(&mut run, pair[0].interpolate(&pair[1], upper));
            }
            previous_ended_at_vertex = upper == 1.0;
        } else {
            flush_clip_run(&mut run, &mut parts);
            previous_ended_at_vertex = false;
        }
    }
    flush_clip_run(&mut run, &mut parts);
    parts
}

fn flush_clip_run(run: &mut Vec<Coord4>, parts: &mut Vec<ClipPart>) {
    if run.is_empty() {
        return;
    }
    let value = std::mem::take(run);
    if value.len() == 1 {
        parts.push(ClipPart::Point(value[0]));
    } else {
        parts.push(ClipPart::Line(value));
    }
}

fn append_wkt_parts(parts: &mut Vec<ClipPart>, geometry: Wkt<f64>) -> GeoDataFusionResult<()> {
    match geometry {
        Wkt::Point(point) => {
            if let Some(coord) = point.coord() {
                parts.push(ClipPart::Point(Coord4 {
                    x: coord.x,
                    y: coord.y,
                    z: coord.z,
                    m: coord.m,
                }));
            }
        }
        Wkt::LineString(line) => {
            parts.push(ClipPart::Line(
                line.coords()
                    .iter()
                    .map(|coord| Coord4 {
                        x: coord.x,
                        y: coord.y,
                        z: coord.z,
                        m: coord.m,
                    })
                    .collect(),
            ));
        }
        Wkt::Polygon(polygon) => {
            parts.push(ClipPart::Polygon(wkt_polygon_rings(&polygon)));
        }
        Wkt::MultiPoint(multi_point) => {
            for point in multi_point.points() {
                if let Some(coord) = point_coord(&point)? {
                    parts.push(ClipPart::Point(coord));
                }
            }
        }
        Wkt::MultiLineString(multi_line) => {
            for line in multi_line.line_strings() {
                parts.push(ClipPart::Line(
                    line.coords()
                        .iter()
                        .map(|coord| Coord4 {
                            x: coord.x,
                            y: coord.y,
                            z: coord.z,
                            m: coord.m,
                        })
                        .collect(),
                ));
            }
        }
        Wkt::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon.polygons() {
                parts.push(ClipPart::Polygon(wkt_polygon_rings(polygon)));
            }
        }
        Wkt::GeometryCollection(collection) => {
            for geometry in collection.geometries() {
                append_wkt_parts(parts, geometry.clone())?;
            }
        }
    }
    Ok(())
}

fn parts_to_line_collection(parts: Vec<ClipPart>, dimension: Dimension) -> Wkt<f64> {
    if parts.iter().any(|part| matches!(part, ClipPart::Point(_))) {
        return parts_to_geometry_collection(parts, dimension);
    }

    let lines = parts
        .into_iter()
        .filter_map(|part| match part {
            ClipPart::Line(coords) => Some(WktLineString::new(
                coords.into_iter().map(Coord4::to_wkt).collect(),
                wkt_dimension(dimension),
            )),
            ClipPart::Point(_) | ClipPart::Polygon(_) => None,
        })
        .collect();
    Wkt::MultiLineString(WktMultiLineString::new(lines, wkt_dimension(dimension)))
}

fn parts_to_geometry_collection(parts: Vec<ClipPart>, dimension: Dimension) -> Wkt<f64> {
    let geometries = parts
        .into_iter()
        .map(|part| match part {
            ClipPart::Point(coord) => point_wkt(Some(coord), dimension),
            ClipPart::Line(coords) => Wkt::LineString(WktLineString::new(
                coords.into_iter().map(Coord4::to_wkt).collect(),
                wkt_dimension(dimension),
            )),
            ClipPart::Polygon(rings) => Wkt::Polygon(WktPolygon::new(
                rings
                    .into_iter()
                    .map(|ring| {
                        WktLineString::new(
                            ring.into_iter().map(Coord4::to_wkt).collect(),
                            wkt_dimension(dimension),
                        )
                    })
                    .collect(),
                wkt_dimension(dimension),
            )),
        })
        .collect();
    Wkt::GeometryCollection(WktGeometryCollection::new(
        geometries,
        wkt_dimension(dimension),
    ))
}

fn wkt_polygon_rings(polygon: &WktPolygon<f64>) -> Vec<Vec<Coord4>> {
    polygon
        .rings()
        .iter()
        .map(|ring| {
            ring.coords()
                .iter()
                .map(|coord| Coord4 {
                    x: coord.x,
                    y: coord.y,
                    z: coord.z,
                    m: coord.m,
                })
                .collect()
        })
        .collect()
}

pub(crate) fn geometry_name(geometry: &impl GeometryTrait<T = f64>) -> &'static str {
    match geometry.as_type() {
        GeometryType::Point(_) => "POINT",
        GeometryType::LineString(_) => "LINESTRING",
        GeometryType::Polygon(_) => "POLYGON",
        GeometryType::MultiPoint(_) => "MULTIPOINT",
        GeometryType::MultiLineString(_) => "MULTILINESTRING",
        GeometryType::MultiPolygon(_) => "MULTIPOLYGON",
        GeometryType::GeometryCollection(_) => "GEOMETRYCOLLECTION",
        GeometryType::Rect(_) => "RECT",
        GeometryType::Line(_) => "LINE",
        GeometryType::Triangle(_) => "TRIANGLE",
    }
}

#[cfg(feature = "geos-3_11")]
pub(crate) fn wkt_type_name(geometry: &Wkt<f64>) -> &'static str {
    match geometry {
        Wkt::Point(_) => "POINT",
        Wkt::LineString(_) => "LINESTRING",
        Wkt::Polygon(_) => "POLYGON",
        Wkt::MultiPoint(_) => "MULTIPOINT",
        Wkt::MultiLineString(_) => "MULTILINESTRING",
        Wkt::MultiPolygon(_) => "MULTIPOLYGON",
        Wkt::GeometryCollection(_) => "GEOMETRYCOLLECTION",
    }
}

pub(crate) fn distance_2d(left: &Coord4, right: &Coord4) -> f64 {
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    (dx * dx + dy * dy).sqrt()
}

fn distance_squared_to_segment(point: &Coord4, start: &Coord4, end: &Coord4) -> f64 {
    if start.x == end.x && start.y == end.y {
        let dx = point.x - start.x;
        let dy = point.y - start.y;
        return dx * dx + dy * dy;
    }

    let ba_x = end.x - start.x;
    let ba_y = end.y - start.y;
    let ab_length_squared = ba_x * ba_x + ba_y * ba_y;
    let ca_x = point.x - start.x;
    let ca_y = point.y - start.y;
    let dot = ca_x * ba_x + ca_y * ba_y;

    if dot <= 0.0 {
        let dx = point.x - start.x;
        let dy = point.y - start.y;
        return dx * dx + dy * dy;
    }
    if dot >= ab_length_squared {
        let dx = point.x - end.x;
        let dy = point.y - end.y;
        return dx * dx + dy * dy;
    }

    let cross = ca_x * ba_y - ca_y * ba_x;
    cross * cross / ab_length_squared
}

fn closest_point_on_segment(point: &Coord4, start: &Coord4, end: &Coord4) -> Coord4 {
    if fp_equal(start.x, end.x) && fp_equal(start.y, end.y) {
        return *start;
    }

    let ratio = ((point.x - start.x) * (end.x - start.x) + (point.y - start.y) * (end.y - start.y))
        / ((end.x - start.x) * (end.x - start.x) + (end.y - start.y) * (end.y - start.y));
    if ratio <= 0.0 {
        *start
    } else if ratio >= 1.0 {
        *end
    } else {
        Coord4 {
            x: start.x + (end.x - start.x) * ratio,
            y: start.y + (end.y - start.y) * ratio,
            z: Some(
                start.z.unwrap_or(0.0) + (end.z.unwrap_or(0.0) - start.z.unwrap_or(0.0)) * ratio,
            ),
            m: Some(
                start.m.unwrap_or(0.0) + (end.m.unwrap_or(0.0) - start.m.unwrap_or(0.0)) * ratio,
            ),
        }
    }
}

fn optional_almost_equal(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => fp_equal(left, right),
        (None, None) => true,
        _ => false,
    }
}
