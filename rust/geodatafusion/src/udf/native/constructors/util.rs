use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::DataFusionError;
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiPointTrait, PointTrait,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{
    CoordType, Dimension, GeometryType as GeoArrowGeometryType, LineStringType, Metadata,
};
use wkb::reader::Wkb;
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString};

use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::is_null_geometry_field;

#[derive(Debug, Clone, Copy)]
pub(super) struct CoordValue {
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>,
    pub m: Option<f64>,
}

pub(super) type GeometryCoords = (Vec<CoordValue>, Dimension);
pub(super) type CoordRows = Vec<Option<GeometryCoords>>;
pub(super) type RingRows = Vec<Option<Vec<GeometryCoords>>>;

impl CoordValue {
    fn from_coord(coord: &impl CoordTrait<T = f64>) -> Self {
        let dim = coord.dim();
        let z = match dim {
            geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm => coord.nth(2),
            _ => None,
        };
        let m = match dim {
            geo_traits::Dimensions::Xym => coord.nth(2),
            geo_traits::Dimensions::Xyzm => coord.nth(3),
            _ => None,
        };

        Self {
            x: coord.x(),
            y: coord.y(),
            z,
            m,
        }
    }

    pub(super) fn to_wkt(self, dim: Dimension) -> WktCoord<f64> {
        WktCoord {
            x: self.x,
            y: self.y,
            z: dimension_has_z(dim).then_some(self.z.unwrap_or(0.0)),
            m: dimension_has_m(dim).then_some(self.m.unwrap_or(0.0)),
        }
    }

    pub(super) fn same_xy(self, other: Self) -> bool {
        self.x == other.x && self.y == other.y
    }
}

#[derive(Debug, Clone)]
pub(super) enum LineElement {
    Point(CoordValue),
    Line {
        coords: Vec<CoordValue>,
        dim: Dimension,
    },
}

#[derive(Debug, Clone)]
pub(super) struct LineParts {
    pub elements: Vec<LineElement>,
    pub dim: Option<Dimension>,
}

impl LineParts {
    pub(super) fn new() -> Self {
        Self {
            elements: Vec::new(),
            dim: None,
        }
    }

    pub(super) fn include_dim(&mut self, dim: Dimension) {
        self.dim = Some(match self.dim {
            Some(current) => union_dimensions(current, dim),
            None => dim,
        });
    }

    pub(super) fn merge(&mut self, mut other: Self) {
        if let Some(dim) = other.dim {
            self.include_dim(dim);
        }
        self.elements.append(&mut other.elements);
    }

    pub(super) fn build_coords(
        &self,
        function_name: &str,
    ) -> GeoDataFusionResult<(Vec<CoordValue>, Dimension)> {
        let dim = self.dim.unwrap_or(Dimension::XY);
        let mut coords = Vec::new();

        for element in &self.elements {
            match element {
                LineElement::Point(coord) => coords.push(*coord),
                LineElement::Line {
                    coords: line,
                    dim: line_dim,
                } if !line.is_empty() => {
                    if *line_dim != dim {
                        return Err(DataFusionError::Execution(format!(
                            "{function_name}: cannot append mixed-dimensional LineString inputs"
                        ))
                        .into());
                    }

                    let start =
                        usize::from(coords.last().is_some_and(|last| last.same_xy(line[0])));
                    coords.extend_from_slice(&line[start..]);
                }
                LineElement::Line { .. } => {}
            }
        }

        Ok((coords, dim))
    }
}

/// Decode one WKB geometry using the same permissive policy as the aggregate
/// `ST_MakeLineAgg`. Unsupported geometry families contribute no coordinates.
pub(super) fn line_parts_from_wkb(bytes: &[u8]) -> GeoDataFusionResult<Option<LineParts>> {
    let geometry = Wkb::try_new(bytes).map_err(|error| {
        DataFusionError::Execution(format!("ST_MakeLineAgg input WKB is invalid: {error}"))
    })?;
    let mut parts = LineParts::new();

    match geometry.as_type() {
        GeometryType::Point(point) => {
            parts.include_dim(geometry_dimension(&geometry)?);
            if let Some(coord) = point.coord() {
                parts
                    .elements
                    .push(LineElement::Point(CoordValue::from_coord(&coord)));
            }
        }
        GeometryType::LineString(line) => {
            let dim = geometry_dimension(&geometry)?;
            parts.include_dim(dim);
            let coords = line
                .coords()
                .map(|coord| CoordValue::from_coord(&coord))
                .collect();
            if line.num_coords() != 0 {
                parts.elements.push(LineElement::Line { coords, dim });
            }
        }
        GeometryType::MultiPoint(multi_point) => {
            parts.include_dim(geometry_dimension(&geometry)?);
            parts
                .elements
                .extend(multi_point.points().filter_map(|point| {
                    point
                        .coord()
                        .map(|coord| LineElement::Point(CoordValue::from_coord(&coord)))
                }));
        }
        _ => return Ok(None),
    }

    Ok(Some(parts))
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum InvalidGeometry {
    Scalar,
    Ignore,
}

pub(super) fn collect_line_parts(
    array: &dyn GeoArrowArray,
    policy: InvalidGeometry,
) -> GeoDataFusionResult<Vec<Option<LineParts>>> {
    collect_line_parts_with_mask(array, policy, None)
}

pub(super) fn collect_line_parts_with_mask(
    array: &dyn GeoArrowArray,
    policy: InvalidGeometry,
    row_mask: Option<&[bool]>,
) -> GeoDataFusionResult<Vec<Option<LineParts>>> {
    downcast_geoarrow_array!(array, collect_line_parts_impl, policy, row_mask)
}

fn collect_line_parts_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    policy: InvalidGeometry,
    row_mask: Option<&[bool]>,
) -> GeoDataFusionResult<Vec<Option<LineParts>>> {
    let mut output = Vec::with_capacity(array.len());
    for (row, item) in array.iter().enumerate() {
        if row_mask.is_some_and(|mask| !mask[row]) {
            output.push(None);
            continue;
        }
        let Some(geom) = item.transpose()? else {
            output.push(None);
            continue;
        };

        let mut parts = LineParts::new();
        match geom.as_type() {
            GeometryType::Point(point) => {
                parts.include_dim(geometry_dimension(&geom)?);
                if let Some(coord) = point.coord() {
                    parts
                        .elements
                        .push(LineElement::Point(CoordValue::from_coord(&coord)));
                }
            }
            GeometryType::LineString(line) => {
                let dim = geometry_dimension(&geom)?;
                parts.include_dim(dim);
                let coords = line
                    .coords()
                    .map(|coord| CoordValue::from_coord(&coord))
                    .collect();
                if line.num_coords() != 0 {
                    parts.elements.push(LineElement::Line { coords, dim });
                }
            }
            GeometryType::MultiPoint(multi_point) => {
                if policy == InvalidGeometry::Scalar {
                    return Err(DataFusionError::Execution(
                        "ST_MakeLine scalar inputs must be Point or LineString geometries"
                            .to_string(),
                    )
                    .into());
                }
                parts.include_dim(geometry_dimension(&geom)?);
                parts
                    .elements
                    .extend(multi_point.points().filter_map(|point| {
                        point
                            .coord()
                            .map(|coord| LineElement::Point(CoordValue::from_coord(&coord)))
                    }));
            }
            _ if policy == InvalidGeometry::Ignore => {}
            _ => {
                return Err(DataFusionError::Execution(
                    "ST_MakeLine scalar inputs must be Point or LineString geometries".to_string(),
                )
                .into());
            }
        }
        output.push(Some(parts));
    }
    Ok(output)
}

pub(super) fn collect_multipoint_coords(
    array: &dyn GeoArrowArray,
) -> GeoDataFusionResult<CoordRows> {
    downcast_geoarrow_array!(array, collect_multipoint_coords_impl)
}

fn collect_multipoint_coords_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<CoordRows> {
    let mut output = Vec::with_capacity(array.len());
    for item in array.iter() {
        let Some(geom) = item.transpose()? else {
            output.push(None);
            continue;
        };

        let GeometryType::MultiPoint(multi_point) = geom.as_type() else {
            return Err(DataFusionError::Execution(
                "ST_LineFromMultiPoint input must be a MultiPoint geometry".to_string(),
            )
            .into());
        };
        let dim = geometry_dimension(&geom)?;
        let coords = multi_point
            .points()
            .filter_map(|point| point.coord().map(|coord| CoordValue::from_coord(&coord)))
            .collect();
        output.push(Some((coords, dim)));
    }
    Ok(output)
}

pub(super) fn collect_ring(array: &dyn GeoArrowArray) -> GeoDataFusionResult<CoordRows> {
    collect_ring_with_mask(array, None)
}

pub(super) fn collect_ring_with_mask(
    array: &dyn GeoArrowArray,
    row_mask: Option<&[bool]>,
) -> GeoDataFusionResult<CoordRows> {
    downcast_geoarrow_array!(array, collect_ring_impl, row_mask)
}

fn collect_ring_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    row_mask: Option<&[bool]>,
) -> GeoDataFusionResult<CoordRows> {
    let mut output = Vec::with_capacity(array.len());
    for (row, item) in array.iter().enumerate() {
        if row_mask.is_some_and(|mask| !mask[row]) {
            output.push(None);
            continue;
        }
        let Some(geom) = item.transpose()? else {
            output.push(None);
            continue;
        };

        let GeometryType::LineString(line) = geom.as_type() else {
            return Err(DataFusionError::Execution(
                "polygon rings must be LineString geometries".to_string(),
            )
            .into());
        };
        let coords = line
            .coords()
            .map(|coord| CoordValue::from_coord(&coord))
            .collect();
        output.push(Some((coords, geometry_dimension(&geom)?)));
    }
    Ok(output)
}

pub(super) fn validate_ring(
    function_name: &str,
    role: &str,
    coords: &[CoordValue],
    dim: Dimension,
    expected_dim: &mut Option<Dimension>,
) -> GeoDataFusionResult<()> {
    if coords.len() < 4 {
        return Err(DataFusionError::Execution(format!(
            "{function_name}: {role} must have at least 4 points"
        ))
        .into());
    }
    if !coords[0].same_xy(*coords.last().expect("coords is non-empty")) {
        return Err(
            DataFusionError::Execution(format!("{function_name}: {role} must be closed")).into(),
        );
    }

    match expected_dim {
        Some(expected) if *expected != dim => {
            return Err(DataFusionError::Execution(format!(
                "{function_name}: mixed dimensioned rings"
            ))
            .into());
        }
        Some(_) => {}
        None => *expected_dim = Some(dim),
    }
    Ok(())
}

pub(super) fn wkt_line_string(coords: Vec<CoordValue>, dim: Dimension) -> WktLineString<f64> {
    WktLineString::new(
        coords.into_iter().map(|coord| coord.to_wkt(dim)).collect(),
        wkt_dimension(dim),
    )
}

pub(super) fn field_dimension(field: &Field) -> GeoDataFusionResult<Option<Dimension>> {
    if is_null_geometry_field(field) {
        return Ok(None);
    }
    Ok(geoarrow_schema::GeoArrowType::from_arrow_field(field)?.dimension())
}

pub(super) fn list_element_field(field: &Field) -> GeoDataFusionResult<FieldRef> {
    match field.data_type() {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Ok(element.clone()),
        other => Err(DataFusionError::Execution(format!(
            "expected an array argument, got {other}"
        ))
        .into()),
    }
}

pub(super) fn output_line_field(
    dim: Option<Dimension>,
    metadata: Arc<Metadata>,
    coord_type: CoordType,
) -> Field {
    match dim {
        Some(dim) => LineStringType::new(dim, metadata)
            .with_coord_type(coord_type)
            .to_field("", true),
        None => GeoArrowGeometryType::new(metadata)
            .with_coord_type(coord_type)
            .to_field("", true),
    }
}

pub(super) fn geometry_dimension(
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoDataFusionResult<Dimension> {
    Ok(Dimension::try_from(geometry.dim())?)
}

pub(super) fn geo_array_is_null(array: &dyn GeoArrowArray, row: usize) -> bool {
    array
        .logical_nulls()
        .is_some_and(|nulls| nulls.is_null(row))
}

pub(super) fn union_dimensions(left: Dimension, right: Dimension) -> Dimension {
    match (
        dimension_has_z(left) || dimension_has_z(right),
        dimension_has_m(left) || dimension_has_m(right),
    ) {
        (false, false) => Dimension::XY,
        (true, false) => Dimension::XYZ,
        (false, true) => Dimension::XYM,
        (true, true) => Dimension::XYZM,
    }
}

fn dimension_has_z(dim: Dimension) -> bool {
    matches!(dim, Dimension::XYZ | Dimension::XYZM)
}

fn dimension_has_m(dim: Dimension) -> bool {
    matches!(dim, Dimension::XYM | Dimension::XYZM)
}

pub(super) fn wkt_dimension(dim: Dimension) -> WktDimension {
    match dim {
        Dimension::XY => WktDimension::XY,
        Dimension::XYZ => WktDimension::XYZ,
        Dimension::XYM => WktDimension::XYM,
        Dimension::XYZM => WktDimension::XYZM,
    }
}

pub(super) fn ensure_same_dimension(
    function_name: &str,
    left: Option<Dimension>,
    right: Option<Dimension>,
) -> GeoDataFusionResult<Option<Dimension>> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => Err(DataFusionError::Execution(format!(
            "{function_name}: mixed dimensioned rings"
        ))
        .into()),
        (Some(left), _) => Ok(Some(left)),
        (_, Some(right)) => Ok(Some(right)),
        (None, None) => Ok(None),
    }
}
