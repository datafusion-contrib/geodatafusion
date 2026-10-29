//! Collection conversion and polygon-orientation editors.
//!
//! These functions can change the top-level geometry type, so they return the
//! generic GeoArrow Geometry type. `StrictGeometryBuilder` keeps explicit
//! GeometryCollection wrappers instead of flattening single-member
//! collections.
//!
//! GeoArrow's generic geometry union and `MixedGeometryArray` have no nested
//! GeometryCollection member. Nested collections are outside the representable
//! input contract: the shared strict builder rejects them explicitly instead of
//! silently flattening them, while top-level collections are preserved.

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType};
use wkt::Wkt;
use wkt::types::{
    Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

use crate::data_types::{any_geometry_type, any_single_geometry_type_input};
use crate::error::GeoDataFusionResult;
use crate::udf::native::editors::transform::geometry_to_wkt;
use crate::udf::native::geometry::{StrictGeometryBuilder, geometry_array_from_arrow};

const EXTRACT_TYPE_ERROR: &str =
    "ST_CollectionExtract: only point, linestring and polygon may be extracted";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PrimitiveKind {
    Point,
    LineString,
    Polygon,
}

impl PrimitiveKind {
    fn rank(self) -> u8 {
        match self {
            Self::Point => 0,
            Self::LineString => 1,
            Self::Polygon => 2,
        }
    }

    fn empty(self, dimension: WktDimension) -> Wkt<f64> {
        match self {
            Self::Point => Wkt::Point(Point::empty(dimension)),
            Self::LineString => Wkt::LineString(LineString::empty(dimension)),
            Self::Polygon => Wkt::Polygon(Polygon::empty(dimension)),
        }
    }

    fn multi(self, geometries: Vec<Wkt<f64>>, dimension: WktDimension) -> Wkt<f64> {
        match self {
            Self::Point => Wkt::MultiPoint(MultiPoint::new(
                geometries
                    .into_iter()
                    .map(|geometry| match geometry {
                        Wkt::Point(point) => point,
                        _ => unreachable!("point extraction only yields points"),
                    })
                    .collect(),
                dimension,
            )),
            Self::LineString => Wkt::MultiLineString(MultiLineString::new(
                geometries
                    .into_iter()
                    .map(|geometry| match geometry {
                        Wkt::LineString(line_string) => line_string,
                        _ => unreachable!("line extraction only yields line strings"),
                    })
                    .collect(),
                dimension,
            )),
            Self::Polygon => Wkt::MultiPolygon(MultiPolygon::new(
                geometries
                    .into_iter()
                    .map(|geometry| match geometry {
                        Wkt::Polygon(polygon) => polygon,
                        _ => unreachable!("polygon extraction only yields polygons"),
                    })
                    .collect(),
                dimension,
            )),
        }
    }
}

fn primitive_kind(geometry: &Wkt<f64>) -> Option<PrimitiveKind> {
    match geometry {
        Wkt::Point(_) => Some(PrimitiveKind::Point),
        Wkt::LineString(_) => Some(PrimitiveKind::LineString),
        Wkt::Polygon(_) => Some(PrimitiveKind::Polygon),
        _ => None,
    }
}

fn is_collection(geometry: &Wkt<f64>) -> bool {
    matches!(
        geometry,
        Wkt::MultiPoint(_)
            | Wkt::MultiLineString(_)
            | Wkt::MultiPolygon(_)
            | Wkt::GeometryCollection(_)
    )
}

fn is_empty(geometry: &Wkt<f64>) -> bool {
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
            collection.geometries().is_empty() || collection.geometries().iter().all(is_empty)
        }
    }
}

fn largest_primitive(geometry: &Wkt<f64>) -> Option<PrimitiveKind> {
    match geometry {
        Wkt::Point(_) | Wkt::LineString(_) | Wkt::Polygon(_) => primitive_kind(geometry),
        Wkt::MultiPoint(multi_point) => {
            (!multi_point.points().is_empty()).then_some(PrimitiveKind::Point)
        }
        Wkt::MultiLineString(multi_line_string) => {
            (!multi_line_string.line_strings().is_empty()).then_some(PrimitiveKind::LineString)
        }
        Wkt::MultiPolygon(multi_polygon) => {
            (!multi_polygon.polygons().is_empty()).then_some(PrimitiveKind::Polygon)
        }
        Wkt::GeometryCollection(collection) => collection
            .geometries()
            .iter()
            .filter_map(largest_primitive)
            .max_by_key(|kind| kind.rank()),
    }
}

fn collect_primitives(geometry: &Wkt<f64>, target: PrimitiveKind, output: &mut Vec<Wkt<f64>>) {
    match geometry {
        Wkt::Point(_) | Wkt::LineString(_) | Wkt::Polygon(_) => {
            if primitive_kind(geometry) == Some(target) && !is_empty(geometry) {
                output.push(geometry.clone());
            }
        }
        Wkt::MultiPoint(multi_point) => {
            if target == PrimitiveKind::Point {
                for point in multi_point.points() {
                    let point = Wkt::Point(point.clone());
                    if !is_empty(&point) {
                        output.push(point);
                    }
                }
            }
        }
        Wkt::MultiLineString(multi_line_string) => {
            if target == PrimitiveKind::LineString {
                for line_string in multi_line_string.line_strings() {
                    let line_string = Wkt::LineString(line_string.clone());
                    if !is_empty(&line_string) {
                        output.push(line_string);
                    }
                }
            }
        }
        Wkt::MultiPolygon(multi_polygon) => {
            if target == PrimitiveKind::Polygon {
                for polygon in multi_polygon.polygons() {
                    let polygon = Wkt::Polygon(polygon.clone());
                    if !is_empty(&polygon) {
                        output.push(polygon);
                    }
                }
            }
        }
        Wkt::GeometryCollection(collection) => {
            for geometry in collection.geometries() {
                collect_primitives(geometry, target, output);
            }
        }
    }
}

fn collection_extract(geometry: &Wkt<f64>, requested: Option<PrimitiveKind>) -> Wkt<f64> {
    if !is_collection(geometry) {
        return match requested {
            None => geometry.clone(),
            Some(kind) if primitive_kind(geometry) == Some(kind) => geometry.clone(),
            Some(kind) => kind.empty(geometry.dimension()),
        };
    }

    let dimension = geometry.dimension();
    let Some(target) = requested.or_else(|| largest_primitive(geometry)) else {
        return Wkt::GeometryCollection(GeometryCollection::empty(dimension));
    };

    let mut geometries = Vec::new();
    collect_primitives(geometry, target, &mut geometries);
    target.multi(geometries, dimension)
}

#[derive(Default)]
struct HomogenizeBuffer {
    points: Vec<Wkt<f64>>,
    line_strings: Vec<Wkt<f64>>,
    polygons: Vec<Wkt<f64>>,
}

fn build_homogenize_buffer(geometry: &Wkt<f64>, buffer: &mut HomogenizeBuffer) {
    match geometry {
        Wkt::Point(_) => buffer.points.push(geometry.clone()),
        Wkt::LineString(_) => buffer.line_strings.push(geometry.clone()),
        Wkt::Polygon(_) => buffer.polygons.push(geometry.clone()),
        Wkt::MultiPoint(multi_point) => {
            buffer
                .points
                .extend(multi_point.points().iter().cloned().map(Wkt::Point));
        }
        Wkt::MultiLineString(multi_line_string) => {
            buffer.line_strings.extend(
                multi_line_string
                    .line_strings()
                    .iter()
                    .cloned()
                    .map(Wkt::LineString),
            );
        }
        Wkt::MultiPolygon(multi_polygon) => {
            buffer
                .polygons
                .extend(multi_polygon.polygons().iter().cloned().map(Wkt::Polygon));
        }
        Wkt::GeometryCollection(collection) => {
            for geometry in collection.geometries() {
                build_homogenize_buffer(geometry, buffer);
            }
        }
    }
}

fn empty_collection_like(geometry: &Wkt<f64>) -> Wkt<f64> {
    match geometry {
        Wkt::MultiPoint(multi_point) => Wkt::MultiPoint(MultiPoint::empty(multi_point.dimension())),
        Wkt::MultiLineString(multi_line_string) => {
            Wkt::MultiLineString(MultiLineString::empty(multi_line_string.dimension()))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            Wkt::MultiPolygon(MultiPolygon::empty(multi_polygon.dimension()))
        }
        Wkt::GeometryCollection(collection) => {
            Wkt::GeometryCollection(GeometryCollection::empty(collection.dimension()))
        }
        _ => geometry.clone(),
    }
}

pub(crate) fn collection_homogenize(geometry: &Wkt<f64>) -> Wkt<f64> {
    if is_empty(geometry) {
        return if is_collection(geometry) {
            empty_collection_like(geometry)
        } else {
            geometry.clone()
        };
    }

    match geometry {
        Wkt::Point(_) | Wkt::LineString(_) | Wkt::Polygon(_) => geometry.clone(),
        Wkt::MultiPoint(multi_point) if multi_point.points().len() == 1 => {
            Wkt::Point(multi_point.points()[0].clone())
        }
        Wkt::MultiLineString(multi_line_string) if multi_line_string.line_strings().len() == 1 => {
            Wkt::LineString(multi_line_string.line_strings()[0].clone())
        }
        Wkt::MultiPolygon(multi_polygon) if multi_polygon.polygons().len() == 1 => {
            Wkt::Polygon(multi_polygon.polygons()[0].clone())
        }
        Wkt::MultiPoint(_) | Wkt::MultiLineString(_) | Wkt::MultiPolygon(_) => geometry.clone(),
        Wkt::GeometryCollection(collection) => {
            let dimension = collection.dimension();
            let mut buffer = HomogenizeBuffer::default();
            for geometry in collection.geometries() {
                build_homogenize_buffer(geometry, &mut buffer);
            }

            let groups = [
                (PrimitiveKind::Point, buffer.points),
                (PrimitiveKind::LineString, buffer.line_strings),
                (PrimitiveKind::Polygon, buffer.polygons),
            ]
            .into_iter()
            .filter(|(_, geometries)| !geometries.is_empty())
            .collect::<Vec<_>>();

            match groups.as_slice() {
                [] => Wkt::GeometryCollection(GeometryCollection::empty(dimension)),
                [(_, geometries)] if geometries.len() == 1 => geometries[0].clone(),
                [(kind, geometries)] => kind.multi(geometries.clone(), dimension),
                groups => Wkt::GeometryCollection(GeometryCollection::new(
                    groups
                        .iter()
                        .map(|(kind, geometries)| {
                            if geometries.len() == 1 {
                                geometries[0].clone()
                            } else {
                                kind.multi(geometries.clone(), dimension)
                            }
                        })
                        .collect(),
                    dimension,
                )),
            }
        }
    }
}

fn force_collection(geometry: &Wkt<f64>) -> Wkt<f64> {
    match geometry {
        Wkt::MultiPoint(multi_point) => Wkt::GeometryCollection(GeometryCollection::new(
            multi_point
                .points()
                .iter()
                .cloned()
                .map(Wkt::Point)
                .collect(),
            multi_point.dimension(),
        )),
        Wkt::MultiLineString(multi_line_string) => {
            Wkt::GeometryCollection(GeometryCollection::new(
                multi_line_string
                    .line_strings()
                    .iter()
                    .cloned()
                    .map(Wkt::LineString)
                    .collect(),
                multi_line_string.dimension(),
            ))
        }
        Wkt::MultiPolygon(multi_polygon) => Wkt::GeometryCollection(GeometryCollection::new(
            multi_polygon
                .polygons()
                .iter()
                .cloned()
                .map(Wkt::Polygon)
                .collect(),
            multi_polygon.dimension(),
        )),
        Wkt::GeometryCollection(collection) => Wkt::GeometryCollection(collection.clone()),
        Wkt::Point(_) | Wkt::LineString(_) | Wkt::Polygon(_) => Wkt::GeometryCollection(
            GeometryCollection::new(vec![geometry.clone()], geometry.dimension()),
        ),
    }
}

fn force_multi(geometry: &Wkt<f64>) -> Wkt<f64> {
    match geometry {
        Wkt::Point(point) => {
            if point.coord().is_none() {
                Wkt::MultiPoint(MultiPoint::empty(point.dimension()))
            } else {
                Wkt::MultiPoint(MultiPoint::new(vec![point.clone()], point.dimension()))
            }
        }
        Wkt::LineString(line_string) => {
            if line_string.coords().is_empty() {
                Wkt::MultiLineString(MultiLineString::empty(line_string.dimension()))
            } else {
                Wkt::MultiLineString(MultiLineString::new(
                    vec![line_string.clone()],
                    line_string.dimension(),
                ))
            }
        }
        Wkt::Polygon(polygon) => {
            if is_empty(geometry) {
                Wkt::MultiPolygon(MultiPolygon::empty(polygon.dimension()))
            } else {
                Wkt::MultiPolygon(MultiPolygon::new(
                    vec![polygon.clone()],
                    polygon.dimension(),
                ))
            }
        }
        Wkt::MultiPoint(_)
        | Wkt::MultiLineString(_)
        | Wkt::MultiPolygon(_)
        | Wkt::GeometryCollection(_) => geometry.clone(),
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Orientation {
    Clockwise,
    CounterClockwise,
}

impl Orientation {
    fn opposite(self) -> Self {
        match self {
            Self::Clockwise => Self::CounterClockwise,
            Self::CounterClockwise => Self::Clockwise,
        }
    }
}

fn ring_signed_area(ring: &LineString<f64>) -> f64 {
    let coords = ring.coords();
    if coords.len() < 3 {
        return 0.0;
    }

    let origin = coords[0];
    coords[1..]
        .windows(2)
        .map(|pair| {
            let (first, second) = (pair[0], pair[1]);
            (first.x - origin.x) * (second.y - origin.y)
                - (first.y - origin.y) * (second.x - origin.x)
        })
        .sum::<f64>()
        / 2.0
}

fn ring_has_orientation(ring: &LineString<f64>, orientation: Orientation) -> bool {
    let area = ring_signed_area(ring);
    match orientation {
        Orientation::Clockwise => area < 0.0,
        Orientation::CounterClockwise => area >= 0.0,
    }
}

fn reverse_line_string(line_string: &LineString<f64>) -> LineString<f64> {
    let (mut coords, dimension) = line_string.clone().into_inner();
    coords.reverse();
    LineString::new(coords, dimension)
}

fn orient_ring(ring: &LineString<f64>, orientation: Orientation) -> LineString<f64> {
    if ring_has_orientation(ring, orientation) {
        ring.clone()
    } else {
        reverse_line_string(ring)
    }
}

fn orient_polygon(polygon: &Polygon<f64>, orientation: Orientation) -> Polygon<f64> {
    if polygon
        .rings()
        .first()
        .is_none_or(|ring| ring.coords().is_empty())
    {
        return polygon.clone();
    }

    Polygon::new(
        polygon
            .rings()
            .iter()
            .enumerate()
            .map(|(index, ring)| {
                let orientation = if index == 0 {
                    orientation
                } else {
                    orientation.opposite()
                };
                orient_ring(ring, orientation)
            })
            .collect(),
        polygon.dimension(),
    )
}

fn force_polygon_cw(geometry: &Wkt<f64>) -> Wkt<f64> {
    match geometry {
        Wkt::Polygon(polygon) => Wkt::Polygon(orient_polygon(polygon, Orientation::Clockwise)),
        Wkt::MultiPolygon(multi_polygon) => Wkt::MultiPolygon(MultiPolygon::new(
            multi_polygon
                .polygons()
                .iter()
                .map(|polygon| orient_polygon(polygon, Orientation::Clockwise))
                .collect(),
            multi_polygon.dimension(),
        )),
        Wkt::GeometryCollection(collection) => Wkt::GeometryCollection(GeometryCollection::new(
            collection
                .geometries()
                .iter()
                .map(force_polygon_cw)
                .collect(),
            collection.dimension(),
        )),
        _ => geometry.clone(),
    }
}

fn reverse_polygon_rings(geometry: &Wkt<f64>) -> Wkt<f64> {
    match geometry {
        Wkt::Polygon(polygon) => Wkt::Polygon(Polygon::new(
            polygon.rings().iter().map(reverse_line_string).collect(),
            polygon.dimension(),
        )),
        Wkt::MultiPolygon(multi_polygon) => Wkt::MultiPolygon(MultiPolygon::new(
            multi_polygon
                .polygons()
                .iter()
                .map(|polygon| {
                    Polygon::new(
                        polygon.rings().iter().map(reverse_line_string).collect(),
                        polygon.dimension(),
                    )
                })
                .collect(),
            multi_polygon.dimension(),
        )),
        _ => geometry.clone(),
    }
}

/// PostGIS 3.6 implements `ST_ForcePolygonCCW` as
/// `ST_Reverse(ST_ForcePolygonCW(geom))` for Polygon and MultiPolygon inputs.
/// Every other input type is returned unchanged.
fn force_polygon_ccw(geometry: &Wkt<f64>) -> Wkt<f64> {
    match geometry {
        Wkt::Polygon(_) | Wkt::MultiPolygon(_) => {
            reverse_polygon_rings(&force_polygon_cw(geometry))
        }
        _ => geometry.clone(),
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SfsVersion {
    V11,
    V12,
}

fn parse_sfs_version(version: &str) -> SfsVersion {
    if version.starts_with("1.2") {
        SfsVersion::V12
    } else {
        SfsVersion::V11
    }
}

fn force_sfs(geometry: &Wkt<f64>, version: SfsVersion) -> Wkt<f64> {
    match version {
        // Curves, TRIANGLE, TIN, and POLYHEDRALSURFACE are absent from the
        // GeoArrow generic geometry union, so both versions reduce to
        // recursive collection traversal for every representable input.
        SfsVersion::V11 | SfsVersion::V12 => force_sfs_geometry(geometry),
    }
}

fn force_sfs_geometry(geometry: &Wkt<f64>) -> Wkt<f64> {
    match geometry {
        Wkt::GeometryCollection(collection) => Wkt::GeometryCollection(GeometryCollection::new(
            collection
                .geometries()
                .iter()
                .map(force_sfs_geometry)
                .collect(),
            collection.dimension(),
        )),
        _ => geometry.clone(),
    }
}

fn generic_return_type(function_name: &str) -> Result<DataType> {
    Err(DataFusionError::Internal(format!(
        "{function_name} requires return_field_from_args"
    )))
}

fn generic_return_field(args: ReturnFieldArgs) -> GeoDataFusionResult<FieldRef> {
    let input_field = args.arg_fields[0].as_ref();
    let input_type = GeoArrowType::from_arrow_field(input_field)?;
    let output_type = GeoArrowType::Geometry(
        GeometryType::new(input_type.metadata().clone())
            .with_coord_type(input_type.coord_type().unwrap_or(CoordType::Interleaved)),
    );
    Ok(Arc::new(
        output_type.to_field(input_field.name(), input_field.is_nullable()),
    ))
}

fn invoke_edit<F>(args: ScalarFunctionArgs, operation: F) -> GeoDataFusionResult<ColumnarValue>
where
    F: Fn(usize, &[ArrayRef], &Wkt<f64>) -> GeoDataFusionResult<Option<Wkt<f64>>>,
{
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geometry = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let edited = map_geometry_array(geometry.as_ref(), &output_type, &|index, geometry| {
        operation(index, arrays.as_slice(), geometry)
    })?;
    Ok(ColumnarValue::Array(edited))
}

fn map_geometry_array<F>(
    array: &dyn GeoArrowArray,
    output_type: &GeoArrowType,
    operation: &F,
) -> GeoDataFusionResult<ArrayRef>
where
    F: Fn(usize, &Wkt<f64>) -> GeoDataFusionResult<Option<Wkt<f64>>>,
{
    downcast_geoarrow_array!(array, map_geometry_array_impl, output_type, operation)
}

fn map_geometry_array_impl<'a, F>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeoArrowType,
    operation: &F,
) -> GeoDataFusionResult<ArrayRef>
where
    F: Fn(usize, &Wkt<f64>) -> GeoDataFusionResult<Option<Wkt<f64>>>,
{
    let GeoArrowType::Geometry(typ) = output_type else {
        return Err(DataFusionError::Internal(format!(
            "collection editors require a generic Geometry return type, got {output_type:?}"
        ))
        .into());
    };

    let mut builder = StrictGeometryBuilder::new(typ.clone());
    for (index, geometry) in array.iter().enumerate() {
        let geometry = geometry.transpose()?;
        let Some(geometry) = geometry else {
            builder.push_geometry(None::<&Wkt<f64>>)?;
            continue;
        };
        let geometry = geometry_to_wkt(&geometry)?;
        match operation(index, &geometry)? {
            Some(edited) => builder.push_geometry(Some(&edited))?,
            None => builder.push_geometry(None::<&Wkt<f64>>)?,
        }
    }
    Ok(builder.finish().to_array_ref())
}

fn integer_value(array: &dyn Array, index: usize) -> GeoDataFusionResult<i32> {
    match array.data_type() {
        DataType::Int32 => Ok(array.as_primitive::<Int32Type>().value(index)),
        DataType::Int64 => {
            let value = array.as_primitive::<Int64Type>().value(index);
            i32::try_from(value)
                .map_err(|_| DataFusionError::Execution(EXTRACT_TYPE_ERROR.into()).into())
        }
        data_type => Err(DataFusionError::Internal(format!(
            "ST_CollectionExtract expected an integer type argument, got {data_type}"
        ))
        .into()),
    }
}

fn text_value(array: &dyn Array, index: usize) -> GeoDataFusionResult<&str> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().value(index)),
        DataType::LargeUtf8 => Ok(array.as_string::<i64>().value(index)),
        DataType::Utf8View => Ok(array.as_string_view().value(index)),
        data_type => Err(DataFusionError::Internal(format!(
            "ST_ForceSFS expected a text version argument, got {data_type}"
        ))
        .into()),
    }
}

fn extract_type_from_code(code: i32) -> GeoDataFusionResult<Option<PrimitiveKind>> {
    match code {
        0 => Ok(None),
        1 => Ok(Some(PrimitiveKind::Point)),
        2 => Ok(Some(PrimitiveKind::LineString)),
        3 => Ok(Some(PrimitiveKind::Polygon)),
        _ => Err(DataFusionError::Execution(EXTRACT_TYPE_ERROR.into()).into()),
    }
}

fn optional_integer_signature() -> Signature {
    let geometry_types = any_geometry_type();
    let mut signatures = vec![TypeSignature::Uniform(1, geometry_types.clone())];
    for geometry in geometry_types {
        signatures.push(TypeSignature::Exact(vec![
            geometry.clone(),
            DataType::Int32,
        ]));
        signatures.push(TypeSignature::Exact(vec![geometry, DataType::Int64]));
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

fn optional_text_signature() -> Signature {
    let geometry_types = any_geometry_type();
    let mut signatures = vec![TypeSignature::Uniform(1, geometry_types.clone())];
    for geometry in geometry_types {
        for text_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            signatures.push(TypeSignature::Exact(vec![geometry.clone(), text_type]));
        }
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CollectionExtract {
    signature: Signature,
}

impl CollectionExtract {
    pub fn new() -> Self {
        Self {
            signature: optional_integer_signature(),
        }
    }
}

impl Default for CollectionExtract {
    fn default() -> Self {
        Self::new()
    }
}

static COLLECTION_EXTRACT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for CollectionExtract {
    fn name(&self) -> &str {
        "st_collectionextract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        generic_return_type("ST_CollectionExtract")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(generic_return_field(args)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_edit(args, |index, arrays, geometry| {
            let Some(type_array) = arrays.get(1) else {
                return Ok(Some(collection_extract(geometry, None)));
            };
            if type_array.is_null(index) {
                return Ok(None);
            }
            let requested = extract_type_from_code(integer_value(type_array, index)?)?;
            Ok(Some(collection_extract(geometry, requested)))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(COLLECTION_EXTRACT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a multi-geometry containing only elements of the requested type. The type argument accepts 1 for POINT, 2 for LINESTRING, and 3 for POLYGON; when omitted, the highest-dimension element type is used.",
                "ST_CollectionExtract(collection[, type])",
            )
            .with_argument("collection", "geometry collection")
            .with_argument("type", "1 for POINT, 2 for LINESTRING, or 3 for POLYGON")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CollectionHomogenize;

impl CollectionHomogenize {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CollectionHomogenize {
    fn default() -> Self {
        Self::new()
    }
}

static COLLECTION_HOMOGENIZE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for CollectionHomogenize {
    fn name(&self) -> &str {
        "st_collectionhomogenize"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        generic_return_type("ST_CollectionHomogenize")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(generic_return_field(args)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_edit(args, |_, _, geometry| {
            Ok(Some(collection_homogenize(geometry)))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(COLLECTION_HOMOGENIZE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the simplest representation of a geometry collection.",
                "ST_CollectionHomogenize(collection)",
            )
            .with_argument("collection", "geometry collection")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ForceCollection;

impl ForceCollection {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ForceCollection {
    fn default() -> Self {
        Self::new()
    }
}

static FORCE_COLLECTION_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ForceCollection {
    fn name(&self) -> &str {
        "st_forcecollection"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        generic_return_type("ST_ForceCollection")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(generic_return_field(args)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_edit(args, |_, _, geometry| {
            Ok(Some(force_collection(geometry)))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_COLLECTION_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Converts a geometry into a GEOMETRYCOLLECTION.",
                "ST_ForceCollection(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Multi;

impl Multi {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Multi {
    fn default() -> Self {
        Self::new()
    }
}

static MULTI_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Multi {
    fn name(&self) -> &str {
        "st_multi"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        generic_return_type("ST_Multi")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(generic_return_field(args)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_edit(args, |_, _, geometry| {
            Ok(Some(force_multi(geometry)))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MULTI_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the geometry as a MULTI* geometry. Existing multi-geometries and geometry collections are returned unchanged.",
                "ST_Multi(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

macro_rules! polygon_orientation_udf {
    ($name:ident, $doc_name:ident, $udf_name:literal, $description:literal, $usage:literal, $operation:expr) => {
        #[derive(Debug, Eq, PartialEq, Hash)]
        pub struct $name;

        impl $name {
            pub fn new() -> Self {
                Self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        static $doc_name: OnceLock<Documentation> = OnceLock::new();

        impl ScalarUDFImpl for $name {
            fn name(&self) -> &str {
                $udf_name
            }

            fn signature(&self) -> &Signature {
                any_single_geometry_type_input()
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                generic_return_type($udf_name)
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                Ok(generic_return_field(args)?)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                Ok(invoke_edit(args, |_, _, geometry| {
                    Ok(Some($operation(geometry)))
                })?)
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some($doc_name.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $description, $usage)
                        .with_argument("geometry", "geometry")
                        .build()
                }))
            }
        }
    };
}

polygon_orientation_udf!(
    ForcePolygonCW,
    FORCE_POLYGON_CW_DOC,
    "st_forcepolygoncw",
    "Orients Polygon and MultiPolygon exterior rings clockwise and interior rings counter-clockwise.",
    "ST_ForcePolygonCW(geometry)",
    force_polygon_cw
);

polygon_orientation_udf!(
    ForcePolygonCCW,
    FORCE_POLYGON_CCW_DOC,
    "st_forcepolygonccw",
    "Orients Polygon and MultiPolygon exterior rings counter-clockwise and interior rings clockwise.",
    "ST_ForcePolygonCCW(geometry)",
    force_polygon_ccw
);

polygon_orientation_udf!(
    ForceRHR,
    FORCE_RHR_DOC,
    "st_forcerhr",
    "Forces polygon rings to follow the Right-Hand Rule. This function is a synonym for ST_ForcePolygonCW.",
    "ST_ForceRHR(geometry)",
    force_polygon_cw
);

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ForceSFS {
    signature: Signature,
}

impl ForceSFS {
    pub fn new() -> Self {
        Self {
            signature: optional_text_signature(),
        }
    }
}

impl Default for ForceSFS {
    fn default() -> Self {
        Self::new()
    }
}

static FORCE_SFS_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ForceSFS {
    fn name(&self) -> &str {
        "st_forcesfs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        generic_return_type("ST_ForceSFS")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(generic_return_field(args)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_edit(args, |index, arrays, geometry| {
            let Some(version_array) = arrays.get(1) else {
                return Ok(Some(force_sfs(geometry, SfsVersion::V11)));
            };
            if version_array.is_null(index) {
                return Ok(None);
            }
            let version = parse_sfs_version(text_value(version_array, index)?);
            Ok(Some(force_sfs(geometry, version)))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_SFS_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces geometries to use SFS 1.1 geometry types. A version beginning with 1.2 preserves SFS 1.2 types; other values use SFS 1.1.",
                "ST_ForceSFS(geometry[, version])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("version", "SFS version, for example 1.1 or 1.2")
            .build()
        }))
    }
}
