//! Coordinate-level geometry editors.
//!
//! These editors reuse the shared WKT edit pipeline and GeoArrow builders.
//! PostGIS semantics for offsets, dimensions, and NULL arguments are kept in
//! the public UDF implementations, while WKT helpers stay independent from
//! the Arrow layer.

use std::sync::OnceLock;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::CoordTrait;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

use super::transform::{
    GeometryEdit, OutputKind, integer_value, invoke_with_edits, return_field_for,
};
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

#[derive(Clone, Copy)]
enum ArgRole {
    Geometry,
    Float,
    Integer,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineExtend {
    signature: Signature,
}

impl LineExtend {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for LineExtend {
    fn default() -> Self {
        Self::new()
    }
}

static LINE_EXTEND_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineExtend {
    fn name(&self) -> &str {
        "st_lineextend"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let roles: &[ArgRole] = match arg_types.len() {
            2 => &[ArgRole::Geometry, ArgRole::Float],
            3 => &[ArgRole::Geometry, ArgRole::Float, ArgRole::Float],
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "ST_LineExtend does not accept {} arguments",
                    arg_types.len()
                )));
            }
        };
        coerce_types("ST_LineExtend", arg_types, roles)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_LineExtend requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::LineString)?)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        add_default_float_arg(&mut args, 2);
        Ok(invoke_with_edits(
            args,
            OutputKind::LineString,
            |index, arrays| {
                if any_null(arrays, index) {
                    return Ok(None);
                }
                Ok(Some(GeometryEdit::LineExtend {
                    forward: float_value(&arrays[1], index)?.expect("non-null forward"),
                    backward: float_value(&arrays[2], index)?.expect("non-null backward"),
                }))
            },
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_EXTEND_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Extends a LineString forward and backward by the given distances.",
                "ST_LineExtend(linestring, distance_forward[, distance_backward])",
            )
            .with_argument("linestring", "LineString geometry")
            .with_argument("distance_forward", "distance to extend past the last point")
            .with_argument(
                "distance_backward",
                "optional distance to extend before the first point",
            )
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct QuantizeCoordinates {
    signature: Signature,
}

impl QuantizeCoordinates {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for QuantizeCoordinates {
    fn default() -> Self {
        Self::new()
    }
}

static QUANTIZE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for QuantizeCoordinates {
    fn name(&self) -> &str {
        "st_quantizecoordinates"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=5).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(format!(
                "ST_QuantizeCoordinates does not accept {} arguments",
                arg_types.len()
            )));
        }
        let roles = [
            ArgRole::Geometry,
            ArgRole::Integer,
            ArgRole::Integer,
            ArgRole::Integer,
            ArgRole::Integer,
        ];
        coerce_types(
            "ST_QuantizeCoordinates",
            arg_types,
            &roles[..arg_types.len()],
        )
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_QuantizeCoordinates requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::Preserve)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_with_edits(
            args,
            OutputKind::Preserve,
            |index, arrays| {
                if value_is_null(&arrays[0], index) {
                    return Ok(None);
                }
                let Some(prec_x) = integer_value(&arrays[1], index)? else {
                    return Err(
                        DataFusionError::Execution("Must specify precision".to_string()).into(),
                    );
                };
                let precision_for = |position: usize| -> GeoDataFusionResult<i32> {
                    if arrays.len() <= position {
                        return Ok(prec_x as i32);
                    }
                    match integer_value(&arrays[position], index)? {
                        Some(value) => Ok(value as i32),
                        None => Ok(prec_x as i32),
                    }
                };
                Ok(Some(GeometryEdit::Quantize {
                    precision: [
                        prec_x as i32,
                        precision_for(2)?,
                        precision_for(3)?,
                        precision_for(4)?,
                    ],
                }))
            },
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(QUANTIZE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Sets the least significant bits of the coordinates to zero while preserving the requested decimal precision.",
                "ST_QuantizeCoordinates(geometry, prec_x[, prec_y[, prec_z[, prec_m]]])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("prec_x", "decimal digits to preserve for X")
            .with_argument("prec_y", "optional decimal digits for Y, defaults to prec_x")
            .with_argument("prec_z", "optional decimal digits for Z, defaults to prec_x")
            .with_argument("prec_m", "optional decimal digits for M, defaults to prec_x")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct RemoveRepeatedPoints {
    signature: Signature,
}

impl RemoveRepeatedPoints {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RemoveRepeatedPoints {
    fn default() -> Self {
        Self::new()
    }
}

static REMOVE_REPEATED_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for RemoveRepeatedPoints {
    fn name(&self) -> &str {
        "st_removerepeatedpoints"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let roles: &[ArgRole] = match arg_types.len() {
            1 => &[ArgRole::Geometry],
            2 => &[ArgRole::Geometry, ArgRole::Float],
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "ST_RemoveRepeatedPoints does not accept {} arguments",
                    arg_types.len()
                )));
            }
        };
        coerce_types("ST_RemoveRepeatedPoints", arg_types, roles)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_RemoveRepeatedPoints requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::Preserve)?)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        add_default_float_arg(&mut args, 1);
        Ok(invoke_with_edits(
            args,
            OutputKind::Preserve,
            |index, arrays| {
                if any_null(arrays, index) {
                    return Ok(None);
                }
                Ok(Some(GeometryEdit::RemoveRepeatedPoints {
                    tolerance: float_value(&arrays[1], index)?.expect("non-null tolerance"),
                }))
            },
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(REMOVE_REPEATED_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Removes consecutive repeated points within the given tolerance.",
                "ST_RemoveRepeatedPoints(geometry[, tolerance])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("tolerance", "optional distance tolerance, default 0")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Segmentize {
    signature: Signature,
}

impl Segmentize {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Segmentize {
    fn default() -> Self {
        Self::new()
    }
}

static SEGMENTIZE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Segmentize {
    fn name(&self) -> &str {
        "st_segmentize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_types(
            "ST_Segmentize",
            arg_types,
            &[ArgRole::Geometry, ArgRole::Float],
        )
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Segmentize requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::Preserve)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_with_edits(
            args,
            OutputKind::Preserve,
            |index, arrays| {
                if any_null(arrays, index) {
                    return Ok(None);
                }
                Ok(Some(GeometryEdit::Segmentize {
                    max_length: float_value(&arrays[1], index)?.expect("non-null max_length"),
                }))
            },
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SEGMENTIZE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a geometry with no segment longer than the given 2D distance.",
                "ST_Segmentize(geometry, max_segment_length)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("max_segment_length", "maximum 2D segment length")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ShiftLongitude {
    signature: Signature,
}

impl ShiftLongitude {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for ShiftLongitude {
    fn default() -> Self {
        Self::new()
    }
}

static SHIFT_LONGITUDE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ShiftLongitude {
    fn name(&self) -> &str {
        "st_shiftlongitude"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_types("ST_ShiftLongitude", arg_types, &[ArgRole::Geometry])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_ShiftLongitude requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::Preserve)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_with_edits(
            args,
            OutputKind::Preserve,
            |index, arrays| {
                if value_is_null(&arrays[0], index) {
                    return Ok(None);
                }
                Ok(Some(GeometryEdit::ShiftLongitude))
            },
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SHIFT_LONGITUDE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Shifts longitudes between -180..180 and 0..360.",
                "ST_ShiftLongitude(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct WrapX {
    signature: Signature,
}

impl WrapX {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for WrapX {
    fn default() -> Self {
        Self::new()
    }
}

static WRAP_X_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for WrapX {
    fn name(&self) -> &str {
        "st_wrapx"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_types(
            "ST_WrapX",
            arg_types,
            &[ArgRole::Geometry, ArgRole::Float, ArgRole::Float],
        )
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_WrapX requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::Geometry)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_with_edits(
            args,
            OutputKind::Geometry,
            |index, arrays| {
                if any_null(arrays, index) {
                    return Ok(None);
                }
                Ok(Some(GeometryEdit::WrapX {
                    cut: float_value(&arrays[1], index)?.expect("non-null wrap"),
                    amount: float_value(&arrays[2], index)?.expect("non-null move"),
                }))
            },
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(WRAP_X_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Wraps a geometry around the given X value by the given amount. Splitting a crossing polygon requires the geos-3_11 feature because the split is performed through GEOS, like PostGIS.",
                "ST_WrapX(geometry, wrap, move)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("wrap", "X value used as the wrapping cut")
            .with_argument("move", "amount to move the wrapped part")
            .build()
        }))
    }
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(LineExtend::new().into());
    session_context.register_udf(QuantizeCoordinates::new().into());
    session_context.register_udf(RemoveRepeatedPoints::new().into());
    session_context.register_udf(Segmentize::new().into());
    session_context.register_udf(ShiftLongitude::new().into());
    session_context.register_udf(WrapX::new().into());
}

fn coerce_types(name: &str, arg_types: &[DataType], roles: &[ArgRole]) -> Result<Vec<DataType>> {
    if arg_types.len() != roles.len() {
        return Err(DataFusionError::Plan(format!(
            "{name} does not accept {} arguments",
            arg_types.len()
        )));
    }

    arg_types
        .iter()
        .zip(roles)
        .map(|(data_type, role)| match role {
            ArgRole::Geometry => {
                if *data_type == DataType::Null || is_supported_geometry_type(data_type) {
                    Ok(data_type.clone())
                } else {
                    Err(DataFusionError::Plan(format!(
                        "{name} does not support argument type {data_type}"
                    )))
                }
            }
            ArgRole::Float => match data_type {
                DataType::Null
                | DataType::Float32
                | DataType::Float64
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => Ok(DataType::Float64),
                other => Err(DataFusionError::Plan(format!(
                    "{name} numeric arguments must be numbers, got {other}"
                ))),
            },
            ArgRole::Integer => match data_type {
                DataType::Null | DataType::Int32 | DataType::Int64 => Ok(match data_type {
                    DataType::Null => DataType::Int32,
                    other => other.clone(),
                }),
                other => Err(DataFusionError::Plan(format!(
                    "{name} integer arguments must be integers, got {other}"
                ))),
            },
        })
        .collect()
}

fn is_supported_geometry_type(data_type: &DataType) -> bool {
    any_geometry_type()
        .iter()
        .any(|candidate| candidate == data_type)
}

fn add_default_float_arg(args: &mut ScalarFunctionArgs, index: usize) {
    if args.args.len() == index {
        args.args.push(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Float64(Some(0.0)),
        ));
    }
}

fn any_null(arrays: &[ArrayRef], index: usize) -> bool {
    arrays.iter().any(|array| value_is_null(array, index))
}

fn value_is_null(array: &ArrayRef, index: usize) -> bool {
    array.data_type() == &DataType::Null || array.is_null(index)
}

fn float_value(array: &ArrayRef, index: usize) -> GeoDataFusionResult<Option<f64>> {
    match array.data_type() {
        DataType::Null => Ok(None),
        DataType::Float64 => {
            let values = array.as_primitive::<Float64Type>();
            if values.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(values.value(index)))
            }
        }
        data_type => Err(DataFusionError::Execution(format!(
            "expected a floating point argument, got {data_type}"
        ))
        .into()),
    }
}

pub(crate) fn edit_line_extend(
    geometry: Wkt<f64>,
    forward: f64,
    backward: f64,
) -> GeoArrowResult<Option<Wkt<f64>>> {
    let Wkt::LineString(line) = geometry else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Argument must be LINESTRING geometry".to_string(),
        ));
    };
    let (coords, dimension) = line.into_inner();
    if coords.is_empty() {
        return Ok(None);
    }
    if line_length_2d(&coords) <= 0.0 {
        return Ok(Some(Wkt::LineString(LineString::new(coords, dimension))));
    }
    if forward < 0.0 || backward < 0.0 {
        return Err(GeoArrowError::InvalidGeoArrow(
            "lwline_extend: distances must be non-negative".to_string(),
        ));
    }

    let mut extended = Vec::with_capacity(coords.len() + 2);
    if backward > 0.0 {
        let index = first_distinct_from_start(&coords, dimension).ok_or_else(|| {
            GeoArrowError::InvalidGeoArrow(
                "lwline_extend: line must have at least two distinct points".to_string(),
            )
        })?;
        extended.push(project_pt_pt(
            &coords[index],
            &coords[0],
            backward,
            dimension,
        ));
    }
    extended.extend(coords.iter().cloned());
    if forward > 0.0 {
        let index = first_distinct_from_end(&coords, dimension).ok_or_else(|| {
            GeoArrowError::InvalidGeoArrow(
                "lwline_extend: line must have at least two distinct points".to_string(),
            )
        })?;
        let last = coords.len() - 1;
        extended.push(project_pt_pt(
            &coords[index],
            &coords[last],
            forward,
            dimension,
        ));
    }

    Ok(Some(Wkt::LineString(LineString::new(extended, dimension))))
}

pub(crate) fn edit_quantize(geometry: Wkt<f64>, precision: [i32; 4]) -> Wkt<f64> {
    map_geometry_coords(geometry, &|coord, dimension| {
        let mut out = *coord;
        out.x = trim_preserve_decimal_digits(coord.x, precision[0]);
        out.y = trim_preserve_decimal_digits(coord.y, precision[1]);
        if has_z(dimension) {
            out.z = coord
                .z
                .map(|value| trim_preserve_decimal_digits(value, precision[2]));
        }
        if has_m(dimension) {
            out.m = coord
                .m
                .map(|value| trim_preserve_decimal_digits(value, precision[3]));
        }
        out
    })
}

pub(crate) fn edit_shift_longitude(geometry: Wkt<f64>) -> Wkt<f64> {
    map_geometry_coords(geometry, &|coord, _| Coord {
        x: shift_longitude(coord.x),
        y: coord.y,
        z: coord.z,
        m: coord.m,
    })
}

pub(crate) fn edit_remove_repeated_points(geometry: Wkt<f64>, tolerance: f64) -> Wkt<f64> {
    match geometry {
        Wkt::Point(point) => Wkt::Point(point),
        Wkt::LineString(line_string) => {
            let (coords, dimension) = line_string.into_inner();
            Wkt::LineString(LineString::new(
                remove_repeated_coords(&coords, dimension, tolerance, 2),
                dimension,
            ))
        }
        Wkt::Polygon(polygon) => {
            let (rings, dimension) = polygon.into_inner();
            let rings = rings
                .into_iter()
                .filter_map(|ring| {
                    let (coords, _) = ring.into_inner();
                    let coords = remove_repeated_coords(&coords, dimension, tolerance, 4);
                    (coords.len() >= 4).then(|| LineString::new(coords, dimension))
                })
                .collect::<Vec<_>>();
            Wkt::Polygon(Polygon::new(rings, dimension))
        }
        Wkt::MultiPoint(multi_point) => {
            let (points, dimension) = multi_point.into_inner();
            Wkt::MultiPoint(MultiPoint::new(
                remove_repeated_points_from_multipoint(points, tolerance),
                dimension,
            ))
        }
        Wkt::MultiLineString(multi_line_string) => {
            let (lines, dimension) = multi_line_string.into_inner();
            let lines = lines
                .into_iter()
                .filter_map(|line| {
                    let (coords, _) = line.into_inner();
                    let coords = remove_repeated_coords(&coords, dimension, tolerance, 2);
                    (!coords.is_empty()).then(|| LineString::new(coords, dimension))
                })
                .collect::<Vec<_>>();
            Wkt::MultiLineString(MultiLineString::new(lines, dimension))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            let polygons = polygons
                .into_iter()
                .filter_map(|polygon| {
                    let (rings, _) = polygon.into_inner();
                    let rings = rings
                        .into_iter()
                        .filter_map(|ring| {
                            let (coords, _) = ring.into_inner();
                            let coords = remove_repeated_coords(&coords, dimension, tolerance, 4);
                            (coords.len() >= 4).then(|| LineString::new(coords, dimension))
                        })
                        .collect::<Vec<_>>();
                    (!rings.is_empty()).then(|| Polygon::new(rings, dimension))
                })
                .collect::<Vec<_>>();
            Wkt::MultiPolygon(MultiPolygon::new(polygons, dimension))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            let geometries = geometries
                .into_iter()
                .filter_map(|geometry| {
                    let geometry = edit_remove_repeated_points(geometry, tolerance);
                    (!wkt_is_empty(&geometry)).then_some(geometry)
                })
                .collect::<Vec<_>>();
            Wkt::GeometryCollection(GeometryCollection::new(geometries, dimension))
        }
    }
}

pub(crate) fn edit_segmentize(
    geometry: Wkt<f64>,
    max_length: f64,
) -> GeoArrowResult<Option<Wkt<f64>>> {
    match &geometry {
        Wkt::Point(_) | Wkt::MultiPoint(_) => return Ok(Some(geometry)),
        _ => {}
    }
    if max_length.is_nan() || max_length <= 0.0 {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "ST_Segmentize: invalid max_distance {max_length:?} (must be >= 0)"
        )));
    }
    if wkt_is_empty(&geometry) {
        return Ok(Some(geometry));
    }

    let geometry = segmentize_wkt(geometry, max_length)?;
    Ok(Some(geometry))
}

pub(crate) fn edit_wrap_x(geometry: Wkt<f64>, cut: f64, amount: f64) -> GeoArrowResult<Wkt<f64>> {
    if wkt_is_empty(&geometry) || amount == 0.0 {
        return Ok(geometry);
    }

    match geometry {
        Wkt::Point(point) => wrap_point(point, cut, amount).map(Wkt::Point),
        Wkt::MultiPoint(multi_point) => {
            let (points, dimension) = multi_point.into_inner();
            let points = points
                .into_iter()
                .map(|point| wrap_point(point, cut, amount))
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Ok(Wkt::MultiPoint(MultiPoint::new(points, dimension)))
        }
        Wkt::LineString(line_string) => wrap_line_string(line_string, cut, amount),
        Wkt::MultiLineString(multi_line_string) => {
            let (lines, dimension) = multi_line_string.into_inner();
            let mut wrapped = Vec::with_capacity(lines.len());
            let mut promoted = false;
            for line in lines {
                let line = wrap_line_string(line, cut, amount)?;
                if matches!(line, Wkt::MultiLineString(_)) {
                    promoted = true;
                }
                wrapped.push(line);
            }
            if promoted {
                Ok(Wkt::GeometryCollection(GeometryCollection::new(
                    wrapped, dimension,
                )))
            } else {
                let lines = wrapped
                    .into_iter()
                    .map(|line| match line {
                        Wkt::LineString(line) => line,
                        _ => unreachable!("promotion handled above"),
                    })
                    .collect();
                Ok(Wkt::MultiLineString(MultiLineString::new(lines, dimension)))
            }
        }
        Wkt::Polygon(polygon) => wrap_polygon(polygon, cut, amount),
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            let mut wrapped = Vec::with_capacity(polygons.len());
            let mut promoted = false;
            for polygon in polygons {
                let polygon = wrap_polygon(polygon, cut, amount)?;
                if matches!(polygon, Wkt::MultiPolygon(_)) {
                    promoted = true;
                }
                wrapped.push(polygon);
            }
            if promoted {
                Ok(Wkt::GeometryCollection(GeometryCollection::new(
                    wrapped, dimension,
                )))
            } else {
                let polygons = wrapped
                    .into_iter()
                    .map(|polygon| match polygon {
                        Wkt::Polygon(polygon) => polygon,
                        _ => unreachable!("promotion handled above"),
                    })
                    .collect();
                Ok(Wkt::MultiPolygon(MultiPolygon::new(polygons, dimension)))
            }
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            let geometries = geometries
                .into_iter()
                .map(|geometry| edit_wrap_x(geometry, cut, amount))
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Ok(Wkt::GeometryCollection(GeometryCollection::new(
                geometries, dimension,
            )))
        }
    }
}

pub(crate) fn quantize_coord(
    coord: &impl CoordTrait<T = f64>,
    dimension: geo_traits::Dimensions,
    precision: [i32; 4],
) -> GeoArrowResult<Coord<f64>> {
    Ok(Coord {
        x: trim_preserve_decimal_digits(coord.x(), precision[0]),
        y: trim_preserve_decimal_digits(coord.y(), precision[1]),
        z: has_z_dimensions(dimension)
            .then(|| trim_preserve_decimal_digits(coord.nth(2).unwrap_or(0.0), precision[2])),
        m: has_m_dimensions(dimension)
            .then(|| trim_preserve_decimal_digits(coord.nth(3).unwrap_or(0.0), precision[3])),
    })
}

pub(crate) fn shift_longitude_coord(
    coord: &impl CoordTrait<T = f64>,
    dimension: geo_traits::Dimensions,
) -> GeoArrowResult<Coord<f64>> {
    Ok(Coord {
        x: shift_longitude(coord.x()),
        y: coord.y(),
        z: has_z_dimensions(dimension).then(|| coord.nth(2).unwrap_or(0.0)),
        m: has_m_dimensions(dimension).then(|| coord.nth(3).unwrap_or(0.0)),
    })
}

fn shift_longitude(x: f64) -> f64 {
    if x < 0.0 {
        x + 360.0
    } else if x > 180.0 {
        x - 360.0
    } else {
        x
    }
}

fn trim_preserve_decimal_digits(value: f64, decimal_digits: i32) -> f64 {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 2047) as i32 - 1023;
    let mut bits_needed = 1 + exponent + ((i64::from(decimal_digits) * 851 + 255) / 256) as i32;
    if decimal_digits < 0 {
        bits_needed -= 1;
    }
    if bits_needed >= 52 {
        return value;
    }
    if bits_needed < 1 {
        bits_needed = 1;
    }
    let mask = 0xffff_ffff_ffff_ffffu64 << (52 - bits_needed);
    f64::from_bits(bits & mask)
}

fn map_geometry_coords(
    geometry: Wkt<f64>,
    map: &impl Fn(&Coord<f64>, WktDimension) -> Coord<f64>,
) -> Wkt<f64> {
    match geometry {
        Wkt::Point(point) => {
            let (coord, dimension) = point.into_inner();
            Wkt::Point(Point::new(
                coord.map(|coord| map(&coord, dimension)),
                dimension,
            ))
        }
        Wkt::LineString(line_string) => {
            let (coords, dimension) = line_string.into_inner();
            Wkt::LineString(LineString::new(
                map_coords(&coords, dimension, map),
                dimension,
            ))
        }
        Wkt::Polygon(polygon) => {
            let (rings, dimension) = polygon.into_inner();
            let rings = rings
                .into_iter()
                .map(|ring| {
                    let (coords, _) = ring.into_inner();
                    LineString::new(map_coords(&coords, dimension, map), dimension)
                })
                .collect();
            Wkt::Polygon(Polygon::new(rings, dimension))
        }
        Wkt::MultiPoint(multi_point) => {
            let (points, dimension) = multi_point.into_inner();
            let points = points
                .into_iter()
                .map(|point| {
                    let (coord, _) = point.into_inner();
                    Point::new(coord.map(|coord| map(&coord, dimension)), dimension)
                })
                .collect();
            Wkt::MultiPoint(MultiPoint::new(points, dimension))
        }
        Wkt::MultiLineString(multi_line_string) => {
            let (lines, dimension) = multi_line_string.into_inner();
            let lines = lines
                .into_iter()
                .map(|line| {
                    let (coords, _) = line.into_inner();
                    LineString::new(map_coords(&coords, dimension, map), dimension)
                })
                .collect();
            Wkt::MultiLineString(MultiLineString::new(lines, dimension))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            let polygons = polygons
                .into_iter()
                .map(|polygon| {
                    let (rings, _) = polygon.into_inner();
                    Polygon::new(
                        rings
                            .into_iter()
                            .map(|ring| {
                                let (coords, _) = ring.into_inner();
                                LineString::new(map_coords(&coords, dimension, map), dimension)
                            })
                            .collect(),
                        dimension,
                    )
                })
                .collect();
            Wkt::MultiPolygon(MultiPolygon::new(polygons, dimension))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            Wkt::GeometryCollection(GeometryCollection::new(
                geometries
                    .into_iter()
                    .map(|geometry| map_geometry_coords(geometry, map))
                    .collect(),
                dimension,
            ))
        }
    }
}

fn map_coords(
    coords: &[Coord<f64>],
    dimension: WktDimension,
    map: &impl Fn(&Coord<f64>, WktDimension) -> Coord<f64>,
) -> Vec<Coord<f64>> {
    coords.iter().map(|coord| map(coord, dimension)).collect()
}

fn line_length_2d(coords: &[Coord<f64>]) -> f64 {
    coords
        .windows(2)
        .map(|pair| distance_sqr_2d(&pair[0], &pair[1]).sqrt())
        .sum()
}

fn distance_sqr_2d(first: &Coord<f64>, second: &Coord<f64>) -> f64 {
    let dx = first.x - second.x;
    let dy = first.y - second.y;
    dx * dx + dy * dy
}

fn first_distinct_from_start(coords: &[Coord<f64>], dimension: WktDimension) -> Option<usize> {
    let first = &coords[0];
    (1..coords.len()).find(|&index| !p4d_same(first, &coords[index], dimension))
}

fn first_distinct_from_end(coords: &[Coord<f64>], dimension: WktDimension) -> Option<usize> {
    let last = &coords[coords.len() - 1];
    (0..coords.len() - 1)
        .rev()
        .find(|&index| !p4d_same(last, &coords[index], dimension))
}

fn p4d_same(first: &Coord<f64>, second: &Coord<f64>, dimension: WktDimension) -> bool {
    coord_slots(first, dimension) == coord_slots(second, dimension)
}

fn coord_slots(coord: &Coord<f64>, dimension: WktDimension) -> [u64; 4] {
    [
        coord.x.to_bits(),
        coord.y.to_bits(),
        if has_z(dimension) {
            coord.z.unwrap_or(0.0).to_bits()
        } else {
            0.0f64.to_bits()
        },
        if has_m(dimension) {
            coord.m.unwrap_or(0.0).to_bits()
        } else {
            0.0f64.to_bits()
        },
    ]
}

fn project_pt_pt(
    from: &Coord<f64>,
    to: &Coord<f64>,
    distance: f64,
    dimension: WktDimension,
) -> Coord<f64> {
    let length = distance_sqr_2d(from, to).sqrt();
    let proportion = distance / length;
    let mut result = Coord {
        x: to.x + (to.x - from.x) * proportion,
        y: to.y + (to.y - from.y) * proportion,
        z: None,
        m: None,
    };
    if has_z(dimension) {
        let dz = (to.z.unwrap_or(0.0) - from.z.unwrap_or(0.0)) * proportion;
        result.z = Some(to.z.unwrap_or(0.0) + dz);
    }
    if has_m(dimension) {
        let dm = (to.m.unwrap_or(0.0) - from.m.unwrap_or(0.0)) * proportion;
        result.m = Some(to.m.unwrap_or(0.0) + dm);
    }
    result
}

fn has_z(dimension: WktDimension) -> bool {
    matches!(dimension, WktDimension::XYZ | WktDimension::XYZM)
}

fn has_m(dimension: WktDimension) -> bool {
    matches!(dimension, WktDimension::XYM | WktDimension::XYZM)
}

fn has_z_dimensions(dimension: geo_traits::Dimensions) -> bool {
    matches!(
        dimension,
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
    )
}

fn has_m_dimensions(dimension: geo_traits::Dimensions) -> bool {
    matches!(
        dimension,
        geo_traits::Dimensions::Xym | geo_traits::Dimensions::Xyzm
    )
}

fn wkt_is_empty(geometry: &Wkt<f64>) -> bool {
    match geometry {
        Wkt::Point(point) => point.coord().is_none(),
        Wkt::LineString(line) => line.clone().into_inner().0.is_empty(),
        Wkt::Polygon(polygon) => {
            let (rings, _) = polygon.clone().into_inner();
            rings
                .first()
                .is_none_or(|ring| ring.clone().into_inner().0.is_empty())
        }
        Wkt::MultiPoint(multi) => multi.clone().into_inner().0.is_empty(),
        Wkt::MultiLineString(multi) => multi.clone().into_inner().0.is_empty(),
        Wkt::MultiPolygon(multi) => multi.clone().into_inner().0.is_empty(),
        Wkt::GeometryCollection(collection) => collection.clone().into_inner().0.is_empty(),
    }
}

fn remove_repeated_coords(
    coords: &[Coord<f64>],
    dimension: WktDimension,
    tolerance: f64,
    min_points: usize,
) -> Vec<Coord<f64>> {
    let n_points = coords.len();
    if n_points <= min_points {
        return coords.to_vec();
    }

    let tolsq = tolerance * tolerance;
    let mut out = Vec::with_capacity(n_points);
    out.push(coords[0]);
    let mut out_len = 1usize;
    let mut last_kept = 0usize;
    let mut distance = f64::MAX;

    for (index, point) in coords.iter().enumerate().skip(1) {
        let last_point = index == n_points - 1;
        if n_points + out_len > min_points + index {
            if tolerance > 0.0 {
                distance = distance_sqr_2d(&coords[last_kept], point);
                if !last_point && distance <= tolsq {
                    continue;
                }
            } else if coord_slots(&coords[last_kept], dimension) == coord_slots(point, dimension) {
                continue;
            }

            if last_point && out_len > 1 && tolerance > 0.0 && distance <= tolsq {
                out_len -= 1;
                out.pop();
            }
        }

        out.push(*point);
        out_len += 1;
        last_kept = index;
    }

    out
}

fn remove_repeated_points_from_multipoint(
    points: Vec<Point<f64>>,
    tolerance: f64,
) -> Vec<Point<f64>> {
    let tolsq = tolerance * tolerance;
    let mut points = points;

    for by_y in [true, false] {
        points.sort_by(|left, right| compare_points(left, right, by_y));
        let mut removed = vec![false; points.len()];
        for i in 0..points.len() {
            if removed[i] {
                continue;
            }
            let Some(base) = points[i].coord() else {
                continue;
            };
            for j in i + 1..points.len() {
                if removed[j] {
                    continue;
                }
                let Some(other) = points[j].coord() else {
                    continue;
                };
                let delta = if by_y {
                    other.y - base.y
                } else {
                    other.x - base.x
                };
                if delta > tolerance {
                    break;
                }
                let dx = base.x - other.x;
                let dy = base.y - other.y;
                if dx * dx + dy * dy <= tolsq {
                    removed[j] = true;
                }
            }
        }
        let mut compacted = Vec::with_capacity(points.len());
        for (index, point) in points.into_iter().enumerate() {
            if !removed[index] && point.coord().is_some() {
                compacted.push(point);
            }
        }
        points = compacted;
    }

    points
}

fn compare_points(left: &Point<f64>, right: &Point<f64>, by_y: bool) -> std::cmp::Ordering {
    match (left.coord(), right.coord()) {
        (Some(left), Some(right)) => {
            let (left, right) = if by_y {
                (left.y, right.y)
            } else {
                (left.x, right.x)
            };
            left.partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
    }
}

fn segmentize_wkt(geometry: Wkt<f64>, max_length: f64) -> GeoArrowResult<Wkt<f64>> {
    Ok(match geometry {
        Wkt::Point(point) => Wkt::Point(point),
        Wkt::MultiPoint(multi_point) => Wkt::MultiPoint(multi_point),
        Wkt::LineString(line_string) => {
            let (coords, dimension) = line_string.into_inner();
            Wkt::LineString(segmentize_line(&coords, dimension, max_length)?)
        }
        Wkt::Polygon(polygon) => {
            let (rings, dimension) = polygon.into_inner();
            let rings = rings
                .into_iter()
                .map(|ring| {
                    let (coords, _) = ring.into_inner();
                    segmentize_line(&coords, dimension, max_length)
                })
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::Polygon(Polygon::new(rings, dimension))
        }
        Wkt::MultiLineString(multi_line_string) => {
            let (lines, dimension) = multi_line_string.into_inner();
            let lines = lines
                .into_iter()
                .map(|line| {
                    let (coords, _) = line.into_inner();
                    segmentize_line(&coords, dimension, max_length)
                })
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::MultiLineString(MultiLineString::new(lines, dimension))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            let polygons = polygons
                .into_iter()
                .map(|polygon| {
                    let (rings, _) = polygon.into_inner();
                    let rings = rings
                        .into_iter()
                        .map(|ring| {
                            let (coords, _) = ring.into_inner();
                            segmentize_line(&coords, dimension, max_length)
                        })
                        .collect::<GeoArrowResult<Vec<_>>>()?;
                    Ok(Polygon::new(rings, dimension))
                })
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::MultiPolygon(MultiPolygon::new(polygons, dimension))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            let geometries = geometries
                .into_iter()
                .map(|geometry| segmentize_wkt(geometry, max_length))
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::GeometryCollection(GeometryCollection::new(geometries, dimension))
        }
    })
}

fn segmentize_line(
    coords: &[Coord<f64>],
    dimension: WktDimension,
    max_length: f64,
) -> GeoArrowResult<LineString<f64>> {
    if coords.is_empty() {
        return Ok(LineString::new(Vec::new(), dimension));
    }

    let allow_duplicate = coords.len() == 2;
    let mut out = Vec::with_capacity(coords.len());
    out.push(coords[0]);

    for pair in coords.windows(2) {
        let first = &pair[0];
        let second = &pair[1];
        let segment_length = distance_sqr_2d(first, second).sqrt();
        let segments = (segment_length / max_length).ceil();
        if !segments.is_finite() || segments >= f64::from(i32::MAX) {
            return Ok(LineString::new(Vec::new(), dimension));
        }
        let segments = segments as u32;
        for step in 1..segments {
            out.push(segmentize_interpolate(
                first, second, step, segments, dimension,
            ));
        }
        if allow_duplicate
            || coord_slots(out.last().expect("first point"), dimension)
                != coord_slots(second, dimension)
        {
            out.push(*second);
        }
    }

    Ok(LineString::new(out, dimension))
}

fn interpolate_coord(
    first: &Coord<f64>,
    second: &Coord<f64>,
    proportion: f64,
    dimension: WktDimension,
) -> Coord<f64> {
    let mut result = Coord {
        x: first.x + (second.x - first.x) * proportion,
        y: first.y + (second.y - first.y) * proportion,
        z: None,
        m: None,
    };
    if has_z(dimension) {
        let first_z = first.z.unwrap_or(0.0);
        let second_z = second.z.unwrap_or(0.0);
        result.z = Some(first_z + (second_z - first_z) * proportion);
    }
    if has_m(dimension) {
        let first_m = first.m.unwrap_or(0.0);
        let second_m = second.m.unwrap_or(0.0);
        result.m = Some(first_m + (second_m - first_m) * proportion);
    }
    result
}

fn segmentize_interpolate(
    first: &Coord<f64>,
    second: &Coord<f64>,
    step: u32,
    segments: u32,
    dimension: WktDimension,
) -> Coord<f64> {
    let mut result = Coord {
        x: first.x + (second.x - first.x) * f64::from(step) / f64::from(segments),
        y: first.y + (second.y - first.y) * f64::from(step) / f64::from(segments),
        z: None,
        m: None,
    };
    if has_z(dimension) {
        let first_z = first.z.unwrap_or(0.0);
        let second_z = second.z.unwrap_or(0.0);
        result.z = Some(first_z + (second_z - first_z) * f64::from(step) / f64::from(segments));
    }
    if has_m(dimension) {
        let first_m = first.m.unwrap_or(0.0);
        let second_m = second.m.unwrap_or(0.0);
        result.m = Some(first_m + (second_m - first_m) * f64::from(step) / f64::from(segments));
    }
    result
}

fn wrap_point(point: Point<f64>, cut: f64, amount: f64) -> GeoArrowResult<Point<f64>> {
    let (coord, dimension) = point.into_inner();
    Ok(Point::new(
        coord.map(|mut coord| {
            if (amount < 0.0 && coord.x > cut) || (amount > 0.0 && coord.x < cut) {
                coord.x += amount;
            }
            coord
        }),
        dimension,
    ))
}

fn wrap_line_string(
    line_string: LineString<f64>,
    cut: f64,
    amount: f64,
) -> GeoArrowResult<Wkt<f64>> {
    let (coords, dimension) = line_string.into_inner();
    let Some((xmin, xmax)) = coord_range(&coords) else {
        return Ok(Wkt::LineString(LineString::new(coords, dimension)));
    };

    if (amount < 0.0 && xmin >= cut) || (amount > 0.0 && xmax <= cut) {
        return Ok(Wkt::LineString(LineString::new(
            shift_coords(&coords, amount),
            dimension,
        )));
    }
    if (amount < 0.0 && xmax <= cut) || (amount > 0.0 && xmin >= cut) {
        return Ok(Wkt::LineString(LineString::new(coords, dimension)));
    }

    // The PostGIS split path goes through GEOS, which keeps Z but drops M.
    let split_dimension = if has_z(dimension) {
        WktDimension::XYZ
    } else {
        WktDimension::XY
    };
    let coords = drop_m_coords(&coords);
    let pieces = split_line_at_cut(&coords, split_dimension, cut);
    let mut wrapped = Vec::with_capacity(pieces.len());
    for piece in pieces {
        let Some((piece_min, piece_max)) = coord_range(&piece) else {
            continue;
        };
        if (amount < 0.0 && piece_min >= cut) || (amount > 0.0 && piece_max <= cut) {
            wrapped.push(shift_coords(&piece, amount));
        } else {
            wrapped.push(piece);
        }
    }

    Ok(union_line_pieces(&wrapped, split_dimension))
}

fn wrap_polygon(polygon: Polygon<f64>, cut: f64, amount: f64) -> GeoArrowResult<Wkt<f64>> {
    let (rings, dimension) = polygon.into_inner();
    let Some(exterior) = rings.first() else {
        return Ok(Wkt::Polygon(Polygon::new(rings, dimension)));
    };
    let (exterior_coords, _) = exterior.clone().into_inner();
    let Some((xmin, xmax)) = coord_range(&exterior_coords) else {
        return Ok(Wkt::Polygon(Polygon::new(rings, dimension)));
    };

    if (amount < 0.0 && xmin >= cut) || (amount > 0.0 && xmax <= cut) {
        return Ok(Wkt::Polygon(shift_polygon(
            Polygon::new(rings, dimension),
            amount,
        )));
    }
    if (amount < 0.0 && xmax <= cut) || (amount > 0.0 && xmin >= cut) {
        return Ok(Wkt::Polygon(Polygon::new(rings, dimension)));
    }

    // PostGIS splits crossing polygons through GEOS: the ring linework and the
    // blade line are polygonized, the pieces fully on the move side are
    // translated, and the shifted collection is unary-unioned. The same path
    // keeps Z interpolation and drops M like PostGIS; a native half-plane clip
    // cannot represent notched or multi-part splits.
    #[cfg(feature = "geos-3_11")]
    {
        geos_split_polygon(Polygon::new(rings, dimension), cut, amount)
    }
    #[cfg(not(feature = "geos-3_11"))]
    {
        Err(GeoArrowError::InvalidGeoArrow(
            "ST_WrapX: splitting polygons requires the geos-3_11 feature".to_string(),
        ))
    }
}

fn drop_m_coords(coords: &[Coord<f64>]) -> Vec<Coord<f64>> {
    coords
        .iter()
        .map(|coord| Coord {
            x: coord.x,
            y: coord.y,
            z: coord.z,
            m: None,
        })
        .collect()
}

fn coord_range(coords: &[Coord<f64>]) -> Option<(f64, f64)> {
    let first = coords.first()?;
    let mut xmin = first.x;
    let mut xmax = first.x;
    for coord in coords.iter().skip(1) {
        xmin = xmin.min(coord.x);
        xmax = xmax.max(coord.x);
    }
    Some((xmin, xmax))
}

fn shift_coords(coords: &[Coord<f64>], amount: f64) -> Vec<Coord<f64>> {
    coords
        .iter()
        .map(|coord| Coord {
            x: coord.x + amount,
            y: coord.y,
            z: coord.z,
            m: coord.m,
        })
        .collect()
}

fn shift_polygon(polygon: Polygon<f64>, amount: f64) -> Polygon<f64> {
    let (rings, dimension) = polygon.into_inner();
    let rings = rings
        .into_iter()
        .map(|ring| {
            let (coords, _) = ring.into_inner();
            LineString::new(shift_coords(&coords, amount), dimension)
        })
        .collect();
    Polygon::new(rings, dimension)
}

/// Splits a crossing polygon through GEOS and unions the wrapped pieces.
///
/// `ST_WrapX` only needs a split when `x_min < cut < x_max`; the native fast
/// paths above handle everything else. The left half is the GEOS intersection
/// with the half-plane rectangle around `cut`, the right half is the GEOS
/// difference against it, pieces fully on the move side are translated, and
/// the shifted pieces are unary-unioned. The halves share only the cut edge,
/// so their areas add up to the input area, holes crossing the cut become
/// notches of both halves, and the results stay valid polygons. GEOS
/// interpolates Z at the cut nodes and drops M, so the polygon is normalized
/// to XY or XYZ before it is encoded.
#[cfg(feature = "geos-3_11")]
fn geos_split_polygon(polygon: Polygon<f64>, cut: f64, amount: f64) -> GeoArrowResult<Wkt<f64>> {
    use wkb::reader::Wkb;
    use wkb::writer::{WriteOptions, geometry_wkb_size, write_geometry};

    let (rings, dimension) = polygon.into_inner();
    let dimension = if has_z(dimension) {
        WktDimension::XYZ
    } else {
        WktDimension::XY
    };
    let rings = rings
        .into_iter()
        .map(|ring| {
            let (coords, _) = ring.into_inner();
            LineString::new(drop_m_coords(&coords), dimension)
        })
        .collect::<Vec<_>>();

    let source = Wkt::Polygon(Polygon::new(rings, dimension));
    let mut input = Vec::with_capacity(geometry_wkb_size(&source));
    write_geometry(&mut input, &source, &WriteOptions::default()).map_err(|error| {
        GeoArrowError::InvalidGeoArrow(format!("ST_WrapX: could not encode GEOS input: {error}"))
    })?;
    let pieces = crate::udf::geos::editors::split_polygon_wkb(&input, cut).map_err(|error| {
        GeoArrowError::InvalidGeoArrow(format!("ST_WrapX: GEOS polygon split failed: {error}"))
    })?;

    let mut wrapped = Vec::new();
    for piece in pieces {
        let piece = Wkb::try_new(&piece).map_err(|error| {
            GeoArrowError::InvalidGeoArrow(format!(
                "ST_WrapX: GEOS split returned invalid WKB: {error}"
            ))
        })?;
        match super::transform::geometry_to_wkt(&piece)? {
            Wkt::Polygon(piece) => wrapped.extend(shift_split_piece(piece, cut, amount)),
            Wkt::MultiPolygon(multi_polygon) => {
                let (polygons, _) = multi_polygon.into_inner();
                for piece in polygons {
                    wrapped.extend(shift_split_piece(piece, cut, amount));
                }
            }
            _ => {
                return Err(GeoArrowError::InvalidGeoArrow(
                    "ST_WrapX: GEOS polygon split returned a non-polygon piece".to_string(),
                ));
            }
        }
    }

    let collection = Wkt::MultiPolygon(MultiPolygon::new(wrapped, dimension));
    let mut input = Vec::with_capacity(geometry_wkb_size(&collection));
    write_geometry(&mut input, &collection, &WriteOptions::default()).map_err(|error| {
        GeoArrowError::InvalidGeoArrow(format!("ST_WrapX: could not encode GEOS input: {error}"))
    })?;
    let union = crate::udf::geos::editors::unary_union_wkb(&input).map_err(|error| {
        GeoArrowError::InvalidGeoArrow(format!("ST_WrapX: GEOS unary union failed: {error}"))
    })?;
    let union = Wkb::try_new(&union).map_err(|error| {
        GeoArrowError::InvalidGeoArrow(format!("ST_WrapX: GEOS returned invalid WKB: {error}"))
    })?;
    super::transform::geometry_to_wkt(&union)
}

/// Translates a split piece when its whole extent is on the move side.
#[cfg(feature = "geos-3_11")]
fn shift_split_piece(piece: Polygon<f64>, cut: f64, amount: f64) -> Option<Polygon<f64>> {
    let (rings, _) = piece.clone().into_inner();
    let exterior = rings.first()?;
    let (coords, _) = exterior.clone().into_inner();
    let (piece_min, piece_max) = coord_range(&coords)?;
    if (amount < 0.0 && piece_min >= cut) || (amount > 0.0 && piece_max <= cut) {
        Some(shift_polygon(piece, amount))
    } else {
        Some(piece)
    }
}

fn split_line_at_cut(
    coords: &[Coord<f64>],
    dimension: WktDimension,
    cut: f64,
) -> Vec<Vec<Coord<f64>>> {
    if coords.is_empty() {
        return vec![Vec::new()];
    }
    let mut pieces = Vec::new();
    let mut current = vec![coords[0]];

    for pair in coords.windows(2) {
        let first = &pair[0];
        let second = &pair[1];
        let crosses = (first.x < cut && second.x > cut) || (first.x > cut && second.x < cut);
        if crosses {
            let proportion = (cut - first.x) / (second.x - first.x);
            let intersection = interpolate_coord(first, second, proportion, dimension);
            current.push(intersection);
            pieces.push(std::mem::take(&mut current));
            current.push(intersection);
        }
        current.push(*second);
    }
    pieces.push(current);
    pieces
}

fn union_line_pieces(pieces: &[Vec<Coord<f64>>], dimension: WktDimension) -> Wkt<f64> {
    let mut segments = Vec::new();
    for piece in pieces {
        for pair in piece.windows(2) {
            segments.push(LineSegment {
                start: pair[0],
                end: pair[1],
            });
        }
    }
    if segments.is_empty() {
        return Wkt::LineString(LineString::new(Vec::new(), dimension));
    }

    let mut splits = vec![Vec::<f64>::new(); segments.len()];
    for split in &mut splits {
        split.push(0.0);
        split.push(1.0);
    }
    for first in 0..segments.len() {
        for second in first + 1..segments.len() {
            match segment_intersection(&segments[first], &segments[second]) {
                SegmentIntersection::None => {}
                SegmentIntersection::Point { first_t, second_t } => {
                    splits[first].push(first_t);
                    splits[second].push(second_t);
                }
                SegmentIntersection::Collinear {
                    first_start,
                    first_end,
                    second_start,
                    second_end,
                } => {
                    splits[first].push(first_start);
                    splits[first].push(first_end);
                    splits[second].push(second_start);
                    splits[second].push(second_end);
                }
            }
        }
    }

    let mut noded = Vec::new();
    for (segment, mut params) in segments.iter().zip(splits) {
        params.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        params.dedup_by(|left, right| (*left - *right).abs() <= f64::EPSILON);
        for window in params.windows(2) {
            let start = interpolate_coord(&segment.start, &segment.end, window[0], dimension);
            let end = interpolate_coord(&segment.start, &segment.end, window[1], dimension);
            if coord_slots(&start, dimension) == coord_slots(&end, dimension) {
                continue;
            }
            noded.push((start, end));
        }
    }

    let mut seen = std::collections::HashSet::new();
    let mut unique = Vec::new();
    for (start, end) in noded {
        let key = if point_key(&start, dimension) <= point_key(&end, dimension) {
            (point_key(&start, dimension), point_key(&end, dimension))
        } else {
            (point_key(&end, dimension), point_key(&start, dimension))
        };
        if seen.insert(key) {
            unique.push((start, end));
        }
    }

    // GEOS line unions dissolve duplicate overlap but keep every remaining
    // noded segment as its own component.
    let mut segments = unique
        .into_iter()
        .map(|(start, end)| LineString::new(vec![start, end], dimension))
        .collect::<Vec<_>>();

    if segments.len() == 1 {
        Wkt::LineString(segments.remove(0))
    } else {
        Wkt::MultiLineString(MultiLineString::new(segments, dimension))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PointKey(Vec<u64>);

fn point_key(coord: &Coord<f64>, dimension: WktDimension) -> PointKey {
    PointKey(coord_slots(coord, dimension).to_vec())
}

struct LineSegment {
    start: Coord<f64>,
    end: Coord<f64>,
}

enum SegmentIntersection {
    None,
    Point {
        first_t: f64,
        second_t: f64,
    },
    Collinear {
        first_start: f64,
        first_end: f64,
        second_start: f64,
        second_end: f64,
    },
}

fn segment_intersection(first: &LineSegment, second: &LineSegment) -> SegmentIntersection {
    let p = first.start;
    let r = Coord {
        x: first.end.x - first.start.x,
        y: first.end.y - first.start.y,
        z: None,
        m: None,
    };
    let q = second.start;
    let s = Coord {
        x: second.end.x - second.start.x,
        y: second.end.y - second.start.y,
        z: None,
        m: None,
    };
    let denom = cross(&r, &s);
    let qmp = Coord {
        x: q.x - p.x,
        y: q.y - p.y,
        z: None,
        m: None,
    };
    if denom != 0.0 {
        let first_t = cross(&qmp, &s) / denom;
        let second_t = cross(&qmp, &r) / denom;
        if (0.0..=1.0).contains(&first_t) && (0.0..=1.0).contains(&second_t) {
            return SegmentIntersection::Point { first_t, second_t };
        }
        return SegmentIntersection::None;
    }
    if cross(&qmp, &r) != 0.0 {
        return SegmentIntersection::None;
    }
    let r_len = r.x * r.x + r.y * r.y;
    if r_len == 0.0 {
        return SegmentIntersection::None;
    }
    let t0 = ((q.x - p.x) * r.x + (q.y - p.y) * r.y) / r_len;
    let t1 = ((second.end.x - p.x) * r.x + (second.end.y - p.y) * r.y) / r_len;
    let (first_start, first_end) = (t0.min(t1), t0.max(t1));
    if first_end < 0.0 || first_start > 1.0 {
        return SegmentIntersection::None;
    }
    let s_len = s.x * s.x + s.y * s.y;
    if s_len == 0.0 {
        return SegmentIntersection::None;
    }
    let u0 = ((p.x - q.x) * s.x + (p.y - q.y) * s.y) / s_len;
    let u1 = ((first.end.x - q.x) * s.x + (first.end.y - q.y) * s.y) / s_len;
    let (second_start, second_end) = (u0.min(u1), u0.max(u1));
    SegmentIntersection::Collinear {
        first_start: first_start.max(0.0),
        first_end: first_end.min(1.0),
        second_start: second_start.max(0.0),
        second_end: second_end.min(1.0),
    }
}

fn cross(first: &Coord<f64>, second: &Coord<f64>) -> f64 {
    first.x * second.y - first.y * second.x
}
