//! Spherical and spheroidal measurement UDFs.
//!
//! `ST_DistanceSphere` follows PostGIS's geometry overload and treats
//! coordinates as longitude/latitude degrees on a sphere. `ST_DistanceSpheroid`
//! and `ST_LengthSpheroid` follow the documented lon/lat contract, using
//! Karney's geodesic algorithms (via geographiclib-rs).

use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::Float64Builder;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use datafusion::arrow::compute::cast as cast_array;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use geo::Intersects;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPolygonTrait, PolygonTrait,
};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use geographiclib_rs::{Geodesic, InverseGeodesic};

use super::dwithin::to_distance_geometry;
use crate::error::{GeoDataFusionError, GeoDataFusionResult};
use crate::udf::native::geometry::{field_metadata, geometry_array_from_arrow};

/// PostGIS's default sphere radius (the GRS80 mean radius rounded to 1e-7 m).
///
/// The pinned reference returns `111195.07973463` for a 1 degree equatorial
/// arc, which corresponds to this constant rather than to geo's slightly
/// rounded `6371008.8` mean radius.
pub(crate) const DEFAULT_SPHERE_RADIUS: f64 = 6371008.771415059;

const WGS84_A: f64 = 6378137.0;
const WGS84_INVERSE_FLATTENING: f64 = 298.257223563;
const GEOMETRIC_EPSILON: f64 = 1e-12;

static DISTANCE_SPHERE_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![TypeSignature::Any(2), TypeSignature::Any(3)],
        Volatility::Immutable,
    )
});
static DISTANCE_SPHEROID_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![TypeSignature::Any(2), TypeSignature::Any(3)],
        Volatility::Immutable,
    )
});
static LENGTH_SPHEROID_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::any(2, Volatility::Immutable));

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DistanceSphere;

impl DistanceSphere {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DistanceSphere {
    fn default() -> Self {
        Self::new()
    }
}

static DISTANCE_SPHERE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for DistanceSphere {
    fn name(&self) -> &str {
        "st_distancesphere"
    }

    fn signature(&self) -> &Signature {
        &DISTANCE_SPHERE_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(distance_sphere_impl(args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DISTANCE_SPHERE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the minimum distance in metres between two lon/lat geometries \
                 on a sphere. The default radius is the PostGIS WGS84 mean radius.",
                "ST_DistanceSphere(geomlonlatA, geomlonlatB[, radius])",
            )
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DistanceSpheroid;

impl DistanceSpheroid {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DistanceSpheroid {
    fn default() -> Self {
        Self::new()
    }
}

static DISTANCE_SPHEROID_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for DistanceSpheroid {
    fn name(&self) -> &str {
        "st_distancespheroid"
    }

    fn signature(&self) -> &Signature {
        &DISTANCE_SPHEROID_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(distance_spheroid_impl(args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DISTANCE_SPHEROID_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the minimum distance between two lon/lat geometries on an \
                 ellipsoid. The optional spheroid is a SPHEROID[\"name\", a, inv_f] \
                 definition; when omitted the WGS84 ellipsoid is used.",
                "ST_DistanceSpheroid(geomlonlatA, geomlonlatB[, spheroid])",
            )
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LengthSpheroid;

impl LengthSpheroid {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LengthSpheroid {
    fn default() -> Self {
        Self::new()
    }
}

static LENGTH_SPHEROID_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LengthSpheroid {
    fn name(&self) -> &str {
        "st_lengthspheroid"
    }

    fn signature(&self) -> &Signature {
        &LENGTH_SPHEROID_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(length_spheroid_impl(args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LENGTH_SPHEROID_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the length or perimeter of a lon/lat geometry on an ellipsoid. \
                 Linear geometries sum geodesic segment lengths; polygons sum all rings; \
                 collections sum their linear and polygonal members.",
                "ST_LengthSpheroid(geomlonlat, spheroid)",
            )
            .build()
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LonLat {
    lon: f64,
    lat: f64,
}

#[derive(Debug, Clone)]
struct Edge {
    start: LonLat,
    end: LonLat,
}

#[derive(Debug, Clone)]
enum Shape {
    Point(LonLat),
    Edges(Vec<Edge>),
    Polygon(geo::Polygon<f64>),
    Collection(Vec<Shape>),
}

#[derive(Debug, Clone, Copy)]
struct Spheroid {
    a: f64,
    inverse_flattening: f64,
}

impl Default for Spheroid {
    fn default() -> Self {
        Self {
            a: WGS84_A,
            inverse_flattening: WGS84_INVERSE_FLATTENING,
        }
    }
}

impl Spheroid {
    fn geodesic(self) -> Geodesic {
        let flattening = if self.inverse_flattening == 0.0 {
            0.0
        } else {
            1.0 / self.inverse_flattening
        };
        Geodesic::new(self.a, flattening)
    }
}

#[derive(Clone)]
enum GeometryValue {
    Null,
    Empty,
    Geo(geo::Geometry<f64>),
}

fn distance_sphere_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != 2 && arrays.len() != 3 {
        return Err(DataFusionError::Internal(format!(
            "ST_DistanceSphere expects 2 or 3 arguments, got {}",
            arrays.len()
        ))
        .into());
    }

    validate_lonlat_field(&args.arg_fields[0], "ST_DistanceSphere")?;
    validate_lonlat_field(&args.arg_fields[1], "ST_DistanceSphere")?;

    let left = geometry_values(&arrays[0], &args.arg_fields[0])?;
    let right = geometry_values(&arrays[1], &args.arg_fields[1])?;
    let explicit_radius = arrays.len() == 3;
    let radii = optional_float_values(&args, &arrays, 2, DEFAULT_SPHERE_RADIUS)?;

    let number_rows = args
        .number_rows
        .max(arrays.iter().map(|array| array.len()).max().unwrap_or(0));
    let mut builder = Float64Builder::with_capacity(number_rows);
    for index in 0..number_rows {
        let Some(radius) = radii.get(index).copied().flatten() else {
            builder.append_null();
            continue;
        };
        match (&left.get(index), &right.get(index)) {
            (Some(GeometryValue::Geo(left)), Some(GeometryValue::Geo(right))) => {
                let value = shape_distance_metric(left, right, Some(radius), &Geodesic::wgs84());
                builder.append_value(value);
            }
            (Some(GeometryValue::Empty), _) | (_, Some(GeometryValue::Empty)) => {
                if explicit_radius {
                    builder.append_value(-1.0);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn distance_spheroid_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != 2 && arrays.len() != 3 {
        return Err(DataFusionError::Internal(format!(
            "ST_DistanceSpheroid expects 2 or 3 arguments, got {}",
            arrays.len()
        ))
        .into());
    }

    validate_lonlat_field(&args.arg_fields[0], "ST_DistanceSpheroid")?;
    validate_lonlat_field(&args.arg_fields[1], "ST_DistanceSpheroid")?;

    let left = geometry_values(&arrays[0], &args.arg_fields[0])?;
    let right = geometry_values(&arrays[1], &args.arg_fields[1])?;
    let spheroids = optional_spheroid_values(&args, &arrays, 2)?;

    let number_rows = args
        .number_rows
        .max(arrays.iter().map(|array| array.len()).max().unwrap_or(0));
    let mut builder = Float64Builder::with_capacity(number_rows);
    for index in 0..number_rows {
        let (Some(left), Some(right)) = (&left.get(index), &right.get(index)) else {
            builder.append_null();
            continue;
        };
        let Some(spheroid) = spheroids.get(index).copied().flatten() else {
            builder.append_null();
            continue;
        };
        match (left, right) {
            (GeometryValue::Geo(left), GeometryValue::Geo(right)) => {
                let geod = spheroid.geodesic();
                builder.append_value(shape_distance_metric(left, right, None, &geod));
            }
            // PostGIS returns -1 for unknown distance to an empty geometry.
            (GeometryValue::Empty, _) | (_, GeometryValue::Empty) => builder.append_value(-1.0),
            _ => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn length_spheroid_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "ST_LengthSpheroid expects 2 arguments, got {}",
            arrays.len()
        ))
        .into());
    }

    validate_lonlat_field(&args.arg_fields[0], "ST_LengthSpheroid")?;
    let spheroids = optional_spheroid_values(&args, &arrays, 1)?;
    let lengths = geometry_length_values(&arrays[0], &args.arg_fields[0], &spheroids)?;

    let number_rows = args
        .number_rows
        .max(arrays.iter().map(|array| array.len()).max().unwrap_or(0));
    let mut builder = Float64Builder::with_capacity(number_rows);
    for index in 0..number_rows {
        match lengths.get(index).copied().flatten() {
            Some(length) => builder.append_value(length),
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn geometry_values(array: &ArrayRef, field: &Field) -> GeoDataFusionResult<Vec<GeometryValue>> {
    if matches!(field.data_type(), DataType::Null) {
        return Ok(vec![GeometryValue::Null; array.len()]);
    }
    let geo_array = geometry_array_from_arrow(array, field)?;
    let array = geo_array.as_ref();
    downcast_geoarrow_array!(array, geometry_values_impl)
}

fn geometry_values_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<Vec<GeometryValue>> {
    let mut values = Vec::with_capacity(array.len());
    for geometry in array.iter() {
        let value = match geometry {
            None => GeometryValue::Null,
            Some(geometry) => match to_distance_geometry(&geometry?)? {
                Some(geometry) if shape_from_geometry(&geometry).is_some() => {
                    GeometryValue::Geo(geometry)
                }
                _ => GeometryValue::Empty,
            },
        };
        values.push(value);
    }
    Ok(values)
}

fn geometry_length_values(
    array: &ArrayRef,
    field: &Field,
    spheroids: &[Option<Spheroid>],
) -> GeoDataFusionResult<Vec<Option<f64>>> {
    if matches!(field.data_type(), DataType::Null) {
        return Ok(vec![None; array.len()]);
    }
    let geo_array = geometry_array_from_arrow(array, field)?;
    let array = geo_array.as_ref();
    downcast_geoarrow_array!(array, geometry_length_values_impl, spheroids)
}

fn geometry_length_values_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    spheroids: &[Option<Spheroid>],
) -> GeoDataFusionResult<Vec<Option<f64>>> {
    let mut lengths = Vec::with_capacity(array.len());
    for (index, geometry) in array.iter().enumerate() {
        let value = match geometry {
            None => None,
            Some(geometry) => {
                let geometry = geometry?;
                if to_distance_geometry(&geometry)?.is_none() {
                    Some(0.0)
                } else {
                    match spheroids.get(index).copied().flatten() {
                        Some(spheroid) => Some(geometry_length(&geometry, &spheroid.geodesic())?),
                        None => None,
                    }
                }
            }
        };
        lengths.push(value);
    }
    Ok(lengths)
}

fn geometry_length<G: GeometryTrait<T = f64>>(
    geometry: &G,
    geod: &Geodesic,
) -> GeoDataFusionResult<f64> {
    Ok(match geometry.as_type() {
        GeometryType::Point(_) | GeometryType::MultiPoint(_) => 0.0,
        GeometryType::Line(line) => {
            let start = coord_position(&line.start());
            let end = coord_position(&line.end());
            segment_length(start, end, geod)
        }
        GeometryType::LineString(line_string) => coords_length(
            line_string.coords().map(|coord| coord_position(&coord)),
            geod,
        ),
        GeometryType::Polygon(polygon) => polygon_length(polygon, geod),
        GeometryType::MultiLineString(multi_line_string) => {
            let mut length = 0.0;
            for line_string in multi_line_string.line_strings() {
                length += coords_length(
                    line_string.coords().map(|coord| coord_position(&coord)),
                    geod,
                );
            }
            length
        }
        GeometryType::MultiPolygon(multi_polygon) => {
            let mut length = 0.0;
            for polygon in multi_polygon.polygons() {
                length += polygon_length(&polygon, geod);
            }
            length
        }
        GeometryType::GeometryCollection(collection) => {
            let mut length = 0.0;
            for child in collection.geometries() {
                length += geometry_length(&child, geod)?;
            }
            length
        }
        GeometryType::Rect(_) | GeometryType::Triangle(_) => {
            let geo = to_distance_geometry(geometry)?;
            geo.and_then(|geo| shape_from_geometry(&geo))
                .map(|shape| shape_length(&shape, geod))
                .unwrap_or(0.0)
        }
    })
}

fn polygon_length<A: PolygonTrait<T = f64>>(polygon: &A, geod: &Geodesic) -> f64 {
    let mut length = 0.0;
    if let Some(exterior) = polygon.exterior() {
        length += coords_length(exterior.coords().map(|coord| coord_position(&coord)), geod);
    }
    for interior in polygon.interiors() {
        length += coords_length(interior.coords().map(|coord| coord_position(&coord)), geod);
    }
    length
}

fn coord_position<C: CoordTrait<T = f64>>(coord: &C) -> (LonLat, Option<f64>) {
    coord_position_xy(coord.x(), coord.y(), coord_z(coord))
}

fn coord_z<C: CoordTrait<T = f64>>(coord: &C) -> Option<f64> {
    match coord.dim() {
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm => coord.nth(2),
        _ => None,
    }
}

fn coord_position_xy(x: f64, y: f64, z: Option<f64>) -> (LonLat, Option<f64>) {
    (LonLat { lon: x, lat: y }, z)
}

fn coords_length(coords: impl IntoIterator<Item = (LonLat, Option<f64>)>, geod: &Geodesic) -> f64 {
    positions_length(&coords.into_iter().collect::<Vec<_>>(), geod)
}

fn positions_length(positions: &[(LonLat, Option<f64>)], geod: &Geodesic) -> f64 {
    positions
        .windows(2)
        .map(|pair| segment_length(pair[0], pair[1], geod))
        .sum()
}

fn segment_length(
    start: (LonLat, Option<f64>),
    end: (LonLat, Option<f64>),
    geod: &Geodesic,
) -> f64 {
    let horizontal: f64 = geod.inverse(start.0.lat, start.0.lon, end.0.lat, end.0.lon);
    match (start.1, end.1) {
        (Some(start_z), Some(end_z)) => {
            (horizontal * horizontal + (end_z - start_z) * (end_z - start_z)).sqrt()
        }
        _ => horizontal,
    }
}

fn optional_float_values(
    args: &ScalarFunctionArgs,
    arrays: &[ArrayRef],
    position: usize,
    default: f64,
) -> GeoDataFusionResult<Vec<Option<f64>>> {
    if arrays.len() <= position {
        return Ok(vec![Some(default); args.number_rows]);
    }
    float_values(&arrays[position])
}

fn float_values(array: &ArrayRef) -> GeoDataFusionResult<Vec<Option<f64>>> {
    let casted = if array.data_type() == &DataType::Float64 {
        array.clone()
    } else {
        cast_array(array.as_ref(), &DataType::Float64)?
    };
    let values = casted.as_primitive::<arrow_array::types::Float64Type>();
    Ok((0..values.len())
        .map(|index| {
            if values.is_null(index) {
                None
            } else {
                Some(values.value(index))
            }
        })
        .collect())
}

fn optional_spheroid_values(
    args: &ScalarFunctionArgs,
    arrays: &[ArrayRef],
    position: usize,
) -> GeoDataFusionResult<Vec<Option<Spheroid>>> {
    if arrays.len() <= position {
        return Ok(vec![Some(Spheroid::default()); args.number_rows]);
    }
    let strings = string_values(&arrays[position])?;
    strings
        .into_iter()
        .map(|value| match value {
            None => Ok(None),
            Some(value) => parse_spheroid(&value).map(Some),
        })
        .collect()
}

fn string_values(array: &ArrayRef) -> GeoDataFusionResult<Vec<Option<String>>> {
    let casted = cast_array(array.as_ref(), &DataType::Utf8)?;
    let values = casted.as_string::<i32>();
    Ok((0..values.len())
        .map(|index| {
            if values.is_null(index) {
                None
            } else {
                Some(values.value(index).to_string())
            }
        })
        .collect())
}

fn parse_spheroid(value: &str) -> GeoDataFusionResult<Spheroid> {
    let trimmed = value.trim();
    let prefix_len = "SPHEROID".len();
    if !trimmed
        .get(..prefix_len)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SPHEROID"))
        || !trimmed[prefix_len..].trim_start().starts_with('[')
    {
        return Err(spheroid_error(value));
    }
    let mut rest = &trimmed[prefix_len..];
    rest = rest.trim_start();
    rest = &rest[1..];
    rest = rest.trim_start();
    if !rest.starts_with('"') {
        return Err(spheroid_error(value));
    }
    let quote_end = rest[1..].find('"').ok_or_else(|| spheroid_error(value))?;
    let _name = &rest[1..quote_end + 1];
    rest = &rest[quote_end + 2..];
    rest = rest.trim_start();
    if !rest.starts_with(',') {
        return Err(spheroid_error(value));
    }
    rest = &rest[1..];
    let close = rest.rfind(']').ok_or_else(|| spheroid_error(value))?;
    let inner = rest[..close].trim();
    let mut parts = inner.split(',');
    let a_text = parts.next().ok_or_else(|| spheroid_error(value))?;
    let inv_f_text = parts.next().ok_or_else(|| spheroid_error(value))?;
    if parts.next().is_some_and(|remaining| {
        !remaining
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("AUTHORITY")
    }) {
        return Err(spheroid_error(value));
    }
    let a = a_text
        .trim()
        .parse::<f64>()
        .map_err(|_| spheroid_error(value))?;
    let inverse_flattening = inv_f_text
        .trim()
        .parse::<f64>()
        .map_err(|_| spheroid_error(value))?;
    if !a.is_finite() || a <= 0.0 || !inverse_flattening.is_finite() || inverse_flattening < 0.0 {
        return Err(spheroid_error(value));
    }
    Ok(Spheroid {
        a,
        inverse_flattening,
    })
}

fn spheroid_error(value: &str) -> GeoDataFusionError {
    GeoDataFusionError::DataFusion(DataFusionError::Execution(format!(
        "invalid SPHEROID definition, expected SPHEROID[\"name\", a, inv_f]: {value}"
    )))
}

fn validate_lonlat_field(field: &Field, function: &str) -> Result<()> {
    if matches!(field.data_type(), DataType::Null) {
        return Ok(());
    }
    let metadata = field_metadata(field)?;
    let Some(crs) = metadata.crs().crs_value() else {
        return Ok(());
    };
    let Some(value) = crs.as_str() else {
        return Err(DataFusionError::Plan(format!(
            "{function} requires lon/lat coordinates; PROJJSON CRS metadata is not supported"
        )));
    };
    if matches!(value, "EPSG:0" | "0") {
        return Ok(());
    }
    if !matches!(value, "EPSG:4326" | "OGC:CRS84" | "CRS84") {
        return Err(DataFusionError::Plan(format!(
            "{function} requires WGS84 lon/lat degrees and does not infer another datum from CRS metadata; {value} is not supported (use EPSG:4326, OGC:CRS84, or an unknown SRID)"
        )));
    }
    Ok(())
}

fn lonlat_from_point(point: &geo::Point<f64>) -> LonLat {
    LonLat {
        lon: point.x(),
        lat: point.y(),
    }
}

fn vector(lon: f64, lat: f64) -> [f64; 3] {
    let lon = lon.to_radians();
    let lat = lat.to_radians();
    [lat.cos() * lon.cos(), lat.cos() * lon.sin(), lat.sin()]
}

fn dot(left: [f64; 3], right: [f64; 3]) -> f64 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

fn cross(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    ]
}

fn normalize(mut value: [f64; 3]) -> Option<[f64; 3]> {
    let length = dot(value, value).sqrt();
    if length <= GEOMETRIC_EPSILON {
        return None;
    }
    for component in &mut value {
        *component /= length;
    }
    Some(value)
}

fn lonlat_from_vector(vector: [f64; 3]) -> LonLat {
    LonLat {
        lon: vector[1].atan2(vector[0]).to_degrees(),
        lat: vector[2].asin().to_degrees(),
    }
}

fn angular_distance(left: LonLat, right: LonLat) -> f64 {
    let left = vector(left.lon, left.lat);
    let right = vector(right.lon, right.lat);
    let numerator = dot(cross(left, right), cross(left, right)).sqrt();
    let denominator = dot(left, right).clamp(-1.0, 1.0);
    numerator.atan2(denominator)
}

fn sphere_distance(left: LonLat, right: LonLat, radius: f64) -> f64 {
    radius * angular_distance(left, right)
}

fn point_on_arc(point: [f64; 3], start: [f64; 3], end: [f64; 3], _normal: [f64; 3]) -> bool {
    // Match PostGIS geography's bisector-cone test. In particular, this
    // intentionally keeps the reference behavior for an edge whose endpoints
    // straddle the antimeridian.
    let Some(bisector) = normalize([start[0] + end[0], start[1] + end[1], start[2] + end[2]])
    else {
        return true;
    };
    let start_projection = dot(start, bisector);
    let point_projection = dot(point, bisector);
    point_projection > start_projection || (point_projection - start_projection).abs() < 2e-16
}

fn edge_to_point(edge: &Edge, point: LonLat) -> (f64, LonLat) {
    let start = vector(edge.start.lon, edge.start.lat);
    let end = vector(edge.end.lon, edge.end.lat);
    let point = vector(point.lon, point.lat);

    let mut candidates = Vec::with_capacity(3);
    if let Some(normal) = normalize(cross(start, end)) {
        let projected_length = dot(point, normal);
        let projected = [
            point[0] - normal[0] * projected_length,
            point[1] - normal[1] * projected_length,
            point[2] - normal[2] * projected_length,
        ];
        if let Some(projected) = normalize(projected)
            && point_on_arc(projected, start, end, normal)
        {
            candidates.push((
                angular_distance(point_to_lonlat(point), lonlat_from_vector(projected)),
                lonlat_from_vector(projected),
            ));
        }
    }
    candidates.push((
        angular_distance(point_to_lonlat(point), edge.start),
        edge.start,
    ));
    candidates.push((angular_distance(point_to_lonlat(point), edge.end), edge.end));
    let mut best = candidates[0];
    for candidate in candidates.into_iter().skip(1) {
        if candidate.0 < best.0 {
            best = candidate;
        }
    }
    best
}

fn point_to_lonlat(point: [f64; 3]) -> LonLat {
    lonlat_from_vector(point)
}

fn arcs_intersect(left: &Edge, right: &Edge) -> bool {
    let a = vector(left.start.lon, left.start.lat);
    let b = vector(left.end.lon, left.end.lat);
    let c = vector(right.start.lon, right.start.lat);
    let d = vector(right.end.lon, right.end.lat);

    let ab_normal = normalize(cross(a, b));
    let cd_normal = normalize(cross(c, d));
    let Some(ab_normal) = ab_normal else {
        return endpoint_on_edge(&left.start, right);
    };
    let Some(cd_normal) = cd_normal else {
        return endpoint_on_edge(&right.start, left);
    };
    let intersection = normalize(cross(ab_normal, cd_normal));
    let Some(intersection) = intersection else {
        return arcs_overlap(left, right);
    };
    point_on_arc(intersection, a, b, ab_normal) && point_on_arc(intersection, c, d, cd_normal)
        || point_on_arc(
            [-intersection[0], -intersection[1], -intersection[2]],
            a,
            b,
            ab_normal,
        ) && point_on_arc(
            [-intersection[0], -intersection[1], -intersection[2]],
            c,
            d,
            cd_normal,
        )
}

fn endpoint_on_edge(endpoint: &LonLat, edge: &Edge) -> bool {
    let endpoint = vector(endpoint.lon, endpoint.lat);
    let start = vector(edge.start.lon, edge.start.lat);
    let end = vector(edge.end.lon, edge.end.lat);
    let Some(normal) = normalize(cross(start, end)) else {
        return dot(endpoint, start) > 0.0;
    };
    point_on_arc(endpoint, start, end, normal)
}

fn arcs_overlap(left: &Edge, right: &Edge) -> bool {
    // Collinear great-circle arcs overlap when any endpoint lies on the other.
    endpoint_on_edge(&left.start, right) || endpoint_on_edge(&left.end, right)
}

fn edge_pair_angular_distance(left: &Edge, right: &Edge) -> (f64, LonLat, LonLat) {
    let candidates = [
        {
            let (distance, closest) = edge_to_point(left, right.start);
            (distance, closest, right.start)
        },
        {
            let (distance, closest) = edge_to_point(left, right.end);
            (distance, closest, right.end)
        },
        {
            let (distance, closest) = edge_to_point(right, left.start);
            (distance, left.start, closest)
        },
        {
            let (distance, closest) = edge_to_point(right, left.end);
            (distance, left.end, closest)
        },
    ];
    let mut best = candidates[0];
    for candidate in candidates.into_iter().skip(1) {
        if candidate.0 < best.0 {
            best = candidate;
        }
    }
    best
}

fn metric_distance(
    left: LonLat,
    right: LonLat,
    sphere_radius: Option<f64>,
    geod: &Geodesic,
) -> f64 {
    match sphere_radius {
        Some(radius) => sphere_distance(left, right, radius),
        None => geod.inverse(left.lat, left.lon, right.lat, right.lon),
    }
}

fn shape_distance_metric(
    left: &geo::Geometry<f64>,
    right: &geo::Geometry<f64>,
    sphere_radius: Option<f64>,
    geod: &Geodesic,
) -> f64 {
    let Some(left) = shape_from_geometry(left) else {
        return f64::NAN;
    };
    let Some(right) = shape_from_geometry(right) else {
        return f64::NAN;
    };
    shape_distance(&left, &right, sphere_radius, geod)
}

fn shape_distance(left: &Shape, right: &Shape, sphere_radius: Option<f64>, geod: &Geodesic) -> f64 {
    match (left, right) {
        (Shape::Collection(children), _) => children
            .iter()
            .map(|child| shape_distance(child, right, sphere_radius, geod))
            .fold(f64::INFINITY, f64::min),
        (_, Shape::Collection(children)) => children
            .iter()
            .map(|child| shape_distance(left, child, sphere_radius, geod))
            .fold(f64::INFINITY, f64::min),
        (Shape::Point(left), Shape::Point(right)) => {
            metric_distance(*left, *right, sphere_radius, geod)
        }
        (Shape::Point(point), Shape::Edges(edges)) | (Shape::Edges(edges), Shape::Point(point)) => {
            point_edges_distance(point, edges, sphere_radius, geod)
        }
        (Shape::Point(point), Shape::Polygon(polygon))
        | (Shape::Polygon(polygon), Shape::Point(point)) => {
            if polygon.intersects(&geo::Point::new(point.lon, point.lat)) {
                0.0
            } else {
                point_edges_distance(point, &polygon_edges(polygon), sphere_radius, geod)
            }
        }
        (Shape::Edges(left), Shape::Edges(right)) => {
            let mut best = f64::INFINITY;
            for left in left {
                for right in right {
                    if arcs_intersect(left, right) {
                        return 0.0;
                    }
                    let (_, left_point, right_point) = edge_pair_angular_distance(left, right);
                    best = best.min(metric_distance(
                        left_point,
                        right_point,
                        sphere_radius,
                        geod,
                    ));
                }
            }
            best
        }
        (Shape::Polygon(left), Shape::Polygon(right)) => {
            if left.intersects(right) {
                0.0
            } else {
                polygon_edges_distance(left, &polygon_edges(right), sphere_radius, geod)
            }
        }
        (Shape::Polygon(polygon), Shape::Edges(edges))
        | (Shape::Edges(edges), Shape::Polygon(polygon)) => {
            if edges.iter().any(|edge| {
                polygon.intersects(&geo::Point::new(edge.start.lon, edge.start.lat))
                    || polygon.intersects(&geo::Point::new(edge.end.lon, edge.end.lat))
            }) {
                0.0
            } else {
                polygon_edges_distance(polygon, edges, sphere_radius, geod)
            }
        }
    }
}

fn point_edges_distance(
    point: &LonLat,
    edges: &[Edge],
    sphere_radius: Option<f64>,
    geod: &Geodesic,
) -> f64 {
    edges
        .iter()
        .map(|edge| {
            let (_, closest) = edge_to_point(edge, *point);
            metric_distance(*point, closest, sphere_radius, geod)
        })
        .fold(f64::INFINITY, f64::min)
}

fn polygon_edges(polygon: &geo::Polygon<f64>) -> Vec<Edge> {
    let mut edges = Vec::new();
    for ring in std::iter::once(polygon.exterior()).chain(polygon.interiors()) {
        edges.extend(edges_from_coords(ring.coords()));
    }
    edges
}

fn polygon_edges_distance(
    left: &geo::Polygon<f64>,
    right: &[Edge],
    sphere_radius: Option<f64>,
    geod: &Geodesic,
) -> f64 {
    let left_edges = polygon_edges(left);
    shape_distance(
        &Shape::Edges(left_edges),
        &Shape::Edges(right.to_vec()),
        sphere_radius,
        geod,
    )
}

fn edges_from_coords<'a>(coords: impl Iterator<Item = &'a geo::Coord<f64>>) -> Vec<Edge> {
    let mut edges = Vec::new();
    let mut previous: Option<geo::Coord<f64>> = None;
    for coord in coords {
        if let Some(previous) = previous {
            edges.push(Edge {
                start: LonLat {
                    lon: previous.x,
                    lat: previous.y,
                },
                end: LonLat {
                    lon: coord.x,
                    lat: coord.y,
                },
            });
        }
        previous = Some(*coord);
    }
    edges
}

fn shape_from_geometry(geometry: &geo::Geometry<f64>) -> Option<Shape> {
    match geometry {
        geo::Geometry::Point(point) => Some(Shape::Point(lonlat_from_point(point))),
        geo::Geometry::Line(line) => Some(Shape::Edges(vec![Edge {
            start: LonLat {
                lon: line.start.x,
                lat: line.start.y,
            },
            end: LonLat {
                lon: line.end.x,
                lat: line.end.y,
            },
        }])),
        geo::Geometry::LineString(line_string) => {
            if line_string.0.len() < 2 {
                return None;
            }
            Some(Shape::Edges(edges_from_coords(line_string.coords())))
        }
        geo::Geometry::Polygon(polygon)
            if polygon.exterior().0.is_empty() && polygon.interiors().is_empty() =>
        {
            None
        }
        geo::Geometry::Polygon(polygon) => Some(Shape::Polygon(polygon.clone())),
        geo::Geometry::MultiPoint(multi_point) => collection_from_shapes(
            multi_point
                .iter()
                .map(|point| shape_from_geometry(&geo::Geometry::Point(*point))),
        ),
        geo::Geometry::MultiLineString(multi_line_string) => {
            collection_from_shapes(multi_line_string.iter().map(|line_string| {
                shape_from_geometry(&geo::Geometry::LineString(line_string.clone()))
            }))
        }
        geo::Geometry::MultiPolygon(multi_polygon) => collection_from_shapes(
            multi_polygon
                .iter()
                .map(|polygon| shape_from_geometry(&geo::Geometry::Polygon(polygon.clone()))),
        ),
        geo::Geometry::GeometryCollection(collection) => {
            collection_from_shapes(collection.iter().map(shape_from_geometry))
        }
        geo::Geometry::Rect(rect) => {
            let polygon = rect.to_polygon();
            shape_from_geometry(&geo::Geometry::Polygon(polygon))
        }
        geo::Geometry::Triangle(triangle) => {
            let polygon = triangle.to_polygon();
            shape_from_geometry(&geo::Geometry::Polygon(polygon))
        }
    }
}

fn collection_from_shapes(shapes: impl Iterator<Item = Option<Shape>>) -> Option<Shape> {
    let shapes = shapes.flatten().collect::<Vec<_>>();
    if shapes.is_empty() {
        None
    } else {
        Some(Shape::Collection(shapes))
    }
}

fn shape_length(shape: &Shape, geod: &Geodesic) -> f64 {
    match shape {
        Shape::Point(_) => 0.0,
        Shape::Edges(edges) => edges
            .iter()
            .map(|edge| {
                let distance: f64 =
                    geod.inverse(edge.start.lat, edge.start.lon, edge.end.lat, edge.end.lon);
                distance
            })
            .sum(),
        Shape::Polygon(polygon) => polygon_edges(polygon)
            .iter()
            .map(|edge| {
                let distance: f64 =
                    geod.inverse(edge.start.lat, edge.start.lon, edge.end.lat, edge.end.lon);
                distance
            })
            .sum(),
        Shape::Collection(shapes) => shapes.iter().map(|shape| shape_length(shape, geod)).sum(),
    }
}
