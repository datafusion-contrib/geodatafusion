//! Google encoded polyline input and output.

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType as NativeGeometryType, LineStringTrait,
    MultiPointTrait, PointTrait,
};
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Dimension, LineStringType, Metadata};
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString};

use super::format::{
    coerce_geometry_with_optional_ints, coerce_text_with_optional_ints, is_plain_wkt_field,
    plain_wkt_array,
};
use super::geojson::{srid_from_metadata, text_value};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    expand_rect_to_geometry, field_metadata, geometry_array_from_arrow, metadata_for_srid,
};

const DEFAULT_PRECISION: i32 = 5;
const ENCODED_POLYLINE_SRID: i32 = 4326;
const MAX_ENCODE_PRECISION: i32 = 18;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineFromEncodedPolyline {
    signature: Signature,
}

impl LineFromEncodedPolyline {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for LineFromEncodedPolyline {
    fn default() -> Self {
        Self::new()
    }
}

static LINE_FROM_ENCODED_POLYLINE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineFromEncodedPolyline {
    fn name(&self) -> &str {
        "st_linefromencodedpolyline"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_text_with_optional_ints(arg_types, 1, "ST_LineFromEncodedPolyline")
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_LineFromEncodedPolyline output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let geo_type = LineStringType::new(Dimension::XY, metadata_for_srid(ENCODED_POLYLINE_SRID))
            .with_coord_type(CoordType::Separated);
        Ok(geo_type.to_field(input_field.name(), nullable).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_from_polyline_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_FROM_ENCODED_POLYLINE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a LineString from a Google encoded polyline. The optional precision \
                 defaults to 5; negative precision also selects the default. Output SRID is 4326. \
                 Deltas and the running coordinate total must fit in signed 64-bit integers; \
                 truncated input and larger values raise an error instead of returning wrapped \
                 coordinates.",
                "ST_LineFromEncodedPolyline(polyline[, precision])",
            )
            .with_argument("polyline", "encoded polyline text")
            .with_argument("precision", "optional decimal precision")
            .build()
        }))
    }
}

fn line_from_polyline_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
    let geo_type =
        LineStringType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let mut builder = geoarrow_array::builder::LineStringBuilder::new(geo_type);

    for row in 0..arrays[0].len() {
        if arrays.iter().any(|array| array.is_null(row)) {
            builder.push_line_string(Option::<&WktLineString<f64>>::None)?;
            continue;
        }

        let Some(text) = text_value(&arrays[0], row) else {
            builder.push_line_string(Option::<&WktLineString<f64>>::None)?;
            continue;
        };
        let precision = precision_value(&arrays, 1, row);
        let coords = decode_polyline(text, precision).map_err(DataFusionError::Execution)?;
        let line = WktLineString::new(
            coords
                .into_iter()
                .map(|(x, y)| WktCoord {
                    x,
                    y,
                    z: None,
                    m: None,
                })
                .collect(),
            WktDimension::XY,
        );
        builder.push_line_string(Some(&line))?;
    }

    Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsEncodedPolyline {
    signature: Signature,
}

impl AsEncodedPolyline {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for AsEncodedPolyline {
    fn default() -> Self {
        Self::new()
    }
}

static AS_ENCODED_POLYLINE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsEncodedPolyline {
    fn name(&self) -> &str {
        "st_asencodedpolyline"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_geometry_with_optional_ints(arg_types, 1, "ST_AsEncodedPolyline", true)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_AsEncodedPolyline output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Field::new(input_field.name(), DataType::Utf8, nullable).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(as_encoded_polyline_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_ENCODED_POLYLINE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Google encoded polyline from a LineString or MultiPoint with SRID 4326. \
                 The optional precision defaults to 5; negative precision also selects the default. \
                 Precision above 18 raises an error because 10^19 does not fit the signed 64-bit \
                 coordinate domain; every encoding it emits decodes with ST_LineFromEncodedPolyline.",
                "ST_AsEncodedPolyline(geometry[, precision])",
            )
            .with_argument("geometry", "LineString or MultiPoint with SRID 4326")
            .with_argument("precision", "optional decimal precision")
            .build()
        }))
    }
}

fn as_encoded_polyline_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        let mut output = arrow_array::builder::StringBuilder::new();
        for _ in 0..args.number_rows {
            output.append_null();
        }
        return Ok(ColumnarValue::Array(Arc::new(output.finish())));
    }
    let plain_wkt = is_plain_wkt_field(&args.arg_fields[0]);
    let metadata = if plain_wkt {
        Arc::new(Metadata::default())
    } else {
        field_metadata(&args.arg_fields[0])?
    };
    let srid = srid_from_metadata(&metadata);
    let input = if plain_wkt {
        plain_wkt_array(&arrays[0])?
    } else {
        geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?
    };
    let geometry_nulls = input.logical_nulls();
    let expanded = expand_rect_to_geometry(input.as_ref())?;
    let input: &dyn GeoArrowArray = expanded
        .as_ref()
        .map_or(input.as_ref(), |expanded| expanded);
    let wkb = to_wkb::<i32>(input)?;
    let mut output = arrow_array::builder::StringBuilder::new();

    for row in 0..wkb.len() {
        if geometry_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.is_null(row))
            || arrays.iter().skip(1).any(|array| array.is_null(row))
        {
            output.append_null();
            continue;
        }
        if srid != ENCODED_POLYLINE_SRID {
            return Err(
                DataFusionError::Execution("Only SRID 4326 is supported.".to_string()).into(),
            );
        }
        let precision = precision_value(&arrays, 1, row);
        let geometry = wkb
            .value(row)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let encoded = encode_geometry(&geometry, precision).map_err(DataFusionError::Execution)?;
        output.append_value(encoded);
    }

    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

fn precision_value(arrays: &[ArrayRef], index: usize, row: usize) -> i32 {
    let Some(array) = arrays.get(index) else {
        return DEFAULT_PRECISION;
    };
    if array.is_null(row) {
        return DEFAULT_PRECISION;
    }
    match array.data_type() {
        DataType::Int64 => {
            let value = array
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row);
            i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX })
        }
        DataType::Int32 => array
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(row),
        DataType::Null => DEFAULT_PRECISION,
        other => panic!("unexpected precision array type {other}"),
    }
}

fn decode_polyline(text: &str, precision: i32) -> std::result::Result<Vec<(f64, f64)>, String> {
    let precision = if precision < 0 {
        DEFAULT_PRECISION
    } else {
        precision
    };
    if precision > 308 {
        return Err(format!(
            "encoded polyline precision {precision} exceeds the supported range"
        ));
    }

    let bytes = text.as_bytes();
    let scale = 10f64.powi(precision);
    let mut latitude = 0i64;
    let mut longitude = 0i64;
    let mut index = 0usize;
    let mut points = Vec::new();

    while index < bytes.len() {
        let delta_lat = decode_polyline_value(bytes, &mut index)?;
        latitude = latitude.checked_add(delta_lat).ok_or_else(|| {
            "encoded polyline accumulated latitude exceeds the supported 64-bit range".to_string()
        })?;
        let delta_lon = decode_polyline_value(bytes, &mut index)?;
        longitude = longitude.checked_add(delta_lon).ok_or_else(|| {
            "encoded polyline accumulated longitude exceeds the supported 64-bit range".to_string()
        })?;
        points.push((longitude as f64 / scale, latitude as f64 / scale));
    }

    Ok(points)
}

fn decode_polyline_value(bytes: &[u8], index: &mut usize) -> std::result::Result<i64, String> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        if shift >= 64 {
            return Err(
                "encoded polyline value exceeds the supported 64-bit delta range".to_string(),
            );
        }
        let byte = if *index < bytes.len() {
            let byte = bytes[*index];
            *index += 1;
            byte
        } else if shift == 0 {
            // PostGIS reads the string terminator as a zero byte for a value
            // that starts after the last encoded byte; keep that pinned
            // behavior for inputs such as "?".
            0
        } else {
            return Err("encoded polyline is truncated".to_string());
        };

        let c = (byte as i8) as i32 - 63;
        let low = (c & 0x1f) as u64;
        if low > (u64::MAX >> shift) {
            return Err(
                "encoded polyline value exceeds the supported 64-bit delta range".to_string(),
            );
        }
        result |= low << shift;
        if c < 0x20 {
            break;
        }
        shift += 5;
    }
    let magnitude = (result >> 1) as i64;
    Ok(if result & 1 != 0 {
        -magnitude - 1
    } else {
        magnitude
    })
}

fn encode_geometry(
    geometry: &impl GeometryTrait<T = f64>,
    precision: i32,
) -> std::result::Result<String, String> {
    let precision = if precision < 0 {
        DEFAULT_PRECISION
    } else {
        precision
    };
    if precision > MAX_ENCODE_PRECISION {
        return Err(format!(
            "encoded polyline precision {precision} exceeds the supported range 0..={MAX_ENCODE_PRECISION}"
        ));
    }
    let scale = 10f64.powi(precision);
    let points = match geometry.as_type() {
        NativeGeometryType::LineString(line) => line_coords(line)?,
        NativeGeometryType::MultiPoint(multi_point) => multi_point_coords(multi_point)?,
        _other => {
            return Err(format!(
                "lwgeom_to_encoded_polyline: '{}' geometry type not supported",
                geometry_type_name(geometry)
            ));
        }
    };
    if points.is_empty() {
        return Ok(String::new());
    }

    let mut rounded = Vec::with_capacity(points.len());
    for (x, y) in points {
        let scaled_x = round_coordinate(x, scale)?;
        let scaled_y = round_coordinate(y, scale)?;
        rounded.push((scaled_x, scaled_y));
    }

    let mut deltas = Vec::with_capacity(rounded.len() * 2);
    let mut previous = (0i64, 0i64);
    for (x, y) in rounded {
        deltas.push(delta_i64(y, previous.1)?);
        deltas.push(delta_i64(x, previous.0)?);
        previous = (x, y);
    }

    let mut output = String::new();
    for delta in deltas {
        let encoded = zigzag_encode(delta)?;
        let mut value = encoded;
        while value >= 0x20 {
            output.push(char::from_u32((0x20 | (value & 0x1f)) as u32 + 63).unwrap());
            value >>= 5;
        }
        output.push(char::from_u32(value as u32 + 63).unwrap());
    }
    Ok(output)
}

fn line_coords(
    line: &impl LineStringTrait<T = f64>,
) -> std::result::Result<Vec<(f64, f64)>, String> {
    let mut points = Vec::with_capacity(line.num_coords());
    for index in 0..line.num_coords() {
        let coord = line
            .coord(index)
            .ok_or_else(|| "missing line coordinate".to_string())?;
        points.push((coord.x(), coord.y()));
    }
    Ok(points)
}

fn multi_point_coords(
    multi_point: &impl MultiPointTrait<T = f64>,
) -> std::result::Result<Vec<(f64, f64)>, String> {
    let mut points = Vec::with_capacity(multi_point.num_points());
    for index in 0..multi_point.num_points() {
        let point = multi_point
            .point(index)
            .ok_or_else(|| "missing multipoint member".to_string())?;
        let coord = point
            .coord()
            .ok_or_else(|| "empty point in MultiPoint is not supported".to_string())?;
        points.push((coord.x(), coord.y()));
    }
    Ok(points)
}

fn round_coordinate(value: f64, scale: f64) -> std::result::Result<i64, String> {
    // `i64::MAX` is not representable in f64, so its nearest f64 value is 2^63.
    // The upper bound is therefore exclusive, while the exactly representable
    // -2^63 stays a legal coordinate.
    const I64_MIN_AS_F64: f64 = -9_223_372_036_854_775_808.0;
    const I64_MAX_EXCLUSIVE_AS_F64: f64 = 9_223_372_036_854_775_808.0;

    let scaled = (value * scale).round();
    if !scaled.is_finite() || scaled < I64_MIN_AS_F64 || scaled >= I64_MAX_EXCLUSIVE_AS_F64 {
        return Err(format!(
            "encoded polyline coordinate {value} exceeds the supported integer range"
        ));
    }
    Ok(scaled as i64)
}

fn zigzag_encode(value: i64) -> std::result::Result<u64, String> {
    let doubled = (value as i128) << 1;
    let encoded = if value < 0 { !doubled } else { doubled };
    u64::try_from(encoded)
        .map_err(|_| "encoded polyline delta exceeds the supported 64-bit range".to_string())
}

fn delta_i64(value: i64, previous: i64) -> std::result::Result<i64, String> {
    i64::try_from(i128::from(value) - i128::from(previous))
        .map_err(|_| "encoded polyline delta exceeds the supported 64-bit range".to_string())
}

fn geometry_type_name(geometry: &impl GeometryTrait<T = f64>) -> &'static str {
    match geometry.as_type() {
        NativeGeometryType::Point(_) => "Point",
        NativeGeometryType::LineString(_) => "LineString",
        NativeGeometryType::Polygon(_) => "Polygon",
        NativeGeometryType::MultiPoint(_) => "MultiPoint",
        NativeGeometryType::MultiLineString(_) => "MultiLineString",
        NativeGeometryType::MultiPolygon(_) => "MultiPolygon",
        NativeGeometryType::GeometryCollection(_) => "GeometryCollection",
        NativeGeometryType::Rect(_) => "Rect",
        NativeGeometryType::Triangle(_) => "Triangle",
        NativeGeometryType::Line(_) => "Line",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_example_decodes_and_roundtrips() {
        let encoded = "_p~iF~ps|U_ulLnnqC_mqNvxq`@";
        let points = decode_polyline(encoded, 5).unwrap();
        assert_eq!(
            points,
            vec![(-120.2, 38.5), (-120.95, 40.7), (-126.453, 43.252)]
        );
    }

    #[test]
    fn truncated_input_is_rejected_explicitly() {
        let error = decode_polyline("_", 5).unwrap_err();
        assert!(error.contains("truncated"));
        assert_eq!(decode_polyline("?", 5).unwrap(), vec![(-0.00001, 0.0)]);
    }

    #[test]
    fn oversized_varint_is_rejected_explicitly() {
        let error = decode_polyline("zzzzzzzzzzzzz", 5).unwrap_err();
        assert!(error.contains("64-bit"), "{error}");
    }

    #[test]
    fn continuation_cut_off_by_end_of_input_is_rejected() {
        // "__" ends inside the second delta instead of completing it with a
        // phantom string terminator.
        let error = decode_polyline("__", 5).unwrap_err();
        assert!(error.contains("truncated"), "{error}");
    }

    #[test]
    fn delta_bits_are_checked_before_they_wrap() {
        // Twelve continuation groups followed by five more bits at shift 60
        // do not fit in the 64-bit delta range.
        let error = decode_polyline_value(&[b'~'; 13], &mut 0).unwrap_err();
        assert!(error.contains("64-bit delta range"), "{error}");

        // The signed i64 minimum still round-trips.
        let encoded = b"~~~~~~~~~~~~N";
        assert_eq!(decode_polyline_value(encoded, &mut 0).unwrap(), i64::MIN);
    }

    #[test]
    fn precision_18_output_decodes_back_to_the_same_line() {
        let line = WktLineString::new(
            vec![
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
                WktCoord {
                    x: 1.0,
                    y: 1.0,
                    z: None,
                    m: None,
                },
            ],
            WktDimension::XY,
        );
        let encoded = encode_geometry(&line, 18).unwrap();
        assert_eq!(
            decode_polyline(&encoded, 18).unwrap(),
            vec![(0.0, 0.0), (1.0, 1.0)]
        );
    }

    #[test]
    fn coordinate_domain_rejects_positive_two_to_the_sixty_third() {
        // 2^63 is one past i64::MAX and is representable in f64; a saturating
        // `as i64` cast would silently clamp it to i64::MAX.
        let error = round_coordinate(9_223_372_036_854_775_808.0, 1.0).unwrap_err();
        assert!(
            error.contains("exceeds the supported integer range"),
            "{error}"
        );

        // -2^63 is representable and stays legal.
        assert_eq!(
            round_coordinate(-9_223_372_036_854_775_808.0, 1.0).unwrap(),
            i64::MIN
        );

        // The largest f64 below +2^63 is 2^63 - 1024 and is still exact.
        assert_eq!(
            round_coordinate(9_223_372_036_854_774_784.0, 1.0).unwrap(),
            9_223_372_036_854_774_784
        );
    }
}
