//! GeoJSON geometry input and output.
//!
//! The parser follows the GeoJSON geometry fragment contract implemented by
//! PostGIS: only geometry objects are accepted, absent CRS means EPSG:4326,
//! and a coordinate array that contains a third ordinate upgrades the whole
//! geometry to XYZ.

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
use datafusion::scalar::ScalarValue;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType as NativeGeometryType,
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait,
    PolygonTrait, RectTrait,
};
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, GeometryType, Metadata};
use serde_json::{Map, Value};
use wkt::Wkt;
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, GeometryCollection as WktGeometryCollection,
    LineString as WktLineString, MultiLineString as WktMultiLineString,
    MultiPoint as WktMultiPoint, MultiPolygon as WktMultiPolygon, Point as WktPoint,
    Polygon as WktPolygon,
};

use super::common::geometry_field_from_physical;
use super::format::{
    coerce_geometry_with_optional_ints, format_bbox_ordinate, format_ordinate, is_plain_wkt_field,
    plain_wkt_array,
};
use super::geojson_record::RecordGeoJsonSerializer;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, expand_rect_to_geometry, field_metadata, geometry_array_from_arrow,
    metadata_for_srid,
};

const GEOJSON_DEFAULT_SRID: i32 = 4326;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeomFromGeoJSON;

impl GeomFromGeoJSON {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GeomFromGeoJSON {
    fn default() -> Self {
        Self::new()
    }
}

static GEOM_FROM_GEOJSON_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromGeoJSON {
    fn name(&self) -> &str {
        "st_geomfromgeojson"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| {
            Signature::uniform(
                1,
                vec![
                    DataType::Utf8,
                    DataType::LargeUtf8,
                    DataType::Utf8View,
                    DataType::Null,
                ],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_GeomFromGeoJSON output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        let srid = geojson_literal_srid(&args)?;
        let metadata = metadata_for_srid(srid);
        let geo_type = GeometryType::new(metadata).with_coord_type(CoordType::Separated);
        Ok(geo_type
            .to_field(input_field.name(), input_field.is_nullable())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(geom_from_geojson_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_GEOJSON_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry from a GeoJSON geometry fragment. Feature and \
                 FeatureCollection documents are rejected. SRID defaults to EPSG:4326 and may \
                 be supplied by the object's CRS member. For column input the output field is \
                 planned as EPSG:4326, so every non-null row must use that SRID. EPSG codes are \
                 retained as authority metadata without querying spatial_ref_sys.",
                "ST_GeomFromGeoJSON(json)",
            )
            .with_argument("json", "GeoJSON geometry fragment")
            .build()
        }))
    }
}

fn geom_from_geojson_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .expect("ST_GeomFromGeoJSON has one argument");
    let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
    let planned_srid = srid_from_metadata(&metadata);
    let to_type = GeometryType::new(metadata).with_coord_type(CoordType::Separated);
    let mut builder = StrictGeometryBuilder::new(to_type);

    for row in 0..array.len() {
        let Some(text) = text_value(&array, row) else {
            builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            continue;
        };

        let (geometry, row_srid) = parse_geojson(text).map_err(execution_error)?;
        if row_srid != planned_srid {
            return Err(DataFusionError::Execution(format!(
                "ST_GeomFromGeoJSON row {row} has SRID {row_srid}, but the output field is \
                 declared with SRID {planned_srid}; GeoArrow stores CRS in field metadata"
            ))
            .into());
        }
        builder.push_geometry(Some(&geometry))?;
    }

    Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsGeoJSON {
    signature: Signature,
}

impl AsGeoJSON {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for AsGeoJSON {
    fn default() -> Self {
        Self::new()
    }
}

static AS_GEOJSON_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsGeoJSON {
    fn name(&self) -> &str {
        "st_asgeojson"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_geojson_types(arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_AsGeoJSON output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        validate_geojson_argument_fields(args.arg_fields)?;
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Field::new(input_field.name(), DataType::Utf8, nullable).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if is_record_input_field(&args.arg_fields[0]) {
            Ok(as_geojson_record_impl(args)?)
        } else {
            Ok(as_geojson_geometry_impl(args)?)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_GEOJSON_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a GeoJSON geometry object or a Feature for a record. For geometry, \
                 options uses 1=bbox, 2=short CRS, 4=long CRS, and 8=short CRS when the SRID \
                 is not 4326. For records, geom_column defaults to the first geometry field, \
                 and id_column promotes a property to the Feature id member. Record properties \
                 support booleans, numeric types, UTF-8, binary, lists, and structs; other \
                 Arrow types return an explicit error instead of being dropped or stringified.",
                "ST_AsGeoJSON(geometry[, maxdecimaldigits[, options]]) or \
                 ST_AsGeoJSON(record[, geom_column[, maxdecimaldigits[, pretty_bool[, id_column]]]])",
            )
            .with_argument("geometry_or_record", "input geometry or record")
            .with_argument("geom_column", "optional record geometry column name")
            .with_argument("maxdecimaldigits", "optional maximum decimal digits")
            .with_argument("options_or_pretty", "optional CRS/bbox bit mask or pretty flag")
            .with_argument("id_column", "optional record feature id column name")
            .build()
        }))
    }
}

fn coerce_geojson_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.is_empty() {
        return Err(DataFusionError::Plan(
            "ST_AsGeoJSON expects between 1 and 5 arguments".to_string(),
        ));
    }

    if is_geometry_physical_type(&arg_types[0]) {
        return coerce_geometry_with_optional_ints(arg_types, 2, "ST_AsGeoJSON", true);
    }

    if !matches!(arg_types[0], DataType::Struct(_)) {
        return Err(DataFusionError::Plan(format!(
            "ST_AsGeoJSON expects a geometry or record argument, got {}",
            arg_types[0]
        )));
    }
    if arg_types.len() > 5 {
        return Err(DataFusionError::Plan(
            "ST_AsGeoJSON record form expects between 1 and 5 arguments".to_string(),
        ));
    }

    let mut coerced = Vec::with_capacity(arg_types.len());
    coerced.push(arg_types[0].clone());
    for (index, data_type) in arg_types.iter().enumerate().skip(1) {
        let target = match index {
            1 | 4 => {
                if data_type == &DataType::Null
                    || matches!(
                        data_type,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    )
                {
                    DataType::Utf8
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "ST_AsGeoJSON record column names must be text, got {data_type}"
                    )));
                }
            }
            2 => {
                if data_type == &DataType::Null || data_type.is_integer() {
                    DataType::Int64
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "ST_AsGeoJSON maxdecimaldigits must be an integer, got {data_type}"
                    )));
                }
            }
            3 => {
                if data_type == &DataType::Null || data_type == &DataType::Boolean {
                    DataType::Boolean
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "ST_AsGeoJSON pretty_bool must be boolean, got {data_type}"
                    )));
                }
            }
            _ => unreachable!("record argument count is validated above"),
        };
        coerced.push(target);
    }
    Ok(coerced)
}

fn is_geometry_physical_type(data_type: &DataType) -> bool {
    data_type == &DataType::Null
        || matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
        || crate::data_types::any_geometry_type().contains(data_type)
}

fn is_record_input_field(field: &Field) -> bool {
    matches!(field.data_type(), DataType::Struct(_))
        && geometry_field_from_physical(field).is_none()
}

fn validate_geojson_argument_fields(fields: &[FieldRef]) -> Result<()> {
    if is_record_input_field(&fields[0]) {
        if fields.len() > 5 {
            return Err(DataFusionError::Plan(
                "ST_AsGeoJSON record form expects between 1 and 5 arguments".to_string(),
            ));
        }
        return Ok(());
    }
    coerce_geometry_with_optional_ints(
        &fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>(),
        2,
        "ST_AsGeoJSON",
        true,
    )
    .map(|_| ())
}

fn as_geojson_record_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let records = arrays[0].as_struct();
    let serializer = RecordGeoJsonSerializer::new(&args.arg_fields[0], &arrays[0])?;
    let mut output = arrow_array::builder::StringBuilder::new();

    for row in 0..args.number_rows {
        if records.is_null(row) || arrays.iter().skip(1).any(|array| array.is_null(row)) {
            output.append_null();
            continue;
        }

        let geom_column = arrays.get(1).and_then(|array| text_value(array, row));
        let precision = arrays
            .get(2)
            .and_then(|array| robust_i64_value(array, row))
            .map(|value| value as i32)
            .unwrap_or(9);
        let pretty = arrays
            .get(3)
            .and_then(|array| boolean_value(array, row))
            .unwrap_or(false);
        let id_column = arrays.get(4).and_then(|array| text_value(array, row));

        output.append_value(serializer.write_row(
            records,
            row,
            geom_column,
            precision,
            pretty,
            id_column,
        )?);
    }

    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

fn as_geojson_geometry_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
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
    let input: Arc<dyn GeoArrowArray> = if plain_wkt {
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

        let precision = arrays
            .get(1)
            .and_then(|array| robust_i64_value(array, row))
            .map(|value| value as i32)
            .unwrap_or(9);
        let options = arrays
            .get(2)
            .and_then(|array| robust_i64_value(array, row))
            .map(|value| value as i32)
            .unwrap_or(8);
        let bbox_precision = if precision < 0 { 6 } else { precision };
        if bbox_precision > 1024 {
            return Err(DataFusionError::Execution(format!(
                "ST_AsGeoJSON bbox precision {bbox_precision} exceeds the supported range"
            ))
            .into());
        }
        let geometry = wkb
            .value(row)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let mut json = String::new();
        write_geometry(
            &mut json,
            &geometry,
            precision,
            options,
            srid,
            bbox_precision,
            true,
        )
        .map_err(execution_error)?;
        output.append_value(json);
    }

    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

pub(crate) fn text_value(array: &ArrayRef, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row)),
        DataType::Utf8View => Some(array.as_string_view().value(row)),
        DataType::Null => None,
        other => panic!("unexpected text array type {other}"),
    }
}

fn robust_i64_value(array: &ArrayRef, row: usize) -> Option<i64> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Int64 => Some(
            array
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        ),
        DataType::Int32 => Some(i64::from(
            array
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(row),
        )),
        DataType::Int16 => Some(i64::from(
            array
                .as_primitive::<arrow_array::types::Int16Type>()
                .value(row),
        )),
        DataType::Int8 => Some(i64::from(
            array
                .as_primitive::<arrow_array::types::Int8Type>()
                .value(row),
        )),
        DataType::UInt64 => Some(
            i64::try_from(
                array
                    .as_primitive::<arrow_array::types::UInt64Type>()
                    .value(row),
            )
            .ok()?,
        ),
        DataType::UInt32 => Some(i64::from(
            array
                .as_primitive::<arrow_array::types::UInt32Type>()
                .value(row),
        )),
        DataType::UInt16 => Some(i64::from(
            array
                .as_primitive::<arrow_array::types::UInt16Type>()
                .value(row),
        )),
        DataType::UInt8 => Some(i64::from(
            array
                .as_primitive::<arrow_array::types::UInt8Type>()
                .value(row),
        )),
        DataType::Null => None,
        other => panic!("unexpected integer array type {other}"),
    }
}

fn boolean_value(array: &ArrayRef, row: usize) -> Option<bool> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Boolean => Some(array.as_boolean().value(row)),
        DataType::Null => None,
        other => panic!("unexpected boolean array type {other}"),
    }
}

fn geojson_literal_srid(args: &ReturnFieldArgs) -> Result<i32> {
    let Some(Some(value)) = args.scalar_arguments.first() else {
        return Ok(GEOJSON_DEFAULT_SRID);
    };
    let text = match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => value,
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None) => return Ok(GEOJSON_DEFAULT_SRID),
        _ => return Ok(GEOJSON_DEFAULT_SRID),
    };
    match parse_geojson(text) {
        Ok((_, srid)) => Ok(srid),
        // Keep planning permissive for malformed literals; execution reports the parse error.
        Err(_) => Ok(GEOJSON_DEFAULT_SRID),
    }
}

pub(crate) fn srid_from_metadata(metadata: &Metadata) -> i32 {
    let Some(value) = metadata.crs().crs_value() else {
        return 0;
    };
    match value {
        Value::String(value) => {
            let parsed = parse_srs_name(value);
            if parsed != 0 {
                parsed
            } else {
                value
                    .trim()
                    .parse::<i64>()
                    .ok()
                    .and_then(valid_epsg_code)
                    .unwrap_or(0)
            }
        }
        Value::Object(object) => object
            .get("id")
            .and_then(Value::as_object)
            .and_then(|id| {
                let authority = id.get("authority")?.as_str()?;
                let code = id.get("code")?;
                if !authority.eq_ignore_ascii_case("EPSG") {
                    return None;
                }
                code.as_i64()
                    .or_else(|| code.as_str()?.parse::<i64>().ok())
                    .and_then(valid_epsg_code)
            })
            .unwrap_or(0),
        _ => 0,
    }
}

fn parse_srs_name(name: &str) -> i32 {
    let upper = name.trim().to_ascii_uppercase();
    let digits = if let Some(rest) = upper.strip_prefix("EPSG:") {
        rest
    } else if upper.starts_with("URN:OGC:DEF:CRS:EPSG:") {
        upper.rsplit_once(':').map(|(_, last)| last).unwrap_or("")
    } else {
        return 0;
    };
    digits
        .parse::<i64>()
        .ok()
        .and_then(valid_epsg_code)
        .unwrap_or(0)
}

fn valid_epsg_code(code: i64) -> Option<i32> {
    // PostGIS resolves CRS through spatial_ref_sys. GeoDataFusion only has
    // authority metadata, so it accepts the SRID range that PostGIS accepts
    // for user-defined entries and leaves table lookup to the caller.
    if (1..=998_999).contains(&code) {
        i32::try_from(code).ok()
    } else {
        None
    }
}

fn execution_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RawCoord {
    x: f64,
    y: f64,
    z: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
enum RawGeometry {
    Point(Option<RawCoord>),
    LineString(Vec<RawCoord>),
    Polygon(Vec<Vec<RawCoord>>),
    MultiPoint(Vec<Option<RawCoord>>),
    MultiLineString(Vec<Vec<RawCoord>>),
    MultiPolygon(Vec<Vec<Vec<RawCoord>>>),
    GeometryCollection(Vec<RawGeometry>),
}

struct GeoJsonParser {
    has_z: bool,
}

fn parse_geojson(text: &str) -> std::result::Result<(Wkt<f64>, i32), String> {
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(error) if error.to_string().contains("number out of range") => {
            // The reference JSON parser maps overflowing numeric literals to
            // infinity. serde_json rejects them, so normalize only those
            // tokens and parse the resulting JSON document.
            let normalized = normalize_overflow_numbers(text)?;
            serde_json::from_str(&normalized).map_err(|error| error.to_string())?
        }
        Err(error) => return Err(error.to_string()),
    };
    let object = value
        .as_object()
        .ok_or_else(|| "invalid GeoJSON representation".to_string())?;
    let srid = parse_geojson_crs(object)?;
    let mut parser = GeoJsonParser { has_z: false };
    let raw = parser.parse_geometry(&value)?;
    let dimension = if parser.has_z {
        WktDimension::XYZ
    } else {
        WktDimension::XY
    };
    Ok((raw_to_wkt(&raw, dimension), srid))
}

fn parse_geojson_crs(object: &Map<String, Value>) -> std::result::Result<i32, String> {
    let Some(crs) = member(object, "crs") else {
        return Ok(GEOJSON_DEFAULT_SRID);
    };
    if crs.is_null() {
        return Ok(GEOJSON_DEFAULT_SRID);
    }
    let Some(crs) = crs.as_object() else {
        return Ok(GEOJSON_DEFAULT_SRID);
    };

    let crs_type = member(crs, "type");
    if let Some(properties) = member(crs, "properties").and_then(Value::as_object) {
        // This mirrors the empty-object diagnostic produced by the reference
        // implementation's member lookup.
        if crs_type.is_some() && properties.is_empty() {
            return Err("invalid GeoJSON representation".to_string());
        }
        if crs_type
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("name"))
            && let Some(name) = member(properties, "name").and_then(Value::as_str)
        {
            return Ok(parse_srs_name(name));
        }
    }
    Ok(GEOJSON_DEFAULT_SRID)
}

fn member<'a>(object: &'a Map<String, Value>, name: &str) -> Option<&'a Value> {
    object
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

impl GeoJsonParser {
    fn parse_geometry(&mut self, value: &Value) -> std::result::Result<RawGeometry, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "invalid GeoJSON representation".to_string())?;
        let type_name = member(object, "type")
            .and_then(Value::as_str)
            .ok_or_else(|| "unknown GeoJSON type".to_string())?;

        match type_name.to_ascii_lowercase().as_str() {
            "point" => self.parse_point(object),
            "linestring" => self.parse_line_string(object),
            "polygon" => self.parse_polygon(object),
            "multipoint" => self.parse_multi_point(object),
            "multilinestring" => self.parse_multi_line_string(object),
            "multipolygon" => self.parse_multi_polygon(object),
            "geometrycollection" => self.parse_geometry_collection(object),
            _ => Err("invalid GeoJson representation".to_string()),
        }
    }

    fn coordinates<'a>(
        &self,
        object: &'a Map<String, Value>,
    ) -> std::result::Result<&'a Value, String> {
        let coordinates = member(object, "coordinates")
            .ok_or_else(|| "Unable to find 'coordinates' in GeoJSON string".to_string())?;
        if !coordinates.is_array() {
            return Err("The 'coordinates' in GeoJSON are not an array".to_string());
        }
        Ok(coordinates)
    }

    fn parse_point(
        &mut self,
        object: &Map<String, Value>,
    ) -> std::result::Result<RawGeometry, String> {
        let coordinates = self.coordinates(object)?;
        Ok(RawGeometry::Point(self.parse_coord(coordinates)?))
    }

    fn parse_line_string(
        &mut self,
        object: &Map<String, Value>,
    ) -> std::result::Result<RawGeometry, String> {
        let coordinates = self.coordinates(object)?;
        let mut line = Vec::new();
        for coordinate in array_values(coordinates) {
            if let Some(coord) = self.parse_coord(coordinate)? {
                line.push(coord);
            }
        }
        Ok(RawGeometry::LineString(line))
    }

    fn parse_polygon(
        &mut self,
        object: &Map<String, Value>,
    ) -> std::result::Result<RawGeometry, String> {
        let rings = self.coordinates(object)?;
        Ok(RawGeometry::Polygon(
            self.parse_polygon_rings(rings)?.unwrap_or_default(),
        ))
    }

    fn parse_polygon_rings(
        &mut self,
        rings: &Value,
    ) -> std::result::Result<Option<Vec<Vec<RawCoord>>>, String> {
        if !rings.is_array() {
            return Ok(None);
        }
        let mut result = Vec::new();
        for (ring_index, ring) in array_values(rings).iter().enumerate() {
            if !ring.is_array() {
                return Err("The 'coordinates' in GeoJSON ring are not an array".to_string());
            }
            if ring.as_array().is_some_and(Vec::is_empty) {
                if ring_index == 0 {
                    break;
                }
                continue;
            }
            let mut parsed = Vec::new();
            for coordinate in array_values(ring) {
                let coord = self.parse_coord(coordinate)?.ok_or_else(|| {
                    "The 'coordinates' in GeoJSON are not sufficiently nested".to_string()
                })?;
                parsed.push(coord);
            }
            result.push(parsed);
        }
        Ok(Some(result))
    }

    fn parse_multi_point(
        &mut self,
        object: &Map<String, Value>,
    ) -> std::result::Result<RawGeometry, String> {
        let coordinates = self.coordinates(object)?;
        let mut points = Vec::new();
        for coordinate in array_values(coordinates) {
            points.push(self.parse_coord(coordinate)?);
        }
        Ok(RawGeometry::MultiPoint(points))
    }

    fn parse_multi_line_string(
        &mut self,
        object: &Map<String, Value>,
    ) -> std::result::Result<RawGeometry, String> {
        let coordinates = self.coordinates(object)?;
        let mut lines = Vec::new();
        for line in array_values(coordinates) {
            if !line.is_array() {
                return Err("The 'coordinates' in GeoJSON are not sufficiently nested".to_string());
            }
            let mut parsed = Vec::new();
            for coordinate in array_values(line) {
                if let Some(coord) = self.parse_coord(coordinate)? {
                    parsed.push(coord);
                }
            }
            lines.push(parsed);
        }
        Ok(RawGeometry::MultiLineString(lines))
    }

    fn parse_multi_polygon(
        &mut self,
        object: &Map<String, Value>,
    ) -> std::result::Result<RawGeometry, String> {
        let coordinates = self.coordinates(object)?;
        let mut polygons = Vec::new();
        for polygon in array_values(coordinates) {
            if let Some(polygon) = self.parse_polygon_rings(polygon)? {
                polygons.push(polygon);
            }
        }
        Ok(RawGeometry::MultiPolygon(polygons))
    }

    fn parse_geometry_collection(
        &mut self,
        object: &Map<String, Value>,
    ) -> std::result::Result<RawGeometry, String> {
        let geometries = member(object, "geometries")
            .ok_or_else(|| "Unable to find 'geometries' in GeoJSON string".to_string())?;
        let mut parsed = Vec::new();
        if geometries.is_array() {
            for geometry in array_values(geometries) {
                parsed.push(self.parse_geometry(geometry)?);
            }
        }
        Ok(RawGeometry::GeometryCollection(parsed))
    }

    fn parse_coord(&mut self, value: &Value) -> std::result::Result<Option<RawCoord>, String> {
        let Some(array) = value.as_array() else {
            return Err("The 'coordinates' in GeoJSON are not sufficiently nested".to_string());
        };
        if array.is_empty() {
            return Ok(None);
        }
        if array.len() < 2 {
            return Err("Too few ordinates in GeoJSON".to_string());
        }
        let z = if array.len() > 2 {
            self.has_z = true;
            Some(json_number(&array[2]))
        } else {
            None
        };
        Ok(Some(RawCoord {
            x: json_number(&array[0]),
            y: json_number(&array[1]),
            z,
        }))
    }
}

fn array_values(value: &Value) -> &[Value] {
    value
        .as_array()
        .map(Vec::as_slice)
        .expect("array_values requires an array")
}

fn json_number(value: &Value) -> f64 {
    match value {
        Value::Number(number) => number.as_f64().unwrap_or(0.0),
        Value::String(value) if value == POSITIVE_INFINITY_MARKER => f64::INFINITY,
        Value::String(value) if value == NEGATIVE_INFINITY_MARKER => f64::NEG_INFINITY,
        // The reference JSON parser treats non-numeric ordinates as zero.
        _ => 0.0,
    }
}

const POSITIVE_INFINITY_MARKER: &str = "\0geodatafusion-inf";
const NEGATIVE_INFINITY_MARKER: &str = "\0geodatafusion-negative-inf";

fn normalize_overflow_numbers(text: &str) -> std::result::Result<String, String> {
    let bytes = text.as_bytes();
    let mut output = String::with_capacity(text.len());
    let mut index = 0usize;

    while index < bytes.len() {
        if bytes[index] == b'"' {
            let start = index;
            index += 1;
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' => index = (index + 2).min(bytes.len()),
                    b'"' => {
                        index += 1;
                        break;
                    }
                    _ => index += 1,
                }
            }
            output.push_str(&text[start..index]);
            continue;
        }

        if bytes[index] == b'-' || bytes[index].is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len()
                && matches!(bytes[index], b'0'..=b'9' | b'e' | b'E' | b'+' | b'-' | b'.')
            {
                index += 1;
            }
            let token = &text[start..index];
            match token.parse::<f64>() {
                Ok(value) if value == f64::INFINITY => {
                    output.push_str("\"\\u0000geodatafusion-inf\"");
                }
                Ok(value) if value == f64::NEG_INFINITY => {
                    output.push_str("\"\\u0000geodatafusion-negative-inf\"");
                }
                _ => output.push_str(token),
            }
            continue;
        }

        let character = text[index..]
            .chars()
            .next()
            .ok_or_else(|| "invalid UTF-8 boundary in GeoJSON".to_string())?;
        output.push(character);
        index += character.len_utf8();
    }

    Ok(output)
}

fn raw_to_wkt(geometry: &RawGeometry, dimension: WktDimension) -> Wkt<f64> {
    match geometry {
        RawGeometry::Point(point) => Wkt::Point(WktPoint::new(
            point.map(|coord| raw_coord_to_wkt(coord, dimension)),
            dimension,
        )),
        RawGeometry::LineString(line) => Wkt::LineString(WktLineString::new(
            line.iter()
                .map(|coord| raw_coord_to_wkt(*coord, dimension))
                .collect(),
            dimension,
        )),
        RawGeometry::Polygon(rings) => Wkt::Polygon(WktPolygon::new(
            rings
                .iter()
                .map(|ring| {
                    WktLineString::new(
                        ring.iter()
                            .map(|coord| raw_coord_to_wkt(*coord, dimension))
                            .collect(),
                        dimension,
                    )
                })
                .collect(),
            dimension,
        )),
        RawGeometry::MultiPoint(points) => Wkt::MultiPoint(WktMultiPoint::new(
            points
                .iter()
                .map(|coord| {
                    WktPoint::new(
                        coord.map(|coord| raw_coord_to_wkt(coord, dimension)),
                        dimension,
                    )
                })
                .collect(),
            dimension,
        )),
        RawGeometry::MultiLineString(lines) => Wkt::MultiLineString(WktMultiLineString::new(
            lines
                .iter()
                .map(|line| {
                    WktLineString::new(
                        line.iter()
                            .map(|coord| raw_coord_to_wkt(*coord, dimension))
                            .collect(),
                        dimension,
                    )
                })
                .collect(),
            dimension,
        )),
        RawGeometry::MultiPolygon(polygons) => Wkt::MultiPolygon(WktMultiPolygon::new(
            polygons
                .iter()
                .map(|polygon| {
                    WktPolygon::new(
                        polygon
                            .iter()
                            .map(|ring| {
                                WktLineString::new(
                                    ring.iter()
                                        .map(|coord| raw_coord_to_wkt(*coord, dimension))
                                        .collect(),
                                    dimension,
                                )
                            })
                            .collect(),
                        dimension,
                    )
                })
                .collect(),
            dimension,
        )),
        RawGeometry::GeometryCollection(geometries) => {
            Wkt::GeometryCollection(WktGeometryCollection::new(
                geometries
                    .iter()
                    .map(|geometry| raw_to_wkt(geometry, dimension))
                    .collect(),
                dimension,
            ))
        }
    }
}

fn raw_coord_to_wkt(coord: RawCoord, dimension: WktDimension) -> WktCoord<f64> {
    WktCoord {
        x: coord.x,
        y: coord.y,
        z: (dimension == WktDimension::XYZ).then_some(coord.z.unwrap_or(0.0)),
        m: None,
    }
}

#[derive(Debug, Clone, Copy)]
struct Bounds {
    min_x: f64,
    min_y: f64,
    min_z: f64,
    max_x: f64,
    max_y: f64,
    max_z: f64,
    has_z: bool,
    coordinate_count: usize,
    element_count: usize,
}

impl Bounds {
    fn empty(has_z: bool) -> Self {
        Self {
            min_x: 0.0,
            min_y: 0.0,
            min_z: 0.0,
            max_x: 0.0,
            max_y: 0.0,
            max_z: 0.0,
            has_z,
            coordinate_count: 0,
            element_count: 0,
        }
    }

    fn add_point(&mut self, x: f64, y: f64, z: Option<f64>) {
        if self.coordinate_count == 0 {
            self.min_x = x;
            self.max_x = x;
            self.min_y = y;
            self.max_y = y;
            if let Some(z) = z {
                self.min_z = z;
                self.max_z = z;
            }
        } else {
            self.min_x = self.min_x.min(x);
            self.max_x = self.max_x.max(x);
            self.min_y = self.min_y.min(y);
            self.max_y = self.max_y.max(y);
            if let Some(z) = z {
                self.min_z = self.min_z.min(z);
                self.max_z = self.max_z.max(z);
            }
        }
        self.coordinate_count += 1;
    }
}

fn bounds(geometry: &impl GeometryTrait<T = f64>) -> Bounds {
    let has_z = has_z(geometry.dim());
    let mut result = Bounds::empty(has_z);
    collect_bounds(geometry, &mut result);
    result
}

fn collect_bounds(geometry: &impl GeometryTrait<T = f64>, result: &mut Bounds) {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => {
            if let Some(coord) = point.coord() {
                add_coord_bounds(result, &coord);
            }
        }
        NativeGeometryType::LineString(line) => {
            add_line_bounds(result, line);
        }
        NativeGeometryType::Polygon(polygon) => {
            if let Some(ring) = polygon.exterior() {
                add_line_bounds(result, &ring);
            }
            for ring in polygon.interiors() {
                add_line_bounds(result, &ring);
            }
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            for index in 0..multi_point.num_points() {
                if let Some(point) = multi_point.point(index)
                    && let Some(coord) = point.coord()
                {
                    add_coord_bounds(result, &coord);
                }
            }
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            for index in 0..multi_line.num_line_strings() {
                if let Some(line) = multi_line.line_string(index) {
                    add_line_bounds(result, &line);
                }
            }
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            for index in 0..multi_polygon.num_polygons() {
                if let Some(polygon) = multi_polygon.polygon(index) {
                    if let Some(ring) = polygon.exterior() {
                        add_line_bounds(result, &ring);
                    }
                    for ring in polygon.interiors() {
                        add_line_bounds(result, &ring);
                    }
                }
            }
        }
        NativeGeometryType::GeometryCollection(collection) => {
            result.element_count += collection.num_geometries();
            for index in 0..collection.num_geometries() {
                if let Some(child) = collection.geometry(index) {
                    collect_bounds(&child, result);
                }
            }
        }
        NativeGeometryType::Rect(rect) => {
            let min = rect.min();
            let max = rect.max();
            add_coord_bounds(result, &min);
            add_coord_bounds(result, &max);
        }
        NativeGeometryType::Triangle(_) | NativeGeometryType::Line(_) => {}
    }
}

fn has_z(dimensions: geo_traits::Dimensions) -> bool {
    matches!(
        dimensions,
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
    )
}

fn add_line_bounds(result: &mut Bounds, line: &impl LineStringTrait<T = f64>) {
    for index in 0..line.num_coords() {
        if let Some(coord) = line.coord(index) {
            add_coord_bounds(result, &coord);
        }
    }
}

fn add_coord_bounds(result: &mut Bounds, coordinate: &impl CoordTrait<T = f64>) {
    let z = matches!(
        coordinate.dim(),
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
    )
    .then(|| coordinate.nth_or_panic(2));
    result.add_point(coordinate.x(), coordinate.y(), z);
}

pub(super) fn write_geometry(
    output: &mut String,
    geometry: &impl GeometryTrait<T = f64>,
    precision: i32,
    options: i32,
    srid: i32,
    bbox_precision: i32,
    top_level: bool,
) -> std::result::Result<(), String> {
    let bounds = bounds(geometry);
    let include_srs = top_level && srid > 0 && (options & 2 != 0 || options & 4 != 0);
    let include_bbox = top_level && options & 1 != 0;
    let crs = if include_srs {
        Some(if options & 4 != 0 {
            format!("urn:ogc:def:crs:EPSG::{srid}")
        } else {
            format!("EPSG:{srid}")
        })
    } else if top_level && options & 8 != 0 && srid > 0 && srid != GEOJSON_DEFAULT_SRID {
        Some(format!("EPSG:{srid}"))
    } else {
        None
    };

    match geometry.as_type() {
        NativeGeometryType::Point(_) => write_geometry_body(
            output,
            "Point",
            geometry,
            precision,
            crs.as_deref(),
            include_bbox.then_some(&bounds),
            bbox_precision,
        )?,
        NativeGeometryType::LineString(_) => write_geometry_body(
            output,
            "LineString",
            geometry,
            precision,
            crs.as_deref(),
            include_bbox.then_some(&bounds),
            bbox_precision,
        )?,
        NativeGeometryType::Polygon(_) => write_geometry_body(
            output,
            "Polygon",
            geometry,
            precision,
            crs.as_deref(),
            include_bbox.then_some(&bounds),
            bbox_precision,
        )?,
        NativeGeometryType::MultiPoint(_) => write_geometry_body(
            output,
            "MultiPoint",
            geometry,
            precision,
            crs.as_deref(),
            include_bbox.then_some(&bounds),
            bbox_precision,
        )?,
        NativeGeometryType::MultiLineString(_) => write_geometry_body(
            output,
            "MultiLineString",
            geometry,
            precision,
            crs.as_deref(),
            include_bbox.then_some(&bounds),
            bbox_precision,
        )?,
        NativeGeometryType::MultiPolygon(_) => write_geometry_body(
            output,
            "MultiPolygon",
            geometry,
            precision,
            crs.as_deref(),
            include_bbox.then_some(&bounds),
            bbox_precision,
        )?,
        NativeGeometryType::GeometryCollection(collection) => {
            output.push_str("{\"type\":\"GeometryCollection\",");
            write_crs(output, crs.as_deref());
            if include_bbox && collection.num_geometries() > 0 {
                write_bbox(output, &bounds, bbox_precision);
            }
            output.push_str("\"geometries\":[");
            for index in 0..collection.num_geometries() {
                if index > 0 {
                    output.push(',');
                }
                let child = collection
                    .geometry(index)
                    .ok_or_else(|| "missing geometry collection member".to_string())?;
                if matches!(child.as_type(), NativeGeometryType::GeometryCollection(_)) {
                    return Err("GeoJson: geometry not supported.".to_string());
                }
                write_geometry(output, &child, precision, 0, 0, bbox_precision, false)?;
            }
            output.push_str("]}");
        }
        NativeGeometryType::Rect(rect) => {
            output.push_str("{\"type\":\"Polygon\",");
            write_crs(output, crs.as_deref());
            if include_bbox {
                write_bbox(output, &bounds, bbox_precision);
            }
            output.push_str("\"coordinates\":[[");
            let min = rect.min();
            let max = rect.max();
            write_coord(output, min.x(), min.y(), None, precision);
            output.push(',');
            write_coord(output, min.x(), max.y(), None, precision);
            output.push(',');
            write_coord(output, max.x(), max.y(), None, precision);
            output.push(',');
            write_coord(output, max.x(), min.y(), None, precision);
            output.push(',');
            write_coord(output, min.x(), min.y(), None, precision);
            output.push_str("]]}");
        }
        NativeGeometryType::Triangle(_) => {
            return Err("GeoJson: geometry not supported.".to_string());
        }
        NativeGeometryType::Line(_) => return Err("GeoJson: geometry not supported.".to_string()),
    }
    Ok(())
}

fn write_geometry_body(
    output: &mut String,
    type_name: &str,
    geometry: &impl GeometryTrait<T = f64>,
    precision: i32,
    crs: Option<&str>,
    bbox: Option<&Bounds>,
    bbox_precision: i32,
) -> std::result::Result<(), String> {
    output.push_str("{\"type\":\"");
    output.push_str(type_name);
    output.push_str("\",");
    write_crs(output, crs);
    if let Some(bounds) = bbox {
        write_bbox(output, bounds, bbox_precision);
    }
    output.push_str("\"coordinates\":");
    write_coordinates(output, geometry, precision)?;
    output.push('}');
    Ok(())
}

fn write_crs(output: &mut String, crs: Option<&str>) {
    if let Some(crs) = crs {
        output.push_str("\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"");
        output.push_str(crs);
        output.push_str("\"}},");
    }
}

fn write_bbox(output: &mut String, bounds: &Bounds, precision: i32) {
    output.push_str("\"bbox\":[");
    let values: Vec<f64> = if bounds.has_z {
        vec![
            bounds.min_x,
            bounds.min_y,
            bounds.min_z,
            bounds.max_x,
            bounds.max_y,
            bounds.max_z,
        ]
    } else {
        vec![bounds.min_x, bounds.min_y, bounds.max_x, bounds.max_y]
    };
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str(&format_bbox_ordinate(*value, precision));
    }
    output.push_str("],");
}

fn write_coordinates(
    output: &mut String,
    geometry: &impl GeometryTrait<T = f64>,
    precision: i32,
) -> std::result::Result<(), String> {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => {
            if let Some(coord) = point.coord() {
                write_coord_array(output, &coord, precision);
            } else {
                output.push_str("[]");
            }
        }
        NativeGeometryType::LineString(line) => write_line_coords(output, line, precision)?,
        NativeGeometryType::Polygon(polygon) => {
            let Some(exterior) = polygon.exterior() else {
                output.push_str("[]");
                return Ok(());
            };
            output.push('[');
            write_line_coords(output, &exterior, precision)?;
            for ring in polygon.interiors() {
                output.push(',');
                write_line_coords(output, &ring, precision)?;
            }
            output.push(']');
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            output.push('[');
            for index in 0..multi_point.num_points() {
                if index > 0 {
                    output.push(',');
                }
                let point = multi_point
                    .point(index)
                    .ok_or_else(|| "missing multipoint member".to_string())?;
                if let Some(coord) = point.coord() {
                    write_coord_array(output, &coord, precision);
                } else {
                    output.push_str("[]");
                }
            }
            output.push(']');
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            output.push('[');
            for index in 0..multi_line.num_line_strings() {
                if index > 0 {
                    output.push(',');
                }
                let line = multi_line
                    .line_string(index)
                    .ok_or_else(|| "missing multilinestring member".to_string())?;
                write_line_coords(output, &line, precision)?;
            }
            output.push(']');
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            output.push('[');
            for index in 0..multi_polygon.num_polygons() {
                if index > 0 {
                    output.push(',');
                }
                let polygon = multi_polygon
                    .polygon(index)
                    .ok_or_else(|| "missing multipolygon member".to_string())?;
                let Some(exterior) = polygon.exterior() else {
                    output.push_str("[]");
                    continue;
                };
                output.push('[');
                write_line_coords(output, &exterior, precision)?;
                for ring in polygon.interiors() {
                    output.push(',');
                    write_line_coords(output, &ring, precision)?;
                }
                output.push(']');
            }
            output.push(']');
        }
        NativeGeometryType::GeometryCollection(collection) => {
            output.push('[');
            for index in 0..collection.num_geometries() {
                if index > 0 {
                    output.push(',');
                }
                let child = collection
                    .geometry(index)
                    .ok_or_else(|| "missing geometry collection member".to_string())?;
                if matches!(child.as_type(), NativeGeometryType::GeometryCollection(_)) {
                    return Err("GeoJson: geometry not supported.".to_string());
                }
                write_geometry(output, &child, precision, 0, 0, precision, false)?;
            }
            output.push(']');
        }
        NativeGeometryType::Rect(rect) => {
            let min = rect.min();
            let max = rect.max();
            output.push_str("[[");
            write_coord(output, min.x(), min.y(), None, precision);
            output.push(',');
            write_coord(output, min.x(), max.y(), None, precision);
            output.push(',');
            write_coord(output, max.x(), max.y(), None, precision);
            output.push(',');
            write_coord(output, max.x(), min.y(), None, precision);
            output.push(',');
            write_coord(output, min.x(), min.y(), None, precision);
            output.push_str("]]");
        }
        NativeGeometryType::Triangle(_) | NativeGeometryType::Line(_) => {
            return Err("GeoJson: geometry not supported.".to_string());
        }
    }
    Ok(())
}

fn write_line_coords(
    output: &mut String,
    line: &impl LineStringTrait<T = f64>,
    precision: i32,
) -> std::result::Result<(), String> {
    output.push('[');
    for index in 0..line.num_coords() {
        if index > 0 {
            output.push(',');
        }
        let coord = line
            .coord(index)
            .ok_or_else(|| "missing line coordinate".to_string())?;
        write_coord_array(output, &coord, precision);
    }
    output.push(']');
    Ok(())
}

fn write_coord_array(output: &mut String, coord: &impl CoordTrait<T = f64>, precision: i32) {
    output.push('[');
    output.push_str(&format_ordinate(coord.x(), precision));
    output.push(',');
    output.push_str(&format_ordinate(coord.y(), precision));
    if matches!(
        coord.dim(),
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
    ) {
        output.push(',');
        output.push_str(&format_ordinate(coord.nth_or_panic(2), precision));
    }
    output.push(']');
}

fn write_coord(output: &mut String, x: f64, y: f64, z: Option<f64>, precision: i32) {
    output.push_str(&format_ordinate(x, precision));
    output.push(' ');
    output.push_str(&format_ordinate(y, precision));
    if let Some(z) = z {
        output.push(' ');
        output.push_str(&format_ordinate(z, precision));
    }
}
