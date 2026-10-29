use std::sync::{Arc, OnceLock};

use arrow_array::StringArray;
use arrow_array::builder::StringBuilder;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geo_traits::{CoordTrait, GeometryTrait, GeometryType as NativeGeometryType, PointTrait};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use super::column::TextColumn;
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

const DEFAULT_FORMAT: &str = "D°M'S.SSS\"C";
const MAX_FORMAT_BYTES: usize = 1024;

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct AsLatLonText {
    signature: Signature,
}

impl AsLatLonText {
    pub(crate) fn new() -> Self {
        Self {
            signature: lat_lon_text_signature(),
        }
    }
}

impl Default for AsLatLonText {
    fn default() -> Self {
        Self::new()
    }
}

static AS_LAT_LON_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsLatLonText {
    fn name(&self) -> &str {
        "st_aslatlontext"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<Arc<Field>> {
        let input = &args.arg_fields[0];
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(input.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let format = TextColumn::from_array(arrays.get(1), Some(""))?;
        let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
        let geo_array = geo_array.as_ref();
        let result = downcast_geoarrow_array!(geo_array, write_lat_lon_array, &format)?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_LAT_LON_TEXT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a point as degrees/minutes/seconds text.",
                "ST_AsLatLonText(point[, format])",
            )
            .with_argument("point", "a point")
            .with_argument("format", "D/MS/C format string")
            .build()
        }))
    }
}

fn lat_lon_text_signature() -> Signature {
    let geometry_types = any_geometry_type();
    let mut signatures = Vec::new();
    for geometry_type in &geometry_types {
        signatures.push(TypeSignature::Exact(vec![geometry_type.clone()]));
        for text_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            signatures.push(TypeSignature::Exact(vec![
                geometry_type.clone(),
                text_type.clone(),
            ]));
        }
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

fn write_lat_lon_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    format: &TextColumn<'_>,
) -> GeoDataFusionResult<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 64);
    for (row, item) in array.iter().enumerate() {
        let Some(format) = format.value(row) else {
            builder.append_null();
            continue;
        };
        match item {
            None => builder.append_null(),
            Some(Err(error)) => return Err(error.into()),
            Some(Ok(geometry)) => {
                let NativeGeometryType::Point(point) = geometry.as_type() else {
                    return Err(DataFusionError::Execution(
                        "ST_AsLatLonText: only points are supported".to_string(),
                    )
                    .into());
                };
                let Some(coord) = point.coord() else {
                    return Err(DataFusionError::Execution(
                        "ST_AsLatLonText: cannot convert an empty point into formatted text"
                            .to_string(),
                    )
                    .into());
                };
                let x = coord.x();
                let y = coord.y();
                if !x.is_finite() || !y.is_finite() {
                    return Err(DataFusionError::Execution(
                        "ST_AsLatLonText: invalid coordinate".to_string(),
                    )
                    .into());
                }
                builder.append_value(format_lat_lon(y, x, format)?);
            }
        }
    }
    Ok(builder.finish())
}

fn format_lat_lon(lat: f64, lon: f64, format: &str) -> Result<String> {
    let parsed = DmsFormat::parse(format)?;
    let (lat, lon) = normalize_lat_lon(lat, lon);
    Ok(format!(
        "{} {}",
        parsed.format_value(lat, "N", "S")?,
        parsed.format_value(lon, "E", "W")?
    ))
}

fn normalize_lat_lon(mut lat: f64, mut lon: f64) -> (f64, f64) {
    while lat > 270.0 {
        lat -= 360.0;
    }
    while lat < -270.0 {
        lat += 360.0;
    }
    if lat > 90.0 {
        lat = 180.0 - lat;
        lon += 180.0;
    }
    if lat < -90.0 {
        lat = -180.0 - lat;
        lon += 180.0;
    }
    while lon > 180.0 {
        lon -= 360.0;
    }
    while lon < -180.0 {
        lon += 360.0;
    }
    (lat, lon)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Reading {
    None,
    Degrees,
    Minutes,
    Seconds,
}

#[derive(Debug)]
struct DmsFormat {
    pieces: Vec<String>,
    degrees: Option<usize>,
    minutes: Option<usize>,
    seconds: Option<usize>,
    compass: Option<usize>,
    degree_digits: usize,
    degree_decimals: usize,
    degree_has_decimal: bool,
    minute_digits: usize,
    minute_decimals: usize,
    minute_has_decimal: bool,
    second_digits: usize,
    second_decimals: usize,
    second_has_decimal: bool,
}

impl DmsFormat {
    fn parse(format: &str) -> Result<Self> {
        if format.is_empty() {
            return Self::parse(DEFAULT_FORMAT);
        }
        if format.len() > MAX_FORMAT_BYTES {
            return Err(DataFusionError::Execution(format!(
                "ST_AsLatLonText: bad format, exceeds maximum length ({MAX_FORMAT_BYTES})"
            )));
        }

        let pieces = vec![String::new()];
        let mut current = 0usize;
        let mut reading = Reading::None;
        let mut dms = Self {
            pieces,
            degrees: None,
            minutes: None,
            seconds: None,
            compass: None,
            degree_digits: 0,
            degree_decimals: 0,
            degree_has_decimal: false,
            minute_digits: 0,
            minute_decimals: 0,
            minute_has_decimal: false,
            second_digits: 0,
            second_decimals: 0,
            second_has_decimal: false,
        };

        for character in format.chars() {
            match character {
                'D' => {
                    if reading != Reading::Degrees {
                        if dms.degree_digits > 0 {
                            return Err(format_error(
                                "bad format, cannot include degrees (DD.DDD) more than once",
                            ));
                        }
                        current = dms.advance(current);
                        dms.degrees = Some(current);
                        reading = Reading::Degrees;
                    }
                    if dms.degree_has_decimal {
                        dms.degree_decimals += 1;
                    } else {
                        dms.degree_digits += 1;
                    }
                }
                'M' => {
                    if reading != Reading::Minutes {
                        if dms.minute_digits > 0 {
                            return Err(format_error(
                                "bad format, cannot include minutes (MM.MMM) more than once",
                            ));
                        }
                        current = dms.advance(current);
                        dms.minutes = Some(current);
                        reading = Reading::Minutes;
                    }
                    if dms.minute_has_decimal {
                        dms.minute_decimals += 1;
                    } else {
                        dms.minute_digits += 1;
                    }
                }
                'S' => {
                    if reading != Reading::Seconds {
                        if dms.second_digits > 0 {
                            return Err(format_error(
                                "bad format, cannot include seconds (SS.SSS) more than once",
                            ));
                        }
                        current = dms.advance(current);
                        dms.seconds = Some(current);
                        reading = Reading::Seconds;
                    }
                    if dms.second_has_decimal {
                        dms.second_decimals += 1;
                    } else {
                        dms.second_digits += 1;
                    }
                }
                'C' => {
                    reading = Reading::None;
                    if dms.compass.is_some() {
                        return Err(format_error(
                            "bad format, cannot include compass dir (C) more than once",
                        ));
                    }
                    current = dms.advance(current);
                    dms.compass = Some(current);
                    current = dms.advance(current);
                }
                '.' => match reading {
                    Reading::Degrees => dms.degree_has_decimal = true,
                    Reading::Minutes => dms.minute_has_decimal = true,
                    Reading::Seconds => dms.second_has_decimal = true,
                    Reading::None => dms.pieces[current].push('.'),
                },
                _ => {
                    if reading != Reading::None {
                        reading = Reading::None;
                        current = dms.advance(current);
                    }
                    dms.pieces[current].push(character);
                }
            }
        }

        if dms.degrees.is_none() {
            return Err(format_error(
                "bad format, degrees (DD.DDD) must be included",
            ));
        }
        if dms.seconds.is_some() && dms.minutes.is_none() {
            return Err(format_error(
                "bad format, cannot include seconds (SS.SSS) without including minutes (MM.MMM)",
            ));
        }
        Ok(dms)
    }

    fn advance(&mut self, current: usize) -> usize {
        let next = current + 1;
        if next >= self.pieces.len() {
            self.pieces.push(String::new());
        }
        next
    }

    fn format_value(&self, value: f64, positive: &str, negative: &str) -> Result<String> {
        let is_negative = value < 0.0;
        let value = if is_negative { -value } else { value };
        let mut degrees = value;
        let mut minutes = 0.0;
        let mut seconds = 0.0;

        if self.minutes.is_some() {
            minutes = (value - degrees.floor()) * 60.0;
            degrees = degrees.floor();
        }
        if self.seconds.is_some() {
            seconds = (minutes - minutes.floor()) * 60.0;
            minutes = minutes.floor();

            let second_round = 10f64.powi(self.second_decimals as i32);
            let carry = (seconds * second_round).round() >= 60.0 * second_round;
            if carry {
                minutes += 1.0;
                seconds = 0.0;
                if (minutes * second_round).round() >= 60.0 * second_round {
                    degrees += 1.0;
                    minutes = 0.0;
                }
            }
        }

        let mut pieces = self.pieces.clone();
        if let Some(index) = self.compass {
            pieces[index] = if is_negative { negative } else { positive }.to_string();
        } else if is_negative {
            degrees = -degrees;
        }

        if let Some(index) = self.degrees {
            pieces[index] = format_field(degrees, self.degree_digits, self.degree_decimals);
        }
        if let Some(index) = self.minutes {
            pieces[index] = format_field(minutes, self.minute_digits, self.minute_decimals);
        }
        if let Some(index) = self.seconds {
            pieces[index] = format_field(seconds, self.second_digits, self.second_decimals);
        }
        Ok(pieces.concat())
    }
}

fn format_field(value: f64, width: usize, precision: usize) -> String {
    format!("{value:>width$.precision$}")
}

fn format_error(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("ST_AsLatLonText: {message}"))
}

#[cfg(test)]
mod tests {
    use super::format_lat_lon;

    #[test]
    fn default_format_matches_postgis() {
        assert_eq!(
            format_lat_lon(-2.32498, -3.2342342, "").unwrap(),
            "2°19'29.928\"S 3°14'3.243\"W"
        );
    }

    #[test]
    fn signed_and_decimal_formats_match_postgis() {
        assert_eq!(
            format_lat_lon(-2.32498, -3.2342342, "D°M'S.SSS\"").unwrap(),
            "-2°19'29.928\" -3°14'3.243\""
        );
        assert_eq!(
            format_lat_lon(-2.32498, -3.2342342, "D.DDDD degrees C").unwrap(),
            "2.3250 degrees S 3.2342 degrees W"
        );
    }

    #[test]
    fn normalizes_wrapped_values() {
        assert_eq!(
            format_lat_lon(-792.32498, -302.2342342, "").unwrap(),
            "72°19'29.928\"S 57°45'56.757\"E"
        );
    }

    #[test]
    fn rejects_format_without_degrees() {
        let error = format_lat_lon(1.0, 2.0, "M").unwrap_err();
        assert!(error.to_string().contains("degrees"));
    }
}
