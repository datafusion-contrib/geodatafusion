//! Text formatting shared by the native geometry exchange formats.
//!
//! PostGIS uses a fixed-decimal representation for ordinary magnitudes and
//! switches to exponent notation outside `[1e-8, 1e15)`. The functions here
//! reproduce that observable contract without depending on liblwgeom.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{GenericWktArray, WktViewArray};
use geoarrow_schema::Metadata;

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

const SCIENTIFIC_LOWER_BOUND: f64 = 1e-8;
const SCIENTIFIC_UPPER_BOUND: f64 = 1e15;

/// Shared coercion rule for geometry functions with optional integer arguments.
pub(crate) fn coerce_geometry_with_optional_ints(
    arg_types: &[DataType],
    max_integer_arguments: usize,
    function_name: &str,
    allow_wkt_text: bool,
) -> Result<Vec<DataType>> {
    let maximum_arguments = max_integer_arguments + 1;
    if arg_types.is_empty() || arg_types.len() > maximum_arguments {
        return Err(DataFusionError::Plan(format!(
            "{function_name} expects between 1 and {maximum_arguments} arguments"
        )));
    }

    let geometry_types = any_geometry_type();
    let mut coerced = Vec::with_capacity(arg_types.len());
    for (index, data_type) in arg_types.iter().enumerate() {
        if index == 0 {
            let is_geometry = data_type == &DataType::Null
                || geometry_types.contains(data_type)
                || (allow_wkt_text
                    && matches!(
                        data_type,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ));
            if is_geometry {
                coerced.push(data_type.clone());
            } else {
                return Err(DataFusionError::Plan(format!(
                    "{function_name} expects a geometry argument, got {data_type}"
                )));
            }
        } else if data_type == &DataType::Null || data_type.is_integer() {
            coerced.push(DataType::Int64);
        } else {
            return Err(DataFusionError::Plan(format!(
                "{function_name} expects integer arguments, got {data_type}"
            )));
        }
    }
    Ok(coerced)
}

/// Whether an argument is untyped text that SQL supplied as a WKT literal.
///
/// A GeoArrow WKT column has the same physical type as plain text, so the
/// extension metadata is the only way to tell a typed geometry column from an
/// untyped literal.
pub(crate) fn is_plain_wkt_field(field: &Field) -> bool {
    matches!(
        field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) && field.extension_type_name().is_none()
}

/// Wraps an untyped WKT text array as a geometry array with unknown CRS.
pub(crate) fn plain_wkt_array(array: &ArrayRef) -> GeoDataFusionResult<Arc<dyn GeoArrowArray>> {
    // These functions parse the wrapped values, so the shared text bound is
    // applied before the array can reach the unbounded `wkt` reader.
    crate::udf::native::io::wkt::ensure_wkt_array_depth(array.as_ref())?;
    let metadata = Arc::new(Metadata::default());
    Ok(match array.data_type() {
        DataType::Utf8 => Arc::new(GenericWktArray::<i32>::new(
            array.as_string::<i32>().clone(),
            metadata,
        )),
        DataType::LargeUtf8 => Arc::new(GenericWktArray::<i64>::new(
            array.as_string::<i64>().clone(),
            metadata,
        )),
        DataType::Utf8View => Arc::new(WktViewArray::new(array.as_string_view().clone(), metadata)),
        other => {
            return Err(DataFusionError::Execution(format!(
                "plain WKT wrapper received unsupported type {other}"
            ))
            .into());
        }
    })
}

/// Shared coercion rule for text functions with optional integer arguments.
pub(crate) fn coerce_text_with_optional_ints(
    arg_types: &[DataType],
    max_integer_arguments: usize,
    function_name: &str,
) -> Result<Vec<DataType>> {
    let maximum_arguments = max_integer_arguments + 1;
    if arg_types.is_empty() || arg_types.len() > maximum_arguments {
        return Err(DataFusionError::Plan(format!(
            "{function_name} expects between 1 and {maximum_arguments} arguments"
        )));
    }

    let mut coerced = Vec::with_capacity(arg_types.len());
    for (index, data_type) in arg_types.iter().enumerate() {
        if index == 0 {
            match data_type {
                DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    coerced.push(DataType::Utf8)
                }
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "{function_name} expects a text argument, got {other}"
                    )));
                }
            }
        } else if data_type == &DataType::Null || data_type.is_integer() {
            coerced.push(DataType::Int64);
        } else {
            return Err(DataFusionError::Plan(format!(
                "{function_name} expects integer arguments, got {data_type}"
            )));
        }
    }
    Ok(coerced)
}

/// Format one ordinate the way `ST_AsGeoJSON` and `ST_AsSVG` do.
pub(crate) fn format_ordinate(value: f64, max_decimal_digits: i32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f64::INFINITY {
        return "Infinity".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    if value == 0.0 {
        return "0".to_string();
    }

    let precision = max_decimal_digits.max(0) as usize;
    let magnitude = value.abs();
    if magnitude <= SCIENTIFIC_LOWER_BOUND || magnitude >= SCIENTIFIC_UPPER_BOUND {
        format_scientific(value, precision)
    } else {
        format_fixed(value, precision)
    }
}

/// Format a GeoJSON bbox element. Bbox values use C `%f` semantics, which
/// keeps the requested number of decimal places and prints non-finite values
/// in lower case.
pub(crate) fn format_bbox_ordinate(value: f64, precision: i32) -> String {
    let precision = precision.max(0) as usize;
    if value.is_nan() {
        return "nan".to_string();
    }
    if value == f64::INFINITY {
        return "inf".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if value == 0.0 {
        // `%f` never prints a negative zero.
        return format!("{:.precision$}", 0.0);
    }
    format!("{value:.precision$}")
}

fn format_fixed(value: f64, precision: usize) -> String {
    let negative = value.is_sign_negative();
    let (digits, exponent) = shortest_digits(value.abs());
    let needed_fractional_digits = (digits.len() as i32 - 1 - exponent).max(0) as usize;
    let precision = precision.min(needed_fractional_digits);
    let significand = digits_to_i128(&digits);
    let shift = exponent - (digits.len() as i32 - 1) + precision as i32;
    let scaled = if shift >= 0 {
        significand.saturating_mul(pow10(shift as usize))
    } else {
        round_div(significand, pow10((-shift) as usize))
    };
    let divisor = pow10(precision);
    let integer = scaled / divisor;
    let fraction = scaled % divisor;
    let mut result = integer.to_string();
    if precision > 0 {
        result.push('.');
        let fraction = format!("{fraction:0>width$}", width = precision);
        result.push_str(fraction.trim_end_matches('0'));
        if result.ends_with('.') {
            result.pop();
        }
    }
    if negative && result != "0" {
        result.insert(0, '-');
    }
    result
}

fn format_scientific(value: f64, precision: usize) -> String {
    let negative = value.is_sign_negative();
    let (digits, mut exponent) = shortest_digits(value.abs());
    let precision = precision.min(digits.len().saturating_sub(1));
    let (mut digits, carry) = round_significant(&digits, precision + 1);
    if carry {
        digits = vec![1];
        exponent += 1;
    }
    let mut mantissa = digits[0].to_string();
    if digits.len() > 1 {
        mantissa.push('.');
        for digit in &digits[1..] {
            mantissa.push(char::from(b'0' + *digit));
        }
        while mantissa.ends_with('0') {
            mantissa.pop();
        }
        if mantissa.ends_with('.') {
            mantissa.pop();
        }
    }
    let sign = if negative { "-" } else { "" };
    format!("{sign}{mantissa}e{exponent:+}")
}

fn shortest_digits(value: f64) -> (Vec<u8>, i32) {
    let formatted = format!("{value:e}");
    let (mantissa, exponent) = formatted
        .split_once('e')
        .expect("Rust scientific formatting always contains an exponent");
    let digits = mantissa
        .bytes()
        .filter(u8::is_ascii_digit)
        .map(|byte| byte - b'0')
        .collect::<Vec<_>>();
    let exponent = exponent
        .parse::<i32>()
        .expect("Rust scientific formatting always has an integer exponent");
    (digits, exponent)
}

fn digits_to_i128(digits: &[u8]) -> i128 {
    digits
        .iter()
        .fold(0i128, |value, digit| value * 10 + i128::from(*digit))
}

fn pow10(power: usize) -> i128 {
    10i128.saturating_pow(power as u32)
}

fn round_div(dividend: i128, divisor: i128) -> i128 {
    let quotient = dividend / divisor;
    let remainder = dividend % divisor;
    let half = divisor / 2;
    if remainder > half || (remainder == half && quotient % 2 != 0) {
        quotient + 1
    } else {
        quotient
    }
}

fn round_significant(digits: &[u8], keep: usize) -> (Vec<u8>, bool) {
    if keep >= digits.len() {
        return (digits.to_vec(), false);
    }
    let mut kept = digits[..keep].to_vec();
    let first_discarded = digits[keep];
    let rest_nonzero = digits[keep + 1..].iter().any(|digit| *digit != 0);
    let round_up = first_discarded > 5
        || (first_discarded == 5
            && (rest_nonzero || kept.last().is_some_and(|digit| digit % 2 != 0)));
    if round_up {
        for digit in kept.iter_mut().rev() {
            if *digit == 9 {
                *digit = 0;
            } else {
                *digit += 1;
                return (kept, false);
            }
        }
        return (vec![1], true);
    }
    (kept, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_and_scientific_formatting_matches_postgis_boundaries() {
        assert_eq!(format_ordinate(1.1234567891234567, 9), "1.123456789");
        assert_eq!(format_ordinate(2.9876543219876543, 9), "2.987654322");
        assert_eq!(format_ordinate(1.1234567891234567, 15), "1.123456789123457");
        assert_eq!(format_ordinate(0.5, 0), "0");
        assert_eq!(format_ordinate(1.5, 0), "2");
        assert_eq!(format_ordinate(1e-9, 15), "1e-9");
        assert_eq!(format_ordinate(1e15, 15), "1e+15");
        assert_eq!(format_ordinate(-0.0, 15), "0");
        assert_eq!(format_ordinate(0.1, 20), "0.1");
        assert_eq!(
            format_ordinate(1.1234567891234567, 20),
            "1.1234567891234566"
        );
        assert_eq!(format_ordinate(100000000000000.0, 20), "100000000000000");
        assert_eq!(
            format_ordinate(1000000000000000.5, 20),
            "1.0000000000000005e+15"
        );
    }

    #[test]
    fn non_finite_formatting_keeps_postgis_case() {
        assert_eq!(format_ordinate(f64::NAN, 9), "NaN");
        assert_eq!(format_ordinate(f64::INFINITY, 9), "Infinity");
        assert_eq!(format_ordinate(f64::NEG_INFINITY, 9), "-Infinity");
        assert_eq!(format_bbox_ordinate(f64::NAN, 9), "nan");
        assert_eq!(format_bbox_ordinate(f64::INFINITY, 9), "inf");
        assert_eq!(format_bbox_ordinate(f64::NEG_INFINITY, 9), "-inf");
        assert_eq!(format_bbox_ordinate(1.0, 9), "1.000000000");
    }
}
