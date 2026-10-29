//! PostGIS-compatible coordinate text formatting.

/// Format a coordinate using PostGIS's maximum decimal digit rules.
pub(crate) fn format_ordinate(value: f64, precision: i32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    if value == 0.0 {
        return "0".to_string();
    }

    let precision = precision.max(0) as usize;
    let absolute = value.abs();
    // PostGIS switches to exponential notation outside this range.
    let scientific = absolute <= 1e-8 || absolute >= 1e15;

    // Rust's `LowerExp` yields the shortest round-trip digits. PostGIS rounds these
    // digits instead of rounding the exact binary value.
    let formatted = format!("{value:e}");
    let (negative, formatted) = match formatted.strip_prefix('-') {
        Some(formatted) => (true, formatted),
        None => (false, formatted.as_str()),
    };
    let (mantissa, exponent) = formatted
        .split_once('e')
        .expect("exponential formatting always contains an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("exponential formatting always contains an integer exponent");
    let digits = mantissa.replace('.', "");

    let (digits, overflowed) = if scientific {
        round_significant_digits(&digits, precision.saturating_add(1))
    } else {
        round_fixed_digits(&digits, exponent, precision)
    };
    let unsigned = if scientific && overflowed {
        // PostGIS retains the rounded mantissa rather than renormalizing it.
        format!(
            "10e{}{}",
            if exponent < 0 { "-" } else { "+" },
            exponent.abs()
        )
    } else if scientific {
        render_scientific(&digits, exponent)
    } else if overflowed {
        render_fixed("1", exponent + 1)
    } else {
        render_fixed(&digits, exponent)
    };

    if negative && unsigned != "0" {
        format!("-{unsigned}")
    } else {
        unsigned
    }
}

fn round_significant_digits(digits: &str, keep: usize) -> (String, bool) {
    let keep = keep.min(digits.len());
    if keep == digits.len() {
        return (digits.to_string(), false);
    }
    let bytes = digits.as_bytes();
    let discarded = &bytes[keep..];
    let round_up = discarded[0] > b'5'
        || (discarded[0] == b'5'
            && (discarded[1..].iter().any(|digit| *digit != b'0')
                || (bytes[keep - 1] - b'0') % 2 == 1));
    if !round_up {
        return (digits[..keep].to_string(), false);
    }

    let mut rounded = bytes[..keep].to_vec();
    let mut index = keep;
    while index > 0 {
        index -= 1;
        if rounded[index] == b'9' {
            rounded[index] = b'0';
        } else {
            rounded[index] += 1;
            break;
        }
    }
    if index == 0 && rounded[0] == b'0' {
        (String::new(), true)
    } else {
        (String::from_utf8(rounded).expect("digits are ASCII"), false)
    }
}

fn round_fixed_digits(digits: &str, exponent: i32, precision: usize) -> (String, bool) {
    let decimal_places = (digits.len() as i64 - 1 - i64::from(exponent)).max(0) as usize;
    if precision >= decimal_places {
        return (digits.to_string(), false);
    }

    let keep = digits.len() as i64 - (decimal_places - precision) as i64;
    let bytes = digits.as_bytes();
    if keep < 0 {
        return (String::new(), false);
    }
    if keep == 0 {
        let round_up =
            bytes[0] > b'5' || (bytes[0] == b'5' && bytes[1..].iter().any(|digit| *digit != b'0'));
        return if round_up {
            (String::new(), true)
        } else {
            (String::new(), false)
        };
    }

    let keep = keep as usize;
    let round_up = bytes[keep] > b'5'
        || (bytes[keep] == b'5'
            && (bytes[keep + 1..].iter().any(|digit| *digit != b'0')
                || (bytes[keep - 1] - b'0') % 2 == 1));
    if !round_up {
        return (digits[..keep].to_string(), false);
    }

    let mut rounded = bytes[..keep].to_vec();
    let mut index = keep;
    while index > 0 {
        index -= 1;
        if rounded[index] == b'9' {
            rounded[index] = b'0';
        } else {
            rounded[index] += 1;
            break;
        }
    }
    if index == 0 && rounded[0] == b'0' {
        (String::new(), true)
    } else {
        (String::from_utf8(rounded).expect("digits are ASCII"), false)
    }
}

fn render_scientific(digits: &str, exponent: i32) -> String {
    let fraction = digits[1..].trim_end_matches('0');
    let sign = if exponent < 0 { '-' } else { '+' };
    if fraction.is_empty() {
        format!("{}e{sign}{}", &digits[..1], exponent.abs())
    } else {
        format!("{}.{}e{sign}{}", &digits[..1], fraction, exponent.abs())
    }
}

fn render_fixed(digits: &str, exponent: i32) -> String {
    if digits.is_empty() {
        return "0".to_string();
    }
    if exponent >= 0 {
        let integer_digits = exponent as usize + 1;
        if digits.len() <= integer_digits {
            let mut output = digits.to_string();
            output.push_str(&"0".repeat(integer_digits - digits.len()));
            return output;
        }
        let (integer, fraction) = digits.split_at(integer_digits);
        let fraction = fraction.trim_end_matches('0');
        if fraction.is_empty() {
            integer.to_string()
        } else {
            format!("{integer}.{fraction}")
        }
    } else {
        let leading_zeros = (-exponent - 1) as usize;
        let fraction = format!("{}{}", "0".repeat(leading_zeros), digits);
        let fraction = fraction.trim_end_matches('0');
        if fraction.is_empty() {
            "0".to_string()
        } else {
            format!("0.{fraction}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::format_ordinate;

    #[test]
    fn formats_like_postgis() {
        assert_eq!(format_ordinate(12_345_678.123_456_79, 2), "12345678.12");
        assert_eq!(
            format_ordinate(12_345_678.123_456_79, 15),
            "12345678.12345679"
        );
        assert_eq!(format_ordinate(1e-9, 15), "1e-9");
        assert_eq!(format_ordinate(-0.0, 15), "0");
    }
}
