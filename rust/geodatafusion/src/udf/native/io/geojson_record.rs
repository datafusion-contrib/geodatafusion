//! GeoJSON Feature output for Arrow record fields.

use std::fmt::Write as _;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Decimal256Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use geoarrow_array::array::WkbArray;
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use super::common::geometry_field_from_physical;
use super::geojson::{srid_from_metadata, write_geometry};
use crate::udf::native::geometry::{
    expand_rect_to_geometry, field_metadata, geometry_array_from_arrow,
};

const DEFAULT_GEOMETRY_OPTIONS: i32 = 8;
const PROPERTY_GEOMETRY_PRECISION: i32 = 15;
const SHORT_CRS_OPTION: i32 = 2;

/// Prepared record schema and geometry columns for Feature serialization.
pub(super) struct RecordGeoJsonSerializer {
    fields: Vec<FieldRef>,
    geometry_columns: Vec<Option<PreparedGeometryColumn>>,
}

impl RecordGeoJsonSerializer {
    pub(super) fn new(field: &FieldRef, array: &ArrayRef) -> Result<Self> {
        let DataType::Struct(fields) = field.data_type() else {
            return Err(DataFusionError::Execution(format!(
                "ST_AsGeoJSON record input must be a struct, got {}",
                field.data_type()
            )));
        };
        let records = array.as_struct();
        if records.num_columns() != fields.len() {
            return Err(DataFusionError::Execution(format!(
                "ST_AsGeoJSON record has {} arrays for {} fields",
                records.num_columns(),
                fields.len()
            )));
        }

        let geometry_columns = fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let Some(geometry_field) = geometry_field_from_physical(field) else {
                    return Ok(None);
                };
                PreparedGeometryColumn::new(records.column(index), geometry_field.as_ref())
                    .map(Some)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            fields: fields.iter().cloned().collect(),
            geometry_columns,
        })
    }

    pub(super) fn write_row(
        &self,
        records: &StructArray,
        row: usize,
        geom_column: Option<&str>,
        precision: i32,
        pretty: bool,
        id_column: Option<&str>,
    ) -> Result<String> {
        let geom_column = geom_column.filter(|name| !name.is_empty());
        let id_column = id_column.filter(|name| !name.is_empty());
        let geometry_index = match geom_column {
            Some(name) => self
                .fields
                .iter()
                .position(|field| field.name() == name)
                .ok_or_else(|| {
                    DataFusionError::Execution("geometry column is missing".to_string())
                })?,
            None => self
                .geometry_columns
                .iter()
                .position(Option::is_some)
                .ok_or_else(|| {
                    DataFusionError::Execution("geometry column is missing".to_string())
                })?,
        };
        let geometry = self.geometry_columns[geometry_index]
            .as_ref()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "ST_AsGeoJSON geometry column `{}` is not a GeoArrow geometry",
                    self.fields[geometry_index].name()
                ))
            })?;

        let id_index = id_column
            .map(|name| {
                self.fields
                    .iter()
                    .position(|field| field.name() == name)
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Specified id column \"{name}\" is missing"
                        ))
                    })
            })
            .transpose()?;
        if id_index == Some(geometry_index) {
            let name = id_column.expect("id index requires an id column");
            return Err(DataFusionError::Execution(format!(
                "Specified id column \"{name}\" is missing"
            )));
        }

        let mut output = String::from("{\"type\": \"Feature\", \"geometry\": ");
        geometry.write_value(&mut output, row, precision, DEFAULT_GEOMETRY_OPTIONS)?;

        if let Some(id_index) = id_index {
            output.push_str(", \"id\": ");
            write_arrow_value(
                &mut output,
                &self.fields[id_index],
                records.column(id_index),
                row,
            )?;
        }

        output.push_str(", \"properties\": {");
        let mut first = true;
        for (index, field) in self.fields.iter().enumerate() {
            if index == geometry_index || id_index == Some(index) {
                continue;
            }
            if !first {
                output.push_str(if pretty { ",\n " } else { ", " });
            }
            first = false;
            write_json_string(&mut output, field.name())?;
            output.push_str(": ");
            if let Some(geometry) = &self.geometry_columns[index] {
                geometry.write_value(
                    &mut output,
                    row,
                    PROPERTY_GEOMETRY_PRECISION,
                    SHORT_CRS_OPTION,
                )?;
            } else {
                write_arrow_value(&mut output, field, records.column(index), row)?;
            }
        }
        output.push_str("}}");
        Ok(output)
    }
}

struct PreparedGeometryColumn {
    wkb: WkbArray,
    srid: i32,
}

impl PreparedGeometryColumn {
    fn new(array: &ArrayRef, field: &Field) -> Result<Self> {
        let metadata = field_metadata(field)?;
        let srid = srid_from_metadata(&metadata);
        let input = geometry_array_from_arrow(array, field)?;
        let expanded = expand_rect_to_geometry(input.as_ref())?;
        let input: &dyn GeoArrowArray = expanded
            .as_ref()
            .map_or(input.as_ref(), |expanded| expanded);
        let wkb =
            to_wkb::<i32>(input).map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Self { wkb, srid })
    }

    fn write_value(
        &self,
        output: &mut String,
        row: usize,
        precision: i32,
        options: i32,
    ) -> Result<()> {
        if self.wkb.is_null(row) {
            output.push_str("null");
            return Ok(());
        }
        let geometry = self
            .wkb
            .value(row)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        write_geometry(
            output,
            &geometry,
            precision,
            options,
            self.srid,
            precision.max(0),
            true,
        )
        .map_err(DataFusionError::Execution)
    }
}

fn write_arrow_value(
    output: &mut String,
    field: &Field,
    array: &ArrayRef,
    row: usize,
) -> Result<()> {
    if array.is_null(row) {
        output.push_str("null");
        return Ok(());
    }

    if field.extension_type_name().is_some() {
        let value = array.slice(row, 1);
        let geometry = PreparedGeometryColumn::new(&value, field)?;
        return geometry.write_value(output, 0, PROPERTY_GEOMETRY_PRECISION, SHORT_CRS_OPTION);
    }

    match array.data_type() {
        DataType::Null => output.push_str("null"),
        DataType::Boolean => {
            output.push_str(if array.as_boolean().value(row) {
                "true"
            } else {
                "false"
            });
        }
        DataType::Int8 => output.push_str(&array.as_primitive::<Int8Type>().value(row).to_string()),
        DataType::Int16 => {
            output.push_str(&array.as_primitive::<Int16Type>().value(row).to_string())
        }
        DataType::Int32 => {
            output.push_str(&array.as_primitive::<Int32Type>().value(row).to_string())
        }
        DataType::Int64 => {
            output.push_str(&array.as_primitive::<Int64Type>().value(row).to_string())
        }
        DataType::UInt8 => {
            output.push_str(&array.as_primitive::<UInt8Type>().value(row).to_string())
        }
        DataType::UInt16 => {
            output.push_str(&array.as_primitive::<UInt16Type>().value(row).to_string())
        }
        DataType::UInt32 => {
            output.push_str(&array.as_primitive::<UInt32Type>().value(row).to_string())
        }
        DataType::UInt64 => {
            output.push_str(&array.as_primitive::<UInt64Type>().value(row).to_string())
        }
        DataType::Float16 => write_float(
            output,
            f64::from(array.as_primitive::<Float16Type>().value(row)),
        ),
        DataType::Float32 => write_float32(output, array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => write_float(output, array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(_, scale) => write_decimal(
            output,
            &array
                .as_primitive::<Decimal128Type>()
                .value(row)
                .to_string(),
            *scale,
        ),
        DataType::Decimal256(_, scale) => write_decimal(
            output,
            &array
                .as_primitive::<Decimal256Type>()
                .value(row)
                .to_string(),
            *scale,
        ),
        DataType::Utf8 => write_json_string(output, array.as_string::<i32>().value(row))?,
        DataType::LargeUtf8 => write_json_string(output, array.as_string::<i64>().value(row))?,
        DataType::Utf8View => write_json_string(output, array.as_string_view().value(row))?,
        DataType::Binary => write_binary(output, array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => write_binary(output, array.as_binary::<i64>().value(row)),
        DataType::BinaryView => write_binary(output, array.as_binary_view().value(row)),
        DataType::List(item) => {
            write_list(output, &array.as_list::<i32>().value(row), item)?;
        }
        DataType::LargeList(item) => {
            write_list(output, &array.as_list::<i64>().value(row), item)?;
        }
        DataType::FixedSizeList(item, _) => {
            write_list(output, &array.as_fixed_size_list().value(row), item)?;
        }
        DataType::Struct(fields) => {
            let values = array.as_struct();
            output.push('{');
            for (index, field) in fields.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_json_string(output, field.name())?;
                output.push(':');
                write_arrow_value(output, field, values.column(index), row)?;
            }
            output.push('}');
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "ST_AsGeoJSON property `{}` has unsupported Arrow type {other}",
                field.name()
            )));
        }
    }
    Ok(())
}

fn write_list(output: &mut String, values: &ArrayRef, item: &FieldRef) -> Result<()> {
    output.push('[');
    for row in 0..values.len() {
        if row > 0 {
            output.push(',');
        }
        write_arrow_value(output, item, values, row)?;
    }
    output.push(']');
    Ok(())
}

fn write_float(output: &mut String, value: f64) {
    if value.is_nan() {
        output.push_str("\"NaN\"");
    } else if value == f64::INFINITY {
        output.push_str("\"Infinity\"");
    } else if value == f64::NEG_INFINITY {
        output.push_str("\"-Infinity\"");
    } else {
        output.push_str(&value.to_string());
    }
}

fn write_float32(output: &mut String, value: f32) {
    if value.is_nan() {
        output.push_str("\"NaN\"");
    } else if value == f32::INFINITY {
        output.push_str("\"Infinity\"");
    } else if value == f32::NEG_INFINITY {
        output.push_str("\"-Infinity\"");
    } else {
        output.push_str(&value.to_string());
    }
}

fn write_decimal(output: &mut String, value: &str, scale: i8) {
    if scale <= 0 {
        output.push_str(value);
        for _ in 0..scale.unsigned_abs() {
            output.push('0');
        }
        return;
    }

    let scale = usize::from(scale as u8);
    let (sign, digits) = value
        .strip_prefix('-')
        .map_or(("", value), |digits| ("-", digits));
    output.push_str(sign);
    if digits.len() <= scale {
        output.push_str("0.");
        for _ in 0..scale - digits.len() {
            output.push('0');
        }
        output.push_str(digits);
    } else {
        let split = digits.len() - scale;
        output.push_str(&digits[..split]);
        output.push('.');
        output.push_str(&digits[split..]);
    }
}

fn write_json_string(output: &mut String, value: &str) -> Result<()> {
    let encoded = serde_json::to_string(value).map_err(|error| {
        DataFusionError::Execution(format!("ST_AsGeoJSON could not encode text: {error}"))
    })?;
    output.push_str(&encoded);
    Ok(())
}

fn write_binary(output: &mut String, value: &[u8]) {
    output.push_str("\"\\\\x");
    for byte in value {
        write!(output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output.push('"');
}
