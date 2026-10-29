//! `ST_AsMVT` aggregate implementation.

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, Documentation, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata};

use super::encoder::{MvtFeature, MvtValue, encode_tile};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

const DEFAULT_LAYER_NAME: &str = "default";
const DEFAULT_EXTENT: i64 = 4096;
const DEFAULT_GEOMETRY_NAME: &str = "geom";

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsMvt {
    signature: Signature,
}

impl AsMvt {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for AsMvt {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl AggregateUDFImpl for AsMvt {
    fn name(&self) -> &str {
        "st_asmvt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(1..=5).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(format!(
                "ST_AsMVT expects between 1 and 5 arguments, got {}",
                arg_types.len()
            )));
        }

        let mut coerced = Vec::with_capacity(arg_types.len());
        for (index, data_type) in arg_types.iter().enumerate() {
            let target = match index {
                0 => {
                    if !matches!(data_type, DataType::Null | DataType::Struct(_)) {
                        return Err(DataFusionError::Plan(format!(
                            "ST_AsMVT argument 1 must be a record, got {data_type}"
                        )));
                    }
                    data_type.clone()
                }
                1 | 3 | 4 => {
                    if !matches!(
                        data_type,
                        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ) {
                        return Err(DataFusionError::Plan(format!(
                            "ST_AsMVT argument {} must be text, got {data_type}",
                            index + 1
                        )));
                    }
                    DataType::Utf8
                }
                2 => {
                    if !matches!(data_type, DataType::Null) && !data_type.is_integer() {
                        return Err(DataFusionError::Plan(format!(
                            "ST_AsMVT extent must be an integer, got {data_type}"
                        )));
                    }
                    DataType::Int64
                }
                _ => unreachable!(),
            };
            coerced.push(target);
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field(&self, _arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("", DataType::Binary, false)))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(Vec::new())))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "state"),
            DataType::Binary,
            true,
        ))])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let row_field = args.expr_fields[0].clone();
        let layer_name = option_text(&args, 1)?.unwrap_or_else(|| DEFAULT_LAYER_NAME.to_string());
        let extent = option_int(&args, 2)?.unwrap_or(DEFAULT_EXTENT);
        let geometry_name =
            option_text(&args, 3)?.unwrap_or_else(|| DEFAULT_GEOMETRY_NAME.to_string());
        let feature_id_name = option_text(&args, 4)?;

        if i32::try_from(extent).is_err() {
            return Err(DataFusionError::Plan(format!(
                "ST_AsMVT extent {extent} is outside the int32 range"
            )));
        }

        let DataType::Struct(fields) = row_field.data_type() else {
            if matches!(row_field.data_type(), DataType::Null) {
                return Ok(Box::new(AsMvtAccumulator::empty(
                    row_field, layer_name, extent,
                )));
            }
            return Err(DataFusionError::Plan(format!(
                "ST_AsMVT argument 1 must be a record, got {}",
                row_field.data_type()
            )));
        };

        let geometry_index = fields
            .iter()
            .position(|field| field.name() == &geometry_name)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "ST_AsMVT: no geometry column found named `{geometry_name}`"
                ))
            })?;
        let geometry_field = geometry_field(fields[geometry_index].as_ref())?;

        let feature_id_index = match feature_id_name {
            Some(name) => {
                let index = fields
                    .iter()
                    .position(|field| field.name() == &name)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "ST_AsMVT: feature id column `{name}` was not found"
                        ))
                    })?;
                if index == geometry_index {
                    return Err(DataFusionError::Plan(
                        "ST_AsMVT: the geometry column cannot be the feature id column".to_string(),
                    ));
                }
                validate_feature_id_field(fields[index].as_ref())?;
                Some(index)
            }
            None => None,
        };

        let mut property_indices = Vec::new();
        for (index, field) in fields.iter().enumerate() {
            if index == geometry_index || Some(index) == feature_id_index {
                continue;
            }
            validate_property_field(field.as_ref())?;
            property_indices.push(index);
        }

        Ok(Box::new(AsMvtAccumulator {
            row_field,
            geometry_index,
            geometry_field,
            feature_id_index,
            property_indices,
            layer_name,
            extent,
            records: Vec::new(),
        }))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Aggregates rows into one Mapbox Vector Tile layer. The first argument is a \
                 record (STRUCT) containing one GeoArrow geometry field and scalar attributes. \
                 Attribute names and values are encoded as MVT keys and values; NULL attributes \
                 are omitted. The optional layer name, extent, geometry field name, and feature \
                 id field name match PostGIS defaults. JSONB-style nested attributes and \
                 unsupported scalar types are rejected explicitly.",
                "ST_AsMVT(record[, layer_name[, extent[, geom_name[, feature_id_name]]]])",
            )
            .with_argument(
                "record",
                "STRUCT record containing a GeoArrow geometry and scalar attributes",
            )
            .with_argument("layer_name", "optional layer name (default: default)")
            .with_argument("extent", "optional tile extent (default: 4096)")
            .with_argument("geom_name", "optional geometry field name (default: geom)")
            .with_argument(
                "feature_id_name",
                "optional integer field to encode as the MVT feature id",
            )
            .build()
        }))
    }
}

#[derive(Debug)]
struct AsMvtAccumulator {
    row_field: FieldRef,
    geometry_index: usize,
    geometry_field: FieldRef,
    feature_id_index: Option<usize>,
    property_indices: Vec<usize>,
    layer_name: String,
    extent: i64,
    records: Vec<MvtFeature>,
}

impl AsMvtAccumulator {
    fn empty(row_field: FieldRef, layer_name: String, extent: i64) -> Self {
        Self {
            row_field,
            geometry_index: 0,
            geometry_field: Arc::new(Field::new("geom", DataType::Null, true)),
            feature_id_index: None,
            property_indices: Vec::new(),
            layer_name,
            extent,
            records: Vec::new(),
        }
    }

    fn update_rows(&mut self, rows: &arrow_array::StructArray) -> GeoDataFusionResult<()> {
        let DataType::Struct(fields) = self.row_field.data_type() else {
            return Ok(());
        };

        let geometry_wkb = if self.geometry_field.data_type() == &DataType::Null {
            None
        } else {
            let geometry_array = geometry_array_from_arrow(
                rows.column(self.geometry_index).as_ref(),
                &self.geometry_field,
            )?;
            let wkb = to_wkb::<i32>(geometry_array.as_ref())?;
            Some(wkb)
        };

        for row in 0..rows.len() {
            let geometry = if rows.is_null(row) {
                None
            } else if let Some(wkb) = &geometry_wkb {
                let binary = wkb.inner();
                if binary.is_null(row) {
                    None
                } else {
                    Some(binary.value(row).to_vec())
                }
            } else {
                None
            };

            let id = match self.feature_id_index {
                Some(index) => feature_id_value(rows.column(index).as_ref(), row)?,
                None => None,
            };

            let mut properties = Vec::with_capacity(self.property_indices.len());
            for &index in &self.property_indices {
                let field = fields[index].as_ref();
                if let Some(value) = property_value(rows.column(index).as_ref(), row, field)? {
                    properties.push((field.name().clone(), value));
                }
            }

            self.records.push(MvtFeature {
                geometry,
                id,
                properties,
            });
        }

        Ok(())
    }
}

impl Accumulator for AsMvtAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(encode_state(
            &self.records,
        )?))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let tile = encode_tile(&self.layer_name, self.extent, &self.records)?;
        Ok(ScalarValue::Binary(Some(tile)))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let row_array = values.first().ok_or_else(|| {
            DataFusionError::Internal("ST_AsMVT received no input batch".to_string())
        })?;
        if matches!(row_array.data_type(), DataType::Null) {
            return Ok(());
        }
        let rows = row_array.as_struct();
        self.update_rows(rows)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "ST_AsMVT expected one state array, got {}",
                states.len()
            )));
        }
        let state = states[0].as_binary_opt::<i32>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "ST_AsMVT state has type {}, expected Binary",
                states[0].data_type()
            ))
        })?;
        for index in 0..state.len() {
            if state.is_null(index) {
                continue;
            }
            let mut records = decode_state(state.value(index))?;
            self.records.append(&mut records);
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .records
                .iter()
                .map(|feature| {
                    feature.geometry.as_ref().map_or(0, Vec::len)
                        + feature
                            .properties
                            .iter()
                            .map(|(name, value)| name.len() + state_value_size(value))
                            .sum::<usize>()
                })
                .sum::<usize>()
    }
}

fn geometry_field(field: &Field) -> Result<FieldRef> {
    if field.data_type() == &DataType::Null {
        return Ok(Arc::new(field.clone()));
    }
    if GeoArrowType::from_arrow_field(field).is_ok() {
        return Ok(Arc::new(field.clone()));
    }

    // DataFusion's `named_struct` rebuilds child fields and drops extension
    // metadata. A generic GeoArrow geometry is still unambiguous from its
    // physical union layout, so restore the extension type before conversion.
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let candidate =
            GeometryType::new(Arc::new(Metadata::default())).with_coord_type(coord_type);
        if candidate.data_type() == *field.data_type() {
            return Ok(Arc::new(
                candidate.to_field(field.name().clone(), field.is_nullable()),
            ));
        }
    }

    Err(DataFusionError::Plan(format!(
        "ST_AsMVT: column `{}` is not a GeoArrow geometry",
        field.name()
    )))
}

fn validate_feature_id_field(field: &Field) -> Result<()> {
    if field.data_type().is_integer() {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "ST_AsMVT: feature id column `{}` must be an integer, got {}",
            field.name(),
            field.data_type()
        )))
    }
}

fn validate_property_field(field: &Field) -> Result<()> {
    if supported_property_type(field.data_type()) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "ST_AsMVT: unsupported attribute type {} for column `{}`",
            field.data_type(),
            field.name()
        )))
    }
}

fn supported_property_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    ) || matches!(
        data_type,
        DataType::Dictionary(_, value_type)
            if matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
    )
}

fn feature_id_value(array: &dyn Array, row: usize) -> GeoDataFusionResult<Option<u64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Int8 => i128::from(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => i128::from(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => i128::from(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => i128::from(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => i128::from(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => i128::from(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => i128::from(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => i128::from(array.as_primitive::<UInt64Type>().value(row)),
        data_type => {
            return Err(DataFusionError::Internal(format!(
                "ST_AsMVT feature id was not coerced to an integer: {data_type}"
            ))
            .into());
        }
    };
    if value < 0 {
        return Ok(None);
    }
    Ok(Some(u64::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!(
            "ST_AsMVT feature id {value} does not fit in uint64"
        ))
    })?))
}

fn property_value(
    array: &dyn Array,
    row: usize,
    field: &Field,
) -> GeoDataFusionResult<Option<MvtValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Null => return Ok(None),
        DataType::Boolean => MvtValue::Bool(array.as_boolean().value(row)),
        DataType::Int8 => MvtValue::Int(i64::from(array.as_primitive::<Int8Type>().value(row))),
        DataType::Int16 => MvtValue::Int(i64::from(array.as_primitive::<Int16Type>().value(row))),
        DataType::Int32 => MvtValue::Int(i64::from(array.as_primitive::<Int32Type>().value(row))),
        DataType::Int64 => MvtValue::Int(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => MvtValue::UInt(u64::from(array.as_primitive::<UInt8Type>().value(row))),
        DataType::UInt16 => {
            MvtValue::UInt(u64::from(array.as_primitive::<UInt16Type>().value(row)))
        }
        DataType::UInt32 => {
            MvtValue::UInt(u64::from(array.as_primitive::<UInt32Type>().value(row)))
        }
        DataType::UInt64 => MvtValue::UInt(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => MvtValue::from_float32(array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => MvtValue::from_float64(array.as_primitive::<Float64Type>().value(row)),
        DataType::Utf8 => MvtValue::Str(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => MvtValue::Str(array.as_string::<i64>().value(row).to_string()),
        DataType::Utf8View => MvtValue::Str(array.as_string_view().value(row).to_string()),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            let key = dictionary.normalized_keys()[row];
            let values = dictionary.values().as_ref();
            match values.data_type() {
                DataType::Utf8 => MvtValue::Str(values.as_string::<i32>().value(key).to_string()),
                DataType::LargeUtf8 => {
                    MvtValue::Str(values.as_string::<i64>().value(key).to_string())
                }
                DataType::Utf8View => MvtValue::Str(values.as_string_view().value(key).to_string()),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "ST_AsMVT: unsupported dictionary value type {other}"
                    ))
                    .into());
                }
            }
        }
        data_type => {
            return Err(DataFusionError::Execution(format!(
                "ST_AsMVT: unsupported attribute type {data_type} for column `{}`",
                field.name()
            ))
            .into());
        }
    };
    Ok(Some(value))
}

fn option_scalar(args: &AccumulatorArgs, position: usize) -> Result<Option<ScalarValue>> {
    let Some(expr) = args.exprs.get(position) else {
        return Ok(None);
    };
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
    match expr.evaluate(&empty) {
        Ok(ColumnarValue::Scalar(value)) => Ok(if value.is_null() { None } else { Some(value) }),
        Ok(_) => Err(DataFusionError::Plan(
            "ST_AsMVT option arguments must be constant scalars".to_string(),
        )),
        Err(error) => Err(DataFusionError::Plan(format!(
            "ST_AsMVT option arguments must be constant scalars: {error}"
        ))),
    }
}

fn option_text(args: &AccumulatorArgs, position: usize) -> Result<Option<String>> {
    match option_scalar(args, position)? {
        None => Ok(None),
        Some(ScalarValue::Utf8(Some(value)))
        | Some(ScalarValue::LargeUtf8(Some(value)))
        | Some(ScalarValue::Utf8View(Some(value))) => Ok(Some(value)),
        Some(value) => Err(DataFusionError::Plan(format!(
            "ST_AsMVT option {} must be text, got {}",
            position + 1,
            value.data_type()
        ))),
    }
}

fn option_int(args: &AccumulatorArgs, position: usize) -> Result<Option<i64>> {
    match option_scalar(args, position)? {
        None => Ok(None),
        Some(ScalarValue::Int64(Some(value))) => Ok(Some(value)),
        Some(value) => Err(DataFusionError::Plan(format!(
            "ST_AsMVT extent must be an integer, got {}",
            value.data_type()
        ))),
    }
}

fn encode_state(records: &[MvtFeature]) -> GeoDataFusionResult<Vec<u8>> {
    let mut output = b"BMVT1".to_vec();
    put_varint(&mut output, records.len() as u64);
    for record in records {
        match &record.geometry {
            Some(geometry) => {
                output.push(1);
                put_varint(&mut output, geometry.len() as u64);
                output.extend_from_slice(geometry);
            }
            None => output.push(0),
        }
        match record.id {
            Some(id) => {
                output.push(1);
                put_varint(&mut output, id);
            }
            None => output.push(0),
        }
        put_varint(&mut output, record.properties.len() as u64);
        for (name, value) in &record.properties {
            put_varint(&mut output, name.len() as u64);
            output.extend_from_slice(name.as_bytes());
            encode_state_value(&mut output, value);
        }
    }
    Ok(output)
}

fn decode_state(bytes: &[u8]) -> GeoDataFusionResult<Vec<MvtFeature>> {
    let mut input = StateReader::new(bytes);
    if input.bytes(5)? != b"BMVT1" {
        return Err(
            DataFusionError::Execution("ST_AsMVT state has an invalid header".to_string()).into(),
        );
    }
    let count = input.varint()?;
    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let geometry = match input.byte()? {
            0 => None,
            1 => {
                let len = input.varint()? as usize;
                Some(input.bytes(len)?.to_vec())
            }
            value => {
                return Err(DataFusionError::Execution(format!(
                    "ST_AsMVT state has invalid geometry flag {value}"
                ))
                .into());
            }
        };
        let id = match input.byte()? {
            0 => None,
            1 => Some(input.varint()?),
            value => {
                return Err(DataFusionError::Execution(format!(
                    "ST_AsMVT state has invalid id flag {value}"
                ))
                .into());
            }
        };
        let property_count = input.varint()?;
        let mut properties = Vec::with_capacity(property_count as usize);
        for _ in 0..property_count {
            let name_len = input.varint()? as usize;
            let name = String::from_utf8(input.bytes(name_len)?.to_vec()).map_err(|error| {
                DataFusionError::Execution(format!("ST_AsMVT state has invalid UTF-8: {error}"))
            })?;
            properties.push((name, decode_state_value(&mut input)?));
        }
        records.push(MvtFeature {
            geometry,
            id,
            properties,
        });
    }
    if !input.is_empty() {
        return Err(
            DataFusionError::Execution("ST_AsMVT state has trailing bytes".to_string()).into(),
        );
    }
    Ok(records)
}

fn encode_state_value(output: &mut Vec<u8>, value: &MvtValue) {
    match value {
        MvtValue::Int(value) => {
            output.push(1);
            put_varint(output, *value as u64);
        }
        MvtValue::UInt(value) => {
            output.push(2);
            put_varint(output, *value);
        }
        MvtValue::Float32(bits) => {
            output.push(3);
            output.extend_from_slice(&bits.to_le_bytes());
        }
        MvtValue::Float64(bits) => {
            output.push(4);
            output.extend_from_slice(&bits.to_le_bytes());
        }
        MvtValue::Bool(value) => {
            output.push(5);
            output.push(u8::from(*value));
        }
        MvtValue::Str(value) => {
            output.push(6);
            put_varint(output, value.len() as u64);
            output.extend_from_slice(value.as_bytes());
        }
    }
}

fn decode_state_value(input: &mut StateReader<'_>) -> GeoDataFusionResult<MvtValue> {
    match input.byte()? {
        1 => Ok(MvtValue::Int(input.varint()? as i64)),
        2 => Ok(MvtValue::UInt(input.varint()?)),
        3 => Ok(MvtValue::Float32(u32::from_le_bytes(
            input.bytes(4)?.try_into().expect("four bytes"),
        ))),
        4 => Ok(MvtValue::Float64(u64::from_le_bytes(
            input.bytes(8)?.try_into().expect("eight bytes"),
        ))),
        5 => match input.byte()? {
            0 => Ok(MvtValue::Bool(false)),
            1 => Ok(MvtValue::Bool(true)),
            value => Err(DataFusionError::Execution(format!(
                "ST_AsMVT state has invalid bool value {value}"
            ))
            .into()),
        },
        6 => {
            let len = input.varint()? as usize;
            let value = String::from_utf8(input.bytes(len)?.to_vec()).map_err(|error| {
                DataFusionError::Execution(format!("ST_AsMVT state has invalid UTF-8: {error}"))
            })?;
            Ok(MvtValue::Str(value))
        }
        value => Err(DataFusionError::Execution(format!(
            "ST_AsMVT state has invalid value tag {value}"
        ))
        .into()),
    }
}

fn state_value_size(value: &MvtValue) -> usize {
    match value {
        MvtValue::Str(value) => value.len(),
        MvtValue::Int(_)
        | MvtValue::UInt(_)
        | MvtValue::Float32(_)
        | MvtValue::Float64(_)
        | MvtValue::Bool(_) => std::mem::size_of::<MvtValue>(),
    }
}

fn put_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

struct StateReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> StateReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn byte(&mut self) -> GeoDataFusionResult<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn bytes(&mut self, len: usize) -> GeoDataFusionResult<&'a [u8]> {
        let end = self.position.checked_add(len).ok_or_else(|| {
            DataFusionError::Execution("ST_AsMVT state length overflow".to_string())
        })?;
        if end > self.bytes.len() {
            return Err(
                DataFusionError::Execution("ST_AsMVT state is truncated".to_string()).into(),
            );
        }
        self.position = end;
        Ok(&self.bytes[end - len..end])
    }

    fn varint(&mut self) -> GeoDataFusionResult<u64> {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift >= 64 {
                return Err(DataFusionError::Execution(
                    "ST_AsMVT state has an invalid varint".to_string(),
                )
                .into());
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn state_round_trips() {
        let records = vec![MvtFeature {
            geometry: Some(vec![1, 2, 3]),
            id: Some(7),
            properties: vec![
                ("name".to_string(), MvtValue::Str("a".to_string())),
                ("count".to_string(), MvtValue::Int(-2)),
                ("ratio".to_string(), MvtValue::from_float64(1.5)),
            ],
        }];
        let encoded = encode_state(&records).unwrap();
        assert_eq!(decode_state(&encoded).unwrap().len(), 1);
    }
}
