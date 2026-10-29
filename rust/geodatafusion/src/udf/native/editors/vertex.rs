//! Vertex-level LineString editors.

use std::sync::OnceLock;

use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use super::transform::{
    GeometryEdit, OutputKind, WktValue, integer_value, invoke_with_edits, return_field_for,
    wkt_values,
};
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AddPoint {
    signature: Signature,
}

impl AddPoint {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for AddPoint {
    fn default() -> Self {
        Self::new()
    }
}

static ADD_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AddPoint {
    fn name(&self) -> &str {
        "st_addpoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let roles: &[bool] = match arg_types.len() {
            2 => &[true, true],
            3 => &[true, true, false],
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "ST_AddPoint does not accept {} arguments",
                    arg_types.len()
                )));
            }
        };
        coerce_vertex_types("ST_AddPoint", arg_types, roles)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_AddPoint requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::LineString)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(add_point_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(ADD_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Adds a point to a LineString before the zero-based index position. The default position appends the point to the end.",
                "ST_AddPoint(linestring, point[, position])",
            )
            .with_argument("linestring", "LineString geometry")
            .with_argument("point", "Point geometry")
            .with_argument("position", "optional zero-based insertion position")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct RemovePoint {
    signature: Signature,
}

impl RemovePoint {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RemovePoint {
    fn default() -> Self {
        Self::new()
    }
}

static REMOVE_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for RemovePoint {
    fn name(&self) -> &str {
        "st_removepoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_vertex_types("ST_RemovePoint", arg_types, &[true, false])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_RemovePoint requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::LineString)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(remove_point_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(REMOVE_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Removes the zero-based point index from a LineString. A LineString must retain at least two points.",
                "ST_RemovePoint(linestring, offset)",
            )
            .with_argument("linestring", "LineString geometry")
            .with_argument("offset", "zero-based point index")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SetPoint {
    signature: Signature,
}

impl SetPoint {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SetPoint {
    fn default() -> Self {
        Self::new()
    }
}

static SET_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SetPoint {
    fn name(&self) -> &str {
        "st_setpoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_vertex_types("ST_SetPoint", arg_types, &[true, false, true])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_SetPoint requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::LineString)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(set_point_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SET_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Replaces the point at a zero-based LineString index. Negative indexes count from the end.",
                "ST_SetPoint(linestring, zerobasedposition, point)",
            )
            .with_argument("linestring", "LineString geometry")
            .with_argument("zerobasedposition", "zero-based or negative point index")
            .with_argument("point", "Point geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Reverse {
    signature: Signature,
}

impl Reverse {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Reverse {
    fn default() -> Self {
        Self::new()
    }
}

static REVERSE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Reverse {
    fn name(&self) -> &str {
        "st_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_vertex_types("ST_Reverse", arg_types, &[true])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Reverse requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::Preserve)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke_with_edits(args, OutputKind::Preserve, |_, _| {
            Ok(Some(GeometryEdit::Reverse))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(REVERSE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a geometry with vertex order reversed. Collection member order is preserved.",
                "ST_Reverse(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Scroll {
    signature: Signature,
}

impl Scroll {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Scroll {
    fn default() -> Self {
        Self::new()
    }
}

static SCROLL_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Scroll {
    fn name(&self) -> &str {
        "st_scroll"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_vertex_types("ST_Scroll", arg_types, &[true, true])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Scroll requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_for(args, OutputKind::Preserve)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(scroll_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SCROLL_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Changes the start and end point of a closed LineString to the given vertex.",
                "ST_Scroll(linestring, point)",
            )
            .with_argument("linestring", "closed LineString geometry")
            .with_argument("point", "point that occurs in the LineString")
            .build()
        }))
    }
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(AddPoint::new().into());
    session_context.register_udf(RemovePoint::new().into());
    session_context.register_udf(SetPoint::new().into());
    session_context.register_udf(Reverse::new().into());
    session_context.register_udf(Scroll::new().into());
}

fn add_point_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    if args.arg_fields[0].data_type() == &DataType::Null {
        return invoke_with_edits(args, OutputKind::LineString, |_, _| {
            unreachable!("ST_AddPoint short-circuits NULL geometry")
        });
    }

    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let point_values = point_values(&arrays[1], &args.arg_fields[1])?;
    let has_position = arrays.len() == 3;
    invoke_with_edits(args, OutputKind::LineString, |index, arrays| {
        if value_is_null(&arrays[0], index) || value_is_null(&arrays[1], index) {
            return Ok(None);
        }
        if has_position && value_is_null(&arrays[2], index) {
            return Ok(None);
        }

        let Some(point) = point_value(&point_values, index)? else {
            return Ok(None);
        };
        let position = if has_position {
            let Some(position) = integer_value(&arrays[2], index)? else {
                return Ok(None);
            };
            Some(position)
        } else {
            None
        };
        Ok(Some(GeometryEdit::AddPoint { point, position }))
    })
}

fn remove_point_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    invoke_with_edits(args, OutputKind::LineString, |index, arrays| {
        if value_is_null(&arrays[0], index) || value_is_null(&arrays[1], index) {
            return Ok(None);
        }
        let Some(offset) = integer_value(&arrays[1], index)? else {
            return Ok(None);
        };
        Ok(Some(GeometryEdit::RemovePoint { index: offset }))
    })
}

fn set_point_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    if args.arg_fields[0].data_type() == &DataType::Null {
        return invoke_with_edits(args, OutputKind::LineString, |_, _| {
            unreachable!("ST_SetPoint short-circuits NULL geometry")
        });
    }

    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let point_values = point_values(&arrays[2], &args.arg_fields[2])?;
    invoke_with_edits(args, OutputKind::LineString, |index, arrays| {
        if value_is_null(&arrays[0], index)
            || value_is_null(&arrays[1], index)
            || value_is_null(&arrays[2], index)
        {
            return Ok(None);
        }
        let Some(index_value) = integer_value(&arrays[1], index)? else {
            return Ok(None);
        };
        Ok(Some(GeometryEdit::SetPoint {
            index: index_value,
            point: point_value(&point_values, index)?.ok_or_else(|| {
                DataFusionError::Internal("expected a non-null point argument".to_string())
            })?,
        }))
    })
}

fn scroll_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    if args.arg_fields[0].data_type() == &DataType::Null {
        return invoke_with_edits(args, OutputKind::Preserve, |_, _| {
            unreachable!("ST_Scroll short-circuits NULL geometry")
        });
    }

    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let point_values = point_values(&arrays[1], &args.arg_fields[1])?;
    invoke_with_edits(args, OutputKind::Preserve, |index, arrays| {
        if value_is_null(&arrays[0], index) || value_is_null(&arrays[1], index) {
            return Ok(None);
        }
        let Some(point) = point_value(&point_values, index)? else {
            return Ok(None);
        };
        Ok(Some(GeometryEdit::Scroll { point }))
    })
}

fn point_values(
    array: &arrow_array::ArrayRef,
    field: &FieldRef,
) -> GeoDataFusionResult<Vec<Option<WktValue>>> {
    if array.data_type() == &DataType::Null {
        return Ok(vec![None; array.len()]);
    }
    wkt_values(array, field)
}

fn point_value(
    values: &[Option<WktValue>],
    index: usize,
) -> GeoDataFusionResult<Option<wkt::Wkt<f64>>> {
    match values.get(index) {
        Some(Some(Ok(point))) => Ok(Some(point.clone())),
        Some(Some(Err(message))) => Err(DataFusionError::Execution(message.clone()).into()),
        Some(None) => Ok(None),
        None => Err(DataFusionError::Internal(
            "point argument row index is out of range".to_string(),
        )
        .into()),
    }
}

fn coerce_vertex_types(
    function_name: &str,
    arg_types: &[DataType],
    roles: &[bool],
) -> Result<Vec<DataType>> {
    if arg_types.len() != roles.len() {
        return Err(DataFusionError::Plan(format!(
            "{function_name} does not accept {} arguments",
            arg_types.len()
        )));
    }

    arg_types
        .iter()
        .zip(roles)
        .map(|(data_type, is_geometry)| {
            if *is_geometry {
                if *data_type == DataType::Null || is_supported_geometry_type(data_type) {
                    Ok(data_type.clone())
                } else {
                    Err(DataFusionError::Plan(format!(
                        "{function_name} does not support argument type {data_type}"
                    )))
                }
            } else {
                match data_type {
                    DataType::Int32 | DataType::Int64 => Ok(data_type.clone()),
                    DataType::Null => Ok(DataType::Int64),
                    _ => Err(DataFusionError::Plan(format!(
                        "{function_name} index arguments must be integers, got {data_type}"
                    ))),
                }
            }
        })
        .collect()
}

fn is_supported_geometry_type(data_type: &DataType) -> bool {
    any_geometry_type()
        .iter()
        .any(|candidate| candidate == data_type)
}

fn value_is_null(array: &arrow_array::ArrayRef, index: usize) -> bool {
    array.data_type() == &DataType::Null || array.is_null(index)
}
