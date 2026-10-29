use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{BufferParams, CapStyle, Geom, JoinStyle};

use super::super::adapter;
use super::style;
use crate::error::GeoDataFusionResult;

const EMPTY_POLYGON_WKB: &[u8] = &[1, 3, 0, 0, 0, 0, 0, 0, 0];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Side {
    Both,
    Left,
    Right,
}

#[derive(Clone, Copy, Debug)]
struct BufferOptions {
    quadrant_segments: i32,
    end_cap_style: CapStyle,
    join_style: JoinStyle,
    mitre_limit: f64,
    side: Side,
}

impl Default for BufferOptions {
    fn default() -> Self {
        Self {
            quadrant_segments: 8,
            end_cap_style: CapStyle::Round,
            join_style: JoinStyle::Round,
            mitre_limit: 5.0,
            side: Side::Both,
        }
    }
}

impl BufferOptions {
    fn parse(style: &str) -> Result<Self> {
        let mut options = Self::default();
        for parameter in style::style_parameters(style, "ST_Buffer", "buffer")? {
            let (key, value) = (parameter.key, parameter.value);
            match key {
                "quad_segs" => {
                    let value = value.parse::<i32>().map_err(|_| {
                        DataFusionError::Execution(format!(
                            "ST_Buffer quad_segs must be an integer, got '{value}'"
                        ))
                    })?;
                    if value < 0 {
                        return Err(DataFusionError::Execution(
                            "ST_Buffer quad_segs must be non-negative".to_string(),
                        ));
                    }
                    options.quadrant_segments = value;
                }
                "endcap" => {
                    options.end_cap_style = match value {
                        "round" => CapStyle::Round,
                        "flat" | "butt" => CapStyle::Flat,
                        "square" => CapStyle::Square,
                        _ => {
                            return Err(DataFusionError::Execution(format!(
                                "ST_Buffer endcap must be round, flat, butt, or square, got '{value}'"
                            )));
                        }
                    };
                }
                "join" => {
                    options.join_style = match value {
                        "round" => JoinStyle::Round,
                        "mitre" | "miter" => JoinStyle::Mitre,
                        "bevel" => JoinStyle::Bevel,
                        _ => {
                            return Err(DataFusionError::Execution(format!(
                                "ST_Buffer join must be round, mitre, miter, or bevel, got '{value}'"
                            )));
                        }
                    };
                }
                "mitre_limit" | "miter_limit" => {
                    let value = value.parse::<f64>().map_err(|_| {
                        DataFusionError::Execution(format!(
                            "ST_Buffer {key} must be numeric, got '{value}'"
                        ))
                    })?;
                    if !value.is_finite() || value <= 0.0 {
                        return Err(DataFusionError::Execution(format!(
                            "ST_Buffer {key} must be a finite positive number"
                        )));
                    }
                    options.mitre_limit = value;
                }
                "side" => {
                    options.side = match value {
                        "both" => Side::Both,
                        "left" => Side::Left,
                        "right" => Side::Right,
                        _ => {
                            return Err(DataFusionError::Execution(format!(
                                "ST_Buffer side must be both, left, or right, got '{value}'"
                            )));
                        }
                    };
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "ST_Buffer does not support buffer parameter '{key}'"
                    )));
                }
            }
        }
        Ok(options)
    }

    fn with_quadrant_segments(mut self, quadrant_segments: i64) -> Result<Self> {
        self.quadrant_segments = i32::try_from(quadrant_segments).map_err(|_| {
            DataFusionError::Execution(
                "ST_Buffer quad-segment count is outside the Int32 range".to_string(),
            )
        })?;
        if self.quadrant_segments < 0 {
            return Err(DataFusionError::Execution(
                "ST_Buffer quad-segment count must be non-negative".to_string(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Buffer {
    signature: Signature,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Buffer {
    fn name(&self) -> &str {
        "st_buffer"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2, 3], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        coerced[1] = adapter::coerce_float64(&arg_types[1], 1, self.name())?;
        if arg_types.len() == 3 {
            coerced[2] = adapter::coerce_buffer_style(&arg_types[2], self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(buffer_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the planar buffer of a geometry. The optional third argument accepts either an integer quad-segment count or space-separated style parameters: quad_segs=#, endcap=round|flat|butt|square, join=round|mitre|miter|bevel, mitre_limit=# or miter_limit=#, and side=both|left|right. The result is always 2D and never carries M.",
                "ST_Buffer(geometry, radius, buffer_style_parameters)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("radius", "distance in coordinate units")
            .with_argument(
                "buffer_style_parameters",
                "optional integer quad-segment count or style text",
            )
            .build()
        }))
    }
}

fn buffer_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |row, arrays, _original, geometry, writer| {
        let radius = match adapter::float64_value(arrays, 1, row, "ST_Buffer")? {
            Some(radius) => radius,
            None => return Ok(None),
        };
        if !radius.is_finite() {
            return Err(
                DataFusionError::Execution("ST_Buffer radius must be finite".to_string()).into(),
            );
        }

        let options = match arrays.get(2) {
            None => BufferOptions::default(),
            Some(array) if matches!(array.data_type(), DataType::Int64) => {
                let Some(quad_segments) = adapter::int64_value(arrays, 2, row, "ST_Buffer")? else {
                    return Ok(None);
                };
                BufferOptions::default().with_quadrant_segments(quad_segments)?
            }
            Some(_) => {
                let style = adapter::string_value(arrays, 2, row, "ST_Buffer")?;
                match style {
                    Some(style) => BufferOptions::parse(style)?,
                    None => return Ok(None),
                }
            }
        };

        if geometry.is_empty()? {
            return Ok(Some(EMPTY_POLYGON_WKB.to_vec()));
        }

        let width = if options.side == Side::Right {
            -radius
        } else {
            radius
        };
        let params = BufferParams::builder()
            .quadrant_segments(options.quadrant_segments)
            .end_cap_style(options.end_cap_style)
            .join_style(options.join_style)
            .mitre_limit(options.mitre_limit)
            .single_sided(options.side != Side::Both)
            .build()?;
        let result = geometry.buffer_with_params(width, &params)?;
        Ok(Some(writer.write(&result)?))
    })
}
