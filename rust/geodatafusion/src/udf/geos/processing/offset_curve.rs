use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{Geom, JoinStyle};

use super::super::adapter;
use super::style;
use crate::error::GeoDataFusionResult;

#[derive(Clone, Copy, Debug)]
struct OffsetCurveOptions {
    quadrant_segments: i32,
    join_style: JoinStyle,
    mitre_limit: f64,
}

impl Default for OffsetCurveOptions {
    fn default() -> Self {
        Self {
            quadrant_segments: 8,
            join_style: JoinStyle::Round,
            mitre_limit: 5.0,
        }
    }
}

impl OffsetCurveOptions {
    fn parse(style: &str) -> Result<Self> {
        let mut options = Self::default();
        for parameter in style::style_parameters(style, "ST_OffsetCurve", "buffer")? {
            match parameter.key {
                "quad_segs" => {
                    options.quadrant_segments = parameter.value.parse::<i32>().map_err(|_| {
                        DataFusionError::Execution(format!(
                            "ST_OffsetCurve quad_segs must be an integer, got '{}'",
                            parameter.value
                        ))
                    })?;
                }
                "join" => {
                    options.join_style = match parameter.value {
                        "round" => JoinStyle::Round,
                        "mitre" | "miter" => JoinStyle::Mitre,
                        "bevel" => JoinStyle::Bevel,
                        _ => {
                            return Err(DataFusionError::Execution(format!(
                                "ST_OffsetCurve join must be round, mitre, miter, or bevel, got '{}'",
                                parameter.value
                            )));
                        }
                    };
                }
                "mitre_limit" | "miter_limit" => {
                    options.mitre_limit = parameter.value.parse::<f64>().map_err(|_| {
                        DataFusionError::Execution(format!(
                            "ST_OffsetCurve {} must be numeric, got '{}'",
                            parameter.key, parameter.value
                        ))
                    })?;
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "ST_OffsetCurve does not support buffer parameter '{}'",
                        parameter.key
                    )));
                }
            }
        }
        Ok(options)
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct OffsetCurve {
    signature: Signature,
}

impl OffsetCurve {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for OffsetCurve {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for OffsetCurve {
    fn name(&self) -> &str {
        "st_offsetcurve"
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
            coerced[2] = adapter::coerce_text(&arg_types[2], 2, self.name())?;
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
        Ok(offset_curve_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes an offset curve at a signed distance from a linear geometry. A zero distance or empty input returns the input geometry unchanged and may retain M. The optional parameter text accepts quad_segs=#, join=round|mitre|miter|bevel, and mitre_limit=# or miter_limit=#. A non-zero distance is computed in 2D and drops M.",
                "ST_OffsetCurve(line, distance, buffer_style_parameters)",
            )
            .with_argument("line", "linear geometry")
            .with_argument("distance", "signed offset distance")
            .with_argument(
                "buffer_style_parameters",
                "optional space-separated style parameters",
            )
            .build()
        }))
    }
}

fn offset_curve_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |row, arrays, original, geometry, writer| {
        let distance = match adapter::float64_value(arrays, 1, row, "ST_OffsetCurve")? {
            Some(distance) => distance,
            None => return Ok(None),
        };

        // PostGIS returns the input before parsing style parameters in these
        // two cases.
        if distance == 0.0 || geometry.is_empty()? {
            return Ok(Some(original.to_vec()));
        }

        let options = match arrays.get(2) {
            None => OffsetCurveOptions::default(),
            Some(_) => match adapter::string_value(arrays, 2, row, "ST_OffsetCurve")? {
                Some(style) => OffsetCurveOptions::parse(style)?,
                None => return Ok(None),
            },
        };

        let result = geometry.offset_curve(
            distance,
            options.quadrant_segments,
            options.join_style,
            options.mitre_limit,
        )?;
        Ok(Some(writer.write(&result)?))
    })
}
