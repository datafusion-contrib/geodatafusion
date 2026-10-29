use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{Geom, GeometryTypes, MakeValidMethod, MakeValidParams};
use wkb::reader::{Dimension, Wkb};

use super::super::adapter;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeValid {
    signature: Signature,
}

impl MakeValid {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for MakeValid {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeValid {
    fn name(&self) -> &str {
        "st_makevalid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1, 2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() == 2 {
            coerced[1] = adapter::coerce_text(&arg_types[1], 1, self.name())?;
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
        Ok(make_valid_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Repairs invalid geometry and returns a valid geometry. The optional parameter string accepts `method=linework|structure` and `keepcollapsed=true|false`; unknown keys are ignored. M ordinates are dropped before GEOS and are not present in the result.",
                "ST_MakeValid(geom, params)",
            )
            .with_argument("geom", "geometry to repair")
            .with_argument("params", "optional make-valid parameters")
            .build()
        }))
    }
}

#[derive(Debug, Default)]
struct MakeValidOptions {
    method: Option<MakeValidMethod>,
    keep_collapsed: Option<bool>,
}

fn make_valid_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |row, arrays, _original, geometry, writer| {
        if geometry.is_empty()?
            && let Some(empty) = adapter::empty_2d_wkb(geometry.geometry_type()?)
        {
            // PostGIS reports 2D typed empties even when the input carried Z or M.
            return Ok(Some(empty.to_vec()));
        }

        let params = if arrays.len() > 1 {
            let text = adapter::string_value(arrays, 1, row, "ST_MakeValid")?;
            text.map(parse_make_valid_options).transpose()?
        } else {
            None
        };

        let result = match params {
            Some(params) => {
                let mut builder = MakeValidParams::builder();
                if let Some(method) = params.method {
                    builder = builder.method(method);
                }
                if let Some(keep_collapsed) = params.keep_collapsed {
                    builder = builder.keep_collapsed(keep_collapsed);
                }
                geometry.make_valid_with_params(&builder.build()?)?
            }
            None => geometry.make_valid()?,
        };
        if matches!(
            geometry.geometry_type()?,
            GeometryTypes::MultiPoint
                | GeometryTypes::MultiLineString
                | GeometryTypes::MultiPolygon
                | GeometryTypes::GeometryCollection
        ) && matches!(
            result.geometry_type()?,
            GeometryTypes::Point | GeometryTypes::LineString | GeometryTypes::Polygon
        ) {
            // PostGIS wraps a non-collection repair result back into the
            // matching multi-geometry when the input was a collection.
            let wkb = writer.write(&result)?;
            return Ok(Some(wrap_multi(&wkb, result.geometry_type()?)?));
        }
        Ok(Some(writer.write(&result)?))
    })
}

fn wrap_multi(wkb: &[u8], geometry_type: GeometryTypes) -> GeoDataFusionResult<Vec<u8>> {
    let parsed = Wkb::try_new(wkb).map_err(|error| {
        DataFusionError::Execution(format!("ST_MakeValid result WKB is invalid: {error}"))
    })?;
    let base = match geometry_type {
        GeometryTypes::Point => 4u32,
        GeometryTypes::LineString => 5,
        GeometryTypes::Polygon => 6,
        _ => {
            return Err(DataFusionError::Internal(
                "ST_MakeValid tried to wrap a non-atomic geometry".to_string(),
            )
            .into());
        }
    };
    let type_code = base
        + match parsed.dimension() {
            Dimension::Xy => 0,
            Dimension::Xyz => 1000,
            Dimension::Xym => 2000,
            Dimension::Xyzm => 3000,
        };
    let mut output = Vec::with_capacity(wkb.len() + 9);
    output.push(1);
    output.extend_from_slice(&type_code.to_le_bytes());
    output.extend_from_slice(&1u32.to_le_bytes());
    output.extend_from_slice(wkb);
    Ok(output)
}

fn parse_make_valid_options(text: &str) -> GeoDataFusionResult<MakeValidOptions> {
    let mut options = MakeValidOptions::default();
    for entry in text.split(' ') {
        if entry.is_empty() {
            continue;
        }
        let Some((key, value)) = entry.split_once('=') else {
            return Err(DataFusionError::Execution(format!(
                "Option string entry '{entry}' lacks separator '='"
            ))
            .into());
        };
        match key.to_ascii_lowercase().as_str() {
            "method" if value.eq_ignore_ascii_case("linework") => {
                options.method = Some(MakeValidMethod::Linework);
            }
            "method" if value.eq_ignore_ascii_case("structure") => {
                options.method = Some(MakeValidMethod::Structure);
            }
            "method" => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported value for 'method', '{value}'. Use 'linework' or 'structure'."
                ))
                .into());
            }
            "keepcollapsed" if value.eq_ignore_ascii_case("true") => {
                options.keep_collapsed = Some(true);
            }
            "keepcollapsed" if value.eq_ignore_ascii_case("false") => {
                options.keep_collapsed = Some(false);
            }
            "keepcollapsed" => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported value for 'keepcollapsed', '{value}'. Use 'true' or 'false'"
                ))
                .into());
            }
            _ => {}
        }
    }
    Ok(options)
}
