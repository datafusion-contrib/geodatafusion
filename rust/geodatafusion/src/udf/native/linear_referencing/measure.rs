use std::sync::{Arc, OnceLock};

use arrow_array::Float64Array;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{GeometryTrait, GeometryType, MultiLineStringTrait};
use geoarrow_array::builder::MultiLineStringBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{
    GeoArrowType, GeometryType as GeometryDataType, LineStringType, MultiLineStringType,
};
use wkt::Wkt;

use super::util::{
    LineString4, build_generic_geometry_array, build_line_string_array, datafusion_error,
    generic_geometry_type, has_sql_null_argument, planar_metadata, with_measure,
};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AddMeasure;

impl AddMeasure {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AddMeasure {
    fn default() -> Self {
        Self::new()
    }
}

static ADD_MEASURE_DOC: OnceLock<Documentation> = OnceLock::new();
static ADD_MEASURE_SIGNATURE: std::sync::LazyLock<Signature> =
    std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));

impl ScalarUDFImpl for AddMeasure {
    fn name(&self) -> &str {
        "st_addmeasure"
    }

    fn signature(&self) -> &Signature {
        &ADD_MEASURE_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(add_measure_return_field(&args, self.name())?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(add_measure_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(ADD_MEASURE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a LineString or MultiLineString with M values linearly interpolated between the supplied start and end measures along 2D length. Existing M values are replaced and Z values are retained.",
                "ST_AddMeasure(line_string, measure_start, measure_end)",
            )
            .with_argument("line_string", "LineString or MultiLineString")
            .with_argument("measure_start", "float8")
            .with_argument("measure_end", "float8")
            .build()
        }))
    }
}

fn add_measure_return_field(
    args: &ReturnFieldArgs,
    function_name: &str,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = planar_metadata(&args.arg_fields[0], function_name)?;
    if args.arg_fields[0].data_type() == &DataType::Null {
        return Ok(Arc::new(GeometryDataType::new(metadata).to_field("", true)));
    }
    let input_type = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    let field = match input_type {
        GeoArrowType::LineString(typ) => {
            LineStringType::new(with_measure(typ.dimension()), metadata)
                .with_coord_type(typ.coord_type())
                .to_field("", true)
        }
        GeoArrowType::MultiLineString(typ) => {
            MultiLineStringType::new(with_measure(typ.dimension()), metadata)
                .with_coord_type(typ.coord_type())
                .to_field("", true)
        }
        _ => GeometryDataType::new(metadata)
            .with_coord_type(input_type.coord_type().unwrap_or_default())
            .to_field("", true),
    };
    Ok(Arc::new(field))
}

fn add_measure_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let rows = if has_sql_null_argument(args) {
        vec![None; args.number_rows]
    } else {
        let start = args.args[1].cast_to(&DataType::Float64, None)?;
        let end = args.args[2].cast_to(&DataType::Float64, None)?;
        let arrays = ColumnarValue::values_to_arrays(&[args.args[0].clone(), start, end])?;
        let geometry_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
        let starts = arrays[1].as_primitive::<arrow_array::types::Float64Type>();
        let ends = arrays[2].as_primitive::<arrow_array::types::Float64Type>();
        let geometry_ref = geometry_array.as_ref();
        downcast_geoarrow_array!(geometry_ref, add_measure_rows, starts, ends)?
    };

    match output_type {
        GeoArrowType::LineString(typ) => {
            let result = build_line_string_array(rows, typ)?;
            Ok(ColumnarValue::Array(result.to_array_ref()))
        }
        GeoArrowType::MultiLineString(typ) => {
            let mut builder = MultiLineStringBuilder::new(typ);
            for row in rows {
                builder.push_geometry(row.as_ref())?;
            }
            Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
        }
        _ => {
            let result = build_generic_geometry_array(
                rows,
                generic_geometry_type(&args.return_field, "st_addmeasure")?,
            )?;
            Ok(ColumnarValue::Array(result.to_array_ref()))
        }
    }
}

fn add_measure_rows<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    starts: &Float64Array,
    ends: &Float64Array,
) -> GeoDataFusionResult<Vec<Option<Wkt<f64>>>> {
    let mut rows = Vec::with_capacity(array.len());
    for ((geometry, start), end) in array.iter().zip(starts.iter()).zip(ends.iter()) {
        let Some(geometry) = geometry else {
            rows.push(None);
            continue;
        };
        let (Some(start), Some(end)) = (start, end) else {
            rows.push(None);
            continue;
        };
        if !start.is_finite() || !end.is_finite() {
            return Err(datafusion_error("measure values must be finite"));
        }
        let geometry = geometry?;
        let result = match geometry.as_type() {
            GeometryType::LineString(line) => {
                let line = LineString4::from_trait(line)?;
                line.add_measure(start, end)?.to_wkt()
            }
            GeometryType::MultiLineString(multi_line) => {
                measured_multi_line(multi_line, start, end)?
            }
            _ => {
                return Err(datafusion_error(
                    "Only LINESTRING and MULTILINESTRING are supported",
                ));
            }
        };
        rows.push(Some(result));
    }
    Ok(rows)
}

fn measured_multi_line(
    multi_line: &impl MultiLineStringTrait<T = f64>,
    start: f64,
    end: f64,
) -> GeoDataFusionResult<Wkt<f64>> {
    let lines = multi_line
        .line_strings()
        .map(|line| LineString4::from_trait(&line))
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    let dimension = with_measure(
        lines
            .first()
            .map(|line| line.dim)
            .unwrap_or(super::util::geometry_dimension(multi_line)?),
    );
    if lines.is_empty() {
        return Ok(Wkt::MultiLineString(wkt::types::MultiLineString::empty(
            super::util::wkt_dimension(dimension),
        )));
    }

    let total_length: f64 = lines.iter().map(LineString4::length_2d).sum();
    let range = end - start;
    let mut traversed = 0.0;
    let mut measured = Vec::with_capacity(lines.len());
    for line in &lines {
        let sub_length = line.length_2d();
        let (sub_start, sub_end) = (
            start + range * traversed / total_length,
            start + range * (traversed + sub_length) / total_length,
        );
        measured.push(line.add_measure(sub_start, sub_end)?.to_wkt());
        traversed += sub_length;
    }

    let output_lines = measured
        .into_iter()
        .filter_map(|line| match line {
            Wkt::LineString(line) => Some(line),
            _ => None,
        })
        .collect();
    Ok(Wkt::MultiLineString(wkt::types::MultiLineString::new(
        output_lines,
        super::util::wkt_dimension(dimension),
    )))
}
