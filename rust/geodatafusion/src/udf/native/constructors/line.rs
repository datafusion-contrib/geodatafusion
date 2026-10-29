use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, FixedSizeListArray, GenericListArray, OffsetSizeTrait};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{GeometryBuilder, LineStringBuilder};
use geoarrow_schema::{CoordType, Dimension, GeoArrowType};
use wkt::types::LineString as WktLineString;

use super::util::{
    CoordRows, InvalidGeometry, LineParts, collect_line_parts, collect_line_parts_with_mask,
    collect_multipoint_coords, field_dimension, geo_array_is_null, list_element_field,
    output_line_field, wkt_line_string,
};
use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    geometry_array_from_arrow, is_null_geometry_field, merge_metadata,
};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineFromMultiPoint {
    coord_type: CoordType,
}

impl LineFromMultiPoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for LineFromMultiPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static LINE_FROM_MULTIPOINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineFromMultiPoint {
    fn name(&self) -> &str {
        "st_linefrommultipoint"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from GeoArrow metadata.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = args.arg_fields[0].as_ref();
        Ok(Arc::new(output_line_field(
            field_dimension(field)?,
            merge_metadata([field], "ST_LineFromMultiPoint")?,
            self.coord_type,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if is_null_geometry_field(args.arg_fields[0].as_ref()) {
            return Ok(build_line_array_from_coords(
                vec![None; args.number_rows],
                &args.return_field,
            )?);
        }
        let input = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
        let rows = collect_multipoint_coords(input.as_ref())?;
        Ok(build_line_array_from_coords(rows, &args.return_field)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_FROM_MULTIPOINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a LineString from a MultiPoint geometry. XY, XYZ, XYM, and XYZM coordinates are preserved.",
                "ST_LineFromMultiPoint(MULTIPOINT((1 2), (3 4)))",
            )
            .with_argument("multipoint", "MultiPoint geometry")
            .with_related_udf("ST_MakeLine")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeLine {
    signature: Signature,
    coord_type: CoordType,
}

impl MakeLine {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: None,
                    }),
                    TypeSignature::Any(2),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for MakeLine {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_LINE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeLine {
    fn name(&self) -> &str {
        "st_makeline"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from GeoArrow metadata.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (dim, metadata) = match args.arg_fields {
            [array] if array.data_type() == &DataType::Null => {
                (None, merge_metadata([array.as_ref()], "ST_MakeLine")?)
            }
            [array] => {
                let element = list_element_field(array)?;
                (
                    field_dimension(element.as_ref())?,
                    merge_metadata([element.as_ref()], "ST_MakeLine")?,
                )
            }
            [first, second] => (
                merge_dimensions(
                    field_dimension(first.as_ref())?,
                    field_dimension(second.as_ref())?,
                ),
                merge_metadata([first.as_ref(), second.as_ref()], "ST_MakeLine")?,
            ),
            _ => {
                return Err(DataFusionError::Plan(
                    "ST_MakeLine expects one geometry[] or two geometry arguments".to_string(),
                ));
            }
        };

        Ok(Arc::new(output_line_field(dim, metadata, self.coord_type)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let rows = match args.arg_fields.as_slice() {
            [array_field] => {
                if array_field.data_type() == &DataType::Null {
                    return Ok(build_line_array_from_coords(
                        vec![None; args.number_rows],
                        &args.return_field,
                    )?);
                }
                let element = list_element_field(array_field)?;
                collect_makeline_array(&arrays[0], element.as_ref())?
            }
            [left_field, right_field] => {
                if is_null_geometry_field(left_field.as_ref())
                    || is_null_geometry_field(right_field.as_ref())
                {
                    return Ok(build_line_array_from_coords(
                        vec![None; args.number_rows],
                        &args.return_field,
                    )?);
                }
                let left = geometry_array_from_arrow(&arrays[0], left_field)?;
                let right = geometry_array_from_arrow(&arrays[1], right_field)?;
                let row_mask = (0..left.len())
                    .map(|row| {
                        !geo_array_is_null(left.as_ref(), row)
                            && !geo_array_is_null(right.as_ref(), row)
                    })
                    .collect::<Vec<_>>();
                let left_rows = collect_line_parts_with_mask(
                    left.as_ref(),
                    InvalidGeometry::Scalar,
                    Some(&row_mask),
                )?;
                let right_rows = collect_line_parts_with_mask(
                    right.as_ref(),
                    InvalidGeometry::Scalar,
                    Some(&row_mask),
                )?;

                left_rows
                    .into_iter()
                    .zip(right_rows)
                    .map(|(left, right)| match (left, right) {
                        (Some(mut left), Some(right)) => {
                            left.merge(right);
                            Some(left)
                        }
                        _ => None,
                    })
                    .collect()
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "ST_MakeLine received an unsupported number of arguments".to_string(),
                ));
            }
        };

        let coord_rows = rows
            .into_iter()
            .map(|row| {
                row.map(|parts| parts.build_coords("ST_MakeLine"))
                    .transpose()
            })
            .collect::<GeoDataFusionResult<Vec<_>>>()?;
        Ok(build_line_array_from_coords(
            coord_rows,
            &args.return_field,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_LINE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a LineString from two Point or LineString geometries, or from a geometry array containing Point, LineString, or MultiPoint geometries. \
                 LineString leading points that repeat the previous output point are collapsed. The PostGIS aggregate overload \
                 cannot be registered alongside the scalar overload in DataFusion, so it is not provided.",
                "ST_MakeLine(point1, point2) or ST_MakeLine(ARRAY[point1, point2])",
            )
            .with_argument("geom1", "Point, LineString, or geometry array")
            .with_argument("geom2", "optional second Point or LineString")
            .with_related_udf("ST_LineFromMultiPoint")
            .build()
        }))
    }
}

pub(super) fn merge_dimensions(
    left: Option<Dimension>,
    right: Option<Dimension>,
) -> Option<Dimension> {
    match (left, right) {
        (Some(left), Some(right)) => Some(super::util::union_dimensions(left, right)),
        _ => None,
    }
}

fn collect_makeline_array(
    array: &ArrayRef,
    element_field: &Field,
) -> GeoDataFusionResult<Vec<Option<LineParts>>> {
    match array.data_type() {
        DataType::List(_) => collect_generic_list(array.as_list::<i32>(), element_field),
        DataType::LargeList(_) => collect_generic_list(array.as_list::<i64>(), element_field),
        DataType::FixedSizeList(_, _) => {
            collect_fixed_size_list(array.as_fixed_size_list(), element_field)
        }
        other => Err(DataFusionError::Execution(format!(
            "ST_MakeLine array form expects a geometry array, got {other}"
        ))
        .into()),
    }
}

fn collect_generic_list<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    element_field: &Field,
) -> GeoDataFusionResult<Vec<Option<LineParts>>> {
    let mut output = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            output.push(None);
            continue;
        }
        output.push(collect_list_row(list.value(row), element_field)?);
    }
    Ok(output)
}

fn collect_fixed_size_list(
    list: &FixedSizeListArray,
    element_field: &Field,
) -> GeoDataFusionResult<Vec<Option<LineParts>>> {
    let mut output = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            output.push(None);
            continue;
        }
        output.push(collect_list_row(list.value(row), element_field)?);
    }
    Ok(output)
}

fn collect_list_row(
    values: ArrayRef,
    element_field: &Field,
) -> GeoDataFusionResult<Option<LineParts>> {
    if values.data_type() == &DataType::Null {
        return Ok(None);
    }
    if values.is_empty() {
        return Ok(None);
    }

    let geometries = geometry_array_from_arrow(values.as_ref(), element_field)?;
    let rows = collect_line_parts(geometries.as_ref(), InvalidGeometry::Ignore)?;
    let mut output = LineParts {
        elements: Vec::new(),
        dim: None,
    };
    let mut accepted = false;
    for row in rows.into_iter().flatten() {
        accepted = true;
        output.merge(row);
    }
    Ok(accepted.then_some(output))
}

pub(super) fn build_line_array_from_coords(
    rows: CoordRows,
    return_field: &Field,
) -> GeoDataFusionResult<ColumnarValue> {
    let geo_type = GeoArrowType::from_arrow_field(return_field)?;
    match geo_type {
        GeoArrowType::LineString(typ) => {
            let mut builder = LineStringBuilder::new(typ);
            for row in rows {
                match row {
                    Some((coords, dim)) => {
                        let line = wkt_line_string(coords, dim);
                        builder.push_line_string(Some(&line))?;
                    }
                    None => builder.push_line_string(Option::<&WktLineString<f64>>::None)?,
                }
            }
            Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
        }
        GeoArrowType::Geometry(typ) => {
            let mut builder = GeometryBuilder::new(typ);
            for row in rows {
                match row {
                    Some((coords, dim)) => {
                        let geometry = wkt::Wkt::LineString(wkt_line_string(coords, dim));
                        builder.push_geometry(Some(&geometry))?;
                    }
                    None => builder.push_geometry(None::<&wkt::Wkt<f64>>)?,
                }
            }
            Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
        }
        other => Err(DataFusionError::Internal(format!(
            "Expected a LineString or generic geometry return type, got {other:?}"
        ))
        .into()),
    }
}
