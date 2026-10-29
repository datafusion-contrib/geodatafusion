use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, FixedSizeListArray, GenericListArray, OffsetSizeTrait};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::utils::ListCoercion;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{GeometryBuilder, PolygonBuilder};
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, GeometryType, PolygonType};
use wkt::types::Polygon as WktPolygon;

use super::util::{
    CoordValue, GeometryCoords, RingRows, collect_ring, collect_ring_with_mask,
    ensure_same_dimension, field_dimension, geo_array_is_null, list_element_field, validate_ring,
    wkt_dimension, wkt_line_string,
};
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    geometry_array_from_arrow, is_null_geometry_field, merge_metadata, metadata_for_srid,
    normalize_srid, resolve_srid_argument,
};

type PolygonValue = (Vec<Vec<CoordValue>>, Dimension);
type PolygonRows = Vec<Option<PolygonValue>>;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakePolygon {
    signature: Signature,
    coord_type: CoordType,
}

impl MakePolygon {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Element,
                            ArrayFunctionArgument::Array,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                    TypeSignature::Uniform(1, any_geometry_type()),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for MakePolygon {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_POLYGON_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakePolygon {
    fn name(&self) -> &str {
        "st_makepolygon"
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
            [shell] => (
                field_dimension(shell.as_ref())?,
                merge_metadata([shell.as_ref()], "ST_MakePolygon")?,
            ),
            [shell, holes] if holes.data_type() == &DataType::Null => (
                field_dimension(shell.as_ref())?,
                merge_metadata([shell.as_ref()], "ST_MakePolygon")?,
            ),
            [shell, holes] => {
                let holes = list_element_field(holes)?;
                (
                    ensure_same_dimension(
                        "ST_MakePolygon",
                        field_dimension(shell.as_ref())?,
                        field_dimension(holes.as_ref())?,
                    )?,
                    merge_metadata([shell.as_ref(), holes.as_ref()], "ST_MakePolygon")?,
                )
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "ST_MakePolygon expects a LineString or a LineString and geometry array"
                        .to_string(),
                ));
            }
        };

        Ok(Arc::new(output_polygon_field(
            dim,
            metadata,
            self.coord_type,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let shell_is_null = is_null_geometry_field(args.arg_fields[0].as_ref());
        let holes_are_null = args
            .arg_fields
            .get(1)
            .is_some_and(|field| field.data_type() == &DataType::Null);
        if shell_is_null || holes_are_null {
            return Ok(build_polygon_array(
                vec![None; args.number_rows],
                &args.return_field,
            )?);
        }
        let shell = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
        let row_mask = (0..shell.len())
            .map(|row| {
                !geo_array_is_null(shell.as_ref(), row)
                    && !arrays.get(1).is_some_and(|holes| holes.is_null(row))
            })
            .collect::<Vec<_>>();
        let shells = collect_ring_with_mask(shell.as_ref(), Some(&row_mask))?;

        let holes = match args.arg_fields.as_slice() {
            [_] => shells.iter().map(|_| Some(Vec::new())).collect(),
            [_, holes_field] => {
                let element = list_element_field(holes_field)?;
                collect_holes_array(&arrays[1], element.as_ref(), Some(&row_mask))?
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "ST_MakePolygon received an unsupported number of arguments".to_string(),
                ));
            }
        };

        let mut rows = Vec::with_capacity(shells.len());
        for (shell, holes) in shells.into_iter().zip(holes) {
            match (shell, holes) {
                (Some(shell), Some(holes)) => {
                    let mut rings = Vec::with_capacity(holes.len() + 1);
                    rings.push(shell);
                    rings.extend(holes);
                    rows.push(Some(build_polygon("ST_MakePolygon", rings)?));
                }
                _ => rows.push(None),
            }
        }

        Ok(build_polygon_array(rows, &args.return_field)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_POLYGON_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a Polygon from a closed LineString shell and an optional geometry array of closed LineString holes. \
                 Shell and hole dimensions and CRS must match; holes are never discarded.",
                "ST_MakePolygon(shell) or ST_MakePolygon(shell, holes)",
            )
            .with_argument("shell", "closed LineString shell")
            .with_argument("holes", "optional geometry array of closed LineStrings")
            .with_related_udf("ST_Polygon")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Polygon {
    signature: Signature,
    coord_type: CoordType,
}

impl Polygon {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for Polygon {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POLYGON_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Polygon {
    fn name(&self) -> &str {
        "st_polygon"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from GeoArrow metadata and the SRID argument.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let srid = match args.scalar_arguments.get(1) {
            Some(Some(ScalarValue::Int64(Some(value)))) => normalize_srid(*value)?,
            Some(Some(ScalarValue::Int64(None) | ScalarValue::Null)) => 0,
            Some(None) => 0,
            Some(Some(other)) => {
                return Err(DataFusionError::Plan(format!(
                    "ST_Polygon requires SRID to be Int64, got {other}"
                )));
            }
            None => {
                return Err(DataFusionError::Internal(
                    "ST_Polygon did not receive its SRID argument".to_string(),
                ));
            }
        };

        Ok(Arc::new(output_polygon_field(
            field_dimension(args.arg_fields[0].as_ref())?,
            metadata_for_srid(srid),
            self.coord_type,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if is_null_geometry_field(args.arg_fields[0].as_ref())
            || arrays[1].data_type() == &DataType::Null
        {
            return Ok(build_polygon_array(
                vec![None; args.number_rows],
                &args.return_field,
            )?);
        }
        let shell = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
        if arrays[1].data_type() != &DataType::Int64 {
            return Err(DataFusionError::Execution(
                "ST_Polygon SRID must be an Int64 value".to_string(),
            ));
        }
        let srid = arrays[1].as_primitive::<arrow_array::types::Int64Type>();
        let _ = resolve_srid_argument(&args, 1, "ST_Polygon")?;
        let row_mask = (0..shell.len())
            .map(|row| !geo_array_is_null(shell.as_ref(), row) && !srid.is_null(row))
            .collect::<Vec<_>>();
        let shells = collect_ring_with_mask(shell.as_ref(), Some(&row_mask))?;

        let mut rows = Vec::with_capacity(shells.len());
        for (row, shell) in shells.into_iter().enumerate() {
            match shell {
                Some(shell) if !srid.is_null(row) => {
                    rows.push(Some(build_polygon("ST_Polygon", vec![shell])?));
                }
                _ => rows.push(None),
            }
        }

        Ok(build_polygon_array(rows, &args.return_field)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POLYGON_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a Polygon from one closed LineString and assigns the given SRID. \
                 Use ST_MakePolygon to construct a polygon with interior rings.",
                "ST_Polygon(closed_linestring, srid)",
            )
            .with_argument("line", "closed LineString")
            .with_argument("srid", "integer SRID")
            .with_related_udf("ST_MakePolygon")
            .build()
        }))
    }
}

fn output_polygon_field(
    dim: Option<Dimension>,
    metadata: Arc<geoarrow_schema::Metadata>,
    coord_type: CoordType,
) -> Field {
    match dim {
        Some(dim) => PolygonType::new(dim, metadata)
            .with_coord_type(coord_type)
            .to_field("", true),
        None => GeometryType::new(metadata)
            .with_coord_type(coord_type)
            .to_field("", true),
    }
}

fn collect_holes_array(
    array: &ArrayRef,
    element_field: &Field,
    row_mask: Option<&[bool]>,
) -> GeoDataFusionResult<RingRows> {
    match array.data_type() {
        DataType::List(_) => collect_generic_holes(array.as_list::<i32>(), element_field, row_mask),
        DataType::LargeList(_) => {
            collect_generic_holes(array.as_list::<i64>(), element_field, row_mask)
        }
        DataType::FixedSizeList(_, _) => {
            collect_fixed_size_holes(array.as_fixed_size_list(), element_field, row_mask)
        }
        other => Err(DataFusionError::Execution(format!(
            "ST_MakePolygon holes must be a geometry array, got {other}"
        ))
        .into()),
    }
}

fn collect_generic_holes<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    element_field: &Field,
    row_mask: Option<&[bool]>,
) -> GeoDataFusionResult<RingRows> {
    let mut output = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if row_mask.is_some_and(|mask| !mask[row]) {
            output.push(None);
            continue;
        }
        if list.is_null(row) {
            output.push(None);
        } else {
            output.push(Some(collect_hole_row(list.value(row), element_field)?));
        }
    }
    Ok(output)
}

fn collect_fixed_size_holes(
    list: &FixedSizeListArray,
    element_field: &Field,
    row_mask: Option<&[bool]>,
) -> GeoDataFusionResult<RingRows> {
    let mut output = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if row_mask.is_some_and(|mask| !mask[row]) {
            output.push(None);
            continue;
        }
        if list.is_null(row) {
            output.push(None);
        } else {
            output.push(Some(collect_hole_row(list.value(row), element_field)?));
        }
    }
    Ok(output)
}

fn collect_hole_row(
    values: ArrayRef,
    element_field: &Field,
) -> GeoDataFusionResult<Vec<GeometryCoords>> {
    if values.data_type() == &DataType::Null {
        return Ok(Vec::new());
    }
    let holes = geometry_array_from_arrow(values.as_ref(), element_field)?;
    let rings = collect_ring(holes.as_ref())?;
    rings
        .into_iter()
        .enumerate()
        .map(|(index, ring)| {
            ring.ok_or_else(|| {
                DataFusionError::Execution(format!("ST_MakePolygon: hole {index} is NULL")).into()
            })
        })
        .collect()
}

fn build_polygon(
    function_name: &str,
    rings: Vec<GeometryCoords>,
) -> GeoDataFusionResult<PolygonValue> {
    let mut expected_dim = None;
    let mut output = Vec::with_capacity(rings.len());
    for (index, (coords, dim)) in rings.into_iter().enumerate() {
        let role = if index == 0 {
            "shell".to_string()
        } else {
            format!("hole {index}")
        };
        validate_ring(function_name, &role, &coords, dim, &mut expected_dim)?;
        output.push(coords);
    }

    let dim = expected_dim.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{function_name}: polygon must have at least one ring"
        ))
    })?;
    Ok((output, dim))
}

fn build_polygon_array(
    rows: PolygonRows,
    return_field: &Field,
) -> GeoDataFusionResult<ColumnarValue> {
    let geo_type = GeoArrowType::from_arrow_field(return_field)?;
    match geo_type {
        GeoArrowType::Polygon(typ) => {
            let mut builder = PolygonBuilder::new(typ);
            for row in rows {
                match row {
                    Some((rings, dim)) => {
                        let polygon = polygon_value(rings, dim);
                        builder.push_polygon(Some(&polygon))?;
                    }
                    None => builder.push_polygon(Option::<&WktPolygon<f64>>::None)?,
                }
            }
            Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
        }
        GeoArrowType::Geometry(typ) => {
            let mut builder = GeometryBuilder::new(typ);
            for row in rows {
                match row {
                    Some((rings, dim)) => {
                        let geometry = wkt::Wkt::Polygon(polygon_value(rings, dim));
                        builder.push_geometry(Some(&geometry))?;
                    }
                    None => builder.push_geometry(None::<&wkt::Wkt<f64>>)?,
                }
            }
            Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
        }
        other => Err(DataFusionError::Internal(format!(
            "Expected a Polygon or generic geometry return type, got {other:?}"
        ))
        .into()),
    }
}

fn polygon_value(rings: Vec<Vec<CoordValue>>, dim: Dimension) -> WktPolygon<f64> {
    WktPolygon::new(
        rings
            .into_iter()
            .map(|ring| wkt_line_string(ring, dim))
            .collect(),
        wkt_dimension(dim),
    )
}
