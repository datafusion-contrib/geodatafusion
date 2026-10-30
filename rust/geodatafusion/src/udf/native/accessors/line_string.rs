//! Accessors from LineString geometries

use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::{Array, Int64Array};
use arrow_schema::{DataType, FieldRef};
use datafusion::common::types::{NativeType, logical_int64};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait, PointTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, PointArray, from_arrow_array};
use geoarrow_array::builder::{GeometryBuilder, PointBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, PointType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

/// `ST_StartPoint` scalar UDF implementation: returns the first point of a `LineString` geometry as a `Point`.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StartPoint {
    coord_type: CoordType,
}

impl StartPoint {
    /// Creates a new `ST_StartPoint` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for StartPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static START_POINT_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StartPoint {
    fn name(&self) -> &str {
        "st_startpoint"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(point_impl(args, self.coord_type, Mode::Start)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(START_POINT_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Returns the first point of a LINESTRING geometry as a POINT. Returns NULL if the input is not a LINESTRING", "ST_StartPoint(line_string)" )
                .with_argument("g1", "geometry")
                .build()
        }))
    }
}

/// `ST_EndPoint` scalar UDF implementation: returns the last point of a `LineString` geometry as a `Point`.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct EndPoint {
    coord_type: CoordType,
}

impl EndPoint {
    /// Creates a new `ST_EndPoint` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for EndPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static END_POINT_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for EndPoint {
    fn name(&self) -> &str {
        "st_endpoint"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(point_impl(args, self.coord_type, Mode::End)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(END_POINT_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Returns the last point of a LINESTRING geometry as a POINT. Returns NULL if the input is not a LINESTRING.", "ST_EndPoint(line_string)" )
                .with_argument("g1", "geometry")
                .build()
        }))
    }
}

/// `ST_PointN` scalar UDF implementation: returns the Nth point in the first `LineString` of a geometry (1-based; negative indices count backward from the end).
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PointN {
    coord_type: CoordType,
}

impl PointN {
    /// Creates a new `ST_PointN` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for PointN {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_N_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
static POINT_N_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::coercible(
        vec![
            Coercion::new_exact(TypeSignatureClass::Any),
            Coercion::new_implicit(
                TypeSignatureClass::Native(logical_int64()),
                vec![TypeSignatureClass::Integer],
                NativeType::Int64,
            ),
        ],
        Volatility::Immutable,
    )
});

impl ScalarUDFImpl for PointN {
    fn name(&self) -> &str {
        "st_pointn"
    }

    fn signature(&self) -> &Signature {
        &POINT_N_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(point_n_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_N_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Returns the Nth point of a LINESTRING as a POINT. The index is 1-based; negative indices count backward from the end. Returns NULL for zero, out-of-range, empty, or non-LineString inputs.", "ST_PointN(line_string, n)" )
                .with_argument("line_string", "geometry")
                .with_argument("n", "1-based point index")
                .build()
        }))
    }
}

/// For LineString input, return fixed-dim Point output
/// For other geometry input, return variable-dim Geometry output
fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    if args.arg_fields[0].data_type() == &DataType::Null {
        return Ok(null_geometry_output_type(coord_type));
    }

    let input_field = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    match input_field {
        GeoArrowType::LineString(typ) => {
            let output_type =
                PointType::new(typ.dimension(), typ.metadata().clone()).with_coord_type(coord_type);
            Ok(Arc::new(output_type.to_field("", true)))
        }
        _ => {
            let output_type =
                GeometryType::new(input_field.metadata().clone()).with_coord_type(coord_type);
            Ok(Arc::new(output_type.to_field("", true)))
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Start,
    End,
    N(i64),
}

#[derive(Debug, Clone, Copy)]
enum PointSelector<'a> {
    Fixed(Mode),
    PerRow(&'a Int64Array),
}

impl PointSelector<'_> {
    fn coordinate(
        &self,
        line_string: &impl LineStringTrait<T = f64>,
        row: usize,
    ) -> Option<impl CoordTrait<T = f64>> {
        match self {
            Self::Fixed(mode) => get_linestring_coord(line_string, *mode),
            Self::PerRow(indices) => {
                if indices.is_null(row) {
                    None
                } else {
                    get_linestring_coord(line_string, Mode::N(indices.value(row)))
                }
            }
        }
    }
}

fn point_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
    mode: Mode,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        return Ok(null_geometry_array(args.number_rows, coord_type)?
            .into_array_ref()
            .into());
    }

    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let selector = PointSelector::Fixed(mode);

    let out: Arc<dyn GeoArrowArray> = match geo_array.data_type() {
        GeoArrowType::LineString(_) => Arc::new(impl_fixed_dim(
            geo_array.as_line_string(),
            coord_type,
            selector,
        )?),
        _ => Arc::new(downcast_geoarrow_array!(
            geo_array_ref,
            impl_variable_dim,
            coord_type,
            selector
        )?),
    };

    Ok(out.into_array_ref().into())
}

fn point_n_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        return Ok(null_geometry_array(args.number_rows, coord_type)?
            .into_array_ref()
            .into());
    }

    let indices = arrays[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "ST_PointN expected an Int64 index array, got {}",
                arrays[1].data_type()
            ))
        })?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let selector = PointSelector::PerRow(indices);

    let out: Arc<dyn GeoArrowArray> = match geo_array.data_type() {
        GeoArrowType::LineString(_) => Arc::new(impl_fixed_dim(
            geo_array.as_line_string(),
            coord_type,
            selector,
        )?),
        _ => Arc::new(downcast_geoarrow_array!(
            geo_array_ref,
            impl_variable_dim,
            coord_type,
            selector
        )?),
    };

    Ok(out.into_array_ref().into())
}

fn null_geometry_output_type(coord_type: CoordType) -> FieldRef {
    Arc::new(
        GeometryType::new(Default::default())
            .with_coord_type(coord_type)
            .to_field("", true),
    )
}

fn null_geometry_array(
    number_rows: usize,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(output_type);
    for _ in 0..number_rows {
        builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
    }
    Ok(builder.finish())
}

fn impl_fixed_dim(
    array: &LineStringArray,
    coord_type: CoordType,
    selector: PointSelector<'_>,
) -> GeoDataFusionResult<PointArray> {
    let typ = PointType::new(
        array.extension_type().dimension(),
        array.extension_type().metadata().clone(),
    )
    .with_coord_type(coord_type);
    let mut output_builder = PointBuilder::with_capacity(typ, array.len());

    for (row, geom) in array.iter().enumerate() {
        if let Some(ls) = geom {
            output_builder.push_coord(selector.coordinate(&ls?, row).as_ref());
        } else {
            output_builder.push_null();
        }
    }

    Ok(output_builder.finish())
}

fn impl_variable_dim<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
    selector: PointSelector<'_>,
) -> GeoDataFusionResult<GeometryArray> {
    let typ = GeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut output_builder = GeometryBuilder::new(typ);

    for (row, geom) in array.iter().enumerate() {
        if let Some(ls) = geom {
            output_builder.push_geometry(get_geometry_point(&ls?, selector, row).as_ref())?;
        } else {
            output_builder.push_null();
        }
    }

    Ok(output_builder.finish())
}

fn get_geometry_point(
    geom: &impl GeometryTrait<T = f64>,
    selector: PointSelector<'_>,
    row: usize,
) -> Option<impl PointTrait<T = f64>> {
    match geom.as_type() {
        geo_traits::GeometryType::LineString(ls) => {
            selector.coordinate(ls, row).map(coord_to_point)
        }
        _ => None,
    }
}

fn get_linestring_coord(
    geom: &impl LineStringTrait<T = f64>,
    mode: Mode,
) -> Option<impl CoordTrait<T = f64>> {
    match mode {
        Mode::Start => geom.coord(0),
        Mode::End => geom.num_coords().checked_sub(1).and_then(|i| geom.coord(i)),
        Mode::N(index) => {
            point_n_position(geom.num_coords(), index).and_then(|position| geom.coord(position))
        }
    }
}

fn point_n_position(num_coords: usize, index: i64) -> Option<usize> {
    let position = match index {
        1.. => usize::try_from(index - 1).ok()?,
        i64::MIN..=-1 => {
            let distance_from_end = usize::try_from(index.unsigned_abs()).ok()?;
            num_coords.checked_sub(distance_from_end)?
        }
        0 => return None,
    };
    Some(position)
}

/// Convert an arbitrary coord to a Point
fn coord_to_point(coord: impl CoordTrait<T = f64>) -> wkt::types::Point {
    let (z, m) = match coord.dim() {
        geo_traits::Dimensions::Xy => (None, None),
        geo_traits::Dimensions::Xyz => (coord.nth(2), None),
        geo_traits::Dimensions::Xym => (None, coord.nth(2)),
        geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(_) => {
            (coord.nth(2), coord.nth(3))
        }
    };
    let coord = wkt::types::Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    };
    wkt::types::Point::from_coord(coord)
}
