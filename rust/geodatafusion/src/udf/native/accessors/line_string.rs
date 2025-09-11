//! Accessors from LineString geometries

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait, PointTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, PointArray, from_arrow_array};
use geoarrow_array::builder::{GeometryBuilder, PointBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, PointType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct StartPoint {
    signature: Signature,
    coord_type: CoordType,
}

impl StartPoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for StartPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static START_POINT_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StartPoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_startpoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
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

#[derive(Debug)]
pub struct EndPoint {
    signature: Signature,
    coord_type: CoordType,
}

impl EndPoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for EndPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static END_POINT_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for EndPoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_endpoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
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

// Not yet exported because we don't handle the nth point second argument yet
#[derive(Debug)]
#[allow(dead_code)]
struct PointN {
    signature: Signature,
    coord_type: CoordType,
}

impl PointN {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for PointN {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_N_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointN {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_pointn"
    }

    fn signature(&self) -> &Signature {
        &self.signature
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
        Some(POINT_N_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Return the Nth point in a single linestring or circular linestring in the geometry. Negative values are counted backwards from the end of the LineString, so that -1 is the last point. Returns NULL if there is no linestring in the geometry.", "ST_PointN(line_string, 1)" )
                .with_argument("g1", "geometry")
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
    #[allow(dead_code)]
    N(usize),
}

fn point_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
    mode: Mode,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();

    let out: Arc<dyn GeoArrowArray> = match geo_array.data_type() {
        GeoArrowType::LineString(_) => Arc::new(impl_fixed_dim(
            geo_array.as_line_string(),
            coord_type,
            mode,
        )?),
        _ => Arc::new(downcast_geoarrow_array!(
            geo_array_ref,
            impl_variable_dim,
            coord_type,
            mode
        )?),
    };

    Ok(out.into_array_ref().into())
}

fn impl_fixed_dim(
    array: &LineStringArray,
    coord_type: CoordType,
    mode: Mode,
) -> GeoDataFusionResult<PointArray> {
    let typ = PointType::new(
        array.extension_type().dimension(),
        array.extension_type().metadata().clone(),
    )
    .with_coord_type(coord_type);
    let mut output_builder = PointBuilder::with_capacity(typ, array.len());

    for geom in array.iter() {
        if let Some(ls) = geom {
            output_builder.push_coord(get_linestring_coord(&ls?, mode).as_ref());
        } else {
            output_builder.push_null();
        }
    }

    Ok(output_builder.finish())
}

fn impl_variable_dim<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
    mode: Mode,
) -> GeoDataFusionResult<GeometryArray> {
    let typ = GeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut output_builder = GeometryBuilder::new(typ);

    for geom in array.iter() {
        if let Some(ls) = geom {
            output_builder.push_geometry(get_geometry_coord(&ls?, mode).as_ref())?;
        } else {
            output_builder.push_null();
        }
    }

    Ok(output_builder.finish())
}

fn get_geometry_coord(
    geom: &impl GeometryTrait<T = f64>,
    mode: Mode,
) -> Option<impl PointTrait<T = f64>> {
    match geom.as_type() {
        geo_traits::GeometryType::LineString(ls) => {
            get_linestring_coord(ls, mode).map(coord_to_point)
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
        Mode::End => geom.coord(geom.num_coords() - 1),
        Mode::N(n) => {
            if n < geom.num_coords() {
                // Index is 1-based as for OGC specs since version 0.8.0
                // https://postgis.net/docs/ST_PointN.html
                geom.coord(n - 1)
            } else {
                None
            }
        }
    }
}

/// Convert an arbitrary coord to a Point
fn coord_to_point(coord: impl CoordTrait<T = f64>) -> wkt::types::Point {
    let coord = wkt::types::Coord {
        x: coord.x(),
        y: coord.y(),
        z: coord.nth(2),
        m: coord.nth(3),
    };
    wkt::types::Point::from_coord(coord)
}
