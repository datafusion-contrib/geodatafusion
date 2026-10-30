//! LineString constructor from Point or LineString geometries.

use std::sync::{Arc, LazyLock, OnceLock};

use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{CoordTrait, Dimensions, GeometryTrait, LineStringTrait, PointTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, from_arrow_array};
use geoarrow_array::builder::{GeometryBuilder, LineStringBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, GeometryType, LineStringType, Metadata};
use wkt::Wkt;
use wkt::types::{Coord, Dimension as WktDimension, LineString as WktLineString};

use crate::error::GeoDataFusionResult;

/// `ST_MakeLine` scalar UDF implementation: creates a `LineString` from `Point`, `MultiPoint`, or `LineString` geometries.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeLine {
    coord_type: CoordType,
}

impl MakeLine {
    /// Creates a new `ST_MakeLine` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for MakeLine {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| Signature::any(2, Volatility::Immutable));

impl ScalarUDFImpl for MakeLine {
    fn name(&self) -> &str {
        "st_makeline"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(make_line_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a LineString from two Point or LineString geometries.",
                "ST_MakeLine(ST_Point(1, 2), ST_Point(3, 4))",
            )
            .with_argument("geom1", "geometry")
            .with_argument("geom2", "geometry")
            .with_related_udf("st_linefrommultipoint")
            .with_related_udf("st_makepoint")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let left_type = resolve_arg_type(args.arg_fields[0].as_ref())?;
    let right_type = resolve_arg_type(args.arg_fields[1].as_ref())?;
    let metadata = merge_metadata(left_type.as_ref(), right_type.as_ref())?;

    if let (Some(left_dim), Some(right_dim)) = (
        left_type.as_ref().and_then(fixed_line_input_dimension),
        right_type.as_ref().and_then(fixed_line_input_dimension),
    ) {
        let output_type = LineStringType::new(union_dimension(left_dim, right_dim), metadata)
            .with_coord_type(coord_type);
        return Ok(Arc::new(output_type.to_field("", true)));
    }

    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn make_line_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let left_type = resolve_arg_type(args.arg_fields[0].as_ref())?;
    let right_type = resolve_arg_type(args.arg_fields[1].as_ref())?;
    let metadata = merge_metadata(left_type.as_ref(), right_type.as_ref())?;

    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let (Some(left_type), Some(right_type)) = (left_type, right_type) else {
        let output = null_geometry_array(args.number_rows, metadata, coord_type)?;
        return Ok(output.into_array_ref().into());
    };

    let left_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_array = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let left_ref = left_array.as_ref();
    let right_ref = right_array.as_ref();

    let left_inputs = downcast_geoarrow_array!(left_ref, extract_inputs, right_ref)?;
    let right_inputs = downcast_geoarrow_array!(right_ref, extract_inputs, left_ref)?;

    let out: Arc<dyn GeoArrowArray> = if let (Some(left_dim), Some(right_dim)) = (
        fixed_line_input_dimension(&left_type),
        fixed_line_input_dimension(&right_type),
    ) {
        let output_type = LineStringType::new(union_dimension(left_dim, right_dim), metadata)
            .with_coord_type(coord_type);
        Arc::new(build_fixed_dim_array(
            &left_inputs,
            &right_inputs,
            output_type,
        )?)
    } else {
        let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
        Arc::new(build_variable_dim_array(
            &left_inputs,
            &right_inputs,
            output_type,
        )?)
    };

    Ok(out.into_array_ref().into())
}

fn resolve_arg_type(field: &Field) -> GeoDataFusionResult<Option<GeoArrowType>> {
    if field.data_type() == &DataType::Null {
        Ok(None)
    } else {
        Ok(Some(GeoArrowType::from_arrow_field(field)?))
    }
}

fn fixed_line_input_dimension(geo_type: &GeoArrowType) -> Option<Dimension> {
    match geo_type {
        GeoArrowType::Point(typ) => Some(typ.dimension()),
        GeoArrowType::LineString(typ) => Some(typ.dimension()),
        _ => None,
    }
}

fn merge_metadata(
    left: Option<&GeoArrowType>,
    right: Option<&GeoArrowType>,
) -> GeoDataFusionResult<Arc<Metadata>> {
    match (left, right) {
        (None, None) => Ok(Arc::new(Metadata::default())),
        (Some(typ), None) | (None, Some(typ)) => Ok(typ.metadata().clone()),
        (Some(left), Some(right)) => {
            if left.metadata().crs() != right.metadata().crs() {
                return Err(DataFusionError::Execution(
                    "ST_MakeLine cannot operate on mixed SRID geometries".to_string(),
                )
                .into());
            }
            Ok(left.metadata().clone())
        }
    }
}

fn null_geometry_array(
    number_rows: usize,
    metadata: Arc<Metadata>,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(output_type);
    for _ in 0..number_rows {
        builder.push_null();
    }
    Ok(builder.finish())
}

#[derive(Debug)]
enum InputGeometry {
    Point {
        dimension: WktDimension,
        coord: Option<Coord<f64>>,
    },
    LineString {
        dimension: WktDimension,
        coords: Vec<Coord<f64>>,
    },
}

impl InputGeometry {
    fn from_geometry(geometry: &impl GeometryTrait<T = f64>) -> GeoDataFusionResult<Self> {
        match geometry.as_type() {
            geo_traits::GeometryType::Point(point) => {
                let dimension = dimension_from_traits(point.dim())?;
                let coord = point
                    .coord()
                    .map(|coord| extract_coord(&coord, dimension))
                    .transpose()?;
                Ok(Self::Point { dimension, coord })
            }
            geo_traits::GeometryType::LineString(line_string) => {
                let dimension = dimension_from_traits(line_string.dim())?;
                let coords = line_string
                    .coords()
                    .map(|coord| extract_coord(&coord, dimension))
                    .collect::<GeoDataFusionResult<Vec<_>>>()?;
                Ok(Self::LineString { dimension, coords })
            }
            geometry_type => {
                let name = match geometry_type {
                    geo_traits::GeometryType::MultiPoint(_) => "MULTIPOINT",
                    geo_traits::GeometryType::Polygon(_) => "POLYGON",
                    geo_traits::GeometryType::MultiLineString(_) => "MULTILINESTRING",
                    geo_traits::GeometryType::MultiPolygon(_) => "MULTIPOLYGON",
                    geo_traits::GeometryType::GeometryCollection(_) => "GEOMETRYCOLLECTION",
                    geo_traits::GeometryType::Rect(_) => "RECT",
                    geo_traits::GeometryType::Triangle(_) => "TRIANGLE",
                    geo_traits::GeometryType::Line(_) => "LINE",
                    geo_traits::GeometryType::Point(_)
                    | geo_traits::GeometryType::LineString(_) => unreachable!(),
                };
                Err(DataFusionError::Execution(format!(
                    "ST_MakeLine requires POINT or LINESTRING inputs, got {name}"
                ))
                .into())
            }
        }
    }

    fn dimension(&self) -> WktDimension {
        match self {
            Self::Point { dimension, .. } | Self::LineString { dimension, .. } => *dimension,
        }
    }

    fn num_coords(&self) -> usize {
        match self {
            Self::Point { coord, .. } => usize::from(coord.is_some()),
            Self::LineString { coords, .. } => coords.len(),
        }
    }
}

fn extract_inputs<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    other: &dyn GeoArrowArray,
) -> GeoDataFusionResult<Vec<Option<InputGeometry>>> {
    let mut inputs = Vec::with_capacity(array.len());
    for (index, item) in array.iter().enumerate() {
        if other.is_null(index) {
            inputs.push(None);
            continue;
        }
        let Some(geometry) = item else {
            inputs.push(None);
            continue;
        };
        inputs.push(Some(InputGeometry::from_geometry(&geometry?)?));
    }
    Ok(inputs)
}

fn build_fixed_dim_array(
    left_inputs: &[Option<InputGeometry>],
    right_inputs: &[Option<InputGeometry>],
    output_type: LineStringType,
) -> GeoDataFusionResult<LineStringArray> {
    let mut builder = LineStringBuilder::new(output_type);
    for (left, right) in left_inputs.iter().zip(right_inputs) {
        let (Some(left), Some(right)) = (left.as_ref(), right.as_ref()) else {
            builder.push_line_string(None::<&WktLineString>)?;
            continue;
        };
        let line = make_line_pair(left, right)?;
        builder.push_line_string(Some(&line))?;
    }
    Ok(builder.finish())
}

fn build_variable_dim_array(
    left_inputs: &[Option<InputGeometry>],
    right_inputs: &[Option<InputGeometry>],
    output_type: GeometryType,
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = GeometryBuilder::new(output_type);
    for (left, right) in left_inputs.iter().zip(right_inputs) {
        let (Some(left), Some(right)) = (left.as_ref(), right.as_ref()) else {
            builder.push_null();
            continue;
        };
        let line = Wkt::LineString(make_line_pair(left, right)?);
        builder.push_geometry(Some(&line))?;
    }
    Ok(builder.finish())
}

fn make_line_pair(
    left: &InputGeometry,
    right: &InputGeometry,
) -> GeoDataFusionResult<WktLineString> {
    let target_dimension = union_wkt_dimension(left.dimension(), right.dimension());
    let mut coords = Vec::with_capacity(left.num_coords() + right.num_coords());
    append_input(&mut coords, left, target_dimension)?;
    append_input(&mut coords, right, target_dimension)?;
    Ok(WktLineString::new(coords, target_dimension))
}

fn append_input(
    coords: &mut Vec<Coord<f64>>,
    input: &InputGeometry,
    target_dimension: WktDimension,
) -> GeoDataFusionResult<()> {
    match input {
        InputGeometry::Point { coord, .. } => {
            if let Some(coord) = coord {
                coords.push(pad_coord(coord, target_dimension));
            }
        }
        InputGeometry::LineString {
            dimension,
            coords: line_coords,
        } => {
            if line_coords.is_empty() {
                return Ok(());
            }
            if *dimension != target_dimension {
                return Err(DataFusionError::Execution(
                    "ST_MakeLine cannot append LINESTRING with mixed coordinate dimensions"
                        .to_string(),
                )
                .into());
            }
            let skip_first = coords
                .last()
                .zip(line_coords.first())
                .is_some_and(|(last, first)| last.x == first.x && last.y == first.y);
            coords.extend_from_slice(&line_coords[usize::from(skip_first)..]);
        }
    }
    Ok(())
}

fn extract_coord(
    coord: &impl CoordTrait<T = f64>,
    dimension: WktDimension,
) -> GeoDataFusionResult<Coord<f64>> {
    let expected_dimensions = Dimensions::from(dimension);
    if coord.dim().size() != expected_dimensions.size() {
        return Err(DataFusionError::Execution(
            "ST_MakeLine requires coordinates within a geometry to share the same dimension"
                .to_string(),
        )
        .into());
    }

    let z = match dimension {
        WktDimension::XYZ | WktDimension::XYZM => Some(coord.nth_or_panic(2)),
        WktDimension::XY | WktDimension::XYM => None,
    };
    let m = match dimension {
        WktDimension::XYM => Some(coord.nth_or_panic(2)),
        WktDimension::XYZM => Some(coord.nth_or_panic(3)),
        WktDimension::XY | WktDimension::XYZ => None,
    };
    Ok(Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    })
}

fn pad_coord(coord: &Coord<f64>, target_dimension: WktDimension) -> Coord<f64> {
    let z = match target_dimension {
        WktDimension::XYZ | WktDimension::XYZM => Some(coord.z.unwrap_or(0.0)),
        WktDimension::XY | WktDimension::XYM => None,
    };
    let m = match target_dimension {
        WktDimension::XYM | WktDimension::XYZM => Some(coord.m.unwrap_or(0.0)),
        WktDimension::XY | WktDimension::XYZ => None,
    };
    Coord {
        x: coord.x,
        y: coord.y,
        z,
        m,
    }
}

fn dimension_from_traits(dimensions: Dimensions) -> GeoDataFusionResult<WktDimension> {
    match dimensions {
        Dimensions::Xy => Ok(WktDimension::XY),
        Dimensions::Xyz => Ok(WktDimension::XYZ),
        Dimensions::Xym => Ok(WktDimension::XYM),
        Dimensions::Xyzm => Ok(WktDimension::XYZM),
        Dimensions::Unknown(size) => Err(DataFusionError::Execution(format!(
            "ST_MakeLine does not support {size}-dimensional coordinates"
        ))
        .into()),
    }
}

fn union_dimension(left: Dimension, right: Dimension) -> Dimension {
    let has_z = matches!(left, Dimension::XYZ | Dimension::XYZM)
        || matches!(right, Dimension::XYZ | Dimension::XYZM);
    let has_m = matches!(left, Dimension::XYM | Dimension::XYZM)
        || matches!(right, Dimension::XYM | Dimension::XYZM);
    match (has_z, has_m) {
        (false, false) => Dimension::XY,
        (true, false) => Dimension::XYZ,
        (false, true) => Dimension::XYM,
        (true, true) => Dimension::XYZM,
    }
}

fn union_wkt_dimension(left: WktDimension, right: WktDimension) -> WktDimension {
    let has_z = matches!(left, WktDimension::XYZ | WktDimension::XYZM)
        || matches!(right, WktDimension::XYZ | WktDimension::XYZM);
    let has_m = matches!(left, WktDimension::XYM | WktDimension::XYZM)
        || matches!(right, WktDimension::XYM | WktDimension::XYZM);
    match (has_z, has_m) {
        (false, false) => WktDimension::XY,
        (true, false) => WktDimension::XYZ,
        (false, true) => WktDimension::XYM,
        (true, true) => WktDimension::XYZM,
    }
}
