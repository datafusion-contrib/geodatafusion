//! Planar `ST_Project` geometry editor.

use std::sync::OnceLock;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, PointTrait};
use geoarrow_array::array::PointArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, Point as WktPoint};

use super::transform::return_field;
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{StrictGeometryBuilder, geometry_array_from_arrow};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Project {
    signature: Signature,
}

impl Project {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Project {
    fn default() -> Self {
        Self::new()
    }
}

static PROJECT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Project {
    fn name(&self) -> &str {
        "st_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return Err(DataFusionError::Plan(format!(
                "ST_Project does not accept {} arguments",
                arg_types.len()
            )));
        }

        if !is_geometry_or_null(&arg_types[0]) {
            return Err(DataFusionError::Plan(format!(
                "ST_Project does not support argument type {}",
                arg_types[0]
            )));
        }

        let two_geometry = is_supported_geometry_type(&arg_types[1]);
        if two_geometry {
            Ok(vec![
                arg_types[0].clone(),
                arg_types[1].clone(),
                DataType::Float64,
            ])
        } else {
            coerce_float_arg(&arg_types[1], 2)?;
            coerce_float_arg(&arg_types[2], 3)?;
            Ok(vec![
                arg_types[0].clone(),
                DataType::Float64,
                DataType::Float64,
            ])
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Project requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field(args, None)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(project_impl(args)?)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(PROJECT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Projects a point by a planar distance and azimuth. Azimuth is in radians \
                 clockwise from north, and distance is in coordinate-system units. The \
                 two-geometry form projects `geom_to` onward along the vector from `geom_from` \
                 to `geom_to`.",
                "ST_Project(geom, distance, azimuth) or ST_Project(geom_from, geom_to, distance)",
            )
            .with_argument("geom", "Point geometry")
            .with_argument("distance", "planar distance in coordinate-system units")
            .with_argument("azimuth", "radians clockwise from north")
            .build()
        }))
    }
}

fn coerce_float_arg(data_type: &DataType, argument: usize) -> Result<()> {
    match data_type {
        DataType::Null
        | DataType::Float64
        | DataType::Float32
        | DataType::Int64
        | DataType::Int32 => Ok(()),
        other => Err(DataFusionError::Plan(format!(
            "ST_Project argument {argument} must be numeric, got {other}"
        ))),
    }
}

fn is_geometry_or_null(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Null) || is_supported_geometry_type(data_type)
}

fn is_supported_geometry_type(data_type: &DataType) -> bool {
    any_geometry_type()
        .iter()
        .any(|candidate| candidate == data_type)
}

fn project_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let two_geometry = is_supported_geometry_type(args.arg_fields[1].data_type());

    let from = point_values(
        &arrays[0],
        &args.arg_fields[0],
        if two_geometry {
            "Arguments must be POINT geometries"
        } else {
            "Argument must be POINT geometry"
        },
    )?;
    let outputs = if two_geometry {
        let to = point_values(
            &arrays[1],
            &args.arg_fields[1],
            "Arguments must be POINT geometries",
        )?;
        let distances = float_values(&arrays[2])?;
        project_pairs(&from, &to, &distances)
    } else {
        let distances = float_values(&arrays[1])?;
        let azimuths = float_values(&arrays[2])?;
        project_directions(&from, &distances, &azimuths)
    };

    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    Ok(ColumnarValue::Array(build_points(outputs, output_type)?))
}

fn project_pairs(
    from: &[Option<WktPoint>],
    to: &[Option<WktPoint>],
    distances: &[Option<f64>],
) -> Vec<Option<WktPoint>> {
    let mut outputs = Vec::with_capacity(from.len());
    for index in 0..from.len() {
        let (Some(from), Some(to), Some(distance)) = (
            from.get(index).and_then(Option::as_ref),
            to.get(index).and_then(Option::as_ref),
            distances.get(index).copied().flatten(),
        ) else {
            outputs.push(None);
            continue;
        };
        outputs.push(Some(project_to(from, to, distance)));
    }
    outputs
}

fn project_directions(
    from: &[Option<WktPoint>],
    distances: &[Option<f64>],
    azimuths: &[Option<f64>],
) -> Vec<Option<WktPoint>> {
    let mut outputs = Vec::with_capacity(from.len());
    for index in 0..from.len() {
        let (Some(from), Some(distance), Some(azimuth)) = (
            from.get(index).and_then(Option::as_ref),
            distances.get(index).copied().flatten(),
            azimuths.get(index).copied().flatten(),
        ) else {
            outputs.push(None);
            continue;
        };
        outputs.push(Some(project_by_direction(from, distance, azimuth)));
    }
    outputs
}

fn project_by_direction(from: &WktPoint, distance: f64, azimuth: f64) -> WktPoint {
    let coord = from
        .coord()
        .expect("point_values, not project_by_direction, handles empty points");
    // PostGIS converts the clockwise-from-north azimuth to a conventional
    // mathematical slope before applying sin/cos.
    let two_pi = 2.0 * std::f64::consts::PI;
    let azimuth = azimuth - two_pi * (azimuth / two_pi).floor();
    let mut slope = two_pi - azimuth + std::f64::consts::FRAC_PI_2;
    if slope > 0.0 && slope > two_pi {
        slope -= two_pi;
    }
    if slope < 0.0 && slope < -two_pi {
        slope += two_pi;
    }
    WktPoint::new(
        Some(WktCoord {
            x: coord.x + distance * slope.cos(),
            y: coord.y + distance * slope.sin(),
            z: coord.z,
            m: coord.m,
        }),
        from.dimension(),
    )
}

fn project_to(from: &WktPoint, to: &WktPoint, distance: f64) -> WktPoint {
    let from_coord = from
        .coord()
        .expect("point_values, not project_to, handles empty points");
    let to_coord = to
        .coord()
        .expect("point_values, not project_to, handles empty points");
    let delta_x = to_coord.x - from_coord.x;
    let delta_y = to_coord.y - from_coord.y;
    let length = (delta_x * delta_x + delta_y * delta_y).sqrt();
    let prop = if length == 0.0 {
        0.0
    } else {
        distance / length
    };

    let has_z = matches!(from.dimension(), WktDimension::XYZ | WktDimension::XYZM);
    let has_m = matches!(from.dimension(), WktDimension::XYM | WktDimension::XYZM);
    let from_z = if has_z {
        from_coord.z.unwrap_or(0.0)
    } else {
        0.0
    };
    let to_z = if has_z {
        to_coord.z.unwrap_or(0.0)
    } else {
        0.0
    };
    let from_m = if has_m {
        from_coord.m.unwrap_or(0.0)
    } else {
        0.0
    };
    let to_m = if has_m {
        to_coord.m.unwrap_or(0.0)
    } else {
        0.0
    };

    WktPoint::new(
        Some(WktCoord {
            x: to_coord.x + delta_x * prop,
            y: to_coord.y + delta_y * prop,
            z: has_z.then_some(to_z + (to_z - from_z) * prop),
            m: has_m.then_some(to_m + (to_m - from_m) * prop),
        }),
        from.dimension(),
    )
}

fn build_points(
    values: Vec<Option<WktPoint>>,
    output_type: GeoArrowType,
) -> GeoDataFusionResult<ArrayRef> {
    match output_type {
        GeoArrowType::Point(point_type) => {
            let mut builder = PointBuilder::new(point_type);
            for value in values {
                match value {
                    Some(value) => builder.push_point(Some(&value)),
                    None => builder.push_null(),
                }
            }
            let array: PointArray = builder.finish();
            Ok(array.into_array_ref())
        }
        GeoArrowType::Geometry(geometry_type) => {
            let mut builder = StrictGeometryBuilder::new(geometry_type);
            for value in values {
                builder.push_geometry(value.as_ref())?;
            }
            Ok(builder.finish().into_array_ref())
        }
        other => Err(DataFusionError::Internal(format!(
            "ST_Project output type {other:?} is not a geometry"
        ))
        .into()),
    }
}

fn point_values(
    array: &ArrayRef,
    field: &FieldRef,
    error_message: &str,
) -> GeoDataFusionResult<Vec<Option<WktPoint>>> {
    if array.data_type() == &DataType::Null {
        return Ok(vec![None; array.len()]);
    }
    let geo_array = geometry_array_from_arrow(array, field)?;
    let geo_array = geo_array.as_ref();
    downcast_geoarrow_array!(geo_array, point_values_impl, error_message)
}

fn point_values_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    error_message: &str,
) -> GeoDataFusionResult<Vec<Option<WktPoint>>> {
    let mut values = Vec::with_capacity(array.len());
    for geometry in array.iter() {
        let value = match geometry {
            None => None,
            Some(geometry) => point_from_geometry(&geometry?, error_message)?,
        };
        values.push(value);
    }
    Ok(values)
}

fn point_from_geometry<G: GeometryTrait<T = f64>>(
    geometry: &G,
    error_message: &str,
) -> GeoDataFusionResult<Option<WktPoint>> {
    let GeometryType::Point(point) = geometry.as_type() else {
        return Err(DataFusionError::Execution(error_message.to_string()).into());
    };
    let Some(coord) = point.coord() else {
        return Ok(None);
    };
    let (z, m) = match coord.dim() {
        geo_traits::Dimensions::Xy => (None, None),
        geo_traits::Dimensions::Xyz => (coord.nth(2), None),
        geo_traits::Dimensions::Xym => (None, coord.nth(2)),
        geo_traits::Dimensions::Xyzm => (coord.nth(2), coord.nth(3)),
        geo_traits::Dimensions::Unknown(_) => (None, None),
    };
    let dimension = match coord.dim() {
        geo_traits::Dimensions::Xy => WktDimension::XY,
        geo_traits::Dimensions::Xyz => WktDimension::XYZ,
        geo_traits::Dimensions::Xym => WktDimension::XYM,
        geo_traits::Dimensions::Xyzm => WktDimension::XYZM,
        geo_traits::Dimensions::Unknown(_) => WktDimension::XY,
    };
    Ok(Some(WktPoint::new(
        Some(WktCoord {
            x: coord.x(),
            y: coord.y(),
            z,
            m,
        }),
        dimension,
    )))
}

fn float_values(array: &ArrayRef) -> GeoDataFusionResult<Vec<Option<f64>>> {
    let values = match array.data_type() {
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .collect::<Vec<_>>(),
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .iter()
            .map(|value| value.map(f64::from))
            .collect(),
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .iter()
            .map(|value| value.map(|value| value as f64))
            .collect(),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .iter()
            .map(|value| value.map(f64::from))
            .collect(),
        DataType::Null => vec![None; array.len()],
        other => {
            return Err(DataFusionError::Execution(format!(
                "ST_Project expected a numeric argument, got {other}"
            ))
            .into());
        }
    };
    Ok(values)
}
