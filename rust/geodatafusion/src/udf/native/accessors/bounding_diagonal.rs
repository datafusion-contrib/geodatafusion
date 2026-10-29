use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::cast::AsArray;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{Dimensions, GeometryTrait};
use geoarrow_array::array::GeometryArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType};
use wkt::types::{Coord, Dimension as WktDimension, LineString as WktLineString};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::bounds::{
    ExtentBounds, GeometryExtent, geometry_extent,
};
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata, field_metadata_or_default, geometry_array_from_arrow,
    is_null_geometry_input, null_geometry_array,
};

static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::user_defined(Volatility::Immutable));

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct BoundingDiagonal {
    coord_type: CoordType,
}

impl BoundingDiagonal {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for BoundingDiagonal {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for BoundingDiagonal {
    fn name(&self) -> &str {
        "st_boundingdiagonal"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata = field_metadata(args.arg_fields[0].as_ref())?;
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(bounding_diagonal_impl(args, self.coord_type)?)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !matches!(arg_types.len(), 1 | 2) {
            return Err(DataFusionError::Plan(format!(
                "ST_BoundingDiagonal expects 1 or 2 arguments, got {}",
                arg_types.len()
            )));
        }

        let geometry_type = &arg_types[0];
        let is_geometry = geometry_type == &DataType::Null
            || any_geometry_type()
                .iter()
                .any(|data_type| data_type == geometry_type);
        if !is_geometry {
            return Err(DataFusionError::Plan(format!(
                "ST_BoundingDiagonal argument 1 must be a geometry, got {geometry_type}"
            )));
        }

        let mut coerced = vec![geometry_type.clone()];
        if let Some(fits_type) = arg_types.get(1) {
            if !matches!(fits_type, DataType::Boolean | DataType::Null) {
                return Err(DataFusionError::Plan(format!(
                    "ST_BoundingDiagonal argument 2 must be boolean, got {fits_type}"
                )));
            }
            coerced.push(DataType::Boolean);
        }
        Ok(coerced)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the diagonal of the geometry's bounding box as a 2-point LineString. Empty inputs return an empty LineString unless fits is true, in which case PostGIS returns a zero-length LineString at the origin.",
                "ST_BoundingDiagonal(geometry[, fits])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("fits", "boolean")
            .build()
        }))
    }
}

fn bounding_diagonal_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let input_field = args.arg_fields[0].as_ref();
    if is_null_geometry_input(input_field, arrays[0].as_ref()) {
        let output_type =
            GeometryType::new(field_metadata_or_default(input_field)?).with_coord_type(coord_type);
        return Ok(null_geometry_array(output_type, arrays[0].len())?.into());
    }

    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let output_type =
        GeometryType::new(geo_array.data_type().metadata().clone()).with_coord_type(coord_type);
    let fits = arrays.get(1).map(AsArray::as_boolean);
    let geo_array_ref = geo_array.as_ref();
    let output =
        downcast_geoarrow_array!(geo_array_ref, bounding_diagonal_array, &output_type, fits)?;
    Ok(output.into_array_ref().into())
}

fn bounding_diagonal_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeometryType,
    fits: Option<&arrow_array::BooleanArray>,
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = StrictGeometryBuilder::new(output_type.clone());

    for (row, geometry) in array.iter().enumerate() {
        let fits = match fits {
            Some(fits) => match fits.iter().nth(row).flatten() {
                Some(fits) => fits,
                None => {
                    builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
                    continue;
                }
            },
            None => false,
        };

        let Some(geometry) = geometry.transpose()? else {
            builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            continue;
        };

        match geometry_extent(&geometry) {
            GeometryExtent::Empty => {
                let line = if fits {
                    zero_line(geometry.dim())?
                } else {
                    empty_line(geometry.dim())?
                };
                builder.push_geometry(Some(&line))?;
            }
            GeometryExtent::NonFinite => {
                return Err(DataFusionError::Execution(
                    "ST_BoundingDiagonal does not support non-finite coordinates".to_string(),
                )
                .into());
            }
            GeometryExtent::Finite(bounds) => {
                let line = diagonal_line(&bounds)?;
                builder.push_geometry(Some(&line))?;
            }
        }
    }

    Ok(builder.finish())
}

fn diagonal_line(bounds: &ExtentBounds) -> GeoDataFusionResult<WktLineString<f64>> {
    let dimension = wkt_dimension(bounds.dimensions())?;
    let min = bounds.min();
    let max = bounds.max();
    Ok(WktLineString::new(
        vec![
            line_coord(dimension, min[0], min[1], min[2], min[3]),
            line_coord(dimension, max[0], max[1], max[2], max[3]),
        ],
        dimension,
    ))
}

fn empty_line(dimension: Dimensions) -> GeoDataFusionResult<WktLineString<f64>> {
    Ok(WktLineString::new(Vec::new(), wkt_dimension(dimension)?))
}

fn zero_line(dimension: Dimensions) -> GeoDataFusionResult<WktLineString<f64>> {
    let dimension = wkt_dimension(dimension)?;
    let coord = line_coord(dimension, 0.0, 0.0, 0.0, 0.0);
    Ok(WktLineString::new(vec![coord, coord], dimension))
}

fn line_coord(dimension: WktDimension, x: f64, y: f64, z: f64, m: f64) -> Coord<f64> {
    Coord {
        x,
        y,
        z: matches!(dimension, WktDimension::XYZ | WktDimension::XYZM).then_some(z),
        m: matches!(dimension, WktDimension::XYM | WktDimension::XYZM).then_some(m),
    }
}

fn wkt_dimension(dimension: Dimensions) -> GeoDataFusionResult<WktDimension> {
    match dimension {
        Dimensions::Xy => Ok(WktDimension::XY),
        Dimensions::Xyz => Ok(WktDimension::XYZ),
        Dimensions::Xym => Ok(WktDimension::XYM),
        Dimensions::Xyzm => Ok(WktDimension::XYZM),
        Dimensions::Unknown(size) => Err(DataFusionError::NotImplemented(format!(
            "ST_BoundingDiagonal does not support geometries with {size} physical dimensions"
        ))
        .into()),
    }
}
