use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo::ConvexHull as _;
use geoarrow_array::array::GeometryArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType};
use wkt::Wkt;

use super::util::{degenerate_hull, empty_wkt_like, is_empty_geometry, polygon_wkt};
use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata_or_default, geometry_array_from_arrow,
    is_null_geometry_input, null_geometry_array,
};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ConvexHull {
    coord_type: CoordType,
}

impl ConvexHull {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for ConvexHull {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ConvexHull {
    fn name(&self) -> &str {
        "st_convexhull"
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
        Ok(convex_hull_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the convex hull of a geometry. The convex hull is the smallest convex geometry that encloses all geometries in the input. The result is two-dimensional; Z and M ordinates are dropped.",
                "ST_ConvexHull(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let input_field = args.arg_fields[0].as_ref();
    let coord_type = GeoArrowType::from_arrow_field(input_field)
        .ok()
        .and_then(|input_type| input_type.coord_type())
        .unwrap_or(coord_type);
    let output_type =
        GeometryType::new(field_metadata_or_default(input_field)?).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn convex_hull_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let input_field = args.arg_fields[0].as_ref();
    let GeoArrowType::Geometry(output_type) =
        GeoArrowType::from_arrow_field(args.return_field.as_ref())?
    else {
        return Err(DataFusionError::Internal(
            "ST_ConvexHull result field is not a generic Geometry".to_string(),
        )
        .into());
    };
    if is_null_geometry_input(input_field, arrays[0].as_ref()) {
        return Ok(null_geometry_array(output_type, arrays[0].len())?.into());
    }

    let geo_array = geometry_array_from_arrow(&arrays[0], input_field)?;
    let geo_array_ref = geo_array.as_ref();
    let output = downcast_geoarrow_array!(geo_array_ref, convex_hull_array, &output_type)?;
    Ok(output.into_array_ref().into())
}

fn convex_hull_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeometryType,
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = StrictGeometryBuilder::new(output_type.clone());
    for item in array.iter() {
        match item {
            None => builder.push_geometry(None::<&Wkt<f64>>)?,
            Some(geometry) => {
                let geometry = geometry?;
                if is_empty_geometry(&geometry) {
                    builder.push_geometry(Some(&empty_wkt_like(&geometry)))?;
                    continue;
                }

                let geometry = geometry_to_geo(&geometry)?;
                let hull = geometry.convex_hull();
                let output = degenerate_hull(&hull).unwrap_or_else(|| polygon_wkt(&hull));
                builder.push_geometry(Some(&output))?;
            }
        }
    }
    Ok(builder.finish())
}
