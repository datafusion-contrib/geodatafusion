use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeValid {
    coord_type: CoordType,
}

impl MakeValid {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for MakeValid {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeValid {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makevalid"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata =
            Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(make_valid_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Attempts to make an invalid polygonal geometry valid using a buffer(0) strategy. Note: this is an approximation; it may not preserve all input vertices. Non-polygonal geometries are returned unchanged.",
                "ST_MakeValid(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn make_valid_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = make_valid_array(&geo_array)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn make_valid_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _make_valid_impl)
}

fn _make_valid_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            // Use buffer(0) as a simple make-valid strategy
            use geo::Buffer;
            let valid = match &geo_geom {
                geo::Geometry::Polygon(_) | geo::Geometry::MultiPolygon(_) => {
                    let buffered = geo_geom.buffer(0.0);
                    geo::Geometry::MultiPolygon(buffered)
                }
                other => other.clone(),
            };
            builder.push_geometry(Some(&valid))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}
