use std::any::Any;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringBuilder;
use arrow_array::Array;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

// ---------------------------------------------------------------------------
// ST_AsGeoJSON
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsGeoJSON;

impl AsGeoJSON {
    pub fn new() -> Self {
        Self {}
    }

    fn invoke_impl(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
        let geo_array = from_arrow_array(&array, field.as_ref())?;
        let result = as_geojson_array(&geo_array)?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

impl Default for AsGeoJSON {
    fn default() -> Self {
        Self::new()
    }
}

static AS_GEOJSON_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsGeoJSON {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_asgeojson"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        Ok(Field::new(input_field.name(), DataType::Utf8, input_field.is_nullable()).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_GEOJSON_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the geometry as a GeoJSON element.",
                "ST_AsGeoJSON(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn as_geojson_array(
    array: &dyn GeoArrowArray,
) -> GeoArrowResult<arrow_array::StringArray> {
    downcast_geoarrow_array!(array, _as_geojson_impl)
}

fn _as_geojson_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<arrow_array::StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 64);
    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let geojson_value = geojson::Value::from(&geo_geom);
            let geojson_geom = geojson::Geometry::new(geojson_value);
            let json_string = serde_json::to_string(&geojson_geom)
                .map_err(|e| geoarrow_schema::error::GeoArrowError::InvalidGeoArrow(e.to_string()))?;
            builder.append_value(&json_string);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

// ---------------------------------------------------------------------------
// ST_GeomFromGeoJSON
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeomFromGeoJSON {
    signature: Signature,
    coord_type: CoordType,
}

impl GeomFromGeoJSON {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }

    fn invoke_impl(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];

        let string_arr = array.as_ref();
        let len = string_arr.len();

        let geom_type = GeometryType::new(Default::default()).with_coord_type(self.coord_type);
        let mut builder = GeometryBuilder::new(geom_type);

        match field.data_type() {
            DataType::Utf8 => {
                let str_arr = string_arr
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                for i in 0..len {
                    if str_arr.is_null(i) {
                        builder.push_null();
                    } else {
                        let geojson_str = str_arr.value(i);
                        let geo_geom = parse_geojson_to_geo(geojson_str)?;
                        builder.push_geometry(Some(&geo_geom))?;
                    }
                }
            }
            DataType::LargeUtf8 => {
                let str_arr = string_arr
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap();
                for i in 0..len {
                    if str_arr.is_null(i) {
                        builder.push_null();
                    } else {
                        let geojson_str = str_arr.value(i);
                        let geo_geom = parse_geojson_to_geo(geojson_str)?;
                        builder.push_geometry(Some(&geo_geom))?;
                    }
                }
            }
            DataType::Utf8View => {
                let str_arr = string_arr
                    .as_any()
                    .downcast_ref::<arrow_array::StringViewArray>()
                    .unwrap();
                for i in 0..len {
                    if str_arr.is_null(i) {
                        builder.push_null();
                    } else {
                        let geojson_str = str_arr.value(i);
                        let geo_geom = parse_geojson_to_geo(geojson_str)?;
                        builder.push_geometry(Some(&geo_geom))?;
                    }
                }
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unexpected data type for ST_GeomFromGeoJSON: {:?}",
                    string_arr.data_type()
                ))
                .into())
            }
        }

        Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
    }
}

fn parse_geojson_to_geo(geojson_str: &str) -> GeoDataFusionResult<geo::Geometry> {
    use geojson::GeoJson;

    let geojson = GeoJson::from_str(geojson_str).map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse GeoJSON: {}", e))
    })?;

    let geometry: geojson::Geometry = match geojson {
        GeoJson::Geometry(g) => g,
        GeoJson::Feature(f) => f.geometry.ok_or_else(|| {
            DataFusionError::Execution("GeoJSON Feature has no geometry".to_string())
        })?,
        GeoJson::FeatureCollection(_) => {
            return Err(DataFusionError::Execution(
                "GeoJSON FeatureCollection is not supported; pass a single geometry".to_string(),
            )
            .into());
        }
    };

    let geo_geom: geo::Geometry =
        geo::Geometry::try_from(geometry).map_err(|e| {
            DataFusionError::Execution(format!("Failed to convert GeoJSON to geo type: {}", e))
        })?;

    Ok(geo_geom)
}

impl Default for GeomFromGeoJSON {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOM_FROM_GEOJSON_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromGeoJSON {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geomfromgeojson"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref()).unwrap_or_default());
        let geom_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(geom_type
            .to_field(input_field.name(), input_field.is_nullable())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_GEOJSON_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Takes a GeoJSON representation of a geometry and creates a geometry object.",
                "ST_GeomFromGeoJSON(text)",
            )
            .with_argument("geojson", "GeoJSON string")
            .build()
        }))
    }
}

#[cfg(test)]
mod test {
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_as_geojson() {
        let ctx = SessionContext::new();

        ctx.register_udf(AsGeoJSON::new().into());
        ctx.register_udf(GeomFromText::default().into());

        // Use ST_GeomFromText to create geometry via SQL to avoid empty-point issues
        // with the test helper arrays from geoarrow_array.
        let sql_df = ctx
            .sql("SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(30 10)'));")
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];

        // Verify output is Utf8
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        assert_eq!(output_field.data_type(), &DataType::Utf8);

        // Verify the GeoJSON contains expected structure
        let str_arr = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(str_arr.len(), 1);
        let first = str_arr.value(0);
        assert!(first.contains("\"type\""));
        assert!(first.contains("\"coordinates\""));
        assert!(first.contains("30"));
        assert!(first.contains("10"));
    }

    #[tokio::test]
    async fn test_geom_from_geojson() {
        let ctx = SessionContext::new();

        ctx.register_udf(GeomFromGeoJSON::new(CoordType::Separated).into());
        ctx.register_udf(AsGeoJSON::new().into());

        let sql_df = ctx
            .sql(r#"SELECT ST_AsGeoJSON(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[30,10]}'));"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let str_arr = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let result = str_arr.value(0);
        assert!(result.contains("30"));
        assert!(result.contains("10"));
    }

    #[tokio::test]
    async fn test_geom_from_geojson_polygon() {
        let ctx = SessionContext::new();

        ctx.register_udf(GeomFromGeoJSON::new(CoordType::Separated).into());
        ctx.register_udf(AsGeoJSON::new().into());

        let sql_df = ctx
            .sql(r#"SELECT ST_AsGeoJSON(ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]]]}'));"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let str_arr = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let result = str_arr.value(0);
        assert!(result.contains("Polygon"));
    }
}
