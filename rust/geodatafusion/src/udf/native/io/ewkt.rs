use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringBuilder;
use arrow_array::Array;
use arrow_schema::{DataType, Field};
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::to_wkt;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::{CrsType, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

// ---------------------------------------------------------------------------
// ST_AsEWKT
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsEWKT;

impl AsEWKT {
    pub fn new() -> Self {
        Self {}
    }

    fn invoke_impl(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
        let geo_array = from_arrow_array(&array, field.as_ref())?;

        // Get WKT representation
        let wkt_arr = to_wkt::<i32>(geo_array.as_ref())?;

        // Extract SRID from metadata if present
        let metadata = Metadata::try_from(field.as_ref()).ok();
        let srid_prefix = metadata
            .as_ref()
            .and_then(|m| {
                let crs = m.crs();
                if crs.crs_type() == Some(CrsType::AuthorityCode) {
                    if let Some(val) = crs.crs_value() {
                        if let Some(code_str) = val.as_str() {
                            if let Some(srid) = code_str.strip_prefix("EPSG:") {
                                return Some(format!("SRID={};", srid));
                            }
                        }
                    }
                }
                None
            })
            .unwrap_or_default();

        // Build output with SRID prefix
        let str_arr = wkt_arr.into_array_ref();
        let wkt_string_arr = str_arr
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();

        let mut builder =
            StringBuilder::with_capacity(wkt_string_arr.len(), wkt_string_arr.len() * 64);
        for i in 0..wkt_string_arr.len() {
            if wkt_string_arr.is_null(i) {
                builder.append_null();
            } else {
                let wkt_val = wkt_string_arr.value(i);
                let ewkt = format!("{}{}", srid_prefix, wkt_val);
                builder.append_value(&ewkt);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Default for AsEWKT {
    fn default() -> Self {
        Self::new()
    }
}

static AS_EWKT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsEWKT {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_asewkt"
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
        Some(AS_EWKT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the Extended Well-Known Text (EWKT) representation of the geometry with SRID metadata.",
                "ST_AsEWKT(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::test::point;
    use geoarrow_schema::{CoordType, Crs, Dimension, Metadata};

    use super::*;

    #[tokio::test]
    async fn test_as_ewkt_with_srid() {
        let ctx = SessionContext::new();

        let crs = Crs::from_authority_code("EPSG:4326".to_string());
        let metadata = Arc::new(Metadata::new(crs, Default::default()));

        let geo_arr = point::array(CoordType::Separated, Dimension::XY).with_metadata(metadata);

        let arr = geo_arr.to_array_ref();
        let field = geo_arr.data_type().to_field("geometry", true);
        let schema = Schema::new([Arc::new(field)]);
        let batch =
            arrow_array::RecordBatch::try_new(Arc::new(schema), vec![arr]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        ctx.register_udf(AsEWKT::new().into());

        let sql_df = ctx
            .sql("SELECT ST_AsEWKT(geometry) FROM t;")
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

        assert!(str_arr.len() > 0);
        let first = str_arr.value(0);
        assert!(
            first.starts_with("SRID=4326;"),
            "Expected EWKT to start with 'SRID=4326;', got: {}",
            first
        );
        assert!(
            first.contains("POINT"),
            "Expected EWKT to contain 'POINT', got: {}",
            first
        );
    }

    #[tokio::test]
    async fn test_as_ewkt_without_srid() {
        let ctx = SessionContext::new();

        let geo_arr = point::array(CoordType::Separated, Dimension::XY);

        let arr = geo_arr.to_array_ref();
        let field = geo_arr.data_type().to_field("geometry", true);
        let schema = Schema::new([Arc::new(field)]);
        let batch =
            arrow_array::RecordBatch::try_new(Arc::new(schema), vec![arr]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        ctx.register_udf(AsEWKT::new().into());

        let sql_df = ctx
            .sql("SELECT ST_AsEWKT(geometry) FROM t;")
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

        assert!(str_arr.len() > 0);
        let first = str_arr.value(0);
        assert!(
            !first.starts_with("SRID="),
            "Expected no SRID prefix when no CRS is set, got: {}",
            first
        );
        assert!(
            first.contains("POINT"),
            "Expected EWKT to contain 'POINT', got: {}",
            first
        );
    }
}
