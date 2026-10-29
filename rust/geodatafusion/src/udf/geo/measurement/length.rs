use std::sync::{Arc, LazyLock, OnceLock};

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{geometry_array_from_arrow, is_null_geometry_field};
use crate::udf::native::measurement::{Axis, is_null_array, linear_length, null_float64};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Length;

impl Length {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Length {
    fn default() -> Self {
        Self::new()
    }
}

static ALIASES: LazyLock<Vec<String>> = LazyLock::new(|| vec!["st_length2d".to_string()]);
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Length {
    fn name(&self) -> &str {
        "st_length"
    }

    fn aliases(&self) -> &[String] {
        ALIASES.as_slice()
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(length_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D Cartesian length of the geometry if it is a LineString or MultiLineString. For areal geometries 0 is returned; use ST_Perimeter instead.",
                "ST_Length(geom)",
            )
            .with_argument("geom", "geometry")
            .with_sql_example(
                "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'));",
            )
            .build()
        }))
    }
}

fn length_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let field = &args.arg_fields[0];
    if is_null_array(&array) || is_null_geometry_field(field) {
        return Ok(ColumnarValue::Array(Arc::new(null_float64(
            args.number_rows,
        ))));
    }
    let geo_array = geometry_array_from_arrow(&array, field)?;
    let result = linear_length(geo_array.as_ref(), Axis::TwoDimensional)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_linestring_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 5.0);
    }

    #[tokio::test]
    async fn test_point_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('POINT(1 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0);
    }

    #[tokio::test]
    async fn test_multilinestring_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('MULTILINESTRING((0 0, 3 4), (0 0, 4 3))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 10.0); // 5.0 + 5.0
    }

    #[tokio::test]
    async fn test_polygon_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0); // Polygons return 0 for length
    }

    #[tokio::test]
    async fn test_geometry_collection_length_uses_linear_components() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0, 1 0, 1 1, 0 0)), LINESTRING(0 0, 3 4))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 5.0);
    }

    #[tokio::test]
    async fn test_length_ignores_z() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('LINESTRING Z (0 0 0, 3 4 12)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 5.0);
    }
}
