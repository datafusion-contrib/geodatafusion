//! GEOS-backed geometry accessor predicates.

use std::sync::{Arc, OnceLock};

use arrow_array::BooleanArray;
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{Geom, GeometryTypes};

use super::adapter::unary_geos_values;
use crate::data_types::any_single_geometry_type_input;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsRing;

impl Default for IsRing {
    fn default() -> Self {
        Self
    }
}

static IS_RING_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsRing {
    fn name(&self) -> &str {
        "st_isring"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let values = unary_geos_values(&args, |_, _, _, geometry| {
            if geometry.is_empty()? {
                return Ok(Some(false));
            }
            if !matches!(
                geometry.geometry_type()?,
                GeometryTypes::LineString | GeometryTypes::LinearRing
            ) {
                return Err(DataFusionError::Execution(
                    "ST_IsRing() should only be called on a linear feature".to_string(),
                )
                .into());
            }
            Ok(Some(geometry.is_ring()?))
        })?;
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(values))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(IS_RING_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Tests if a LineString is closed and simple. Non-empty inputs that are not LineStrings raise an error.",
                "ST_IsRing(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsSimple;

impl Default for IsSimple {
    fn default() -> Self {
        Self
    }
}

static IS_SIMPLE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsSimple {
    fn name(&self) -> &str {
        "st_issimple"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let values = unary_geos_values(&args, |_, _, _, geometry| {
            if geometry.is_empty()? {
                return Ok(Some(true));
            }
            Ok(Some(geometry.is_simple()?))
        })?;
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(values))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(IS_SIMPLE_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Tests if a geometry has no anomalous geometric points, such as self-intersections or self-tangencies.",
                "ST_IsSimple(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(IsRing.into());
    session_context.register_udf(IsSimple.into());
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    fn context() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(IsRing.into());
        ctx.register_udf(IsSimple.into());
        ctx
    }

    #[tokio::test]
    async fn is_ring_and_is_simple_contract() {
        let ctx = context();
        let batch = ctx
            .sql(
                "SELECT
                    ST_IsRing(ST_GeomFromText('LINESTRING(0 0,1 0,1 1,0 0)')) AS closed_simple,
                    ST_IsRing(ST_GeomFromText('LINESTRING(0 0,1 0,1 1)')) AS open,
                    ST_IsSimple(ST_GeomFromText('LINESTRING(0 0,2 2,2 0,0 2)')) AS self_intersection,
                    ST_IsSimple(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')) AS empty_collection,
                    ST_IsSimple(ST_GeomFromText(NULL)) AS null_input",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert!(batch.column(0).as_boolean().value(0));
        assert!(!batch.column(1).as_boolean().value(0));
        assert!(!batch.column(2).as_boolean().value(0));
        assert!(batch.column(3).as_boolean().value(0));
        assert!(batch.column(4).is_null(0));
    }

    #[tokio::test]
    async fn is_ring_rejects_non_linear_inputs() {
        let ctx = context();
        let error = ctx
            .sql("SELECT ST_IsRing(ST_GeomFromText('POINT(0 0)'))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("linear feature"),
            "unexpected error: {error}"
        );
    }
}
