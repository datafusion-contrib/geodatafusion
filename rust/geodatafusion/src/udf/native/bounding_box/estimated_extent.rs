//! `ST_EstimatedExtent` implementation.
//!
//! DataFusion's synchronous scalar function arguments do not carry a session,
//! and `AsyncScalarUDFImpl` does not add one either. The public `register`
//! entrypoint therefore captures the caller's catalog list and default
//! catalog/schema names when it installs the function. The captured catalog
//! list is shared with the context, so tables registered after `register` are
//! visible when the function executes. Changing the default catalog or schema
//! with `SET` after registration requires registering the function again.

use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock, OnceLock};

use arrow_schema::{DataType, FieldRef};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProviderList, TableProvider};
use datafusion::common::{Result, ScalarValue, TableReference, exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::async_udf::AsyncScalarUDFImpl;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_schema::{BoxType, Dimension, GeoArrowType, Metadata};

use crate::spatial_statistics::{SpatialExtent, SpatialStatisticsTable};

const FUNCTION_NAME: &str = "ST_EstimatedExtent";

// `AsyncScalarUDF` does not forward `coerce_types` to its inner implementation,
// so the supported string arities are declared as exact signatures.
static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
        ],
        Volatility::Immutable,
    )
});
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

/// Scalar `ST_EstimatedExtent` bound to one session catalog.
#[derive(Debug)]
pub struct EstimatedExtent {
    catalog_list: Arc<dyn CatalogProviderList>,
    default_catalog: String,
    default_schema: String,
}

impl EstimatedExtent {
    /// Capture the catalog list and default names from the registering context.
    pub fn new(session_context: &SessionContext) -> Self {
        let state = session_context.state();
        let options = state.config_options();
        Self {
            catalog_list: state.catalog_list().clone(),
            default_catalog: options.catalog.default_catalog.clone(),
            default_schema: options.catalog.default_schema.clone(),
        }
    }

    fn box_field() -> FieldRef {
        Arc::new(BoxType::new(Dimension::XY, Arc::new(Metadata::default())).to_field("", true))
    }
}

impl PartialEq for EstimatedExtent {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.catalog_list, &other.catalog_list)
            && self.default_catalog == other.default_catalog
            && self.default_schema == other.default_schema
    }
}

impl Eq for EstimatedExtent {}

impl Hash for EstimatedExtent {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.catalog_list) as *const () as usize).hash(state);
        self.default_catalog.hash(state);
        self.default_schema.hash(state);
    }
}

impl ScalarUDFImpl for EstimatedExtent {
    fn name(&self) -> &str {
        "st_estimatedextent"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Self::box_field().data_type().clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Self::box_field())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::Internal(
            "async function should not be invoked synchronously".to_string(),
        ))
    }

    // `AsyncScalarUDF` does not forward `documentation` in DataFusion 55.1;
    // this stays for forward compatibility and is duplicated in the README.
    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the metadata-derived extent of a spatial table.",
                "ST_EstimatedExtent(table, geometry_column)",
            )
            .with_argument("table", "table name, optionally schema-qualified")
            .with_argument("geometry_column", "GeoArrow geometry column")
            .with_related_udf("st_xmin")
            .with_related_udf("st_ymin")
            .with_related_udf("st_xmax")
            .with_related_udf("st_ymax")
            .build()
        }))
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for EstimatedExtent {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (table, column) = match args.args.as_slice() {
            [table, column] => {
                let table = scalar_utf8(table, "table")?;
                let column = scalar_utf8(column, "column")?;
                (table.map(|table| TableReference::parse_str(&table)), column)
            }
            [schema, table, column] => {
                let schema = scalar_utf8(schema, "schema")?;
                let table = scalar_utf8(table, "table")?;
                let column = scalar_utf8(column, "column")?;
                let table = match (schema, table) {
                    (Some(schema), Some(table)) => Some(TableReference::partial(schema, table)),
                    _ => None,
                };
                (table, column)
            }
            other => {
                return plan_err!(
                    "{FUNCTION_NAME} expects 2 or 3 arguments, got {}",
                    other.len()
                );
            }
        };

        let extent = match (table, column) {
            (Some(table), Some(column)) => {
                resolve_extent(
                    &self.catalog_list,
                    &self.default_catalog,
                    &self.default_schema,
                    &table,
                    &column,
                )
                .await?
            }
            _ => None,
        };

        let mut builder = RectBuilder::with_capacity(
            BoxType::new(Dimension::XY, Arc::new(Metadata::default())),
            args.number_rows,
        );
        match extent {
            Some(extent) => {
                let rect = extent_rect(&extent);
                for _ in 0..args.number_rows {
                    builder.push_rect(Some(&rect));
                }
            }
            None => {
                for _ in 0..args.number_rows {
                    builder.push_null();
                }
            }
        }

        Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
    }
}

fn extent_rect(extent: &SpatialExtent) -> geo::Rect<f64> {
    geo::Rect::new(
        geo::coord! { x: extent.xmin(), y: extent.ymin() },
        geo::coord! { x: extent.xmax(), y: extent.ymax() },
    )
}

async fn resolve_extent(
    catalog_list: &Arc<dyn CatalogProviderList>,
    default_catalog: &str,
    default_schema: &str,
    table: &TableReference,
    column: &str,
) -> Result<Option<SpatialExtent>> {
    let provider = resolve_table(catalog_list, default_catalog, default_schema, table).await?;
    let schema = provider.schema();
    let Some(field) = schema.fields().iter().find(|field| field.name() == column) else {
        return exec_err!(
            "column `{column}` does not exist in table `{}`",
            qualified_name(default_catalog, default_schema, table)
        );
    };
    GeoArrowType::from_arrow_field(field.as_ref()).map_err(|_| {
        DataFusionError::Execution(format!(
            "column `{column}` must be a geometry in table `{}`",
            qualified_name(default_catalog, default_schema, table)
        ))
    })?;

    Ok(provider
        .downcast_ref::<SpatialStatisticsTable>()
        .and_then(|provider| provider.statistics().get(column)))
}

async fn resolve_table(
    catalog_list: &Arc<dyn CatalogProviderList>,
    default_catalog: &str,
    default_schema: &str,
    table: &TableReference,
) -> Result<Arc<dyn TableProvider>> {
    let catalog_name = table.catalog().unwrap_or(default_catalog);
    let schema_name = table.schema().unwrap_or(default_schema);
    let table_name = table.table();

    let catalog = catalog_list.catalog(catalog_name).ok_or_else(|| {
        DataFusionError::Execution(format!("catalog `{catalog_name}` does not exist"))
    })?;
    let schema = catalog.schema(schema_name).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "schema `{catalog_name}.{schema_name}` does not exist"
        ))
    })?;
    schema.table(table_name).await?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "table `{catalog_name}.{schema_name}.{table_name}` does not exist"
        ))
    })
}

fn qualified_name(default_catalog: &str, default_schema: &str, table: &TableReference) -> String {
    let catalog_name = table.catalog().unwrap_or(default_catalog);
    let schema_name = table.schema().unwrap_or(default_schema);
    format!("{catalog_name}.{schema_name}.{}", table.table())
}

fn scalar_utf8(value: &ColumnarValue, argument: &str) -> Result<Option<String>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(value))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(value))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(value)) => Ok(value.clone()),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        ColumnarValue::Scalar(other) => plan_err!(
            "{FUNCTION_NAME} `{argument}` argument must be a string constant, got {}",
            other.data_type()
        ),
        ColumnarValue::Array(_) => {
            plan_err!("{FUNCTION_NAME} `{argument}` argument must be a string constant")
        }
    }
}
