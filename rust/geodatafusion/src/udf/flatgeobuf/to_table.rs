use std::fmt::{self, Formatter};
use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProviderList, TableFunctionArgs, TableFunctionImpl, TableProvider,
};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{MemTable, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures::StreamExt;

use super::decode::decode_bytes;
use super::{constant_bytes, constant_string};

#[derive(Debug)]
pub(super) struct FromFlatGeobufToTable;

impl TableFunctionImpl for FromFlatGeobufToTable {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let [schema, table, bytes] = exprs else {
            return Err(DataFusionError::Execution(
                "ST_FromFlatGeobufToTable expects (schema, table, FlatGeobuf bytes)".to_string(),
            ));
        };
        let schema_name = constant_string(schema, "ST_FromFlatGeobufToTable")?;
        let table_name = constant_string(table, "ST_FromFlatGeobufToTable")?;
        if schema_name.is_empty() || table_name.is_empty() {
            return Err(DataFusionError::Execution(
                "ST_FromFlatGeobufToTable: schema and table names must not be empty".to_string(),
            ));
        }
        let bytes = constant_bytes(bytes, "ST_FromFlatGeobufToTable")?;
        let decoded = decode_bytes(&bytes, false, "geom")?;
        let batches = Arc::new(decoded.batches);
        let registration = Arc::new(RegistrationMutation {
            catalog: args.session().catalog_list(),
            catalog_name: args
                .session()
                .config()
                .options()
                .catalog
                .default_catalog
                .clone(),
            schema_name,
            table_name,
            schema: Arc::clone(&decoded.schema),
            batches: Arc::clone(&batches),
            state: OnceLock::new(),
        });

        Ok(Arc::new(LazyRegistrationTable {
            schema: decoded.schema,
            batches,
            registration,
        }))
    }
}

#[derive(Debug)]
struct RegistrationMutation {
    catalog: Arc<dyn CatalogProviderList>,
    catalog_name: String,
    schema_name: String,
    table_name: String,
    schema: SchemaRef,
    batches: Arc<Vec<RecordBatch>>,
    state: OnceLock<std::result::Result<(), String>>,
}

impl RegistrationMutation {
    fn register(&self) -> Result<()> {
        let result = self
            .state
            .get_or_init(|| self.register_inner().map_err(|error| error.to_string()));
        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(DataFusionError::Execution(error.clone())),
        }
    }

    fn register_inner(&self) -> Result<()> {
        let catalog = self.catalog.catalog(&self.catalog_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ST_FromFlatGeobufToTable: catalog `{}` does not exist",
                self.catalog_name
            ))
        })?;
        let schema = catalog.schema(&self.schema_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ST_FromFlatGeobufToTable: schema `{}` does not exist",
                self.schema_name
            ))
        })?;
        if schema.table_exist(&self.table_name) {
            return Err(DataFusionError::Execution(format!(
                "ST_FromFlatGeobufToTable: table `{}.{}` already exists",
                self.schema_name, self.table_name
            )));
        }
        let table = Arc::new(MemTable::try_new(
            Arc::clone(&self.schema),
            vec![self.batches.as_ref().clone()],
        )?);
        schema.register_table(self.table_name.clone(), table)?;
        Ok(())
    }
}

#[derive(Debug)]
struct LazyRegistrationTable {
    schema: SchemaRef,
    batches: Arc<Vec<RecordBatch>>,
    registration: Arc<RegistrationMutation>,
}

#[async_trait]
impl TableProvider for LazyRegistrationTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (schema, batches) = match projection {
            Some(projection) => {
                let schema = Arc::new(self.schema.project(projection).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "ST_FromFlatGeobufToTable projection failed: {error}"
                    ))
                })?);
                let batches = self
                    .batches
                    .iter()
                    .map(|batch| {
                        batch.project(projection).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "ST_FromFlatGeobufToTable projection failed: {error}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                (schema, Arc::new(batches))
            }
            None => (Arc::clone(&self.schema), Arc::clone(&self.batches)),
        };

        Ok(Arc::new(LazyRegistrationExec::new(
            schema,
            batches,
            Arc::clone(&self.registration),
        )))
    }
}

#[derive(Debug)]
struct LazyRegistrationExec {
    schema: SchemaRef,
    batches: Arc<Vec<RecordBatch>>,
    registration: Arc<RegistrationMutation>,
    properties: Arc<PlanProperties>,
}

impl LazyRegistrationExec {
    fn new(
        schema: SchemaRef,
        batches: Arc<Vec<RecordBatch>>,
        registration: Arc<RegistrationMutation>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            batches,
            registration,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LazyRegistrationExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LazyRegistrationExec(table={})",
                    self.registration.table_name
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "table={}.{}",
                    self.registration.schema_name, self.registration.table_name
                )
            }
        }
    }
}

impl ExecutionPlan for LazyRegistrationExec {
    fn name(&self) -> &str {
        "LazyRegistrationExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            datafusion::physical_plan::ReplaceChildrenOptions::new(
                datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
            ),
        )
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "ST_FromFlatGeobufToTable has one partition, got {partition}"
            )));
        }

        let batches = Arc::clone(&self.batches);
        let registration = Arc::clone(&self.registration);
        let stream = futures::stream::unfold(0usize, move |index| {
            let batches = Arc::clone(&batches);
            let registration = Arc::clone(&registration);
            async move {
                if index == 0
                    && let Err(error) = registration.register()
                {
                    return Some((Err(error), batches.len() + 1));
                }
                batches
                    .get(index)
                    .cloned()
                    .map(|batch| (Ok(batch), index + 1))
            }
        })
        .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &datafusion::physical_plan::StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}
