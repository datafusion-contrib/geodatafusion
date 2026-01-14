use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::error::Result;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::{DisplayFormatType, PhysicalExpr};
use datafusion_datasource::TableSchema;
use datafusion_datasource_parquet::source::ParquetSource;

#[derive(Clone, Debug)]
pub struct GeoParquetSource {
    pub(crate) inner: ParquetSource,
}

/// Allows easy conversion from ParquetSource to Arc\<dyn FileSource\>;
impl From<GeoParquetSource> for Arc<dyn FileSource> {
    fn from(source: GeoParquetSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for GeoParquetSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &datafusion::datasource::physical_plan::FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn datafusion::datasource::physical_plan::FileOpener>> {
        self.inner
            .create_file_opener(object_store, base_config, partition)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        self.inner.with_batch_size(batch_size)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_extra(t, f)
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        self.inner.try_pushdown_filters(filters, config)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let projected_parquet_source = self.inner.try_pushdown_projection(projection)?;
        if let Some(dyn_file_source) = projected_parquet_source {
            let inner = dyn_file_source
                .as_any()
                .downcast_ref::<ParquetSource>()
                .unwrap();
            Ok(Some(Arc::new(GeoParquetSource {
                inner: inner.clone(),
            }) as Arc<dyn FileSource>))
        } else {
            Ok(None)
        }
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&datafusion::physical_plan::projection::ProjectionExprs> {
        self.inner.projection()
    }

    fn supports_repartitioning(&self) -> bool {
        true
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        if config.file_compression_type.is_compressed() || !self.supports_repartitioning() {
            return Ok(None);
        }

        let repartitioned_file_groups_option =
            datafusion_datasource::file_groups::FileGroupPartitioner::new()
                .with_target_partitions(target_partitions)
                .with_repartition_file_min_size(repartition_file_min_size)
                .with_preserve_order_within_groups(output_ordering.is_some())
                .repartition_file_groups(&config.file_groups);

        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            let mut source = config.clone();
            source.file_groups = repartitioned_file_groups;
            return Ok(Some(source));
        }
        Ok(None)
    }

    fn try_reverse_output(
        &self,
        _order: &[datafusion::physical_expr::PhysicalSortExpr],
        _eq_properties: &datafusion::physical_expr::EquivalenceProperties,
    ) -> Result<datafusion::physical_plan::SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(datafusion::physical_plan::SortOrderPushdownResult::Unsupported)
    }
}
