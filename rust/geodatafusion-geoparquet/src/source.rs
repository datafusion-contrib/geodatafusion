//! [`FileSource`] implementation for reading GeoParquet files.

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::{DisplayFormatType, PhysicalExpr};
use datafusion_datasource::TableSchema;
use datafusion_datasource_parquet::source::ParquetSource;
use futures::StreamExt;
use geoarrow_schema::CoordType;
use geoparquet::reader::GeoParquetRecordBatchStream;
#[allow(deprecated)]
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

/// [`FileSource`] wrapper around DataFusion's [`ParquetSource`] for GeoParquet files.
#[derive(Clone, Debug)]
pub struct GeoParquetSource {
    pub(crate) inner: ParquetSource,
    pub(crate) parse_to_native: bool,
    pub(crate) coord_type: CoordType,
}

/// Allows easy conversion from ParquetSource to Arc\<dyn FileSource\>;
impl From<GeoParquetSource> for Arc<dyn FileSource> {
    fn from(source: GeoParquetSource) -> Self {
        Arc::new(source)
    }
}

fn wrap_parquet_source(
    source: Arc<dyn FileSource>,
    parse_to_native: bool,
    coord_type: CoordType,
) -> Arc<dyn FileSource> {
    match source.downcast_ref::<ParquetSource>() {
        Some(inner) => Arc::new(GeoParquetSource {
            inner: inner.clone(),
            parse_to_native,
            coord_type,
        }),
        None => source,
    }
}

#[derive(Clone, Debug)]
struct GeoParquetOpener {
    source: GeoParquetSource,
    object_store: Arc<dyn object_store::ObjectStore>,
}

impl FileOpener for GeoParquetOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let source = self.source.clone();
        let store = Arc::clone(&self.object_store);
        Ok(Box::pin(async move {
            #[allow(deprecated)]
            let reader = ParquetObjectReader::new(store, file.object_meta.location)
                .with_file_size(file.object_meta.size);
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let target_schema = source.inner.table_schema().file_schema().clone();
            let stream = builder
                .build()
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let stream = GeoParquetRecordBatchStream::try_new(stream, target_schema)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let projection = source.inner.projection().cloned().unwrap_or_else(|| {
                ProjectionExprs::from_indices(
                    &(0..source.inner.table_schema().file_schema().fields().len())
                        .collect::<Vec<_>>(),
                    source.inner.table_schema().file_schema(),
                )
            });
            let projector = projection.make_projector(source.inner.table_schema().file_schema())?;
            let stream = stream
                .map(move |batch| {
                    let batch = batch.map_err(|err| DataFusionError::External(Box::new(err)))?;
                    projector.project_batch(&batch)
                })
                .boxed();
            Ok(stream)
        }))
    }
}

impl FileSource for GeoParquetSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &datafusion::datasource::physical_plan::FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn datafusion::datasource::physical_plan::FileOpener>> {
        if self.parse_to_native {
            Ok(Arc::new(GeoParquetOpener {
                source: self.clone(),
                object_store,
            }))
        } else {
            self.inner
                .create_file_opener(object_store, base_config, partition)
        }
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        wrap_parquet_source(
            self.inner.with_batch_size(batch_size),
            self.parse_to_native,
            self.coord_type,
        )
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
        if self.parse_to_native {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; filters.len()],
            ));
        }
        let mut result = self.inner.try_pushdown_filters(filters, config)?;
        if let Some(updated) = result.updated_node.take() {
            result.updated_node = Some(wrap_parquet_source(
                updated,
                self.parse_to_native,
                self.coord_type,
            ));
        }
        Ok(result)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_projection(projection)?
            .map(|source| wrap_parquet_source(source, self.parse_to_native, self.coord_type)))
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

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_expr::projection::ProjectionExprs;
    use datafusion_datasource::TableSchema;
    use datafusion_datasource_parquet::source::ParquetSource;

    use super::*;

    fn source() -> GeoParquetSource {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("geometry", DataType::Binary, true),
        ]));
        GeoParquetSource {
            inner: ParquetSource::new(TableSchema::from(Arc::clone(&schema))),
            parse_to_native: false,
            coord_type: CoordType::Separated,
        }
    }

    fn assert_wrapped(source: Arc<dyn FileSource>) {
        assert!(source.downcast_ref::<GeoParquetSource>().is_some());
    }

    #[test]
    fn source_updates_keep_the_geoparquet_wrapper() {
        let source = source();
        assert_wrapped(source.with_batch_size(128));

        let schema = source.inner.table_schema().table_schema().clone();
        let projection = ProjectionExprs::from_indices(&[0, 1], &schema);
        assert_wrapped(
            source
                .try_pushdown_projection(&projection)
                .unwrap()
                .unwrap(),
        );

        let mut config = ConfigOptions::default();
        config.execution.parquet.pushdown_filters = true;
        let result = source
            .try_pushdown_filters(vec![lit(true)], &config)
            .unwrap();
        assert_wrapped(result.updated_node.unwrap());
    }
}
