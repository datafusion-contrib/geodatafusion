//! Execution plan for reading FlatGeobuf files

use std::any::Any;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::TableSchema;
use futures::{StreamExt, TryStreamExt};
use geoarrow_array::array::from_arrow_array;
use geoarrow_flatgeobuf::reader::{FlatGeobufReaderOptions, FlatGeobufRecordBatchStream};
use geodatafusion::udf::native::bounding_box::util::total_bounds;
use object_store::ObjectStore;

use crate::utils::open_flatgeobuf_reader;

#[derive(Debug, Clone)]
pub struct FlatGeobufSource {
    batch_size: Option<usize>,
    table_schema: TableSchema,
    projection: ProjectionExprs,
    metrics: ExecutionPlanMetricsSet,
    bbox: Option<[f64; 4]>,
}

impl FlatGeobufSource {
    pub fn new(table_schema: TableSchema) -> Self {
        let table_schema_ref = table_schema.table_schema();
        let projection = ProjectionExprs::from_indices(
            &(0..table_schema_ref.fields().len()).collect::<Vec<_>>(),
            table_schema_ref,
        );

        Self {
            batch_size: None,
            table_schema,
            projection,
            metrics: ExecutionPlanMetricsSet::new(),
            bbox: None,
        }
    }

    fn with_bbox(self, bbox: [f64; 4]) -> Self {
        Self {
            bbox: Some(bbox),
            ..self
        }
    }
}

impl From<FlatGeobufSource> for Arc<dyn FileSource> {
    fn from(source: FlatGeobufSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for FlatGeobufSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(FlatGeobufOpener::new(
            Arc::new(self.clone()),
            object_store,
        )))
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "flatgeobuf"
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut pushdown_flags = vec![];
        let mut bbox = self.bbox;
        for filter in filters.iter() {
            if bbox.is_none()
                && let Some(extracted) = extract_bbox(filter)?
            {
                bbox = Some(extracted);
                pushdown_flags.push(PushedDown::Yes);
                continue;
            }
            pushdown_flags.push(PushedDown::No);
        }

        if let Some(bbox) = bbox {
            Ok(
                FilterPushdownPropagation::with_parent_pushdown_result(pushdown_flags)
                    .with_updated_node(Arc::new(self.clone().with_bbox(bbox))),
            )
        } else {
            Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                pushdown_flags,
            ))
        }
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = self.projection.try_merge(projection)?;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection)
    }
}

pub struct FlatGeobufOpener {
    config: Arc<FlatGeobufSource>,
    object_store: Arc<dyn ObjectStore>,
}

impl FlatGeobufOpener {
    pub fn new(config: Arc<FlatGeobufSource>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config,
            object_store,
        }
    }
}

impl FileOpener for FlatGeobufOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let config = self.config.clone();

        Ok(Box::pin(async move {
            let mut file_schema = config.table_schema.file_schema().clone();
            file_schema = Arc::new(config.projection.project_schema(file_schema.as_ref())?);

            let options = FlatGeobufReaderOptions::from_combined_schema(file_schema)
                .map_err(|err| DataFusionError::External(Box::new(err)))?
                .with_batch_size(config.batch_size.unwrap_or(1024));

            let fgb_reader =
                open_flatgeobuf_reader(store, file.object_meta.location.clone()).await?;
            let selection = if let Some([minx, miny, maxx, maxy]) = config.bbox {
                fgb_reader
                    .select_bbox(minx, miny, maxx, maxy)
                    .await
                    .map_err(|err| DataFusionError::External(Box::new(err)))?
            } else {
                fgb_reader
                    .select_all()
                    .await
                    .map_err(|err| DataFusionError::External(Box::new(err)))?
            };
            let stream = FlatGeobufRecordBatchStream::try_new(selection, options)
                .map_err(|err| DataFusionError::External(Box::new(err)))?
                .err_into::<DataFusionError>();
            Ok(stream.boxed())
        }))
    }
}

fn columnar_value_to_bbox(value: ColumnarValue, field: &Field) -> Result<Option<[f64; 4]>> {
    let arrays = ColumnarValue::values_to_arrays(&[value])?;
    if let Ok(geo_arr) = from_arrow_array(&arrays[0], field) {
        let bounds =
            total_bounds(&geo_arr).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Some([
            bounds.minx(),
            bounds.miny(),
            bounds.maxx(),
            bounds.maxy(),
        ]))
    } else {
        Ok(None)
    }
}

fn extract_bbox(expr: &Arc<dyn PhysicalExpr>) -> Result<Option<[f64; 4]>> {
    if let Some(func) = expr.as_any().downcast_ref::<ScalarFunctionExpr>()
        && func.fun().name().eq_ignore_ascii_case("st_intersects")
        && func.args().len() == 2
    {
        let bbox_expr = func.args()[1].clone();
        let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let value = bbox_expr.evaluate(&empty)?;
        let return_field = bbox_expr.return_field(empty.schema_ref())?;
        return columnar_value_to_bbox(value, &return_field);
    }
    Ok(None)
}
