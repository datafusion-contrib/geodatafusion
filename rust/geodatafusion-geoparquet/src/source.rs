//! [`FileSource`] implementation for reading GeoParquet files.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};
use datafusion::arrow::compute::cast;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener, FileSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::{DisplayFormatType, PhysicalExpr};
use datafusion_datasource::TableSchema;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_parquet::{ParquetAccessPlan, RowGroupAccessPlanFilter};
use futures::StreamExt;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{CoordType, GeoArrowType};
use geoparquet::reader::GeoParquetRecordBatchStream;
#[allow(deprecated)]
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
#[allow(deprecated)]
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

/// [`FileSource`] wrapper around DataFusion's [`ParquetSource`] for GeoParquet files.
#[derive(Clone, Debug)]
pub struct GeoParquetSource {
    pub(crate) inner: ParquetSource,
    pub(crate) parse_to_native: bool,
    pub(crate) coord_type: CoordType,
    pub(crate) batch_size: Option<usize>,
}

/// Allows easy conversion from ParquetSource to Arc\<dyn FileSource\>;
impl From<GeoParquetSource> for Arc<dyn FileSource> {
    fn from(source: GeoParquetSource) -> Self {
        Arc::new(source)
    }
}

impl GeoParquetSource {
    fn wrap_inner(&self, source: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        match source.downcast_ref::<ParquetSource>() {
            Some(inner) => Arc::new(Self {
                inner: inner.clone(),
                ..self.clone()
            }),
            None => source,
        }
    }
}

#[derive(Clone, Debug)]
struct GeoParquetOpener {
    source: GeoParquetSource,
    object_store: Arc<dyn object_store::ObjectStore>,
    target_schema: SchemaRef,
}

impl FileOpener for GeoParquetOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let source = self.source.clone();
        let store = Arc::clone(&self.object_store);
        let target_schema = self.target_schema.clone();
        Ok(Box::pin(async move {
            #[allow(deprecated)]
            let mut reader = ParquetObjectReader::new(store, file.object_meta.location)
                .with_file_size(file.object_meta.size);
            let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default())
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            // Let Parquet perform ordinary Arrow conversions (including view types).
            // The GeoParquet decoder only handles geometry columns.
            let fields = metadata
                .schema()
                .fields()
                .iter()
                .map(|field| match target_schema.field_with_name(field.name()) {
                    Ok(target)
                        if !target
                            .extension_type_name()
                            .is_some_and(|name| name.starts_with("geoarrow.")) =>
                    {
                        Arc::new(target.clone())
                    }
                    _ => field.clone(),
                })
                .collect::<Vec<_>>();
            let read_schema = Arc::new(Schema::new_with_metadata(
                fields,
                metadata.schema().metadata().clone(),
            ));
            let metadata = ArrowReaderMetadata::try_new(
                metadata.metadata().clone(),
                ArrowReaderOptions::new().with_schema(read_schema),
            )
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata);
            builder = builder.with_batch_size(source.batch_size.unwrap_or(1024));
            if let Some(range) = &file.range {
                let groups = builder.metadata().row_groups();
                let mut selection =
                    RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(groups.len()));
                selection.prune_by_range(groups, range);
                builder = builder.with_row_groups(selection.row_group_indexes().collect());
            }
            let mut indices = projected_file_indices(builder.schema(), &target_schema);
            // Keep a physical column for row cardinality when the projection only
            // requests partition columns or fields missing from this file.
            if indices.is_empty() && !builder.schema().fields().is_empty() {
                indices.push(0);
            }
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
            let stream = builder
                .build()
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            // GeoParquet 0.9 decodes native encodings in their stored separated
            // layout. Convert them to the requested layout after decoding.
            let decode_fields = stream
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    let Ok(target) = target_schema.field_with_name(field.name()) else {
                        return Ok(field.clone());
                    };
                    if !target
                        .extension_type_name()
                        .is_some_and(|name| name.starts_with("geoarrow."))
                    {
                        return Ok(field.clone());
                    }
                    let typ = GeoArrowType::try_from(target)?;
                    if matches!(
                        typ,
                        GeoArrowType::Wkb(_) | GeoArrowType::LargeWkb(_) | GeoArrowType::WkbView(_)
                    ) {
                        return Ok(field.clone());
                    }
                    let typ = if matches!(
                        field.data_type(),
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                    ) {
                        typ
                    } else {
                        typ.with_coord_type(CoordType::Separated)
                    };
                    Ok(Arc::new(typ.to_field(field.name(), target.is_nullable())))
                })
                .collect::<GeoArrowResult<Vec<_>>>()
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let decode_schema = Arc::new(Schema::new(decode_fields));
            let stream = GeoParquetRecordBatchStream::try_new(stream, decode_schema)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let stream = stream
                .map(move |batch| {
                    let batch = batch.map_err(|err| DataFusionError::External(Box::new(err)))?;
                    let columns = target_schema
                        .fields()
                        .iter()
                        .map(|target| {
                            let Ok(index) = batch.schema().index_of(target.name()) else {
                                return Ok(new_null_array(target.data_type(), batch.num_rows()));
                            };
                            let column = batch.column(index);
                            if column.data_type() == target.data_type() {
                                return Ok(column.clone());
                            }
                            if target
                                .extension_type_name()
                                .is_some_and(|name| name.starts_with("geoarrow."))
                                && !matches!(
                                    target.data_type(),
                                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                                )
                            {
                                convert_native_layout(
                                    column,
                                    batch.schema().field(index),
                                    source.coord_type,
                                )
                                .map_err(|err| DataFusionError::External(Box::new(err)))
                            } else {
                                Ok(cast(column, target.data_type())?)
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(RecordBatch::try_new_with_options(
                        target_schema.clone(),
                        columns,
                        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                    )?)
                })
                .boxed();
            Ok(stream)
        }))
    }
}

fn projected_file_indices(schema: &Schema, target: &Schema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            target
                .field_with_name(field.name())
                .is_ok()
                .then_some(index)
        })
        .collect()
}

fn convert_native_layout(
    array: &ArrayRef,
    field: &Field,
    coord_type: CoordType,
) -> GeoArrowResult<ArrayRef> {
    Ok(match GeoArrowType::try_from(field)? {
        GeoArrowType::Point(_) => PointArray::try_from((array.as_ref(), field))?
            .into_coord_type(coord_type)
            .into_array_ref(),
        GeoArrowType::LineString(_) => LineStringArray::try_from((array.as_ref(), field))?
            .into_coord_type(coord_type)
            .into_array_ref(),
        GeoArrowType::Polygon(_) => PolygonArray::try_from((array.as_ref(), field))?
            .into_coord_type(coord_type)
            .into_array_ref(),
        GeoArrowType::MultiPoint(_) => MultiPointArray::try_from((array.as_ref(), field))?
            .into_coord_type(coord_type)
            .into_array_ref(),
        GeoArrowType::MultiLineString(_) => {
            MultiLineStringArray::try_from((array.as_ref(), field))?
                .into_coord_type(coord_type)
                .into_array_ref()
        }
        GeoArrowType::MultiPolygon(_) => MultiPolygonArray::try_from((array.as_ref(), field))?
            .into_coord_type(coord_type)
            .into_array_ref(),
        typ => {
            return Err(GeoArrowError::GeoParquet(format!(
                "Unsupported native GeoParquet layout conversion for {typ:?}"
            )));
        }
    })
}

impl FileSource for GeoParquetSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &datafusion::datasource::physical_plan::FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn datafusion::datasource::physical_plan::FileOpener>> {
        if self.parse_to_native || self.coord_type != CoordType::Separated {
            if !self.inner.table_schema().virtual_columns().is_empty() {
                return Err(DataFusionError::NotImplemented(
                    "virtual columns are not supported in native GeoParquet scans".to_string(),
                ));
            }
            let schema = self.inner.table_schema();
            let split = self
                .inner
                .projection()
                .map(|projection| SplitProjection::new(schema.file_schema(), projection))
                .unwrap_or_else(|| SplitProjection::unprojected(schema));
            let target_schema = Arc::new(schema.file_schema().project(&split.file_indices)?);
            let opener = Arc::new(GeoParquetOpener {
                source: self.clone(),
                object_store,
                target_schema,
            });
            ProjectionOpener::try_new(split, opener, schema.file_schema())
        } else {
            self.inner
                .create_file_opener(object_store, base_config, partition)
        }
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        source.wrap_inner(self.inner.with_batch_size(batch_size))
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
        if self.parse_to_native || self.coord_type != CoordType::Separated {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; filters.len()],
            ));
        }
        let mut result = self.inner.try_pushdown_filters(filters, config)?;
        if let Some(updated) = result.updated_node.take() {
            result.updated_node = Some(self.wrap_inner(updated));
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
            .map(|source| self.wrap_inner(source)))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&datafusion::physical_plan::projection::ProjectionExprs> {
        self.inner.projection()
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
            batch_size: None,
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
