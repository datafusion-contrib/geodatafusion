//! [`FileFormat`] and [`FileFormatFactory`] implementations for GeoParquet files.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{GetExt, Statistics};
use datafusion::config::{ConfigField, ConfigFileType, TableParquetOptions};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::TableSchema;
use datafusion_datasource::file_format::FileFormatFactory;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource_parquet::ParquetFormat;
use datafusion_datasource_parquet::source::ParquetSource;
use geoarrow_schema::CoordType;
use geoparquet::metadata::{GeoParquetColumnEncoding, GeoParquetMetadata};
use geoparquet::reader::infer_geoarrow_schema;
use object_store::{ObjectMeta, ObjectStore};

use crate::source::GeoParquetSource;

/// Factory used to create [`GeoParquetFormat`] instances.
#[derive(Default, Debug)]
pub struct GeoParquetFormatFactory {
    options: Option<TableParquetOptions>,
    parse_to_native: bool,
    coord_type: CoordType,
}

impl GeoParquetFormatFactory {
    /// Creates an instance of [GeoParquetFormatFactory]
    pub fn new() -> Self {
        <Self as std::default::Default>::default()
    }

    /// Creates an instance of [GeoParquetFormatFactory] with customized default options
    pub fn new_with_options(options: TableParquetOptions) -> Self {
        Self {
            options: Some(options),
            ..<Self as std::default::Default>::default()
        }
    }

    /// Configure whether WKB geometry columns are decoded to native GeoArrow arrays.
    pub fn with_parse_to_native(mut self, parse_to_native: bool) -> Self {
        self.parse_to_native = parse_to_native;
        self
    }

    /// Configure the GeoArrow coordinate layout used for decoded native geometry columns.
    pub fn with_coord_type(mut self, coord_type: CoordType) -> Self {
        self.coord_type = coord_type;
        self
    }

    /// Return the configured Parquet options, if any.
    pub fn options(&self) -> Option<&TableParquetOptions> {
        self.options.as_ref()
    }

    fn make_format(&self, parquet_format: ParquetFormat) -> GeoParquetFormat {
        GeoParquetFormat::new(parquet_format)
            .with_parse_to_native(self.parse_to_native)
            .with_coord_type(self.coord_type)
    }
}

impl FileFormatFactory for GeoParquetFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let parquet_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::PARQUET);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.parquet
            }
            Some(parquet_options) => {
                let mut parquet_options = parquet_options.clone();
                for (k, v) in format_options {
                    parquet_options.set(k, v)?;
                }
                parquet_options
            }
        };

        let parquet_format = ParquetFormat::default().with_options(parquet_options);
        Ok(Arc::new(self.make_format(parquet_format)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        let parquet_format = match &self.options {
            Some(options) => ParquetFormat::default().with_options(options.clone()),
            None => ParquetFormat::default(),
        };
        Arc::new(self.make_format(parquet_format))
    }
}

impl GetExt for GeoParquetFormatFactory {
    fn get_ext(&self) -> String {
        "parquet".to_string()
    }
}

/// GeoParquet `FileFormat` implementation
#[derive(Debug, Default)]
pub struct GeoParquetFormat {
    inner: ParquetFormat,
    parse_to_native: bool,
    coord_type: CoordType,
}

impl GeoParquetFormat {
    /// Creates a new instance of `GeoParquetFormat`
    pub fn new(format: ParquetFormat) -> Self {
        Self {
            inner: format.with_skip_metadata(false),
            parse_to_native: false,
            coord_type: CoordType::default(),
        }
    }

    /// Configure whether WKB geometry columns are decoded to native GeoArrow arrays.
    pub fn with_parse_to_native(mut self, parse_to_native: bool) -> Self {
        self.parse_to_native = parse_to_native;
        self
    }

    /// Configure the GeoArrow coordinate layout used for decoded native geometry columns.
    pub fn with_coord_type(mut self, coord_type: CoordType) -> Self {
        self.coord_type = coord_type;
        self
    }
}

fn validate_geo_metadata(
    schema: &arrow_schema::Schema,
    geo_meta: &GeoParquetMetadata,
) -> Result<()> {
    for field in schema.fields() {
        let Some(column) = geo_meta.columns.get(field.name()) else {
            continue;
        };
        if column.encoding != GeoParquetColumnEncoding::WKB && column.geometry_types.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "GeoParquet metadata for column '{}' uses native encoding {:?} with {} geometry types; exactly one geometry type is required",
                field.name(),
                column.encoding,
                column.geometry_types.len()
            )));
        }
    }
    Ok(())
}

#[async_trait]
impl FileFormat for GeoParquetFormat {
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let schema = self.inner.infer_schema(state, store, objects).await?;
        // Insert GeoArrow metadata onto geometry column
        if let Some(geo_meta_str) = schema.metadata().get("geo") {
            let geo_meta: GeoParquetMetadata = serde_json::from_str(geo_meta_str)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            validate_geo_metadata(&schema, &geo_meta)?;
            let new_schema =
                infer_geoarrow_schema(&schema, &geo_meta, self.parse_to_native, self.coord_type)
                    .map_err(|err| DataFusionError::External(Box::new(err)))?;
            Ok(new_schema)
        } else {
            Ok(schema)
        }
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.inner
            .infer_stats(_state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source = conf.file_source().clone();
        let Some(geoparquet_source) = source.downcast_ref::<GeoParquetSource>() else {
            return Err(DataFusionError::Execution(
                "GeoParquetFormat expected a GeoParquetSource".to_string(),
            ));
        };
        if self.parse_to_native {
            let new_conf = FileScanConfigBuilder::from(conf)
                .with_source(Arc::new(geoparquet_source.clone()))
                .build();
            return Ok(DataSourceExec::from_data_source(new_conf));
        }

        let parquet_source = &geoparquet_source.inner;
        let file_scan_config_builder =
            FileScanConfigBuilder::from(conf).with_source(Arc::new(parquet_source.clone()));
        let new_conf = file_scan_config_builder.build();

        self.inner.create_physical_plan(_state, new_conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "writing not implemented for GeoParquet yet".to_string(),
        ))
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        let parquet_source = self.inner.file_source(table_schema);
        match parquet_source.downcast_ref::<ParquetSource>() {
            Some(inner) => Arc::new(GeoParquetSource {
                inner: inner.clone(),
                parse_to_native: self.parse_to_native,
                coord_type: self.coord_type,
                batch_size: None,
            }),
            None => parquet_source,
        }
    }
}
