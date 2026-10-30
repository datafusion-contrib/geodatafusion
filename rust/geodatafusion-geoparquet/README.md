# `geodatafusion-geoparquet`

[GeoParquet](https://geoparquet.org/) [`FileFormat`](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html) support for [Apache DataFusion](https://datafusion.apache.org/).

## Supported Capabilities

- **Reading (`.parquet`)**: Wraps DataFusion's `ParquetFormat` via [`GeoParquetFormat`] and [`GeoParquetFormatFactory`] to parse `geo` file metadata and attach GeoArrow extension metadata (`geoarrow.wkb` or native GeoArrow types) to geometry columns.
- **Writing**: Writing GeoParquet via `FileFormat` is not yet implemented.

## Usage

```rust,no_run
use std::sync::Arc;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use geodatafusion_geoparquet::GeoParquetFormatFactory;

let file_format = Arc::new(GeoParquetFormatFactory::default());
let state = SessionStateBuilder::new()
    .with_file_formats(vec![file_format])
    .build();
let ctx = SessionContext::new_with_state(state).enable_url_table();
geodatafusion::register(&ctx);
```
