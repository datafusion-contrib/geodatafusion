# `geodatafusion-geoparquet`

[GeoParquet](https://geoparquet.org/) [`FileFormat`](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html) support for [Apache DataFusion](https://datafusion.apache.org/).

## Supported Capabilities

- **Reading (`.parquet`)**: Wraps DataFusion's `ParquetFormat` via [`GeoParquetFormat`] and [`GeoParquetFormatFactory`] to parse `geo` file metadata and attach GeoArrow extension metadata (`geoarrow.wkb` or native GeoArrow types) to geometry columns.
- **Native decoding**: `GeoParquetFormatFactory::new().with_parse_to_native(true)` decodes WKB into native arrays. `with_coord_type(CoordType::Interleaved)` selects the coordinate layout. Native scans support projections, partition columns, file ranges and missing nullable fields, and reuse DataFusion's footer cache and metadata-size hints. Filters remain above the scan for exact evaluation. Virtual columns such as `file_row_index()` return `NotImplemented`; native scans do not support encrypted Parquet or TopK pushdown. The default WKB scan uses DataFusion's Parquet reader and optimizations.
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
