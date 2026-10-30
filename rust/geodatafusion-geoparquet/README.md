# `geodatafusion-geoparquet`

[GeoParquet](https://geoparquet.org/) [`FileFormat`](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html) support for [Apache DataFusion](https://datafusion.apache.org/).

Wraps DataFusion's `ParquetFormat` to infer GeoArrow extension metadata on geometry columns when reading GeoParquet files.

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
let ctx = SessionContext::new_with_state(state);
geodatafusion::register(&ctx);
```
