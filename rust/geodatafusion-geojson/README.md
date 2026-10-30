# `geodatafusion-geojson`

Newline-delimited GeoJSON (`.geojsonl`) [`FileFormat`](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html) support for [Apache DataFusion](https://datafusion.apache.org/).

## Supported Capabilities

- **Writing (`.geojsonl`)**: Supports writing GeoArrow record batches to newline-delimited GeoJSON (`COPY ... TO 'output.geojsonl'`) via [`GeoJsonFormat`], [`GeoJsonFormatFactory`], [`GeoJsonFileFactory`], and [`GeoJsonSink`](crate::file_format::GeoJsonSink). If an `"id"` column is present in the output schema, it is written as the top-level GeoJSON Feature `id`.
- **Reading**: Reading GeoJSON files is not currently implemented and returns `DataFusionError::NotImplemented`.

## Usage

```rust,no_run
use std::sync::Arc;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use geodatafusion_geojson::GeoJsonFileFactory;

let file_format = Arc::new(GeoJsonFileFactory::new());
let state = SessionStateBuilder::new()
    .with_file_formats(vec![file_format])
    .build();
let ctx = SessionContext::new_with_state(state);
geodatafusion::register(&ctx);
```
