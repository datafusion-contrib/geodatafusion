# `geodatafusion-geojson`

Newline-delimited GeoJSON (`.geojsonl`) [`FileFormat`](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html) support for [Apache DataFusion](https://datafusion.apache.org/).

Currently supports writing GeoJSON Lines (`.geojsonl`) via [`GeoJsonFormat`], [`GeoJsonFormatFactory`], and [`GeoJsonFileFactory`] (for example with `COPY ... TO 'output.geojsonl'`). Reading GeoJSON files is not yet implemented.

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
