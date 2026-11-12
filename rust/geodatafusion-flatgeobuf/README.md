# `geodatafusion-flatgeobuf`

[FlatGeobuf](https://flatgeobuf.org/) [`FileFormat`](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html) support for [Apache DataFusion](https://datafusion.apache.org/).

## Supported Capabilities

- **Reading (`.fgb`)**: Streams local and remote (`object_store` HTTP/S3/GCS) FlatGeobuf files into GeoArrow columns via [`FlatGeobufFormat`] and [`FlatGeobufSource`](crate::source::FlatGeobufSource).
- **Spatial Index Pushdown**: Automatically extracts constant bounding boxes from `WHERE ST_Intersects(geometry, ...)` filters and uses the FlatGeobuf R-tree index to prune non-matching features before exact evaluation.
- **Writing (`.fgb`)**: Supports `COPY ... TO 'output.fgb'` via [`FlatGeobufSink`](crate::file_format::FlatGeobufSink).

## Usage

```rust,no_run
use std::sync::Arc;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use geodatafusion_flatgeobuf::FlatGeobufFormatFactory;

let file_format = Arc::new(FlatGeobufFormatFactory::default());
let state = SessionStateBuilder::new()
    .with_file_formats(vec![file_format])
    .build();
let ctx = SessionContext::new_with_state(state);
geodatafusion::register(&ctx);
```

Files without a spatial index are scanned and filtered normally. Empty files produce no rows. Streaming files with an unknown feature count return `NotImplemented`; their statistics do not claim an exact row count. FlatGeobuf files are not split into byte ranges.
