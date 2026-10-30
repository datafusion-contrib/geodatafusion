# `geodatafusion-flatgeobuf`

[FlatGeobuf](https://flatgeobuf.org/) [`FileFormat`](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html) support for [Apache DataFusion](https://datafusion.apache.org/).

Supports reading and writing `.fgb` files (including bounding-box spatial index pushdown for `ST_Intersects` queries) via [`FlatGeobufFormat`] and [`FlatGeobufFileFactory`].

## Usage

```rust,no_run
use std::sync::Arc;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use geodatafusion_flatgeobuf::FlatGeobufFileFactory;

let file_format = Arc::new(FlatGeobufFileFactory::default());
let state = SessionStateBuilder::new()
    .with_file_formats(vec![file_format])
    .build();
let ctx = SessionContext::new_with_state(state);
geodatafusion::register(&ctx);
```
