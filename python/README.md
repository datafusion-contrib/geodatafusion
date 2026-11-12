# `geodatafusion`

Python bindings for `geodatafusion`, providing geospatial extension for the
`datafusion` SQL [query engine](https://github.com/apache/datafusion) and [Python package](https://datafusion.apache.org/python/).

## Install

```
pip install geodatafusion
```

## Usage

Register the spatial User-Defined Functions (UDFs) and Aggregate Functions (UDAFs) provided by `geodatafusion` on your `datafusion.SessionContext`. See the [top-level README](https://github.com/datafusion-contrib/geodatafusion#supported-functions) for the full list of supported SQL functions.

```py
from datafusion import SessionContext
from geodatafusion import register_all

ctx = SessionContext()
register_all(ctx)
```

`register_all` registers the scalar and aggregate functions included in the
compiled Rust crate. The Python package does not export window or table functions.

### Category-Specific Registration & Coordinate Layout

All `register_all*` helpers accept an optional `coord_type="interleaved"` or
`coord_type="separated"` keyword. Omitting it keeps the default separated layout.
The layout configures geometry-producing UDFs in the selected group; outputs with
a fixed representation, such as bounding boxes, retain that representation.

```py
from datafusion import SessionContext, udf
from geodatafusion import native, register_all_native

ctx = SessionContext()
register_all_native(ctx, coord_type="interleaved")

# Individual wrappers can still override a registered UDF.
ctx.register_udf(udf(native.Point(coord_type="separated")))
```

### Running Spatial SQL Queries

Then you can use the UDFs in SQL queries:

```py
sql = "SELECT ST_X(ST_GeomFromText('POINT(1 2)'));"
df = ctx.sql(sql)
df.show()
```

prints:

```
+-------------------------------------------+
| st_x(st_geomfromtext(Utf8("POINT(1 2)"))) |
+-------------------------------------------+
| 1.0                                       |
+-------------------------------------------+
```

## Wheel / DataFusion compatibility

| Python Release | DataFusion Version |
|----------------|-------------------|
| v0.4.0         | 55.x              |
| v0.3.1         | 53.x              | 
| v0.3.0         | 53.x               |
| v0.2.0         | 52.x              |
| v0.1.2         | 50.x              |
| v0.1.1         | 49.x              |
| v0.1.0         | 49.x              |
