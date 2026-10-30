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

You can also register specific function groups (`register_all_native`, `register_all_geo`, `register_all_geohash`) or register individual UDF wrapper classes from `geodatafusion.native`, `geodatafusion.geo`, and `geodatafusion.geohash` with a custom GeoArrow coordinate layout (`coord_type="interleaved"` or `"separated"`):

```py
from datafusion import SessionContext, udf
from geodatafusion import native, register_all_native

ctx = SessionContext()
register_all_native(ctx)

# Override ST_Point to produce interleaved coordinates
ctx.register_udf(udf(native.Point(coord_type="interleaved")))
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
