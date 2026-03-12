# `geodatafusion`

Python bindings for `geodatafusion`, providing geospatial extension for the
`datafusion` SQL [query engine](https://github.com/apache/datafusion) and [Python package](https://datafusion.apache.org/python/).

## Install

```
pip install geodatafusion
```

## Usage

To use, register the User-Defined Functions (UDFs) provided by `geodatafusion` on your `SessionContext`. The easiest way to do this is via `geodatafusion.register_all`. The [top-level Rust README](https://github.com/datafusion-contrib/geodatafusion) contains a tracker of the UDFs currently implemented.

```py
from datafusion import SessionContext
from geodatafusion import register_all

ctx = SessionContext()
register_all(ctx)
```

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

| Python Release | DataFusion Version | Release Date    |
|----------------|-------------------|-----------------|
| py-v0.1.2      | 50.2              | Oct 17, 2025    |
| py-v0.1.1      | 49.0.0            | Sep 15, 2025    |
| py-v0.1.0      | 49.0.0            | Sep 15, 2025    |
