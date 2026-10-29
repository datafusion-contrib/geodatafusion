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

`register_all` registers every function the compiled Rust crate exposes:
scalar, aggregate, window, and table functions. Published wheels are built with
only `pyo3/extension-module`, so the 55 GEOS-backed UDFs that need the
`geos-3_11`/`geos-3_12` Cargo features are not part of them; build the
extension with `--features geos-3_12` where GEOS 3.12 or newer is installed to
include those. [DEVELOP.md](DEVELOP.md) records the measurements and the
compiler pin the wheel ABI depends on.

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
