# `geodatafusion`

Spatial extensions for [Apache DataFusion](https://datafusion.apache.org/), built on the [GeoArrow](https://geoarrow.org/) in-memory columnar format and modeled after the [PostGIS](https://postgis.net/docs/reference.html) SQL API.

## Workspace Overview

This repository provides core spatial SQL functions, file-format integrations for DataFusion, and Python bindings:

| Crate / Package | Path | Capabilities |
| --- | --- | --- |
| [`geodatafusion`](rust/geodatafusion) | `rust/geodatafusion` | Spatial scalar and aggregate UDFs (`ST_*`, `Box2D`, `Box3D`, `GeometryType`) implemented in Rust with `geoarrow`, `geo`, and `geohash`. |
| [`geodatafusion-flatgeobuf`](rust/geodatafusion-flatgeobuf) | `rust/geodatafusion-flatgeobuf` | Read and write [FlatGeobuf](https://flatgeobuf.org/) (`.fgb`) files via DataFusion `FileFormat`, with R-tree bounding-box filter pushdown for `ST_Intersects`. |
| [`geodatafusion-geoparquet`](rust/geodatafusion-geoparquet) | `rust/geodatafusion-geoparquet` | Read [GeoParquet](https://geoparquet.org/) (`.parquet`) files with automatic GeoArrow extension metadata inference. |
| [`geodatafusion-geojson`](rust/geodatafusion-geojson) | `rust/geodatafusion-geojson` | Write newline-delimited GeoJSON (`.geojsonl`) files via DataFusion `FileFormat` (`COPY ... TO 'out.geojsonl'`). |
| [`geodatafusion` (Python)](python) | `python/` | PyO3/FFI Python bindings for registering spatial UDFs onto `datafusion.SessionContext`. |

## Quickstart

### Rust

Add `datafusion` and `geodatafusion` to your `Cargo.toml`:

```toml
[dependencies]
datafusion = "55.0"
geodatafusion = "0.6"
```

Register all spatial functions on a DataFusion `SessionContext` and run spatial SQL queries:

```rust
use datafusion::prelude::SessionContext;

# #[tokio::main]
# async fn main() -> datafusion::error::Result<()> {
let ctx = SessionContext::new();
geodatafusion::register(&ctx);

let df = ctx
    .sql("SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4)) AS dist;")
    .await?;
df.show().await?;
# Ok(())
# }
```

#### Selective Registration & Coordinate Layout

You can register individual function providers or instantiate UDFs with a specific GeoArrow coordinate layout (`CoordType::Separated` by default, or `CoordType::Interleaved`):

```rust
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;
use geoarrow_schema::CoordType;
use geodatafusion::udf;
use geodatafusion::udf::native::constructors::Point;

let ctx = SessionContext::new();

// Register only native and geohash UDFs
udf::native::register(&ctx);
udf::geohash::register(&ctx);

// Or register a single UDF with interleaved coordinates
ctx.register_udf(ScalarUDF::from(Point::new(CoordType::Interleaved)));
```

### Python

Install from PyPI:

```bash
pip install geodatafusion
```

Register functions on a `datafusion.SessionContext`:

```python
from datafusion import SessionContext
from geodatafusion import register_all

ctx = SessionContext()
register_all(ctx)

ctx.sql("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'))").show()
```

See [`python/README.md`](python/README.md) for category-specific registration (`register_all_native`, `register_all_geo`, `register_all_geohash`) and custom `coord_type` configuration.

## Feature Flags & Compatibility

### Version Compatibility

| `geodatafusion` (Rust) | `geodatafusion` (Python) | `datafusion` | `arrow` | `geoarrow` |
| --- | --- | --- | --- | --- |
| `0.6.x` | `0.4.x` | `55.x` | `59.x` | `0.9.x` |
| `0.5.x` | — | `54.x` | `57.x` | `0.8.x` |
| `0.4.x` | `0.3.x` | `53.x` | `57.x` | `0.8.x` |
| `0.3.x` | `0.2.x` | `52.x` | `57.x` | `0.7.x` |
| `0.2.x` | — | `51.x` | `57.x` | `0.7.x` |
| `0.1.x` | `0.1.x` | `49.x`–`50.x` | `55.x`–`56.x` | `0.5.x` |

## Supported Functions

Below are the spatial SQL functions currently registered by `geodatafusion::register`. For the full PostGIS function checklist (including unimplemented functions), see [`POSTGIS_COMPATIBILITY.md`](POSTGIS_COMPATIBILITY.md).

### Geometry Constructors

| Name | Provider | Description |
| --- | --- | --- |
| `ST_LineFromMultiPoint` | `native` | Creates a LineString from a MultiPoint geometry. |
| `ST_MakeEnvelope` | `native` | Creates a rectangular Polygon from minimum and maximum coordinates and optional SRID. |
| `ST_MakeLine` | `native` | Creates a LineString from two Point/LineString geometries or a `List` of Point/MultiPoint/LineString geometries. |
| `ST_MakePoint` | `native` | Creates a 2D XY, 3D XYZ, or 4D XYZM Point geometry. |
| `ST_MakePointM` | `native` | Creates a Point with X, Y, and M (measure) coordinates. |
| `ST_Point` | `native` | Creates a 2D Point with X, Y, and optional SRID values. |
| `ST_PointZ` | `native` | Creates a 3D Point with X, Y, Z, and optional SRID values. |
| `ST_PointM` | `native` | Creates a Point with X, Y, M, and optional SRID values. |
| `ST_PointZM` | `native` | Creates a 4D Point with X, Y, Z, M, and optional SRID values. |

### Geometry Accessors

| Name | Provider | Description |
| --- | --- | --- |
| `GeometryType` | `native` | Returns the type of a geometry as uppercase text (e.g., `'POINT'`, `'LINESTRINGM'`). |
| `ST_CoordDim` | `native` | Returns the coordinate dimension of a geometry (`2`, `3`, or `4`). |
| `ST_Dimension` | `native` | Returns the inherent topological dimension of a geometry (`0`, `1`, or `2`). |
| `ST_Dump` | `native` | Decomposes a geometry into a list of `(path, geom)` structs (combine with `unnest` for set-returning behavior). |
| `ST_EndPoint` | `native` | Returns the last point of a LineString, or `NULL` if the input is not a LineString. |
| `ST_Envelope` | `native` | Returns the minimum axis-aligned bounding box of a geometry as a Polygon, Point, or LineString. |
| `ST_ExteriorRing` | `native` | Returns a LineString representing the exterior ring of a Polygon. |
| `ST_GeometryType` | `native` | Returns the SQL-MM type of a geometry as text (e.g., `'ST_Point'`, `'ST_LineString'`). |
| `ST_HasM` | `native` | Returns `true` if a geometry has an M (measure) dimension. |
| `ST_HasZ` | `native` | Returns `true` if a geometry has a Z dimension. |
| `ST_InteriorRingN` | `native` | Returns the 1-based Nth interior ring (hole) of a Polygon as a LineString. |
| `ST_IsClosed` | `native` | Tests if a LineString's start and end points are coincident. |
| `ST_IsCollection` | `native` | Returns `true` if the geometry type is a collection (`Multi*` or `GeometryCollection`). |
| `ST_IsEmpty` | `native` | Returns `true` if a geometry is topologically empty. |
| `ST_M` | `native` | Returns the M coordinate of a Point. |
| `ST_NDims` | `native` | Returns the coordinate dimension of a geometry (`2`, `3`, or `4`). |
| `ST_NPoints` | `native` | Returns the number of points (vertices) in a geometry. |
| `ST_NRings` | `native` | Returns the number of rings (exterior + interior) in a polygonal geometry. |
| `ST_NumGeometries` | `native` | Returns the number of component geometries in a collection (or `1` for non-empty atomic geometries). |
| `ST_NumInteriorRings` | `native` | Returns the number of interior rings (holes) of a Polygon. |
| `ST_NumInteriorRing` | `native` | Alias for `ST_NumInteriorRings`. |
| `ST_NumPoints` | `native` | Alias for `ST_NPoints`. |
| `ST_PointN` | `native` | Returns the Nth point in the first LineString of a geometry (1-based; negative indices count backward from the end). |
| `ST_StartPoint` | `native` | Returns the first point of a LineString, or `NULL` if the input is not a LineString. |
| `ST_X` | `native` | Returns the X coordinate of a Point. |
| `ST_Y` | `native` | Returns the Y coordinate of a Point. |
| `ST_Z` | `native` | Returns the Z coordinate of a Point. |
| `ST_Zmflag` | `native` | Returns a code indicating the ZM coordinate dimension (`0` = 2D, `1` = 3DM, `2` = 3DZ, `3` = 4D). |

### Geometry Editors

| Name | Provider | Description |
| --- | --- | --- |
| `ST_Force2D` | `native` | Forces geometries into 2D (`XY`) mode. |
| `ST_Force3D` | `native` | Alias for `ST_Force3DZ`. |
| `ST_Force3DZ` | `native` | Forces geometries into `XYZ` mode, filling missing Z coordinates with an optional default (`0.0`). |
| `ST_Force3DM` | `native` | Forces geometries into `XYM` mode, filling missing M coordinates with an optional default (`0.0`). |
| `ST_Force4D` | `native` | Forces geometries into `XYZM` mode, filling missing Z and M coordinates with optional defaults (`0.0`). |

### Geometry Validation

| Name | Provider | Description |
| --- | --- | --- |
| `ST_IsValid` | `geo` | Tests if a geometry is well-formed and valid in 2D. |
| `ST_IsValidReason` | `geo` | Returns text stating `'Valid Geometry'` or the reason for invalidity. |

### Geometry Input & Output (WKT, WKB, GeoHash)

| Name | Provider | Description |
| --- | --- | --- |
| `ST_GeomFromText` | `native` | Constructs a geometry from OGC Well-Known Text (WKT). |
| `ST_GeometryFromText` | `native` | Alias for `ST_GeomFromText`. |
| `ST_WKTToSQL` | `native` | Alias for `ST_GeomFromText`. |
| `ST_GeomFromWKB` | `native` | Constructs a geometry from OGC Well-Known Binary (WKB). |
| `ST_WKBToSQL` | `native` | Alias for `ST_GeomFromWKB`. |
| `ST_AsText` | `native` | Returns the OGC Well-Known Text (WKT) representation of a geometry. |
| `ST_AsBinary` | `native` | Returns the OGC Well-Known Binary (WKB) representation of a geometry. |
| `ST_Box2dFromGeoHash` | `geohash` | Returns a `BOX2D` from a GeoHash string (with optional precision). |
| `ST_PointFromGeoHash` | `geohash` | Returns the center `Point` of a GeoHash string (with optional precision). |
| `ST_GeoHash` | `geohash` | Returns the GeoHash string representation of a geometry's bounding center. |

### Spatial Relationships

| Name | Provider | Description |
| --- | --- | --- |
| `ST_Contains` | `geo` | Tests if every point of `geomB` lies in `geomA`, and their interiors have a point in common. |
| `ST_CoveredBy` | `geo` | Tests if every point of `geomA` lies in `geomB`. |
| `ST_Covers` | `geo` | Tests if every point of `geomB` lies in `geomA`. |
| `ST_Crosses` | `geo` | Tests if two geometries have some, but not all, interior points in common. |
| `ST_Disjoint` | `geo` | Tests if two geometries have no points in common. |
| `ST_Equals` | `geo` | Tests if two geometries represent the same point set. |
| `ST_Intersects` | `geo` | Tests if two geometries share at least one point. |
| `ST_Overlaps` | `geo` | Tests if two geometries of the same dimension intersect, with each having at least one point not in the other. |
| `ST_Touches` | `geo` | Tests if two geometries have at least one point in common, but their interiors do not intersect. |
| `ST_Within` | `geo` | Tests if every point of `geomA` lies in `geomB`, and their interiors have a point in common. |

### Measurement Functions

| Name | Provider | Description |
| --- | --- | --- |
| `ST_Area` | `geo` | Returns the planar area of a polygonal geometry. |
| `ST_Distance` | `geo` | Returns the 2D Cartesian minimum distance between two geometries. |
| `ST_DWithin` | `geo` | Tests if the 2D Cartesian minimum distance between two geometries is within a given radius. |
| `ST_Length` | `geo` | Returns the 2D Cartesian length of a linear geometry. |
| `ST_Length2D` | `geo` | Alias for `ST_Length`. |

### Geometry Processing

| Name | Provider | Description |
| --- | --- | --- |
| `ST_Centroid` | `geo` | Returns the geometric center of mass of a geometry. |
| `ST_ConvexHull` | `geo` | Computes the smallest convex geometry that encloses all vertices of the input. |
| `ST_LineMerge` | `geo` | Sews together the constituent linework of a `MultiLineString`. |
| `ST_OrientedEnvelope` | `geo` | Returns a minimum-area rotated rectangle enclosing a geometry. |
| `ST_PointOnSurface` | `geo` | Computes a point guaranteed to lie on the surface of a geometry. |
| `ST_Simplify` | `geo` | Simplifies a geometry using the Douglas-Peucker algorithm. |
| `ST_SimplifyPreserveTopology` | `geo` | Simplifies a geometry using the topology-preserving Douglas-Peucker algorithm. |
| `ST_SimplifyVW` | `geo` | Simplifies a geometry using the Visvalingam-Whyatt algorithm. |

### Bounding Box Functions

| Name | Provider | Description |
| --- | --- | --- |
| `Box2D` | `native` | Returns a 2D bounding box (`geoarrow.box`) representing the 2D extent of a geometry. |
| `Box3D` | `native` | Returns a 3D bounding box (`geoarrow.box` XYZ) representing the 3D extent of a geometry. |
| `ST_Expand` | `native` | Expands a box or geometry by a uniform or per-axis distance. |
| `ST_Extent` | `native` | Aggregate function returning the 2D `box2d` bounding box that encloses a set of geometries. |
| `ST_MakeBox2D` | `native` | Creates a 2D bounding box defined by two 2D point geometries. |
| `ST_3DMakeBox` | `native` | Creates a 3D bounding box defined by two 3D point geometries. |
| `ST_XMax` | `native` | Returns the X maximum of a 2D/3D bounding box or geometry. |
| `ST_XMin` | `native` | Returns the X minimum of a 2D/3D bounding box or geometry. |
| `ST_YMax` | `native` | Returns the Y maximum of a 2D/3D bounding box or geometry. |
| `ST_YMin` | `native` | Returns the Y minimum of a 2D/3D bounding box or geometry. |
| `ST_ZMax` | `native` | Returns the Z maximum of a 2D/3D bounding box or geometry. |
| `ST_ZMin` | `native` | Returns the Z minimum of a 2D/3D bounding box or geometry. |

## Contributing & Development

See [DEVELOP.md](DEVELOP.md) for build, test, SQL logic test (`sqllogictests`), and contribution guidelines, and [`python/DEVELOP.md`](python/DEVELOP.md) for Python package development.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
