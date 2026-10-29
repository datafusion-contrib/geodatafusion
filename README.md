# `geodatafusion`

Spatial extensions for [Apache DataFusion](https://datafusion.apache.org/), an extensible query engine written in Rust that uses Apache Arrow as its in-memory format.

## Install

Add to your `Cargo.toml`:

```toml
[dependencies]
datafusion = "55.0"
geodatafusion = "0.6"
```

## Functions supported

Functions are explicitly modeled after the [PostGIS API](https://postgis.net/docs/reference.html). We strive to match the PostGIS API as much as possible.

### GEOS-backed UDFs

The GEOS overlay, processing, and planar measurement UDFs require the `geos-3_11` Cargo feature.
`ST_CoverageInvalidEdges` and `ST_CoverageSimplify` additionally require the `geos-3_12` Cargo feature because they use the GEOS 3.12 coverage APIs.
These registrations implement the PostGIS geometry overloads; geography overloads are not provided.
`ST_Difference`, `ST_Intersection`, `ST_SymDifference`, and `ST_UnaryUnion` accept the optional `gridSize` argument; negative values disable GEOS reduced-precision mode.
`ST_Buffer` accepts either an integer quad-segment count or the PostGIS buffer style parameters.
`ST_Node` accepts linework with topological dimension 1, and `ST_BuildArea` returns `POLYGON EMPTY` for empty input and SQL `NULL` for non-empty input that does not enclose an area.
`ST_Relate` boundary node rules 3 and 4 require GEOS >= 3.13; older runtimes reject those rules explicitly instead of returning non-conforming matrices. Rules 1 and 2 remain available on all supported runtimes.
`ST_DelaunayTriangles` accepts flags `0` for polygon output and `1` for edge output; flag `2` requests a TIN, which GeoArrow cannot represent and is rejected explicitly.
`ST_VoronoiLines` and `ST_VoronoiPolygons` treat a NULL `extend_to` argument as absent, matching the PostGIS default.
`ST_IsValidDetail` returns a nullable `(valid, reason, location)` struct.
`ST_MakeValid` accepts the PostGIS `method=linework|structure` and `keepcollapsed=true|false` parameter string and ignores unknown keys.
`ST_ClipByBox2D` takes a GeoArrow box2d built by `ST_MakeBox2D`; a degenerate box returns NULL, and fully outside input returns a 2D typed empty geometry.
`ST_Split` returns a GeometryCollection and preserves Z and M for point splits; line and polygon blades keep Z and drop M.
`ST_Subdivide` returns, per row, a list of `geom` structs for `SELECT s.comp.geom FROM (SELECT unnest(ST_Subdivide(...)) AS comp) AS s`.
`ST_SimplifyPolygonHull` requires polygonal input and treats `is_outer` as true when the third argument is omitted.
`ST_LineMerge` normalizes polygonal input to its boundary linework before merging, including polygonal members of a GeometryCollection; every ring stays whole and is not merged with the surrounding linework.
`ST_ReducePrecision` delegates XY rounding to the native GEOS precision model, so decimal grids such as `0.1` can differ by one ULP between GEOS 3.11 and GEOS 3.14 or newer; Z is preserved and M is dropped.
`ST_ClipByBox2D` preserves Z on every supported GEOS version: clipped vertices keep the input Z interpolated along the input linework, and remaining vertices take the elevation of their cell in a three-by-three model over the input extent.
For those functions, an explicit NULL `tolerance` follows the shared strict NULL rule and returns NULL; the PostGIS SQL wrapper raises an error for that input.
`ST_CoverageInvalidEdges` and `ST_CoverageSimplify` are window functions: NULL, EMPTY, and non-polygonal members of a partition do not participate and return NULL.
Their NULL `tolerance` and `simplifyBoundary` arguments use the PostGIS defaults of `0` and `true`.
`ST_CoverageUnion` is an aggregate that returns a 2D result; its PostGIS `geometry[]` scalar overload is not provided yet because it depends on shared scalar/aggregate SQL dispatch work.

All GEOS-backed UDFs accept XY and XYZ geometry and preserve CRS metadata from the input; each function's output dimension follows its own contract.
[GEOS 3.12](https://libgeos.org/posts/2023-06-27-geos-3-12-released/) introduced native M read/write support, but the shared adapter cannot select M semantics consistently across supported native versions.
Before calling GEOS, the adapter drops M while preserving Z: XYM becomes XY and XYZM becomes XYZ.
This matches PostGIS's computed results, where GEOS-backed operations drop M and keep Z when Z is present.
Operations that return the input unchanged, such as a zero-distance `ST_OffsetCurve` or an empty passthrough, preserve the original M ordinate.
GEOS controls output vertex order, linework segmentation, and the concrete type of empty results, so those details can vary with the installed native GEOS version.
`ST_Buffer` always returns 2D geometry.
`ST_OffsetCurve` always computes in 2D when the distance is non-zero, while `ST_VoronoiLines`, `ST_VoronoiPolygons`, and `ST_SharedPaths` follow GEOS's result dimension.
`ST_LongestLine`, `ST_MaxDistance`, `ST_ClosestPoint`, and `ST_ShortestLine` return a valid maximum- or minimum-distance witness. When several witnesses are tied, the selected geometry is unspecified and may differ from liblwgeom; only unique-distance results are compared bit-for-bit against the pinned PostGIS reference.
`ST_MinimumBoundingCircle` and `ST_MinimumBoundingRadius` use `GEOSMinimumBoundingCircle` for the center and radius; the circle polygon ring and `segs_per_quarter` are generated from the public circle definition. When multiple vertices collapse to one convex-hull point, GEOS returns an empty center, so that degenerate case is explicitly returned as a zero-radius circle at the shared coordinate. The reviewed oracle records measured GEOS-versus-liblwgeom differences: 0 ULP for the center/radius cases and at most 2 ULP for polygon shape measures.
`ST_MaximumInscribedCircle` and `ST_LargestEmptyCircle` use GEOS's iterative optimizers, so the returned center and nearest witness are specified only to the requested distance tolerance. GEOS 3.14's exact MIC path and the older approximate searches select different valid witnesses and can differ by small amounts; the reviewed oracle checks the tolerance and membership contract for those inputs and keeps bit-for-bit baselines only where the outcome is stable across GEOS 3.11 through 3.14.

### Geometry aggregates

`ST_Collect`, `ST_MakeLineAgg`, `ST_MemUnion`, `ST_Polygonize`, and `ST_Union` accept `ORDER BY`, `DISTINCT`, and `FILTER`, and they work as window functions with `OVER`.
`DISTINCT` compares the encoded geometry payload, and the row order without `ORDER BY` follows the engine plan as it does for DataFusion's built-in aggregates.
DataFusion resolves a function name against one registry entry, so the PostGIS overloads of these functions need explicit names:
`ST_CollectPair` and `ST_CollectArray`, `ST_UnionPair` and `ST_UnionArray`, `ST_PolygonizeArray`, and `ST_MakeLineAgg` for the aggregate whose plain name belongs to the released `ST_MakeLine` scalar.
Passing a geometry pair or a geometry array to the aggregate name is rejected with a planning error that names the scalar overload to use.
PostGIS raises an error when a plain function is called with `OVER`; DataFusion 55.1 ignores `OVER` on scalar functions instead, which affects these overloads in the same way as DataFusion builtins such as `abs`.
DataFusion additionally rejects `ARRAY[...]` literals unless its `nested_expressions` feature is enabled, so SQL callers pass geometry list columns to the array overloads.

### Geometry Constructors

| Name                  | Implemented | Description                                                                                                                |
| --------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------- |
| ST_Collect            | ✅          | Creates a GeometryCollection or Multi\* geometry from a set of geometries. `ST_CollectPair` and `ST_CollectArray` provide the PostGIS pair and array overloads. |
| ST_LineFromMultiPoint | ✅          | Creates a LineString from a MultiPoint geometry.                                                                           |
| ST_MakeEnvelope       | ✅          | Creates a rectangular Polygon from minimum and maximum coordinates.                                                        |
| ST_MakeLine           | ✅          | Creates a LineString from two geometries or a geometry array. `ST_MakeLineAgg` aggregates points, multipoints, and linestrings. |
| ST_MakePoint          | ✅          | Creates a 2D, 3DZ or 4D Point.                                                                                             |
| ST_MakePointM         | ✅          | Creates a Point from X, Y and M values.                                                                                    |
| ST_MakePolygon        | ✅          | Creates a Polygon from a shell and optional array of holes.                                                                |
| ST_Point              | ✅          | Creates a Point with X, Y and SRID values.                                                                                 |
| ST_PointZ             | ✅          | Creates a Point with X, Y, Z and SRID values.                                                                              |
| ST_PointM             | ✅          | Creates a Point with X, Y, M and SRID values.                                                                              |
| ST_PointZM            | ✅          | Creates a Point with X, Y, Z, M and SRID values.                                                                           |
| ST_Polygon            | ✅          | Creates a Polygon from a LineString with a specified SRID.                                                                 |
| ST_TileEnvelope       | ✅          | Creates a Web Mercator (SRID:3857) XYZ tile Polygon with an optional margin. Custom bounds are not supported.               |
| ST_HexagonGrid        |             | Returns a set of hexagons and cell indices that completely cover the bounds of the geometry argument.                      |
| ST_Hexagon            |             | Returns a single hexagon, using the provided edge size and cell coordinate within the hexagon grid space.                  |
| ST_SquareGrid         |             | Returns a set of grid squares and cell indices that completely cover the bounds of the geometry argument.                  |
| ST_Square             |             | Returns a single square, using the provided edge size and cell coordinate within the square grid space.                    |
| ST_Letters            |             | Renders text as a MultiPolygon with nominal height 100 from an independent public-domain Hershey Simplex font; exact glyph metrics and advances differ from PostGIS. Empty or glyphless text returns NULL; custom fonts use base64 TWKB and preserve XY/XYZ/XYM/XYZM. |

`ST_Letters` does not use PostGIS's embedded font. Its exact glyph geometry,
member/point counts, and advances are therefore not PostGIS-compatible: for
example, PostGIS 3.6.4 renders `A` as one 20-point part with `xmax = 55.8`,
while this implementation renders three parts and 15 points with
`xmax = 58.1077`. The shared contract covers non-empty 2D output for supported
printable ASCII, layout starting at `x = 0`, character order, positive
inter-glyph advances, and `NULL` for empty or glyphless input. Use a custom
font when PostGIS-exact glyph output is required.

### Geometry Accessors

| Name                | Implemented | Description                                                                                             |
| ------------------- | ----------- | ------------------------------------------------------------------------------------------------------- |
| GeometryType        | ✅          | Returns the type of a geometry as text; measured geometry carries the M suffix.                         |
| ST_Boundary         | ✅          | Returns the boundary of a geometry.                                                                     |
| ST_BoundingDiagonal |             | Returns the diagonal of a geometry's bounding box.                                                      |
| ST_CoordDim         | ✅          | Return the coordinate dimension of a geometry.                                                          |
| ST_Dimension        | ✅          | Returns the topological dimension of a geometry.                                                        |
| ST_Dump             | ✅          | Returns a set of geometry_dump rows for the components of a geometry.                                   |
| ST_DumpPoints       | ✅          | Returns a set of geometry_dump rows for the coordinates in a geometry.                                  |
| ST_DumpSegments     | ✅          | Returns a set of geometry_dump rows for the segments in a geometry.                                     |
| ST_DumpRings        | ✅          | Returns a set of geometry_dump rows for the exterior and interior rings of a Polygon.                   |
| ST_EndPoint         | ✅          | Returns the last point of a LineString; NULL for other geometry types, EMPTY, and NULL input.           |
| ST_Envelope         | ✅          | Returns a geometry representing the bounding box of a geometry.                                         |
| ST_ExteriorRing     | ✅          | Returns a LineString representing the exterior ring of a Polygon.                                       |
| ST_GeometryN        | ✅          | Return an element of a geometry collection.                                                             |
| ST_GeometryType     | ✅          | Returns the SQL-MM type of a geometry as text.                                                          |
| ST_InteriorRingN    | ✅          | Returns the Nth interior ring (hole) of a Polygon.                                                      |
| ST_IsClosed         | ✅          | Returns true when a geometry is non-empty, every part is non-empty, and every line-like part is closed. |
| ST_IsCollection     | ✅          | Tests if a geometry is a geometry collection type.                                                      |
| ST_IsEmpty          | ✅          | Tests if a geometry is empty.                                                                           |
| ST_IsPolygonCCW     | ✅          | Tests if Polygons have exterior rings oriented counter-clockwise and interior rings oriented clockwise. |
| ST_IsPolygonCW      | ✅          | Tests if Polygons have exterior rings oriented clockwise and interior rings oriented counter-clockwise. |
| ST_IsRing           | ✅          | Tests if a LineString is closed and simple.                                                             |
| ST_IsSimple         | ✅          | Tests if a geometry has no points of self-intersection or self-tangency.                                |
| ST_M                | ✅          | Returns the M coordinate of a Point, or NULL when the point has no M; other types raise an error.       |
| ST_MemSize          | ✅          | Returns Arrow buffer bytes referenced by one row: offsets, union slots, and payload (not GSERIALIZED).  |
| ST_NDims            | ✅          | Returns the coordinate dimension of a geometry.                                                         |
| ST_NPoints          | ✅          | Returns the number of points (vertices) in a geometry.                                                  |
| ST_NRings           | ✅          | Returns the number of rings in a polygonal geometry.                                                    |
| ST_NumGeometries    | ✅          | Returns the number of elements in a geometry collection.                                                |
| ST_NumInteriorRings | ✅          | Returns the number of interior rings (holes) of a Polygon.                                              |
| ST_NumInteriorRing  | ✅          | Returns the number of interior rings (holes) of a Polygon. Alias for ST_NumInteriorRings.               |
| ST_NumPoints        | ✅          | Returns the number of points in a LineString; NULL for other geometry types.                            |
| ST_PointN           | ✅          | Returns the Nth point in the first LineString or circular LineString in a geometry.                     |
| ST_Points           | ✅          | Returns a MultiPoint containing the coordinates of a geometry.                                          |
| ST_StartPoint       | ✅          | Returns the first point of a geometry in element order; NULL for EMPTY and NULL input.                  |
| ST_Summary          | ✅          | Summarizes geometry contents, including nested GeometryCollections from typed WKB; the cached-bbox B flag is not emitted. |
| ST_X                | ✅          | Returns the X coordinate of a Point; other geometry types raise an error.                               |
| ST_Y                | ✅          | Returns the Y coordinate of a Point; other geometry types raise an error.                               |
| ST_Z                | ✅          | Returns the Z coordinate of a Point, or NULL when the point has no Z; other types raise an error.       |
| ST_Zmflag           | ✅          | Returns a code indicating the ZM coordinate dimension of a geometry.                                    |
| ST_HasZ             | ✅          | Checks if a geometry has a Z dimension.                                                                 |
| ST_HasM             | ✅          | Checks if a geometry has an M (measure) dimension.                                                      |

### Geometry Editors

`ST_Project` follows the PostGIS geometry overloads. It treats coordinates as
planar values in the geometry's CRS units and does not infer geography from
`EPSG:4326` metadata.

| Name                             | Implemented | Description                                                                                         |
| -------------------------------- | ----------- | --------------------------------------------------------------------------------------------------- |
| ST_AddPoint                      | ✅          | Add a point to a LineString.                                                                        |
| ST_CollectionExtract             | ✅          | Given a geometry collection, returns a multi-geometry containing only elements of a specified type. |
| ST_CollectionHomogenize          | ✅          | Returns the simplest representation of a geometry collection.                                       |
| ST_Scroll                        | ✅          | Change start point of a closed LineString.                                                          |
| ST_FlipCoordinates               | ✅          | Returns a version of a geometry with X and Y axis flipped.                                          |
| ST_Force2D                       | ✅          | Force the geometries into a "2-dimensional mode".                                                   |
| ST_Force3D                       | ✅          | Force the geometries into XYZ mode. This is an alias for ST_Force3DZ.                               |
| ST_Force3DZ                      | ✅          | Force the geometries into XYZ mode.                                                                 |
| ST_Force3DM                      | ✅          | Force the geometries into XYM mode.                                                                 |
| ST_Force4D                       | ✅          | Force the geometries into XYZM mode.                                                                |
| ST_ForceCollection               | ✅          | Convert the geometry into a GEOMETRYCOLLECTION.                                                     |
| ST_ForcePolygonCCW               | ✅          | Orients all exterior rings counter-clockwise and all interior rings clockwise.                      |
| ST_ForcePolygonCW                | ✅          | Orients all exterior rings clockwise and all interior rings counter-clockwise.                      |
| ST_ForceSFS                      | ✅          | Force the geometries to use SFS 1.1 geometry types only.                                            |
| ST_ForceRHR                      | ✅          | Force the orientation of the vertices in a polygon to follow the Right-Hand-Rule.                   |
| ST_LineExtend                    | ✅          | Returns a line extended forwards and backwards by specified distances.                              |
| ST_Multi                         | ✅          | Return the geometry as a MULTI\* geometry.                                                          |
| ST_Normalize                     | ✅          | Return the geometry in its canonical form.                                                          |
| ST_Project                       | ✅          | Projects a point by a planar distance and azimuth (radians clockwise from north).                    |
| ST_QuantizeCoordinates           | ✅          | Sets least significant bits of coordinates to zero                                                  |
| ST_RemovePoint                   | ✅          | Remove a point from a linestring.                                                                   |
| ST_RemoveRepeatedPoints          | ✅          | Returns a version of a geometry with duplicate points removed.                                      |
| ST_RemoveIrrelevantPointsForView | ✅          | Removes points that are irrelevant for rendering a specific rectangluar view of a geometry.         |
| ST_RemoveSmallParts              | ✅          | Removes small parts (polygon rings or linestrings) of a geometry.                                   |
| ST_Reverse                       | ✅          | Return the geometry with vertex order reversed.                                                     |
| ST_Segmentize                    | ✅          | Returns a modified geometry/geography having no segment longer than a given distance.               |
| ST_SetPoint                      | ✅          | Replace point of a linestring with a given point.                                                   |
| ST_ShiftLongitude                | ✅          | Shifts the longitude coordinates of a geometry between -180..180 and 0..360.                        |
| ST_WrapX                         | ✅          | Wrap a geometry around an X value. Splitting polygons requires `geos-3_11`.                         |
| ST_SnapToGrid                    | ✅          | Snap all points of the input geometry to a regular grid.                                            |
| ST_Snap                          | ✅          | Snap segments and vertices of input geometry to vertices of a reference geometry.                   |
| ST_SwapOrdinates                 | ✅          | Returns a version of the given geometry with given ordinate values swapped.                         |

### Geometry Validation

`ST_IsValid` and `ST_IsValidReason` apply the OGC 2D validity rules of the
`geo` crate. Empty geometries, including empty points and empty members of
multi-geometries, are valid and report `Valid Geometry`. The valid/invalid
outcome agrees with the pinned PostGIS oracle, while the reason text for an
invalid geometry uses the native wording: a bow-tie polygon is reported as
`exterior ring has a self-intersection` where PostGIS reports
`Self-intersection[1 1]`.

| Name             | Implemented | Description                                                                                  |
| ---------------- | ----------- | -------------------------------------------------------------------------------------------- |
| ST_IsValid       | ✅          | Tests if a geometry is well-formed in 2D.                                                    |
| ST_IsValidDetail | ✅          | Returns a valid_detail row stating if a geometry is valid or if not a reason and a location. |
| ST_IsValidReason | ✅          | Returns text stating if a geometry is valid, or a reason for invalidity.                     |
| ST_MakeValid | ✅          | Attempts to make an invalid geometry valid without losing vertices.                          |

### Geometry Input

#### Well-Known Text (WKT)

| Name                | Implemented | Description                                                                                                                                           |
| ------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| ST_BdPolyFromText   | ✅          | Construct a Polygon given an arbitrary collection of closed linestrings as a MultiLineString Well-Known text representation.                          |
| ST_BdMPolyFromText  | ✅          | Construct a MultiPolygon given an arbitrary collection of closed linestrings as a MultiLineString text representation Well-Known text representation. |
| ST_GeomCollFromText | ✅          | Makes a collection Geometry from collection WKT with the given SRID. If SRID is not given, it defaults to 0.                                          |
| ST_GeomFromEWKT     | ✅          | Return a specified ST_Geometry value from Extended Well-Known Text representation (EWKT).                                                             |
| ST_GeometryFromText | ✅          | Return a specified ST_Geometry value from Well-Known Text representation (WKT). This is an alias name for ST_GeomFromText                             |
| ST_GeomFromText     | ✅          | Return a specified ST_Geometry value from Well-Known Text representation (WKT).                                                                       |
| ST_LineFromText     | ✅          | Makes a Geometry from WKT representation with the given SRID. If SRID is not given, it defaults to 0.                                                 |
| ST_MLineFromText    | ✅          | Return a specified ST_MultiLineString value from WKT representation.                                                                                  |
| ST_MPointFromText   | ✅          | Makes a Geometry from WKT with the given SRID. If SRID is not given, it defaults to 0.                                                                |
| ST_MPolyFromText    | ✅          | Makes a MultiPolygon Geometry from WKT with the given SRID. If SRID is not given, it defaults to 0.                                                   |
| ST_PointFromText    | ✅          | Makes a point Geometry from WKT with the given SRID. If SRID is not given, it defaults to unknown.                                                    |
| ST_PolygonFromText  | ✅          | Makes a Geometry from WKT with the given SRID. If SRID is not given, it defaults to 0.                                                                |
| ST_WKTToSQL         | ✅          | Return a specified ST_Geometry value from Well-Known Text representation (WKT). This is an alias name for ST_GeomFromText                             |

#### Well-Known Binary (WKB)

| Name                 | Implemented | Description                                                                                                                                   |
| -------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| ST_GeomFromEWKB      | ✅          | Return a specified ST_Geometry value from Extended Well-Known Binary representation (EWKB).                                                   |
| ST_GeomFromWKB       | ✅          | Creates a geometry instance from a Well-Known Binary geometry representation (WKB) and optional SRID.                                         |
| ST_LineFromWKB       | ✅          | Makes a LINESTRING from WKB with the given SRID                                                                                               |
| ST_LinestringFromWKB | ✅          | Makes a geometry from WKB with the given SRID.                                                                                                |
| ST_PointFromWKB      | ✅          | Makes a geometry from WKB with the given SRID                                                                                                 |
| ST_WKBToSQL          | ✅          | Return a specified ST_Geometry value from Well-Known Binary representation (WKB). This is an alias name for ST_GeomFromWKB that takes no srid |

#### Other Formats

| Name                       | Implemented | Description                                                                                                                             |
| -------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| ST_Box2dFromGeoHash        | ✅          | Return a BOX2D from a GeoHash string.                                                                                                   |
| ST_GeomFromGeoHash         | ✅          | Return a geometry from a GeoHash string.                                                                                                |
| ST_GeomFromGML             | ✅          | Takes as input GML representation of geometry and outputs a PostGIS geometry object                                                     |
| ST_GeomFromGeoJSON         | ✅          | Takes as input a geojson representation of a geometry and outputs a PostGIS geometry object                                             |
| ST_GeomFromKML             | ✅          | Takes as input KML representation of geometry and outputs a PostGIS geometry object                                                     |
| ST_GeomFromTWKB            | ✅          | Creates a geometry instance from a TWKB ("Tiny Well-Known Binary") geometry representation.                                             |
| ST_GMLToSQL                | ✅          | Return a specified ST_Geometry value from GML representation. This is an alias name for ST_GeomFromGML                                  |
| ST_LineFromEncodedPolyline | ✅          | Creates a LineString from an Encoded Polyline; deltas and the running total must fit signed 64-bit integers, otherwise the call errors. |
| ST_PointFromGeoHash        | ✅          | Return a point from a GeoHash string.                                                                                                   |
| ST_FromFlatGeobufToTable   |             | Table function that decodes FlatGeobuf data, registers the table in the caller's catalog when the stream first runs, and returns its rows. Call it in a FROM clause; the PostgreSQL void scalar call shape fails at planning. |
| ST_FromFlatGeobuf          |             | Table function that derives its schema from the FlatGeobuf header: properties in file order, then a geometry column named `geom`. The PostgreSQL template row argument is rejected with an explicit error. |

### Geometry Output

#### Well-Known Text (WKT)

| Name      | Implemented | Description                                                                                      |
| --------- | ----------- | ------------------------------------------------------------------------------------------------ |
| ST_AsEWKT | ✅          | Return the Well-Known Text (WKT) representation of the geometry with SRID meta data.             |
| ST_AsText | ✅          | Return the Well-Known Text (WKT) representation of the geometry/geography without SRID metadata. |

#### Well-Known Binary (WKB)

| Name         | Implemented | Description                                                                                                   |
| ------------ | ----------- | ------------------------------------------------------------------------------------------------------------- |
| ST_AsBinary  | ✅          | Return the OGC/ISO Well-Known Binary (WKB) representation of the geometry/geography without SRID meta data.   |
| ST_AsEWKB    | ✅          | Return the Extended Well-Known Binary (EWKB) representation of the geometry with SRID meta data.              |
| ST_AsHEXEWKB | ✅          | Returns a Geometry in HEXEWKB format (as text) using either little-endian (NDR) or big-endian (XDR) encoding. |

#### Other Formats

| Name                 | Implemented | Description                                                                                                                                                                                                                                                                                                                                                                                         |
| -------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ST_AsEncodedPolyline | ✅          | Returns an Encoded Polyline from a LineString geometry; precision above 18 raises an error and every emitted encoding decodes with ST_LineFromEncodedPolyline.                                                                                                                                                                                                                                     |
| ST_AsFlatGeobuf      |             | Aggregate returning a FlatGeobuf file for a set of rows, passed in the flattened form (geometry followed by attributes). When the first argument is a geometry column, a second constant boolean is the index option instead of a property; the geom_name option is only accepted for a struct row, because the flattened form always takes its geometry from the first argument. A struct row is accepted when it carries GeoArrow metadata. |
| ST_AsGeobuf          | ✅          | Return a Geobuf representation of a set of rows. Property types outside the supported integer, float, boolean, text, and binary set, and empty geometry members, return explicit errors.                                                                                                                                                                                                             |
| ST_AsGeoJSON         | ✅          | Return a geometry or feature in GeoJSON format.                                                                                                                                                                                                                                                                                                                                                     |
| ST_AsGML             | ✅          | Return the geometry as a GML version 2 or 3 element.                                                                                                                                                                                                                                                                                                                                                |
| ST_AsKML             | ✅          | Return the geometry as a KML element.                                                                                                                                                                                                                                                                                                                                                               |
| ST_AsLatLonText      | ✅          | Return the Degrees, Minutes, Seconds representation of the given point.                                                                                                                                                                                                                                                                                                                             |
| ST_AsMVTGeom         | ✅          | Transforms a geometry into the coordinate space of a MVT tile.                                                                                                                                                                                                                                                                                                                                      |
| ST_AsMVT             | ✅          | Aggregate function returning a MVT representation of a set of rows.                                                                                                                                                                                                                                                                                                                                 |
| ST_AsSVG             | ✅          | Returns SVG path data for a geometry; precision above 308 raises an error and EMPTY members emit no fragment.                                                                                                                                                                                                                                                                                       |
| ST_AsTWKB            | ✅          | Returns the geometry as TWKB ("Tiny Well-Known Binary"); an unclassifiable CRS requires an explicit X/Y precision.                                                                                                                                                                                                                                                                                  |
| ST_GeoHash           | ✅          | Return a GeoHash representation of the geometry.                                                                                                                                                                                                                                                                                                                                                    |

### Operators

### Spatial Relationships

#### Topological Relationships

The predicates evaluate in 2D: Z and M ordinates do not change the answer,
which matches PostGIS. Binary predicates reject arguments whose CRS metadata
conflicts with an explicit error, matching PostGIS's `Operation on mixed SRID
geometries`; undeclared CRS metadata conflicts with a declared CRS, so only two
undeclared arguments or two arguments with the same declared CRS evaluate. SQL
NULL propagates as NULL, and empty geometries follow PostGIS: `ST_Disjoint` is
true, `ST_Intersects` is false, and two empty geometries are topologically
equal.

| Name                     | Implemented | Description                                                                                                                             |
| ------------------------ | ----------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| ST_3DIntersects          | ✅          | Tests if two geometries spatially intersect in 3D - only for points, linestrings, polygons, polyhedral surface (area).                  |
| ST_Contains              | ✅          | Tests if every point of B lies in A, and their interiors have a point in common.                                                        |
| ST_ContainsProperly      | ✅          | Tests if every point of B lies in the interior of A.                                                                                    |
| ST_CoveredBy             | ✅          | Tests if every point of A lies in B.                                                                                                    |
| ST_Covers                | ✅          | Tests if every point of B lies in A.                                                                                                    |
| ST_Crosses               | ✅          | Tests if two geometries have some, but not all, interior points in common.                                                              |
| ST_Disjoint              | ✅          | Tests if two geometries have no points in common.                                                                                       |
| ST_Equals                | ✅          | Tests if two geometries include the same set of points.                                                                                 |
| ST_Intersects            | ✅          | Tests if two geometries intersect (they have at least one point in common).                                                             |
| ST_LineCrossingDirection | ✅          | Returns a number indicating the crossing behavior of two LineStrings.                                                                   |
| ST_OrderingEquals        | ✅          | Tests if two geometries represent the same geometry and have points in the same directional order.                                      |
| ST_Overlaps              | ✅          | Tests if two geometries have the same dimension and intersect, but each has at least one point not in the other.                        |
| ST_Relate                | ✅          | Tests if two geometries have a topological relationship matching an Intersection Matrix pattern, or computes their Intersection Matrix. |
| ST_RelateMatch           | ✅          | Tests if a DE-9IM Intersection Matrix matches an Intersection Matrix pattern.                                                           |
| ST_Touches               | ✅          | Tests if two geometries have at least one point in common, but their interiors do not intersect.                                        |
| ST_Within                | ✅          | Tests if every point of A lies in B, and their interiors have a point in common.                                                        |

### Measurement Functions

The spheroidal measurement functions use WGS84 longitude/latitude degrees and
metres. They accept unknown CRS metadata or `EPSG:4326`/`OGC:CRS84`. Declared
CRSs that would require a different datum, including other geographic CRSs
such as `EPSG:4269`, are rejected with an explicit error; the library does not
run a PROJ pipeline or infer a datum. This is deliberately narrower than
PostGIS: PostGIS's geometry overloads ignore SRID for these functions and use
their documented WGS84/default spheroid, so an `EPSG:4269` input receives a
WGS84-based result rather than a NAD83 result. This implementation refuses
that call instead of presenting it as equivalent.
For distance functions, multipoint and collection inputs are evaluated as
component-wise minima. This extends the pinned PostGIS three-argument
sphere/spheroid overloads, which reject component collections.
PostGIS's spheroid/length overloads do not reject a projected SRID; this
implementation does, because interpreting projected coordinates as degrees
would silently produce incorrect results.
Empty-geometry distance behavior follows the reference overload: the
two-argument `ST_DistanceSphere` returns NULL, while its three-argument form
and `ST_DistanceSpheroid` return `-1`.

The 3D measurement and intersection functions operate on planar
`POINT`/`LINESTRING`/`POLYGON` surfaces in three-dimensional space. Polygon
interiors count as surfaces for `ST_3DClosestPoint`, `ST_3DDistance`,
`ST_3DShortestLine`, and `ST_3DIntersects`, while `ST_3DLongestLine` and
`ST_3DMaxDistance` use the vertex-pair definition and inspect only exterior
rings, so polygon holes are ignored. Mixed 2D/3D inputs follow the PostGIS 2.2
missing-Z rules, where an absent Z ordinate is treated as 0. Polyhedral
surfaces and TIN are not representable as GeoArrow geometries: WKT input is
rejected with `Invalid type encountered` and WKB input with `unknown WKB type
code`, so these functions never receive a partially interpreted solid.

| Name                    | Implemented | Description                                                                                                                    |
| ----------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------ |
| ST_Area                 | ✅          | Returns the area of a polygonal geometry.                                                                                      |
| ST_Azimuth              | ✅          | Returns the north-based azimuth of a line between two points.                                                                  |
| ST_Angle                | ✅          | Returns the angle between two vectors defined by 3 or 4 points, or 2 lines.                                                    |
| ST_ClosestPoint         | ✅          | Returns the 2D point on g1 that is closest to g2. This is the first point of the shortest line from one geometry to the other. |
| ST_3DClosestPoint       | ✅          | Returns the 3D point on g1 that is closest to g2. This is the first point of the 3D shortest line.                             |
| ST_Distance             | ✅          | Returns the distance between two geometry or geography values.                                                                 |
| ST_DWithin              | ✅          | Tests if the minimum 2D Cartesian distance between two geometries is at most a finite, non-negative radius.                    |
| ST_3DDistance           | ✅          | Returns the 3D cartesian minimum distance (based on spatial ref) between two geometries in projected units.                    |
| ST_DistanceSphere       | ✅          | Returns minimum distance in metres between two lon/lat geometries using a spherical earth model.                               |
| ST_DistanceSpheroid     | ✅          | Returns the minimum distance between two lon/lat geometries using a spheroidal earth model.                                    |
| ST_FrechetDistance      | ✅          | Returns the Fréchet distance between two geometries.                                                                           |
| ST_HausdorffDistance    | ✅          | Returns the Hausdorff distance between two geometries.                                                                         |
| ST_Length               | ✅          | Returns the 2D length of a linear geometry.                                                                                    |
| ST_Length2D             | ✅          | Returns the 2D length of a linear geometry. Alias for ST_Length                                                                |
| ST_3DLength             | ✅          | Returns the 3D length of a linear geometry.                                                                                    |
| ST_LengthSpheroid       | ✅          | Returns the 2D or 3D length/perimeter of a lon/lat geometry on a spheroid.                                                     |
| ST_LongestLine          | ✅          | Returns the 2D longest line between two geometries.                                                                            |
| ST_3DLongestLine        | ✅          | Returns the 3D longest line between two geometries                                                                             |
| ST_MaxDistance          | ✅          | Returns the 2D largest distance between two geometries in projected units.                                                     |
| ST_3DMaxDistance        | ✅          | Returns the 3D cartesian maximum distance (based on spatial ref) between two geometries in projected units.                    |
| ST_MinimumClearance     | ✅          | Returns the minimum clearance of a geometry, a measure of a geometry's robustness.                                             |
| ST_MinimumClearanceLine | ✅          | Returns the two-point LineString spanning a geometry's minimum clearance.                                                      |
| ST_Perimeter            | ✅          | Returns the length of the boundary of a polygonal geometry or geography.                                                       |
| ST_Perimeter2D          | ✅          | Returns the 2D perimeter of a polygonal geometry. Alias for ST_Perimeter.                                                      |
| ST_3DPerimeter          | ✅          | Returns the 3D perimeter of a polygonal geometry.                                                                              |
| ST_ShortestLine         | ✅          | Returns the 2D shortest line between two geometries                                                                            |
| ST_3DShortestLine       | ✅          | Returns the 3D shortest line between two geometries                                                                            |

### Overlay Functions

| Name             | Implemented | Description                                                                                 |
| ---------------- | ----------- | ------------------------------------------------------------------------------------------- |
| ST_ClipByBox2D | ✅          | Computes the portion of a geometry falling within a rectangle.                              |
| ST_Difference    | ✅          | Computes a geometry representing the part of geometry A that does not intersect geometry B. |
| ST_Intersection  | ✅          | Computes a geometry representing the shared portion of geometries A and B.                  |
| ST_MemUnion      | ✅          | Aggregate function which unions geometries in a memory-efficent but slower way              |
| ST_Node          | ✅          | Nodes a collection of lines.                                                                |
| ST_Split | ✅          | Returns a collection of geometries created by splitting a geometry by another geometry.     |
| ST_Subdivide | ✅          | Computes a rectilinear subdivision of a geometry.                                           |
| ST_SymDifference | ✅          | Computes a geometry representing the portions of geometries A and B that do not intersect.  |
| ST_UnaryUnion    | ✅          | Computes the union of the components of a single geometry.                                  |
| ST_Union         | ✅          | Computes a geometry representing the point-set union of the input geometries. `ST_UnionPair` and `ST_UnionArray` provide the PostGIS pair and array overloads. |

### Geometry Processing

| Name                        | Implemented | Description                                                                                       |
| --------------------------- | ----------- | ------------------------------------------------------------------------------------------------- |
| ST_Buffer                   | ✅          | Computes a geometry covering all points within a given distance from a geometry.                  |
| ST_BuildArea                | ✅          | Creates a polygonal geometry formed by the linework of a geometry.                                |
| ST_Centroid                 | ✅          | Returns the geometric center of a geometry.                                                       |
| ST_ChaikinSmoothing         | ✅          | Returns a smoothed version of a geometry, using the Chaikin algorithm                             |
| ST_ConcaveHull              | ✅          | Computes a possibly concave geometry that contains all input geometry vertices. Requires `geos-3_11`. Polygonal input keeps the holes it already has; `allow_holes` only controls holes the hull itself creates. Z is preserved and M is dropped, matching PostGIS. |
| ST_ConvexHull               | ✅          | Computes the convex hull of a geometry. XYZ, XYM, and XYZM inputs produce a two-dimensional result; Z and M ordinates are dropped. |
| ST_DelaunayTriangles        | ✅          | Returns the Delaunay triangulation of the vertices of a geometry.                                 |
| ST_FilterByM                | ✅          | Removes vertices based on their M value                                                           |
| ST_GeneratePoints           | ✅          | Generates a MultiPoint inside a Polygon or MultiPolygon. Positive seeds replay the points; the default seed is process-local entropy. |
| ST_GeometricMedian          | ✅          | Returns the geometric median of a MultiPoint.                                                     |
| ST_LineMerge                | ✅          | Return the lines formed by sewing together a MultiLineString.                                     |
| ST_MaximumInscribedCircle   | ✅          | Computes the largest circle contained within a geometry.                                          |
| ST_LargestEmptyCircle       | ✅          | Computes the largest circle not overlapping a geometry.                                           |
| ST_MinimumBoundingCircle    | ✅          | Returns the smallest circle polygon that contains a geometry.                                     |
| ST_MinimumBoundingRadius    | ✅          | Returns the center point and radius of the smallest circle that contains a geometry.              |
| ST_OrientedEnvelope         | ✅          | Returns a minimum-area rectangle containing a geometry.                                           |
| ST_OffsetCurve              | ✅          | Returns an offset line at a given distance and side from an input line.                           |
| ST_PointOnSurface           | ✅          | Computes a point guaranteed to lie in a polygon, or on a geometry. All results are two-dimensional; XYZ, XYM, and XYZM lineal inputs drop their Z and M ordinates. |
| ST_Polygonize               | ✅          | Computes a collection of polygons formed from the linework of a set of geometries. `ST_PolygonizeArray` provides the PostGIS array overload. |
| ST_ReducePrecision          | ✅          | Returns a valid geometry with points rounded to a grid tolerance.                                 |
| ST_SharedPaths              | ✅          | Returns a collection containing paths shared by the two input linestrings/multilinestrings.       |
| ST_Simplify                 | ✅          | Returns a simplified representation of a geometry, using the Douglas-Peucker algorithm. For XYZ, XYM, and XYZM inputs, LineString, Polygon, MultiLineString, and MultiPolygon results are two-dimensional and drop Z/M. Point, MultiPoint, and typed GeometryCollection inputs are returned unchanged. |
| ST_SimplifyPreserveTopology | ✅          | Returns a simplified and valid representation of a geometry, using the Douglas-Peucker algorithm. For XYZ, XYM, and XYZM inputs, LineString, Polygon, MultiLineString, and MultiPolygon results are two-dimensional and drop Z/M. Point, MultiPoint, and typed GeometryCollection inputs are returned unchanged. |
| ST_SimplifyPolygonHull | ✅          | Computes a simplifed topology-preserving outer or inner hull of a polygonal geometry.             |
| ST_SimplifyVW               | ✅          | Returns a simplified representation of a geometry, using the Visvalingam-Whyatt algorithm. For XYZ, XYM, and XYZM inputs, LineString, Polygon, MultiLineString, and MultiPolygon results are two-dimensional and drop Z/M. Point, MultiPoint, and typed GeometryCollection inputs are returned unchanged. |
| ST_SetEffectiveArea         | ✅          | Sets the effective area for each vertex, using the Visvalingam-Whyatt algorithm.                  |
| ST_TriangulatePolygon       | ✅          | Computes the constrained Delaunay triangulation of polygons                                       |
| ST_VoronoiLines             | ✅          | Returns the boundaries of the Voronoi diagram of the vertices of a geometry.                      |
| ST_VoronoiPolygons          | ✅          | Returns the cells of the Voronoi diagram of the vertices of a geometry.                           |

`ST_Simplify`, `ST_SimplifyVW`, and `ST_SimplifyPreserveTopology` currently
require a scalar tolerance; a vectorized epsilon argument returns
`NotImplemented`.

### Coverages

| Name                    | Implemented | Description                                                                          |
| ----------------------- | ----------- | ------------------------------------------------------------------------------------ |
| ST_CoverageInvalidEdges | ✅          | Window function that finds locations where polygons fail to form a valid coverage. Requires `geos-3_12`. |
| ST_CoverageSimplify     | ✅          | Window function that simplifies the edges of a polygonal coverage. Requires `geos-3_12`. |
| ST_CoverageUnion        | ✅          | Aggregate that computes the union of a set of polygons forming a coverage by removing shared edges. Requires `geos-3_11`. |

### Affine Transformations

| Name          | Implemented | Description                                                    |
| ------------- | ----------- | -------------------------------------------------------------- |
| ST_Affine     | ✅          | Apply a 3D affine transformation to a geometry.                |
| ST_Rotate     | ✅          | Rotates a geometry about an origin point.                      |
| ST_RotateX    | ✅          | Rotates a geometry about the X axis.                           |
| ST_RotateY    | ✅          | Rotates a geometry about the Y axis.                           |
| ST_RotateZ    | ✅          | Rotates a geometry about the Z axis.                           |
| ST_Scale      | ✅          | Scales a geometry by given factors.                            |
| ST_Translate  | ✅          | Translates a geometry by given offsets.                        |
| ST_TransScale | ✅          | Translates and scales a geometry by given offsets and factors. |

### Clustering Functions

These functions are implemented natively (no GEOS feature required) and verified against the
pinned PostGIS reference in `testing/postgis`. `ST_ClusterDBSCAN`, `ST_ClusterIntersectingWin`,
`ST_ClusterKMeans`, and `ST_ClusterWithinWin` are window functions and read the whole
`PARTITION BY` group; `eps`, `minpoints`, `k`, and `max_radius` come from the first row of the
partition, matching PostGIS. `ST_ClusterIntersecting` and `ST_ClusterWithin` are aggregates that
return one cluster geometry per list element as a Dump-like `List<Struct{geom: geoarrow.wkb}>`.
Read `geom` after `UNNEST`, for example
`SELECT ST_AsEWKT(cluster.geom) FROM (SELECT unnest(ST_ClusterWithin(geom, 1.0)) AS cluster FROM t)`.
The `geom` field carries the input CRS metadata and DataFusion's struct expansion keeps it, so
`ST_AsText`, `ST_AsEWKT`, `ST_Force2D`, and `ST_Buffer` all compose on `cluster.geom`; standard
GeoArrow WKB also preserves nested `GeometryCollection` members instead of flattening them. No
optimizer rule is involved, so sessions that register only the exported UDFs keep the same
behavior. `ST_ClusterWithin` follows PostGIS's aggregate behavior where a NULL or zero tolerance
reports `Tolerance not defined`. DataFusion type coercion for
`UNION`/`UNION ALL` keeps a single field for the combined geometry column, so per-row CRS
differences cannot be detected at runtime; keep inputs that share a column on the same CRS.
Non-finite coordinates are rejected explicitly instead of producing NaN-dependent cluster ids.
PostGIS named-argument notation (`eps => 50`) is not supported; pass arguments positionally.

| Name                      | Implemented | Description                                                                                                         |
| ------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------- |
| ST_ClusterDBSCAN          | ✅ | Window function that returns a cluster id for each input geometry using the DBSCAN algorithm.                       |
| ST_ClusterIntersecting    | ✅ | Aggregate function that clusters input geometries into connected sets.                                              |
| ST_ClusterIntersectingWin | ✅ | Window function that returns a cluster id for each input geometry, clustering input geometries into connected sets. |
| ST_ClusterKMeans          | ✅ | Window function that returns a cluster id for each input geometry using the K-means algorithm.                      |
| ST_ClusterWithin          | ✅ | Aggregate function that clusters geometries by separation distance.                                                 |
| ST_ClusterWithinWin       | ✅ | Window function that returns a cluster id for each input geometry, clustering using separation distance.            |

### Bounding Box Functions

| Name               | Implemented | Description                                                              |
| ------------------ | ----------- | ------------------------------------------------------------------------ |
| Box2D              | ✅          | Returns a BOX2D representing the 2D extent of a geometry.                |
| Box3D              | ✅          | Returns a BOX3D representing the 3D extent of a geometry.                |
| ST_EstimatedExtent | ✅          | Returns the estimated extent of a spatial table.                         |
| ST_Expand          | ✅          | Returns a bounding box expanded from another bounding box or a geometry. |
| ST_Extent          | ✅          | Aggregate function that returns the bounding box of geometries.          |
| ST_3DExtent        |             | Aggregate function that returns the 3D bounding box of geometries.       |
| ST_MakeBox2D       | ✅          | Creates a BOX2D defined by two 2D point geometries.                      |
| ST_3DMakeBox       | ✅          | Creates a BOX3D defined by two 3D point geometries.                      |
| ST_XMax            | ✅          | Returns the X maxima of a 2D or 3D bounding box or a geometry.           |
| ST_XMin            | ✅          | Returns the X minima of a 2D or 3D bounding box or a geometry.           |
| ST_YMax            | ✅          | Returns the Y maxima of a 2D or 3D bounding box or a geometry.           |
| ST_YMin            | ✅          | Returns the Y minima of a 2D or 3D bounding box or a geometry.           |
| ST_ZMax            | ✅          | Returns the Z maxima of a 2D or 3D bounding box or a geometry.           |
| ST_ZMin            | ✅          | Returns the Z minima of a 2D or 3D bounding box or a geometry.           |

The bounding-box overloads keep the engine's typed return values. `ST_MakeBox2D`
and `ST_3DMakeBox` require 2D and 3D points respectively; the mixed-dimension
casts that PostGIS applies implicitly are rejected at planning time.
`ST_Expand` accepts the PostGIS box overloads (box2d plus one or two distances,
box3d plus one, two, or three distances) and keeps returning a box; larger box
calls are rejected instead of switching to PostGIS's geometry overload.
`Box2D`, `Box3D`, and `ST_*Min`/`ST_*Max` return `NULL` for `EMPTY` inputs and
use Z = 0 for 2D inputs while ignoring M, matching PostGIS. A full box3d with
different minimum and maximum Z values can be queried through the extent
accessors but cannot be serialized as a geometry, because GeoArrow cannot
represent the `POLYHEDRALSURFACE` that PostGIS builds from it. `ST_Envelope`
keeps the input's degenerate or empty geometry when the envelope cannot form a
polygon, matching PostGIS.

`ST_EstimatedExtent` is a scalar function: query it as
`SELECT ST_EstimatedExtent('table', 'geometry_column')` (or pass `schema`,
`table`, and `column` as three arguments). It returns a BOX2D when the table
provider has a metadata-derived extent and `NULL` otherwise; it never scans
the table to estimate one. The named column must be a GeoArrow geometry
column, and unknown catalogs, schemas, tables, or columns raise errors.

### Linear Referencing

| Name                       | Implemented | Description                                                                |
| -------------------------- | ----------- | -------------------------------------------------------------------------- |
| ST_LineInterpolatePoint    | ✅          | Returns a point interpolated along a line at a fractional location.        |
| ST_3DLineInterpolatePoint  | ✅          | Returns a point interpolated along a 3D line at a fractional location.     |
| ST_LineInterpolatePoints   | ✅          | Returns points interpolated along a line at a fractional interval.         |
| ST_LineLocatePoint         | ✅          | Returns the fractional location of the closest point on a line to a point. |
| ST_LineSubstring           | ✅          | Returns the part of a line between two fractional locations.               |
| ST_LocateAlong             | ✅          | Returns the point(s) on a geometry that match a measure value.             |
| ST_LocateBetween           | ✅          | Returns the portions of a geometry that match a measure range.             |
| ST_LocateBetweenElevations | ✅          | Returns the portions of a geometry that lie in an elevation (Z) range.     |
| ST_InterpolatePoint        | ✅          | Returns the interpolated measure of a geometry closest to a point.         |
| ST_AddMeasure              | ✅          | Interpolates measures along a linear geometry.                             |
