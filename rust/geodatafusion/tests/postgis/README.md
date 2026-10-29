# PostGIS Differential Oracle

This directory contains reviewed, typed differential cases for GeoDataFusion
function behavior. The expected values come from a pinned PostGIS instance,
never from the implementation under test.

## Layout

Each group has one parameter shape and its own independently maintained SQL:

```
measurement/distance/
  inputs.json       typed arguments, stable ids, source URLs, comparison mode
  postgis.sql       PostgreSQL/PostGIS query using $1, $2, ...
  datafusion.sql    GeoDataFusion query using the same positional parameters
  expected.json     reviewed typed results plus hashes and oracle versions
```

`inputs.json`, `postgis.sql`, and `datafusion.sql` are hashed by exact bytes.
Changing any byte invalidates `expected.json` and requires a new reviewed
baseline.

The current reference environment is
`postgis/postgis:18-3.6@sha256:4a43e3c3e5d55b9208240fc71db543b79c638766280c625a425bd0a2390989c4`.
The baseline records the actual PostgreSQL, PostGIS, and GEOS versions read
from that instance.

## Group Discovery

The PostGIS integration test recursively discovers every directory under
`tests/postgis` that contains any of `inputs.json`, `postgis.sql`,
`datafusion.sql`, or `expected.json`. Groups are sorted by relative path, so
adding a family fixture does not require editing a whitelist. A directory with
any convention file is treated as a group; missing required files, malformed
JSON, or a missing reviewed baseline fail explicitly. Discovery failing to
find any group is also an error.

Every `inputs.json` must explicitly declare `required_features` (an empty list
is allowed). Feature names must be declared in `Cargo.toml` and known to the
test support resolver; an undeclared or unsupported name fails before any SQL
or baseline comparison runs. When a declared feature is unavailable, the group
is still discovered and reported with `feature_skipped_groups` and
`feature_skipped_cases` plus the missing feature reason; the default test run
executes all available groups. Run with `--all-features` to execute every
feature-gated group and require zero feature skips.

Files, malformed inputs, and reviewed baselines are validated before feature
gating, so a disabled feature cannot hide a broken fixture. Case-level
`skip` entries still require `reason`, `issue`, and `review_after`, and a run
with zero executed or zero compared cases still fails. GEOS-backed groups
currently declare `geos-3_11`; future version gates such as `geos-3_12` follow
the same `required_features` path.

## Comparison

- `exact` compares Boolean, integer, text, null, and byte values without
  tolerance. The current first slice uses it for `ST_DWithin` Booleans.
- `float-ulp` compares IEEE-754 bits. Equal bits pass with `max_ulps = 0`;
  other finite values pass only within the declared ULP distance. NaN and
  infinity require identical bits.
- `binary-exact` compares raw bytes without protobuf or geometry decoding.
  TWKB encoding groups use it where the reviewed PostGIS bytes are the
  contract.
- `geobuf-semantic` decodes both payloads with the independent `geobuf-codec`
  decoder, orders features deterministically, and compares the decoded JSON
  so protobuf field ordering does not create false mismatches.
- `geometry-coordinate-ulp` keeps geometry type, nesting, dimensions, SRID,
  and non-coordinate values strict, and accepts only coordinate components
  within `max_ulps`. Use it case-locally for reviewed cross-version numeric
  contracts such as grid rounding; it does not change `geometry-exact`.
- `geometry-exact` decodes EWKB and compares geometry type, nested coordinate
  sequences, dimension, and SRID for points, line strings, polygons,
  multi-geometries, and geometry collections. It does not call a GeoDataFusion
  UDF as its comparator.

Extension geometry outputs are independently encoded to WKB and receive the
field's CRS SRID before comparison. Raw `Binary` outputs from `ST_AsEWKB` or
`ST_AsBinary` are read as already-encoded bytes and are not re-encoded.

Strict geometry comparisons cover unique-distance results. When several
nearest or farthest witnesses share the reference distance, the witness choice
is not portable across implementations; those cases are covered by contract
tests that independently verify endpoint membership, direction, and a declared
numeric bound instead of storing one implementation's choice as expected.

`ST_Dump` uses separate SQL shapes: PostgreSQL returns `geometry_dump` rows,
while GeoDataFusion returns a list that is expanded with `UNNEST`. Both sides
are normalized to typed `path` and EWKB values before comparison.

The `affine/` groups cover the eight affine SQL functions and their numeric,
point-factor, point-origin, 2D/XYZ/XYM/XYZM, NULL, EMPTY, and multi-row
shapes. Rotations stay on the exact comparator: the pinned Linux PostGIS
reference and the local macOS DataFusion build produced zero coordinate ULP
differences for the reviewed trigonometric cases.

The `3d_*` groups cover `ST_3DDistance`, `ST_3DMaxDistance`,
`ST_3DClosestPoint`, `ST_3DShortestLine`, `ST_3DLongestLine`, and
`ST_3DIntersects` with planar points, lines, polygons (with holes),
multi-geometries, Z/M distinction, NULL, EMPTY, mixed 2D/3D input, and
degenerate segments. Geometry results are compared through `ST_AsEWKB` so the
PostGIS side is typed independently of the GeoDataFusion output path. The
intersection group also covers representable near-but-separated points and
segments at ordinary and large coordinate scales; no distance epsilon is used
to turn a positive separation into an intersection.

The implementation uses the mathematical filled-surface contract for planar
polygons: a point in a polygon's interior at exactly the polygon plane has
zero 3D distance and intersects it. Pinned PostGIS 3.6.4 has exact-zero
point-in-ring edge cases at `z = 0` where that same point is reported at
non-zero distance; the differential cases therefore use `z != 0` for point-in
surface checks while the SLT covers the mathematically sound exact-plane
behavior. Polyhedral surfaces, TIN, and curved geometry are outside the current
partial scope.

The `formats/` groups cover `ST_GeomFromGeoJSON`, `ST_AsGeoJSON`,
`ST_GeomFromGeoHash`, `ST_LineFromEncodedPolyline`, `ST_AsEncodedPolyline`,
and `ST_AsSVG` against the same pinned reference. Geometry results are
normalized through `ST_AsBinary`/EWKB decoding, and text results use the exact
comparator. The reviewed SVG cases avoid a reproducible PostGIS 3.6.4
SIGSEGV: `ST_AsSVG('MULTILINESTRING((0 0,1 1),EMPTY)'::geometry, 1, 15)`
terminates the backend with signal 11, while the absolute-mode form and a
two-non-empty relative form return strings. GeoDataFusion emits the
relative-mode fragment and represents the empty member as a trailing `M`
move command. The exact SQL, container transcript, and full Docker log are
recorded in the worker evidence.

The `vertex-editors/` and `dimension-editors/` groups cover the five
LineString vertex editors and the seven dimension editors across XY, XYZ,
XYM, and XYZM inputs, including NULL geometry and NULL extra arguments.

The geodesic groups cover `ST_DistanceSphere`, `ST_DistanceSpheroid`, and
`ST_LengthSpheroid` with line/polygon/collection, antimeridian, pole, antipode,
3D/M, NULL, and EMPTY inputs. The `editors/project` groups cover both real
PostGIS geometry overloads of `ST_Project`: `(point, distance, azimuth)` and
`(from_point, to_point, distance)`. These are planar projections in coordinate
units; azimuth is in radians clockwise from north. The project groups use EWKT
inputs across XY/XYZ/XYM/XYZM, SRID 4326/3857, zero and negative distances,
NULL, and EMPTY. The pinned three-argument sphere/spheroid overloads reject
component geometry collections, so those oracle cases stay on points, lines,
and polygons. Ordinary SLT contracts additionally cover nested component
collections. The differential geodesic groups use unknown CRS inputs. Declared
non-WGS84 geographic CRSs are rejected by these functions with an explicit
error instead of being reprojected or assigned a different datum; that
narrower boundary is covered in `tests/geodesic_functions.rs` and is not
presented as full PostGIS CRS equivalence.

The `coordinate-editors/` groups cover line extension, coordinate quantization,
repeated-point removal, segmentization, longitude shifting, and X wrapping.
The line group exercises all six functions together, and the general group
covers point, polygon, collection, EMPTY, and NULL inputs for the five editors
that accept any geometry.

The `grids/` groups cover `ST_Hexagon`, `ST_Square`, their table-function
forms, and real Arrow input nullability. Table-function cases embed literal
constants because DataFusion resolves table functions before parameter
binding. `ST_EstimatedExtent` is not part of this oracle: PostGIS derives its
result from `ANALYZE` statistics, while this engine exposes an engine-native
scalar function over provider metadata. `tests/estimated_extent.rs`,
`tests/slt/estimated_extent.slt`, and the FlatGeobuf and GeoParquet provider
tests record that contract; `tests/estimated_extent_capability.rs` records why
the generic `TableProvider::statistics` surface is not used.

The `constructors/` groups cover `ST_MakeLine`, `ST_MakeEnvelope`,
`ST_TileEnvelope`, `ST_MakePolygon`/`ST_Polygon`, and
`ST_LineFromMultiPoint`. The `io/` groups cover the type-specific WKT and WKB
constructors, the generic WKT/WKB aliases, `ST_AsBinary`, 2D through XYZM
coordinates, EMPTY and NULL inputs, wrong-type NULL results, and SRID
normalization including values above `SRID_MAXIMUM`.

The `linear_referencing/` groups cover the ten linear referencing functions,
including 2D through XYZM interpolation, degenerate and EMPTY lines, measure
ranges and offsets, polygon and hole clipping, mixed geometry collections, and
NULL arguments. The `linear_referencing/nullability` group uses real Arrow
input fields to exercise SQL NULL propagation and `IS NULL` with a
non-nullable input column.

The `io/geom_from_gml`, `io/geom_from_gml_srid`, `io/gml_to_sql`,
`io/geom_from_kml`, `io/as_gml`, `io/as_gml_options`, `io/as_gml_verbose`,
`io/as_kml`, `io/as_kml_options`, `io/as_latlontext`, and
`io/as_latlontext_format` groups cover GML 2/3.2 and KML 2.2 input,
authority axis order, embedded versus explicit SRIDs, holes, collections,
EMPTY, output precision/options/prefix/id, KML altitude handling, and
degrees/minutes/seconds formatting.
Authority-order (`urn:...`) axis handling is verified only for the explicit
EPSG table in `rust/geodatafusion/src/udf/native/crs.rs`
(4326/4258/4267/4269/4490/4979 as latitude-first, 4978 geocentric, 3857
projected). Unknown SRIDs fail with a clear error; no code range is guessed.

The oracle compares only SQL shapes that both engines provide. The
`ST_MakeLine(geometry[])` and `ST_MakePolygon(shell, holes[])` overloads are
exercised with raw Arrow List inputs in
`rust/geodatafusion/tests/constructors.rs`, because DataFusion SQL array
literals require the consuming build to enable the `nested_expressions`
feature.

## Runtime Error Contracts

A case may declare `runtime_error` with an exact `major.minor` `geos_before`
threshold and a `contains` substring. When the linked GEOS runtime is older
than that threshold, executing the case must fail with an error containing
the substring; the case still counts as executed and compared. At or above
the threshold the reviewed PostGIS baseline is compared normally. The
runtime version comes from the linked GEOS library through `geos::version()`,
not from a Cargo feature name, so the contract follows the engine that
actually executes the query. `relationships/relate_boundary` uses it for
boundary node rules 3 and 4.

## Arrow Input Nullability

An argument may declare `nullable` (`true` by default). When any argument in a
case declares it explicitly, the DataFusion side registers a one-row table
named `oracle_input`; argument N is column `argN` with the argument's Arrow
type and declared nullability. The `datafusion.sql` for such a case references
`arg1`, `arg2`, and so on instead of `$1`, `$2`; PostgreSQL still receives the
values as positional parameters. A `nullable = false` argument with a NULL
value is rejected when the group loads.

Supported scalar argument types are `text` (`Utf8`), `float64`, `int32`,
`int64`, and `boolean`; the declared Arrow width is preserved for both the
PostgreSQL parameter and the DataFusion input field.

A `text` argument may declare `storage`. The default is `text`, which keeps
the historical `Utf8` field and calls such as `ST_GeomFromText(arg1)`.
`storage: "wkb"` encodes the same WKT value as standard little-endian WKB
with the independent `wkb` writer and registers `argN` as a typed `WkbType`
field; PostgreSQL still receives the original text value. Use it for
consumers that need real WKB storage, including nested `GeometryCollection`
structures the native GeoArrow union cannot preserve, and `POINT EMPTY`.
When any case in a group uses real Arrow input fields (`nullable` or
`storage`), every case in that group runs over `oracle_input`, and the group
SQL must reference `argN`; cases without a declaration receive a regular
scalar field of the argument's declared type.

This mode exists because scalar placeholders do not expose the nullability of
real Arrow input fields. The `nullability/input_fields` group uses it to guard
coordinate getters over non-nullable geometry inputs when an ordinate is
absent or an additional parameter is NULL, and the
`linear_referencing/nullability` group guards NULL propagation through linear
referencing expressions.

## Public SQL NULL Smoke

The `nullability/public_smoke` group exercises real SQL over `oracle_input`
columns instead of constant-folded NULL literals. It documents these arities:

- accessors: `ST_X(geometry)`, `ST_Y(geometry)`, `ST_Z(geometry)`,
  `ST_M(geometry)`, `ST_StartPoint(geometry)`, `ST_IsEmpty(geometry)`,
  `ST_GeometryType(geometry)`
- collection editors: `ST_ForceCollection(geometry)`,
  `ST_CollectionHomogenize(geometry)`, `ST_Multi(geometry)`,
  `ST_CollectionExtract(geometry)`, `ST_CollectionExtract(geometry, type)`,
  `ST_Force3DM(geometry[, m])`, `ST_Force4D(geometry[, z, m])`
- relationships: `ST_Contains(g1, g2)`, `ST_Intersects(g1, g2)`,
  `ST_Relate(g1, g2)`, `ST_ContainsProperly(g1, g2)`
- GEOS processing: `ST_Buffer(g, distance)`, `ST_BuildArea(g)`,
  `ST_LineMerge(g)`, `ST_OffsetCurve(g, distance, flags)`,
  `ST_ReducePrecision(g, precision)`, `ST_SharedPaths(g1, g2)`,
  `ST_TriangulatePolygon(g)`
- extents: `ST_XMin(ST_Extent(g))`, `ST_ZMax(ST_3DExtent(g))`

The result columns distinguish SQL NULL from non-null EMPTY and collection
results with `IS NULL` and `ST_IsEmpty` where the engines contractually
differ. Cases cover XY, XYZ, XYZM, empty, NULL, and NULL numeric-argument
shapes.

The `extent_3d/` group compares the six Box3D endpoints produced by
`ST_3DExtent`; PostGIS evaluates `ST_XMin`/`ST_ZMax` on the real box, while the
DataFusion query reads the aggregate's struct fields directly. The
`bounding_diagonal/` and `bounding_diagonal_fits/` groups compare the returned
LineString as EWKB, covering explicit `fits`, EMPTY, and XYZ/XYM/XYZM inputs.

The `io/ewkt_text`, `io/ewkt_precision`, `io/ewkb_text`, `io/ewkb_bytes`,
`io/ewkt_ewkb_multi_row`, and `io/native_metadata` groups cover
`ST_GeomFromEWKT`, `ST_GeomFromEWKB`, `ST_AsEWKT`, `ST_AsEWKB`, and
`ST_AsHEXEWKB` across XY/XYZ/XYM/XYZM, SRID 0/4326/negative/folded values,
precision overloads, EMPTY, single-member geometry collections,
multi-geometries, multi-row inputs, and NULL. Binary comparisons decode EWKB
and compare geometry identity, dimension, and SRID; `io/native_metadata`
returns the native GeoArrow geometry array and verifies its field metadata by
re-encoding it for comparison.

The `collection/` groups cover `ST_CollectionExtract`,
`ST_CollectionHomogenize`, `ST_ForceCollection`, `ST_Multi`,
`ST_ForcePolygonCCW`, `ST_ForcePolygonCW`, `ST_ForceRHR`, and `ST_ForceSFS`
across atomic, multi, collection, empty, nested-multi, Z/M, type-selection,
orientation, and version shapes.

The `relationships/` groups cover exact serialized ordering, proper
containment, all three `ST_Relate` overloads, and DE-9IM string matching.
Text matrix results remain typed in `expected.json`.

The `native-processing/` groups cover `ST_ChaikinSmoothing`,
`ST_FilterByM`, `ST_GeneratePoints`, `ST_GeometricMedian`,
`ST_SetEffectiveArea`, and `ST_Letters`. Geometry-changing functions compare
decoded EWKB exactly. `ST_GeometricMedian` compares `ST_X`/`ST_Y`/`ST_Z`
within a declared float-ULP bound. `ST_GeneratePoints` compares type and
cardinality only: the implementation deliberately uses its own deterministic
seed replay instead of matching PostGIS's PRNG coordinates, and an
independent Rust test verifies polygon membership and seed replay.
`ST_Letters` compares type, EMPTY, dimensions, and has-points because the
built-in public-domain Hershey font is not PostGIS's embedded default font;
the custom-font groups compare exact 2D, 3D, and ZM EWKB glyph output. Default
font member order, positive advances, and glyphless NULL behavior are covered
by engine-side contracts; default glyph geometry, point counts, and advances
are intentionally not stored as PostGIS expectations.

The planar groups `planar_convex_hull`, `planar_point_on_surface`, and the
three `planar_simplify_*` groups apply `ST_Force2D` only to the PostgreSQL
input. These UDFs publicly implement native XY processing, while PostGIS
preserves Z in some raw outputs. `inputs.json` and the DataFusion SQL retain
the original XYZ/XYM/XYZM WKB, so the fixtures record both the original input
dimension and the declared projection/drop behavior without rewriting the
PostGIS reference semantics.

The `accessors/` groups cover the structural accessor SQL contracts.
`geometry_only` compares `ST_Dimension`, `ST_IsCollection`,
`ST_NumGeometries`, `ST_NRings`, `ST_Zmflag`, `ST_HasZ`, and `ST_HasM`;
`indexed` compares `ST_GeometryN`, `ST_PointN`, and `ST_InteriorRingN`
including out-of-range and negative indices; `rings` compares
`ST_ExteriorRing`; `dump_points`, `dump_segments`, and `dump_rings` compare
the `geometry_dump` record sets through `UNNEST` with typed `path` lists;
`points` compares `ST_Points`; and `summary`, `summary_srid` compare
`ST_Summary` text with and without SRID metadata.

The `clustering/` groups cover the four window functions and the two
aggregates across DBSCAN noise and border ordering, distance and intersection
clusters, weighted and 3D k-means, centroid output, mixed-dimension clusters,
EPSG:4326 CRS propagation, and nested `GeometryCollection` members. Window
results compare cluster-id partitions: each case declares the label columns
to canonicalize (`canonical_columns`, currently `cid`), every other column
including the input `id` is compared exactly, and negative labels stay exact
sentinels instead of joining the ordinary label space. The aggregates compare
the EWKB set of the `cluster.geom` values selected after `UNNEST`.

Two reviewed differences are excluded from the numeric oracle.
`ST_MemSize` returns GSERIALIZED `VARSIZE` bytes in PostGIS but the Arrow
buffer bytes referencing one row in GeoDataFusion (list offset entries,
dense-union type id and offset slots, and payload bytes; validity bitmaps,
shared metadata, and allocator padding are excluded, while external byte-view
payloads count only the range referenced by the row).
Those values are asserted in `rust/geodatafusion/tests/memsize.rs` and
`rust/geodatafusion/tests/slt/structural_accessors.slt` instead. PostGIS also
sets the cached-bounding-box `B` flag in `ST_Summary`; the `summary` SQL
normalizes that flag away because GeoArrow has no cached bounding box.

The `accessors/summary` and `geos/line_merge_wkb` groups declare
`storage: "wkb"` for all geometry arguments, so they run over the typed WKB
`oracle_input` field. Their nested `GeometryCollection` cases keep the
original PostGIS baselines: `ST_Summary` reaches the nesting intact, and
`ST_LineMerge` consumes it as legal linework. Native WKT construction rejects
nested collections because the native GeoArrow union cannot represent them;
typed WKB consumers are not globally restricted.

The `coverage/` groups cover the `ST_CoverageUnion` aggregate and the
`ST_CoverageInvalidEdges` and `ST_CoverageSimplify` window functions. Their
`geometry-normalized` baselines compare polygon rings up to rotation and
orientation, and line strings as unordered segment sets, while still checking
dimension and SRID. The cases cover adjacent, overlapping, gapped, holed,
NULL, EMPTY, Z, M, partition, ordering, tolerance, and simplify-boundary
shapes.

## Ordinary Tests

These tests read the reviewed baseline only. They require no Docker, network,
or PostgreSQL client:

```bash
cargo test -p geodatafusion --test postgis
```

MVT groups use `mvt-semantic`: both reviewed PostGIS bytes and DataFusion
bytes are parsed by the independent test decoder, then layers, feature ids,
typed properties, geometry command paths, and coordinates are compared. This
does not require byte-identical protobuf encoder output. `mvt/asmvtgeom` and
`mvt/asmvtgeom_geometry_bounds` cover clipping, buffers, dimensions, EMPTY,
NULL, and box2d versus geometry bounds. `mvt/asmvt`,
`mvt/asmvt_properties`, and `mvt/asmvt_defaults` cover mixed geometry types,
feature-id boundaries, typed/null properties, NULL option defaults, and
empty or NULL geometry rows.

The `bounding_box/` groups cover Box2D and Box3D extrema, ST_MakeBox2D and
ST_3DMakeBox corner normalization, ST_Envelope shapes, ST_Expand geometry and
box overloads, and the six ST_*Min/ST_*Max accessors across 2D, 3D, M, ZM,
EMPTY, NULL, and negative-distance inputs. Box inputs use the PostGIS box
overloads; a full box3d with different minimum and maximum Z values is compared
through extent accessors because serializing it as a geometry remains
unsupported.

Missing baselines, hash mismatches, duplicate case ids, zero compared cases,
and all-skipped inputs fail the test. A case that returns no rows is still a
valid comparison when both the reviewed baseline and the current result have
the same empty shape; `GEOMETRYCOLLECTION EMPTY` covers that path for
`ST_Dump`.

## Live Tests

Start the pinned reference environment:

```bash
docker compose -f testing/postgis/compose.yml up -d --wait
```

Then run the ignored live test with an explicit DSN:

```bash
GEODATAFUSION_POSTGIS_URL=postgresql://geodatafusion:geodatafusion@127.0.0.1:55432/geodatafusion \
GEODATAFUSION_POSTGIS_IMAGE=postgis/postgis:18-3.6@sha256:4a43e3c3e5d55b9208240fc71db543b79c638766280c625a425bd0a2390989c4 \
cargo test -p geodatafusion --test postgis -- --ignored
```

When the DSN is missing, the ignored test fails explicitly. It does not
silently treat a missing reference instance as a pass. The test checks both:

- live PostGIS results against the reviewed baseline, catching reference
  version or result drift;
- live DataFusion results against live PostGIS, catching implementation
  changes.

## Capturing A Candidate

Candidate output is written only when an explicit external directory is
provided. The tool refuses to write inside the repository and refuses to
overwrite an existing candidate:

```bash
GEODATAFUSION_POSTGIS_URL=postgresql://geodatafusion:geodatafusion@127.0.0.1:55432/geodatafusion \
GEODATAFUSION_ORACLE_WRITE=/tmp/geodatafusion-oracle-candidates \
cargo test -p geodatafusion --test postgis -- --ignored
```

Review each candidate against the PostGIS source, including all version
fields and hashes. Copy an approved candidate to the matching group as
`expected.json`, then rerun the ordinary and live tests. Candidate differences
are never copied into the baseline automatically.
