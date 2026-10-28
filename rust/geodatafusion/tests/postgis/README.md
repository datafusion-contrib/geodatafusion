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

## Comparison

- `exact` compares Boolean, integer, text, null, and byte values without
  tolerance. The current first slice uses it for `ST_DWithin` Booleans.
- `float-ulp` compares IEEE-754 bits. Equal bits pass with `max_ulps = 0`;
  other finite values pass only within the declared ULP distance. NaN and
  infinity require identical bits.
- `geometry-exact` decodes EWKB and compares geometry type, coordinate
  sequence, dimension, and SRID. It does not call a GeoDataFusion UDF as its
  comparator.

`ST_Dump` uses separate SQL shapes: PostgreSQL returns `geometry_dump` rows,
while GeoDataFusion returns a list that is expanded with `UNNEST`. Both sides
are normalized to typed `path` and EWKB values before comparison.

## Ordinary Tests

These tests read the reviewed baseline only. They require no Docker, network,
or PostgreSQL client:

```bash
cargo test -p geodatafusion --test postgis
```

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
