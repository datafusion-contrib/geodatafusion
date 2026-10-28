# PostGIS Reference Environment

The differential oracle uses a fixed PostGIS image so geometry behavior and
typed numeric results have an identified reference:

```
postgis/postgis:18-3.6@sha256:4a43e3c3e5d55b9208240fc71db543b79c638766280c625a425bd0a2390989c4
```

The image currently publishes `linux/amd64`. Docker Desktop and OrbStack can
run it on Apple Silicon through emulation; GitHub-hosted `ubuntu-latest`
runners run it natively.

## Start And Stop

```bash
docker compose -f testing/postgis/compose.yml up -d --wait
docker compose -f testing/postgis/compose.yml down -v
```

The service listens on `127.0.0.1:55432` and uses:

```
postgresql://geodatafusion:geodatafusion@127.0.0.1:55432/geodatafusion
```

## Recorded Reference

The reviewed baseline was captured from this image with:

- PostgreSQL `18.6 (Debian 18.6-1.pgdg13+2)`;
- PostGIS `3.6.4 94d984b`;
- GEOS `3.14.1-CAPI-1.20.5`.

The test reads these values from the live connection and compares them with
`expected.json`, so a different database or extension version fails even when
the image tag accidentally resolves to new content.

## Updating The Reference

Pin a new digest in this compose file, in the GitHub Actions service, in
`tests/support/oracle.rs`, and in the test README. Start the new container,
capture candidates to an external directory, review the resulting version and
case differences, and only then replace the group baselines.

Do not use `latest`, an unpinned tag, or a digest copied from a manifest that
has not actually run the differential cases.
