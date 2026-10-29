# Development Guide

This guide explains how to contribute to `geodatafusion`, a spatial extension for Apache DataFusion.

## Project Structure

This is a Rust workspace with multiple crates:

- `rust/geodatafusion` - Core library with spatial User-Defined Functions (UDFs)
    - Internally, each _provider_ of functions is organized in submodules:
        - `native/` - Operations that are natively implemented, without the use of other dependencies like `geo`
        - `geo/` - Operations implemented using the `geo` crate
        - `geos/` - Operations implemented using the `geos` crate (bindings to the native GEOS library), gated behind the optional `geos` feature
        - `geohash/` - GeoHash encoding/decoding, using the `geohash` crate
- `rust/geodatafusion-flatgeobuf` - FlatGeobuf format support
- `rust/geodatafusion-geoparquet` - GeoParquet format support
- `rust/geodatafusion-geojson` - GeoJSON format support
- `python/` - Python bindings (separate workspace)

## Prerequisites

### Rust Development

- Rust. The minimum supported Rust version (MSRV) is defined by `rust-version` in `Cargo.toml`. You can update Rust using [rustup](https://rustup.rs/): `rustup update stable`.

## Getting Started

### Clone the Repository

```bash
git clone https://github.com/datafusion-contrib/geodatafusion.git
cd geodatafusion
```

### Build the Project

```bash
# Build all Rust crates
cargo build

# Build with all features
cargo build --all-features
```

## Development Workflow

### Running Tests

```bash
# Check the default production library configuration without dev dependencies
cargo check -p geodatafusion --lib --locked

# Run all Rust tests
cargo test --all-features

# Run tests for a specific crate
cargo test -p geodatafusion

# Run SQL contract tests through the public registration entry point
cargo test -p geodatafusion --all-features --test sqllogictests

# Run a subset of SQL contract files by relative path
cargo test -p geodatafusion --all-features --test sqllogictests -- dwithin

# Check README claims, the manifest, and the public registry
cargo test -p geodatafusion --all-features --test registration_coverage
```

### Property and Fuzz Testing

Property targets are split by responsibility: `property_tests` covers geometry
measurement and predicate invariants, `codec_properties` covers affine and
native WKT/WKB round trips, and `linear_properties` covers the linear-geometry
properties. They run as ordinary tests with a fixed default seed. Increase the
generated case count for every target when changing shared geometry or
metadata paths:

```bash
cargo test -p geodatafusion --test property_tests --test codec_properties --test linear_properties
GDF_PROP_CASES=512 cargo test -p geodatafusion --test property_tests --test codec_properties --test linear_properties
PROPTEST_RNG_SEED=1234 cargo test -p geodatafusion --test property_tests --test codec_properties --test linear_properties
```

Proptest saves failing seeds per target in
`rust/geodatafusion/tests/property_tests.proptest-regressions`,
`rust/geodatafusion/tests/codec_properties.proptest-regressions`, and
`rust/geodatafusion/tests/linear_properties.proptest-regressions`. Keep useful
regression entries in source control; they replay before new cases are
generated.

The parser fuzz harness is an isolated workspace with its own lockfile. It invokes the public WKT and WKB UDFs through the same Arrow field and `ScalarUDF` entry points used by execution:

```bash
cargo install cargo-fuzz --version 0.13.2 --locked
cargo check --manifest-path fuzz/Cargo.toml --locked
cargo +nightly fuzz build --fuzz-dir fuzz

mkdir -p /tmp/geodatafusion-fuzz/parse_wkt /tmp/geodatafusion-fuzz/parse_wkb
cp fuzz/seeds/parse_wkt/* /tmp/geodatafusion-fuzz/parse_wkt/
cp fuzz/seeds/parse_wkb/* /tmp/geodatafusion-fuzz/parse_wkb/
cargo +nightly fuzz run --fuzz-dir fuzz parse_wkt /tmp/geodatafusion-fuzz/parse_wkt -- -runs=2000
cargo +nightly fuzz run --fuzz-dir fuzz parse_wkb /tmp/geodatafusion-fuzz/parse_wkb -- -runs=2000
```

Use a target directory and corpus outside the repository for longer local runs. `fuzz/corpus/` and `fuzz/artifacts/` are ignored; only small reviewed seeds under `fuzz/seeds/` and minimized parser regressions worth preserving should be committed.

The scheduled `Generative Checks` workflow runs 2048 property cases per property test and 10,000 fuzz executions per parser. Those budgets provide repeatable regression pressure; a passing run does not establish parser correctness.

### SQL Contract Tests

SQL contracts live in `rust/geodatafusion/tests/slt` and run through the
`sqllogictests` integration test target. The runner discovers files
deterministically, creates a fresh `SessionContext` for every file, and calls
the public `geodatafusion::register()` entry point. A run fails when no files
match the filter or a selected file executes no SQL. A query that returns zero
rows still counts as executed; a file whose records are all conditionally
skipped does not.

Use `.slt` files for stable formatted scalar, aggregate, list, and error
contracts. Keep extension metadata, coordinate layout, and CRS checks in
`tests/geoarrow_contracts.rs`, because SQLLogicTest compares text values.

### Math Property Tests

`tests/property_tests.rs` exercises geometry measurement and predicate
invariants through the public `geodatafusion::register()` entry point. The
tests use real Arrow arrays, multiple rows and slices, and both coordinate
layouts. The generated case count defaults to 32; increase it when changing
the shared geometry execution path:

```bash
cargo test -p geodatafusion --test property_tests --test codec_properties --test linear_properties
GDF_PROP_CASES=512 cargo test -p geodatafusion --test property_tests --test codec_properties --test linear_properties
PROPTEST_RNG_SEED=1234 cargo test -p geodatafusion --test property_tests --test codec_properties --test linear_properties
```

Proptest saves minimized failures per target in
`rust/geodatafusion/tests/property_tests.proptest-regressions`,
`rust/geodatafusion/tests/codec_properties.proptest-regressions`, and
`rust/geodatafusion/tests/linear_properties.proptest-regressions`. Keep useful
regression entries in source control.

`tests/linear_properties.rs` is the dedicated target for linear referencing
invariants: interpolation endpoints, monotonic locate/substring identities,
and sliced multi-row equivalence. It uses the same proptest conventions but
keeps its own regression file so independent property families can evolve
separately:

```bash
cargo test -p geodatafusion --test linear_properties
GDF_PROP_CASES=512 cargo test -p geodatafusion --test linear_properties
PROPTEST_RNG_SEED=1234 cargo test -p geodatafusion --test linear_properties
```

Proptest saves minimized failures in
`rust/geodatafusion/tests/linear_properties.proptest-regressions`. Keep useful
regression entries in source control. Run `cargo test -p geodatafusion` to
execute every property target, including this one and the shared
measurement/predicate target; add a dedicated target rather than extending
another family's property file.

Files under a `geos_3_11` or `geos_3_12` directory are feature-gated. The
default runner prints an explicit skip for them; run with `--all-features` to
execute their contracts. Do not use a default-feature pass to claim GEOS
coverage.

### Function Coverage Manifest

`geodatafusion.toml` records every function listed in the README, including its
category, execution kind, feature requirement, state, and available evidence.
The `registration_coverage` test checks the manifest against the README and
against the difference between the registry before and after
`geodatafusion::register()`, so builtins are not counted as spatial UDFs.

- `planned` means the function is not exposed through the public registry.
- `partial` means it is registered or claimed but still has a recorded gap or
  known defect.
- `verified` means it is registered with its declared feature requirement
  enabled, and the evidence listed in the manifest covers its current
  acceptance contract.

Registration alone never makes a function `verified`. A README checkmark must
also be backed by registration and the manifest state; the audit fails when the
README and public registry disagree.

### Code Formatting

We use `rustfmt`:

```bash
cargo +nightly-2025-05-14 fmt -- --unstable-features \
  --config imports_granularity=Module,group_imports=StdExternalCrate
```

We use the nightly compiler for formatting because import ordering is an unstable feature.

### Linting

Run clippy on all crates

```bash
cargo clippy --all-features --tests -- -D warnings
```

### Documentation

Build and view documentation

```bash
cargo doc --all-features --open
```

## Contributing

### Adding New Functions

We follow the [PostGIS API](https://postgis.net/docs/reference.html) as closely as possible. When implementing a new function:

1. **Check the README** - See if the function is listed in the function table
2. **Find similar implementations** - Look at existing functions in the same category
3. **Implement the function** - Follow existing patterns in `rust/geodatafusion/src/udf/`
4. **Add tests** - Add the SQL contract, raw Arrow boundary coverage, and unit tests
   that the function actually needs
5. **Update the manifest** - Record the function, feature requirement, state, and
   evidence in `geodatafusion.toml`
6. **Update documentation** - Add doc comments and only then update the README
   checkboxes

#### Function Categories

Functions are organized by category in `rust/geodatafusion/src/udf/`:

- `native/constructors/` - Geometry constructors (ST_MakePoint, etc.)
- `native/accessors/` - Geometry accessors (ST_X, ST_Y, etc.)
- `native/io/` - Input/output (WKT, WKB)
- `native/bounding_box/` - Bounding box functions
- `geo/measurement/` - Measurement functions (ST_Area, ST_Distance)
- `geo/processing/` - Processing functions (ST_Buffer, ST_Simplify)
- `geo/relationships/` - Spatial relationships (ST_Intersects, etc.)
- `geo/validation/` - Validation functions (ST_IsValid)
- `geos/processing/` - GEOS-backed processing functions (ST_LineMerge)
- `geohash/` - GeoHash functions

### GEOS-backed functions

Functions in the `geos/` provider link the [GEOS](https://libgeos.org/) library, and can be controlled via Cargo features.

The version of GEOS required varies by UDF,
so we expose cargo features with explicit minimum version numbers,
like `geos-3_11` (GEOS >=3.11) and `geos-3_12` (GEOS >=3.12).

#### GEOS linking

This crate will try to link the system-provided GEOS library by default.
If you want to statically link GEOS from a source build,
add a direct dependency on the `geos` crate to your project,
and enable the `static` feature like so:

```toml
geos = { version = "11.1.1", features = ["static"] }
```

If you're using cargo workspaces, make sure that
the crates using geodatafusion directly reference `geos`.

### Code Style

- Use meaningful variable and function names
- Add doc comments for public APIs
- Follow Rust naming conventions (snake_case for functions, PascalCase for types)
- Keep functions focused and single-purpose
- Prefer explicit error handling over panics

### Testing Guidelines

- Test both valid and invalid inputs
- Test edge cases (empty geometries, null values, etc.)
- Use descriptive test names
- Add SQL integration tests when appropriate
- Test against PostGIS behavior when possible

Example test structure:

```rust
#[test]
fn test_st_area_polygon() {
    // Test case description
    let input = /* ... */;
    let expected = /* ... */;
    let result = st_area(input);
    assert_eq!(result, expected);
}
```

## Continuous Integration

Our CI pipeline runs on every pull request and includes:

1. **Formatting** - Checks code formatting with `rustfmt`
2. **Linting** - Runs `clippy` with all features
3. **Tests** - Runs test suite with all features
4. **Documentation** - Ensures docs build without warnings
5. **Python CI** - Tests Python bindings
6. **Conventional Commits** - Validates commit message format

Make sure all checks pass before requesting review.

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `refactor`: Code refactoring
- `chore`: Maintenance tasks
- `ci`: CI/CD changes

Examples:
```
feat(geodatafusion): Add ST_Buffer implementation
fix(geodatafusion-flatgeobuf): Handle multipoint parsing edge case
docs: Update README with ST_Area examples
```

## Getting Help

- **Issues**: Open an issue on [GitHub](https://github.com/datafusion-contrib/geodatafusion/issues)
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check the [README](README.md) and [PostGIS docs](https://postgis.net/docs/)

## Additional Resources

- [Apache DataFusion](https://datafusion.apache.org/)
- [PostGIS Reference](https://postgis.net/docs/reference.html)
- [GeoArrow Specification](https://geoarrow.org/)
- [GeoRust ecosystem](https://github.com/georust)

## License

This project is dual-licensed under MIT OR Apache-2.0. By contributing, you agree to license your contributions under the same terms.
