# Development Guide

This guide explains how to contribute to `geodatafusion`, a spatial extension for Apache DataFusion.

## Project Structure

This is a Rust workspace with multiple crates:

- `rust/geodatafusion` - Core library with spatial User-Defined Functions (UDFs)
    - Internally, each _provider_ of functions is organized in submodules:
        - `native/` - Operations that are natively implemented, without the use of other dependencies like `geo`
        - `geo/` - Operations implemented using the `geo` crate
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
# Run all Rust tests
cargo test --all-features

# Run tests for a specific crate
cargo test -p geodatafusion
```

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
4. **Add tests** - Include unit tests and integration tests
5. **Update documentation** - Add doc comments and update the README checkboxes

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
- `geohash/` - GeoHash functions

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
