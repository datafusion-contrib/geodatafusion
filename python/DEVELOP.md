# Python Development Guide

This guide covers Python-specific development for the `geodatafusion` Python package, which provides Python bindings for the Rust `geodatafusion` library using PyO3.

This guide is **in addition** to the top-level [DEVELOP.md](../DEVELOP.md) that covers Rust development. You should read that first and all Rust-related instructions also apply here.

## Overview

The Python package is a separate workspace that wraps the Rust library using:

- **PyO3** - Rust bindings for Python (automatically installed via Cargo)
- **Maturin** - Build system for PyO3 packages (automatically installed in the dev environment by uv)
- **uv** - Python package and virtual environment manager

## Prerequisites

**uv** is the recommended package manager ([install instructions](https://docs.astral.sh/uv/)).

## Project Structure

```
python/
├── Cargo.toml              # Rust package configuration
├── pyproject.toml          # Python package configuration
├── src/                    # Rust source (PyO3 bindings)
│   ├── lib.rs             # Main module entry point
│   ├── udf/               # UDF registration modules
│   └── utils.rs           # Helper utilities
├── python/                 # Pure Python source
│   └── geodatafusion/
│       └── __init__.py    # Python API
├── tests/                  # Python tests
│   └── udf/               # UDF tests
└── examples/               # Example scripts
```

## Getting Started

### Clone and Setup

```bash
# From the repository root
cd python

# Create virtual environment and install dependencies
uv sync --no-install-package geodatafusion
```

The `--no-install-package geodatafusion` avoids building `geodatafusion` itself (in release mode) during setup. **Maturin** is automatically installed in the dev environment by uv.

### Build the Package

There are two ways to build the package:

#### Development Build (Fast, Debug Mode)

```bash
# Build and install in development mode
uv run --no-project maturin develop --uv
```

**Note**: Debug builds will show a performance warning at runtime. This is expected during development.

#### Release Build (Optimized)

```bash
# Build optimized release version
uv run --no-project maturin develop --uv --release

# Or build wheel for distribution
uv run --no-project maturin build --uv --release
```

## Development Workflow

### Running Tests

```bash
# Run all tests
uv run --no-project pytest

# Run specific test file
uv run --no-project pytest tests/test_register.py

# Run with verbose output
uv run --no-project pytest -v

# Run with output capture disabled (see print statements)
uv run --no-project pytest -s
```

## Adding a new UDF

When a new UDF is added to the Rust library, you need to expose it in Python:

### 0. Implement the UDF in Rust

Follow the instructions in the top-level [DEVELOP.md](../DEVELOP.md) to implement the UDF in Rust first.

### 1. Update Rust Bindings

The UDF modules are in `src/udf/`. Each module corresponds to a category:

- `src/udf/native/` - Native implementations
- `src/udf/geo/` - Geo trait implementations
- `src/udf/geohash/` - GeoHash functions

Wrap the UDF, using one of our existing macros, if possible.

- `impl_udf!`: for UDFs without any instantiation arguments
- `impl_udf_coord_type_arg!`: for UDFs taking a `CoordType` argument upon instantiation

### 2. Add to Python Module

For example, in `src/udf/geo/mod.rs`, register the new function:

```rs
#[pymodule]
pub(crate) fn geo(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<NewUdf>()?;
```

### 3. Update Python API

The Python API is in `python/geodatafusion/__init__.py`. Update `register_all()` or add specific registration functions if needed.

This ensures that the UDF is easily injected onto a DataFusion `SessionContext`.

### 4. Add Tests

Create tests in `tests/udf/` following the existing structure:

```python
from datafusion import SessionContext
from geodatafusion import register_all


def test_my_new_function():
    ctx = SessionContext()
    register_all(ctx)

    sql = "SELECT my_new_function(ST_GeomFromText('POINT(1 2)'));"
    result = ctx.sql(sql)
    assert result.to_arrow_table().columns[0][0].as_py() == expected_value
```

## Package Building

Push a new git tag to GitHub, and the CI workflow will build and publish wheels automatically.
