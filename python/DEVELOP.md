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

`register_all()` exports the scalar and aggregate UDFs from the Rust
`geodatafusion::register()` registry through DataFusion FFI capsules. Category
registration functions use the corresponding Rust module registrars. Adding a
UDF to an existing Rust module therefore makes it available to Python without
maintaining another name list.

Add or update a Python wrapper class only when users need to construct the UDF
with non-default options, such as `coord_type`.

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

## Supported hosts and the compiler pin

The extension targets `datafusion-python` 55.x and is verified against the
official 55.0.0 wheels (built on DataFusion 55.2.0). It links the same
`datafusion-ffi` patch versions as that host: `datafusion*` 55.2.0, `tokio`
1.53.2, `stabby`/`stabby-abi` 72.1.16, `async-ffi` 0.5.1. `python/Cargo.lock`
records those versions; refresh it together with the host rather than on its
own.

Because `datafusion-ffi` moves `repr(Rust)` values across the capsule boundary
(see the layout note below), the extension must be compiled by the same rustc as
the host. `python/rust-toolchain.toml` pins 1.97.0, the value
datafusion-python 55.x pins for itself, so `cargo` run inside `python/` uses it.
Build entries that run from the repository root do not pick that file up —
rustup resolves `rust-toolchain.toml` from the working directory — so the wheel
workflow pins the toolchain explicitly (`rust-toolchain: "1.97.0"` on
`maturin-action`, `toolchain: "1.97.0"` on `dtolnay/rust-toolchain`) for its
`--manifest-path python/Cargo.toml` builds.

## FFI capability boundaries

The package hands each function kind over the DataFusion FFI capsule protocol.
The boundaries below were measured against the pinned `datafusion` 55.0.0 wheel
(built on DataFusion 55.2.0), on macOS arm64 and in a Linux aarch64 container.

| Function kind | Capsule name | Python side | State |
| --- | --- | --- | --- |
| scalar / aggregate | `datafusion_scalar_udf`, `datafusion_aggregate_udf` | `udf()` / `udaf()` take the capsule; register with `register_udf` / `register_udaf` | shipped |
| window | `datafusion_window_udf` | `udwf(capsule)` takes the capsule; register with `register_udwf` | exported by `_registration_exports` and registered by `register_all`; the Rust crate has no window UDFs yet |
| table | `datafusion_table_function` | only through an object exposing `__datafusion_table_function__(session)`; a raw capsule is treated as a Python callable | exported as a `TableFunctionExport` handle and registered by `register_all`; see the two table notes |

- Capsule names are part of the protocol: the host validates the name when it
  reads the capsule, and a mismatched name fails the import with a
  version-mismatch error.
- Window capsules carry the `FFI_WindowUDF` value itself. The host reads it
  through `PyCapsule_GetPointer`, owns a clone of it, and drops that clone
  through the vtable `release` function the exporting library supplied. The
  exporting library keeps the `Arc<WindowUDF>` the FFI value was built from.
- A table function capsule additionally carries the host's logical extension
  codec, taken from `session.__datafusion_logical_extension_codec__()` during
  the capsule getter. Building one from a private `SessionContext` is not the
  supported shape, and neither is passing a raw capsule: the host calls the
  getter to obtain it. `_registration_exports` therefore returns a
  `TableFunctionExport` handle per registered name, and `register_all` hands it
  to `TableFunction(name, handle, ctx)`; the handle builds the capsule against
  the registering session's codec.
- Table functions are the only kind that can read the calling session:
  `TableFunctionArgs::session()` hands over the caller's session, whose
  `catalog_list()` enumerates the catalogs, schemas, and tables visible to the
  caller. That is the hook `ST_FromFlatGeobufToTable`-style functions need.
- **`Option<Handle>` layout note**: `FFI_TableFunction`'s and
  `FFI_TableProvider`'s private data embed `Option<tokio::runtime::Handle>`, a
  `repr(Rust)` value with no FFI contract, and the host reads them back through
  `runtime()`. Two things must match the host's build:
  - The read is the *host's* compilation of `FFI_TableFunction::runtime`, called
    by the host's `ForeignTableFunction::call_with_args` before it invokes the
    exporter's `call_with_args` function pointer, and `FFI_SessionRef::new`
    stores its result in the session it hands over. The exporter's own copy of
    that accessor only reads the exporter's box.
  - The tokio feature set decides the size of `Option<Handle>`: without
    `rt-multi-thread` the extension compiles it as 8 bytes, while
    datafusion-python 55.0.0 (tokio 1.53.2 with `macros`, `rt`,
    `rt-multi-thread` and `sync`) uses 16 bytes. `tokio` in
    `python/Cargo.toml` declares the consumer's feature set on purpose.
  - The rustc version decides which value encodes `None` inside that 16-byte
    layout: rustc 1.97 writes `0xff..ff`, rustc 1.98 and 1.99 write `2`
    (`std::mem::transmute_copy` on `Option<Handle>`, measured with tokio 1.53.2
    and every feature set above). A 1.99-built extension therefore hands the
    host a `None` that the host reads as `Some(<uninitialized pointer>)`, and
    the first table-function call dies in the host's `Arc` drop with exit 139
    (SIGSEGV) before the exported function runs. `python/rust-toolchain.toml`
    pins 1.97.0 to match datafusion-python 55.x;
    `test_table_function_typed_literals_cross_the_ffi` fails, by killing the
    interpreter, whenever that pin is not honored.
- **literal metadata note**: table-function arguments cross the FFI as
  protobuf-serialized expressions, and `datafusion-proto` drops
  `Expr::Literal`'s field metadata in both directions
  (`to_proto.rs`: `Expr::Literal(value, _)`; `from_proto.rs`:
  `Expr::Literal(scalar_value, None)`), so a GeoArrow-typed bounds argument
  arrives with its Arrow shape only. The grid table functions rebuild a
  GeoArrow field from that shape (`untyped_geometry_field` in `grids.rs`) and
  only need it for a bounding box, so `ST_HexagonGrid(1,
  ST_MakeEnvelope(...))`, `ST_SquareGrid(1, ST_GeomFromText(...))` and
  `ST_HexagonGrid(1, NULL)` all run end to end through the host. The rebuilt
  field carries the default metadata: an argument's extension name and CRS do
  not survive the proto round trip, so a grid over a CRS-carrying bounds
  literal is planned CRS-less. `tests/test_register.py` pins the working
  behaviour, and `test_table_function_registers_into_caller_catalog` covers the
  caller-catalog path.
- Expression planners are not reachable from Python: datafusion-python 55.0.0
  exposes no API or capsule for `ExprPlanner`, and the DataFusion 55
  `ExprPlanner` hooks (`plan_aggregate`, `plan_window`) run after function-name
  resolution. A scalar UDF therefore shadows an aggregate with the same name,
  so PostGIS `ST_Union`-style scalar/aggregate overloads need distinct names or
  an upstream planner hook. `tests/test_planner_boundaries.py` pins this
  behaviour.
- An aggregate registered under a name that no scalar UDF uses can already be
  used in an `OVER` clause, so an aggregate-backed window need does not require
  a new FFI path.

### Registration parity with the Rust crate

Measured with `_registration_exports("all")`, which returns canonical names with
aliases nested under them:

| Build | scalar | aggregate | window | table |
| --- | --- | --- | --- | --- |
| Default (`pyo3/extension-module`, published wheels) | 192 | 8 | 4 | 6 |
| `--features geos-3_12` | 240 | 13 | 6 | 6 |

`python/Cargo.toml` forwards the Rust crate's `geos-3_11`/`geos-3_12` features,
so these rows are the Rust registry without and with GEOS. The GEOS build adds
48 scalars, five aggregates (`ST_Collect`, `ST_CoverageUnion`, `ST_MemUnion`,
`ST_Polygonize`, `ST_Union`), and two windows (`ST_CoverageInvalidEdges`,
`ST_CoverageSimplify`); they are exactly the functions those features gate, and
the default build carries the other 210 canonical registrations. The README's
265-row function table and the all-features registry are cross-checked by
`tests/registration_coverage.rs` (`manifest_readme_and_registry_agree`). The row
count and the registry count are not a one-to-one map: `_registration_exports`
leaves out the async scalar `ST_EstimatedExtent` (see "Async scalar UDFs and the
caller context" below), and the README lists several PostGIS aliases as rows of
their own.

GEOS-backed UDFs live in the `native` group: with the feature on, `register_all`
and `register_all_native` expose them, while `register_all_geo` still maps only
the `udf::geo` modules. All six table functions reach Python:
`ST_HexagonGrid`, `st_hexagongrid`, `ST_SquareGrid`, `st_squaregrid`,
`st_fromflatgeobuf`, and `st_fromflatgeobuftotable`.

Categories reuse the Rust entry points rather than repeating them:
`register_group("geo")` calls `geodatafusion::udf::geo::register()`, the same
function the crate-level `register()` calls, so `udf::geo` submodules such as
`three_d` reach both entries from one place. The same holds for `geohash` and
`native`, whose modules already own their `register` functions.

Window UDFs travel the same route: `_registration_exports` returns them next to
the scalar and aggregate exports, and `tests/test_register.py` asserts that
`register_all` and the category helpers cover every window UDF the Rust
registry offers instead of pinning names on the Python side. The path was
exercised end to end with a temporary window UDF in the Rust registry
(`register_all` + `SELECT st_temp_rank() OVER (ORDER BY x)`), then the
temporary UDF was removed. The run, the table-function crash reports, and the
Linux check are kept as task evidence, not as repository code.

### Async scalar UDFs and the caller context

DataFusion 55 has `AsyncScalarUDFImpl` (`datafusion-expr/src/async_udf.rs`).
`AsyncScalarUDF::into_scalar_udf()` registers it as an ordinary `ScalarUDF`,
and the physical planner detects it by downcasting `ScalarUDF::inner()` to
`AsyncScalarUDF` (`datafusion-physical-expr/src/async_scalar_function.rs`); the
synchronous entry point returns "async functions should not be called
directly".

`invoke_async_with_args` receives the same `ScalarFunctionArgs` as the
synchronous path: `args`, `arg_fields`, `number_rows`, `return_field`, and
`config_options`. That struct has no session and no catalog, so a scalar UDF,
async or not, can only use state captured when it was built; it cannot reach
the caller's catalog through its arguments.

That state does not survive the FFI capsule
(`FFI_ScalarUDF::from(Arc<ScalarUDF>)` forwards the synchronous
`invoke_with_args` callback), the async downcast in the planner cannot fire on
a `ForeignScalarUDF`, and datafusion-python 55.0.0 registers no async scalar
UDFs at all. `_registration_exports` therefore skips every scalar UDF where
`ScalarUDF::as_async()` returns `Some` — the same downcast the physical planner
uses — instead of keeping a name list. `ST_EstimatedExtent` is the concrete
instance: it is registered in the Rust registry with the registering
session's catalog, and `tests/test_register.py` asserts that it stays out of
the Python exports.

Keep the two contexts apart when documenting such a function: a catalog
captured by a Rust prototype belongs to whoever built that prototype and runs
the query; a temporary Rust `SessionContext` is not the Python caller's
catalog. The only entry that receives the caller's session is a table function
(`TableFunctionArgs::session()`); a pure-Python UDTF can request it with
`with_session=True`.

## Package Building

Push a new git tag to GitHub, and the CI workflow will build and publish wheels automatically.
