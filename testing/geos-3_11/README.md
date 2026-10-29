# GEOS minimum-runtime check

The `geos-3_11` and `geos-3_12` features declare minimum GEOS releases.
`tests/geos_runtime.rs` fails when the runtime loaded by the test process is
older than the highest enabled feature. When
`GEODATAFUSION_EXPECT_GEOS_VERSION` is set, the test also fails unless the
loaded release matches exactly, so a minimum-version job cannot pass by
falling back to a newer system GEOS.

## Prepare a prefix

Point `GEOS_TEST_PREFIX` at a GEOS installation prefix. To build a private
3.11 prefix from the official source:

```bash
: "${GEOS_TEST_PREFIX:?set GEOS_TEST_PREFIX to the install prefix}"
BUILD_ROOT="${GEOS_TEST_BUILD_ROOT:-$(mktemp -d)}"
ARCHIVE="$BUILD_ROOT/geos-3.11.5.tar.bz2"
SHA256=7d9432f94e2c743cca13977b1f9943dfcc9a83854578870a215fb8dd7d6f21c1

curl -fL -o "$ARCHIVE" \
  https://download.osgeo.org/geos/geos-3.11.5.tar.bz2
python3 -c '
import hashlib, sys
digest = hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest()
assert digest == sys.argv[2], digest
' "$ARCHIVE" "$SHA256"
tar -xjf "$ARCHIVE" -C "$BUILD_ROOT"

cmake -S "$BUILD_ROOT/geos-3.11.5" -B "$BUILD_ROOT/build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$GEOS_TEST_PREFIX" \
  -DBUILD_TESTING=OFF \
  -DBUILD_BENCHMARKS=OFF
cmake --build "$BUILD_ROOT/build" --target install -j4
```

## Run the check

`geos-sys` uses `GEOS_LIB_DIR` and `GEOS_VERSION` when they are set, so the
system `geos-config` is not consulted:

```bash
export GEOS_LIB_DIR="$GEOS_TEST_PREFIX/lib"
export GEOS_VERSION=3.11.5
export GEODATAFUSION_EXPECT_GEOS_VERSION=3.11.5

BIN=$(cargo test -p geodatafusion --features geos-3_11 --locked \
  --test geos_runtime --no-run --message-format=json 2>/dev/null |
  python3 -c '
import json, sys
for line in sys.stdin:
    message = json.loads(line)
    if message.get("reason") == "compiler-artifact" and message.get("executable"):
        print(message["executable"])
')

case "$(uname -s)" in
  Darwin) LOADER=DYLD_LIBRARY_PATH ;;
  *) LOADER=LD_LIBRARY_PATH ;;
esac
env "$LOADER=$GEOS_LIB_DIR" "$BIN"
```

The compiled binary is run directly because Cargo does not propagate
`DYLD_LIBRARY_PATH` to test processes on macOS. For a `geos-3_12` check, use
`--features geos-3_12`, a GEOS 3.12 prefix, and the matching
`GEODATAFUSION_EXPECT_GEOS_VERSION` value.

## CI lane

`.github/workflows/generative.yml` has a scheduled/manual `geos-minimum` job.
It builds and caches both pinned minimum releases:

- `geos-3_11`: GEOS 3.11.5, SHA-256
  `7d9432f94e2c743cca13977b1f9943dfcc9a83854578870a215fb8dd7d6f21c1`
- `geos-3_12`: GEOS 3.12.1, SHA-256
  `d6ea7e492224b51193e8244fe3ec17c4d44d0777f3c32ca4fb171140549a0d03`

Each matrix job runs the full matching feature suite. Every Cargo-emitted test
binary is executed directly with `LD_LIBRARY_PATH` pointing at the private
prefix, so a host-installed newer GEOS cannot satisfy the lane;
`tests/geos_runtime.rs` additionally requires the exact
`GEODATAFUSION_EXPECT_GEOS_VERSION`. The job does not run on pull requests,
and it does not skip failing files.

To reproduce the Linux lane locally:

```bash
export GEOS_LIB_DIR="$GEOS_TEST_PREFIX/lib"
export GEOS_VERSION=3.11.5
export GEODATAFUSION_EXPECT_GEOS_VERSION=3.11.5
FEATURE=geos-3_11
WORK="${GEOS_TEST_WORK_DIR:-$(mktemp -d)}"

cargo test -p geodatafusion --features "$FEATURE" --locked \
  --no-run --message-format=json-render-diagnostics \
  > "$WORK/artifacts.json"

jq -r 'select(
  .reason == "compiler-artifact"
  and .executable != null
  and .profile.test == true
) | .executable' "$WORK/artifacts.json" | sort -u > "$WORK/binaries.txt"

while IFS= read -r binary; do
  LD_LIBRARY_PATH="$GEOS_LIB_DIR" \
    GEODATAFUSION_EXPECT_GEOS_VERSION="$GEOS_VERSION" \
    "$binary"
done < "$WORK/binaries.txt"
```

For the 3.12 lane use `GEOS_VERSION=3.12.1`, the matching expected version,
and `FEATURE=geos-3_12`.
