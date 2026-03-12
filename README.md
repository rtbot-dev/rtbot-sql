# rtbot-sql

SQL compiler for RTBot. Translates SQL queries into RTBot operator graphs (JSON programs) that run on RTBot's streaming runtime.

## Python package quick-start

Python wheels are published as GitHub Release assets for each supported platform and Python ABI.

1. Open the latest release and download the wheel that matches your OS/CPU and Python tag.
2. Install from the downloaded wheel file.

```bash
pip install ./rtbot_sql-<version>-cp313-cp313-<platform>.whl
```

Wheel selection guidance:
- Linux x86_64: `manylinux*_x86_64` with `cp312` or `cp313`
- Linux aarch64: `manylinux*_aarch64` with `cp312` or `cp313`
- macOS x86_64 (Intel): `macosx_*_x86_64` with `cp312` or `cp313`
- macOS arm64 (Apple Silicon): `macosx_*_arm64` with `cp312` or `cp313`
- Windows x86_64: `win_amd64` with `cp312` or `cp313`
- Windows arm64: `win_arm64` with `cp312` or `cp313`

Tiny smoke test:

```python
from rtbot_sql import RtBotSql

print("rtbot-sql import OK:", RtBotSql)
```

## Quick start

```bash
bazel build //apps/cli:rtbot-sql
bazel test //tests/...
```

Build artifacts go to `dist/` (not `bazel-bin/`), configured in `.bazelrc`.

## Project structure

```
libs/
  api/          Top-level compile_sql() function and types
  parser/       SQL parsing via libpg_query, AST conversion
  analyzer/     Scope and name resolution
  catalog/      Stream/view/table schema registry
  compiler/     Expression, function, SELECT, WHERE, GROUP BY compilation
  planner/      Tier classification and query planning

apps/
  cli/          Command-line SQL compiler

tests/
  unit/         Per-module unit tests
  integration/  End-to-end compiler integration tests

examples/
  catalog.json          Sample stream catalog
  queries/              Example SQL files
```

## CLI usage

The CLI compiles SQL into RTBot program JSON. It accepts SQL via positional argument, `--file`, or stdin pipe.

```bash
# Positional argument
dist/bin/apps/cli/rtbot-sql "CREATE TABLE orders (id DOUBLE PRECISION, price DOUBLE PRECISION)"

# From file with catalog
dist/bin/apps/cli/rtbot-sql --file examples/queries/bollinger.sql --catalog examples/catalog.json

# Pipe from stdin
echo "SELECT instrument_id, price FROM trades LIMIT 10" | dist/bin/apps/cli/rtbot-sql --catalog examples/catalog.json

# Compact output
dist/bin/apps/cli/rtbot-sql --file examples/queries/bollinger.sql --catalog examples/catalog.json --format compact

# Write to file
dist/bin/apps/cli/rtbot-sql --file examples/queries/bollinger.sql --catalog examples/catalog.json -o output.json
```

### Options

```
-f, --file <path>      Read SQL from file
-c, --catalog <path>   Catalog JSON with stream/view definitions
-o, --output <path>    Write output to file instead of stdout
    --format <fmt>     Output format: json (default) or compact
-v, --verbose          Show extra compilation details
    --repl             Start interactive REPL mode (not yet implemented)
-h, --help             Print help
```

### Catalog format

The catalog JSON defines available streams and their columns:

```json
{
  "streams": {
    "trades": ["instrument_id", "price", "quantity", "account_id"]
  }
}
```

## Testing

### Run all tests

```bash
bazel test //tests/...
```

### Run specific test suites

```bash
# Unit tests only
bazel test //tests/unit/...

# Integration tests only
bazel test //tests/integration/...

# Single test
bazel test //tests/unit:parser_test
```

### Debug a failing test

Use the `debug` config to stream output and disable caching:

```bash
bazel test --config=debug //tests/unit:expression_test
```

### Testing with the CLI

The CLI is useful for verifying compilation results interactively during development.

**Verify a CREATE STREAM statement:**

```bash
dist/bin/apps/cli/rtbot-sql "CREATE TABLE sensors (device_id DOUBLE PRECISION, temperature DOUBLE PRECISION, humidity DOUBLE PRECISION)"
```

**Verify a GROUP BY query produces correct operators:**

```bash
echo "SELECT instrument_id, SUM(quantity) AS total_qty, COUNT(*) AS cnt FROM trades GROUP BY instrument_id" \
  | dist/bin/apps/cli/rtbot-sql --catalog examples/catalog.json
```

Check the output for `KeyedPipeline`, `CumulativeSum`, `Count`, and `VectorCompose` in the program JSON.

**Verify expression de-duplication (Bollinger Bands):**

```bash
dist/bin/apps/cli/rtbot-sql --file examples/queries/bollinger.sql --catalog examples/catalog.json
```

In the output, `MOVING_AVERAGE(price, 20)` appears 3 times in the SQL but compiles to a single `MovingAverage` operator. Similarly, `STDDEV(price, 20)` appears twice but compiles to one `StandardDeviation` operator.

**Verify error reporting:**

```bash
# Unknown column
echo "SELECT foo FROM trades LIMIT 10" | dist/bin/apps/cli/rtbot-sql --catalog examples/catalog.json

# Unbounded stream query
echo "SELECT * FROM trades" | dist/bin/apps/cli/rtbot-sql --catalog examples/catalog.json

# Parse error
echo "SELEC FROM WHERE" | dist/bin/apps/cli/rtbot-sql
```

All error cases exit with code 1 and include an `errors` array in the output.

**Pipe into jq for inspection:**

```bash
dist/bin/apps/cli/rtbot-sql --file examples/queries/bollinger.sql --catalog examples/catalog.json \
  | jq '.program.operators[] | select(.type == "Prototype") | .operators[].type'
```

## Supported SQL

### Statements

| Statement | Status |
|-----------|--------|
| `CREATE TABLE` (stream) | Supported |
| `CREATE MATERIALIZED VIEW` | Supported |
| `INSERT INTO ... VALUES` | Supported |
| `SELECT ... FROM ... [WHERE] [GROUP BY] [HAVING] [LIMIT]` | Supported |
| `DROP MATERIALIZED VIEW` | Supported |
| `CREATE VIEW` | Stub (Phase 2) |
| `SUBSCRIBE` | Stub (Phase 2) |

### Functions

| Function | Type | Description |
|----------|------|-------------|
| `SUM(expr)` | Cumulative aggregate | Running sum |
| `COUNT(*)` | Cumulative aggregate | Running count |
| `AVG(expr)` | Cumulative aggregate | Running average |
| `MOVING_AVERAGE(expr, N)` | Windowed | N-period moving average |
| `MOVING_SUM(expr, N)` | Windowed | N-period moving sum |
| `STDDEV(expr, N)` | Windowed | N-period standard deviation |
| `ABS`, `FLOOR`, `CEIL`, `ROUND`, `LN`, `EXP`, `POWER` | Scalar math | Element-wise math |
| `FIR(expr, ARRAY[...])` | DSP | Finite impulse response filter |
| `IIR(expr, ARRAY[...], ARRAY[...])` | DSP | Infinite impulse response filter |
| `RESAMPLE(expr, N)` | DSP | Constant-rate resampling |
| `PEAK_DETECT(expr, N)` | DSP | Peak detection |

## License

This project is licensed under the Business Source License 1.1 (`BUSL-1.1`).

- Licensor: `rtbot-dev`
- Change Date: `2029-03-10`
- Change License: `Apache-2.0`

See `LICENSE` for full terms.
