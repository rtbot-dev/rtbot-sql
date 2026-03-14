# RtBot SQL

[![PyPI](https://img.shields.io/pypi/v/rtbot-sql)](https://pypi.org/project/rtbot-sql/)
[![Python](https://img.shields.io/pypi/pyversions/rtbot-sql)](https://pypi.org/project/rtbot-sql/)
[![CI](https://github.com/rtbot-dev/rtbot-sql/actions/workflows/ci.yaml/badge.svg)](https://github.com/rtbot-dev/rtbot-sql/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](LICENSE)

Composable building blocks for real-time analytics that work the same in a notebook and in production. Write SQL to define streaming pipelines — RtBot compiles them into high-performance C++ operator graphs that process data incrementally, one message at a time.

## Install

```bash
pip install rtbot-sql
```

Requires Python 3.13+. Prebuilt wheels are available for Linux, macOS, and Windows on both x86_64 and arm64.

## Quick start

```python
import math
from rtbot_sql import RtBotSql

sql = RtBotSql()
sql.configure_time_format(formatter=lambda ts: ts)

sql.execute("CREATE STREAM sensors (temperature DOUBLE)")
sql.execute("""
  CREATE MATERIALIZED VIEW stats AS
    SELECT temperature,
           MOVING_AVERAGE(temperature, 50) AS avg_temp,
           MOVING_STD(temperature, 50) AS std_temp
    FROM sensors
""")

# Generate 200 readings: smooth sine wave with 3 injected spikes
readings = []
for i in range(200):
    temp = 20.0 + 2.0 * math.sin(i * 2 * math.pi / 60) + 0.3 * math.sin(i * 7.1)
    if i == 80:
        temp = 35.0   # spike high
    elif i == 130:
        temp = 5.0    # spike low
    elif i == 170:
        temp = 38.0   # spike high
    readings.append({"time": i, "temperature": temp})

sql.insert_dataframe("sensors", readings)

# Query only the anomalies
result = sql.execute("""
  SELECT * FROM stats
  WHERE ABS(temperature - avg_temp) > 2 * std_temp
""")

for col in result["columns"]:
    print(f"{col:>15}", end="")
print()
for row in result["rows"]:
    for val in row:
        print(f"{val:>15.2f}", end="")
    print()
```

Output:

```
    temperature       avg_temp       std_temp
          35.00          20.10           2.61
           5.00          19.27           2.39
          38.00          20.23           3.65
```

## Why RtBot SQL

Most real-time pipelines start as a Python prototype and then get rewritten in a production language — a costly translation that introduces bugs and delays. RtBot eliminates that gap:

- **One language, all stages** — the SQL you write in a notebook is the same SQL that runs in production
- **Incremental execution** — each new data point updates pipeline state in constant time, no batch recomputation
- **Deterministic** — same input always produces the same output, regardless of timing or arrival order
- **High performance** — native C++ engine with Python and JavaScript bindings
- **Deploys alongside Redis** — no new infrastructure to operate

## From notebook to production

RtBot SQL compiles your query into an **operator graph** — a JSON structure that describes the computation as a directed graph of operators. This operator graph is the only deployment artifact. It runs identically in every environment because the operators are deterministic.

```
┌─────────────┐     ┌──────────┐     ┌─────────────────────┐
│  Your SQL   │ ──▶ │ Compiler │ ──▶ │ Operator Graph JSON │
└─────────────┘     └──────────┘     └─────────┬───────────┘
                                               │
                          ┌───────────────────┬┴───────────────────┐
                          ▼                   ▼                    ▼
                    ┌───────────┐       ┌────────────┐      ┌────────────┐
                    │  Browser  │       │   Python   │      │   Redis    │
                    │  (WASM)   │       │  (Native)  │      │  (Module)  │
                    └───────────┘       └────────────┘      └────────────┘
                     Playground          Notebooks           Production
```

1. **Prototype** in the browser playground or a Jupyter notebook — write SQL, see results immediately
2. **Validate** in Python — load historical data with `insert_dataframe`, backtest your queries, inspect the operator graph with `explain()` and `show_graph()`
3. **Deploy to Redis** — store the operator graph JSON in Redis, point `RTBOT.CONSUME` at it, and feed data into the input stream

```bash
# Store the compiled program in Redis
redis-cli JSON.SET alerts:program $ "$(cat alerts_program.json)"

# Start the pipeline — consumes from input, writes to output
redis-cli RTBOT.CONSUME alerts:program sensor_data alerts_output kernel_1

# Read results
redis-cli XRANGE alerts_output - +
```

No code to rewrite. No JVM to configure. No cluster to orchestrate. See the [full deployment guide](https://rtbot.dev/docs/user-guide/from-notebook-to-production) for details.

## Supported SQL

### Statements

| Statement | Status |
|-----------|--------|
| `CREATE TABLE` / `CREATE STREAM` | Supported |
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

## Development

### Build and test

```bash
bazel build //apps/cli:rtbot-sql
bazel test //tests/...
```

Build artifacts go to `dist/` (not `bazel-bin/`), configured in `.bazelrc`.

### Project structure

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

runtimes/
  python/       Python bindings (pybind11) and runtime API
  wasm/         WebAssembly bindings for browser/Node.js

tests/
  unit/         Per-module unit tests
  integration/  End-to-end compiler integration tests

examples/
  catalog.json          Sample stream catalog
  queries/              Example SQL files
```

### CLI usage

The CLI compiles SQL into RTBot program JSON. It accepts SQL via positional argument, `--file`, or stdin pipe.

```bash
# Positional argument
dist/bin/apps/cli/rtbot-sql "CREATE TABLE orders (id DOUBLE PRECISION, price DOUBLE PRECISION)"

# From file with catalog
dist/bin/apps/cli/rtbot-sql --file examples/queries/bollinger.sql --catalog examples/catalog.json

# Pipe from stdin
echo "SELECT instrument_id, price FROM trades LIMIT 10" | dist/bin/apps/cli/rtbot-sql --catalog examples/catalog.json
```

### Testing

```bash
# All tests
bazel test //tests/...

# Unit tests only
bazel test //tests/unit/...

# Integration tests only
bazel test //tests/integration/...

# Single test with debug output
bazel test --config=debug //tests/unit:expression_test
```

## Documentation

- [Getting Started](https://www.rtbot.dev/docs/user-guide/getting-started)
- [Python Quickstart](https://www.rtbot.dev/docs/user-guide/quickstart-python)
- [SQL Reference](https://www.rtbot.dev/docs/reference/rtbot-sql/overview)

## License

This project is licensed under the Business Source License 1.1 (`BUSL-1.1`).

- Licensor: `rtbot-dev`
- Change Date: `2029-03-10`
- Change License: `Apache-2.0`

See `LICENSE` for full terms.
