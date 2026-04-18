# RtBot SQL Repository

SQL compiler layer on top of the rtbot C++ streaming engine. Transforms SQL-like syntax into deterministic rtbot operator graphs (JSON DAGs). Bridges the gap between high-level SQL (familiar to data practitioners) and low-level streaming operators (efficient to execute).

The key innovation is **tier-based execution**: simple reads bypass compilation entirely (TIER1), WHERE/projection use compiled expression trees (TIER2), and complex aggregations/windowed functions invoke full operator graph compilation (TIER3).

## Critical: build rules

**Always use Bazel.** Never invoke bare build tools (cmake, make, g++, clang++, pip, python setup.py, etc.) directly. All builds, tests, and dev servers go through Bazel.

## Build system

Bazel with bzlmod (`MODULE.bazel`, version 0.3.0). C++17 with clang. Output goes to `dist/` (configured in `.bazelrc`).

### Key dependencies

- libpg_query 17-6.2.2 (PostgreSQL SQL parser)
- nlohmann/json (JSON), cxxopts (CLI), googletest (testing), protobuf
- pybind11 (Python bindings), emsdk 4.0.15 (WASM)
- Python toolchains: 3.10, 3.11 (default), 3.13, 3.14

### Common commands

```bash
bazel build //apps/cli:rtbot-sql               # CLI compiler
bazel build //runtimes/python:rtbot_sql_whl    # Python wheel
bazel build //runtimes/browser:rtbot_sql_wasm  # WebAssembly
bazel build //runtimes/java:rtbot_sql_jni      # Java JNI

bazel test //tests/...                         # all tests
bazel test //tests/unit/...                    # unit tests only
bazel test --config=debug //tests/unit:expression_test  # single test with debug

bazel build --config=asan //runtimes/python:rtbot_sql_whl  # AddressSanitizer
```

## Directory structure

```
rtbot-sql/
├── libs/
│   ├── parser/        # SQL parsing via libpg_query -> AST
│   ├── analyzer/      # scope resolution and name binding
│   ├── catalog/       # stream/view/table schema registry
│   ├── compiler/      # expression, function, SELECT, GROUP BY compilation
│   ├── planner/       # query tier classification and planning
│   └── api/           # top-level compiler API
├── apps/
│   ├── cli/           # command-line SQL compiler
│   └── redis-ext/     # Redis module integration
├── runtimes/
│   ├── python/        # Python bindings (pybind11) + runtime API
│   ├── java/          # Java JNI bindings
│   ├── browser/       # WebAssembly bindings
│   └── redis/         # Redis module runtime
├── tests/
│   ├── unit/          # per-module unit tests
│   ├── integration/   # end-to-end compiler tests
│   ├── e2e/           # end-to-end runtime tests
│   ├── cross_runtime/ # cross-platform validation
│   └── perf/          # performance benchmarks
├── examples/
│   ├── queries/       # example SQL files
│   ├── catalog.json   # sample stream catalog
│   └── notebooks/     # Jupyter notebooks
├── docs/              # reference documentation
└── MODULE.bazel
```

## Compilation pipeline

```
SQL statement(s)                         Catalog snapshot
    |                                           |
    v                                           v
Parser (libpg_query) --> AST --> Analyzer (scope resolution) --> Planner (tier)
                                                                       |
                                                                       v
                                                Compiler (expression / function / SELECT / WHERE / GROUP BY)
                                                                       |
                          +----------------------------+---------------+
                          |                            |
                          v                            v
                per-statement CompilationResult    consolidated-session compile
                (legacy per-view cascade)           compile_session_program(catalog)
                                                            |
                                                            v
                                       single rtbot Program JSON with a shared Input
                                       per base stream and a Collector sink per
                                       materialized view terminal
                                                            |
                                                            v
          three backends: Browser (WASM) | Python (native) | Java (JNI) | Redis (module)
```

**Consolidated session is the default deployment target.** It compiles every view in the catalog into a single rtbot Program: topo-sorted view subgraphs are merged with a `<view>__<id>` prefix so ids stay unique, wired to one shared `Input` operator per base stream, and every view terminal is exposed via a `Collector` sink. Cycles between views are rejected with a `CycleDetected` error. Multi-base-stream views (cross-select) and table→changelog JOINs are supported: tables appear as additional session input ports.

### Execution tiers

| Tier | Name | When | How |
|------|------|------|-----|
| TIER1_READ | simple read | no WHERE, no GROUP BY, limit only | reads from stream storage, no compilation |
| TIER2_SCAN | filtered scan | WHERE filtering or SELECT projection | compiled expression tree for per-row evaluation |
| TIER3_EPHEMERAL | full compilation | aggregates, windowed functions, complex expressions | full operator graph compiled to rtbot program |

## Core modules

### Parser (`libs/parser/`)

Uses libpg_query (PostgreSQL parser) via C bindings. Converts PostgreSQL parse tree to custom AST.

Key files: `parser.cpp`, `ast_converter.cpp`.

AST types (`include/rtbot_sql/parser/ast.h`):
- **Expressions**: ColumnRef, Constant, StringConstant, ArrayLiteral, BinaryExpr, ComparisonExpr, FuncCall, LogicalExpr, NotExpr, BetweenExpr, CaseExpr
- **Statements**: SelectStmt, CreateStreamStmt, CreateViewStmt, InsertStmt, DropStmt, DeleteStmt

### Analyzer (`libs/analyzer/`)

Scope management and name resolution. Key class: `Scope`.

Registers streams with schemas, resolves ColumnRef nodes to ColumnBinding (stream name + column index), detects ambiguous references, supports nested scopes for joins.

File: `scope.cpp` (~1,700 lines).

### Catalog (`libs/catalog/`)

Schema registry maintaining state about streams, views, and tables. Key class: `Catalog`.

Registers/looks up by name. Maintains **string dictionaries** for TEXT column encoding (TEXT values encoded as double-precision float indices). Tracks known keys for keyed views. Provides snapshot for compilation.

### Compiler (`libs/compiler/`)

Transforms SQL AST into rtbot operator graph JSON. The largest module.

| Component | File | Purpose |
|-----------|------|---------|
| Expression compiler | `expression_compiler.cpp` | expressions -> operator subgraphs, bytecode for FusedExpression |
| Function compiler | `function_compiler.cpp` | SQL functions -> rtbot operators |
| Select compiler | `select_compiler.cpp` | SELECT projection, VectorProject/FusedExpression optimization |
| Where compiler | `where_compiler.cpp` | WHERE predicates -> operator graphs |
| Group By compiler | `group_by_compiler.cpp` | GROUP BY -> KeyedPipeline structures |
| Graph builder | `graph_builder.cpp` | operator graph construction, unique IDs, validation |
| Alias expander | `alias_expander.cpp` | column alias expansion and inlining |

**Supported SQL functions**: SUM, COUNT, AVG, MOVING_AVERAGE, MOVING_STD, MOVING_SUM, STDDEV, ABS, FLOOR, CEIL, ROUND, LN, EXP, POWER, FIR, IIR, RESAMPLE, PEAK_DETECT.

### Planner (`libs/planner/`)

Query optimization and execution planning.

Key classes: `Classifier` (determines tier), `SelectPlan` (execution plan), `Evaluator` (compiled expression tree for TIER2 per-row evaluation).

Evaluator expression types: ColumnAccess, ConstantExpr, BinaryOpExpr, ComparisonEvalExpr, LogicalAndExpr, LogicalOrExpr, NotEvalExpr, UnaryFuncExpr.

### API (`libs/api/`)

Top-level compilation interface (`include/rtbot_sql/api/compiler.h`):

```cpp
// Compile one SQL statement in isolation (legacy per-view form).
CompilationResult compile_sql(const string& sql, const CatalogSnapshot& catalog);

ExpandedCompilationResult compile_sql_expanded(
    const string& sql, const CatalogSnapshot& catalog, int64_t ts_units_per_second);

// Compile every view in the catalog into one rtbot Program. This is the
// canonical path used by all runtimes (Python, Java, browser, Redis).
SessionCompilationResult compile_session_program(const CatalogSnapshot& catalog);

Tier2FilterResult apply_tier2_filter(
    const string& sql, const CatalogSnapshot& catalog,
    const vector<vector<double>>& input_rows, int limit = -1);
```

**CompilationResult** contains: `program_json`, `field_map` (column name -> vector position), `source_streams`, `view_type` (SCALAR/KEYED/TOPK), `select_tier`.

**SessionCompilationResult** contains: `program_json`, `view_terminals` (view → terminal operator id), `view_terminal_ports` (view → output port id), `materialized_views` (subset of terminals attached to Program output Collectors), `base_stream_inputs`/`base_stream_ports` (base-stream → shared Input operator + port), `errors`.

Shared binary I/O infrastructure lives in `include/rtbot_sql/api/batch_decoder.h`:
- `SessionOutputRegistry` — per-Program op_id → index map so runtimes can decode output frames by integer index rather than string lookup.
- `BatchDecoder` — decodes the binary output frame format (`int32 num_outputs`, then per output: `int32 opIdx`, `int32 nVals`, `int64 ts`, `float64[nVals]`). Consumed unchanged by every runtime.

Types defined in `include/rtbot_sql/api/types.h`:
- ColumnType: DOUBLE, TEXT
- ViewType: SCALAR, KEYED, TOPK
- StatementType: CREATE_STREAM, CREATE_VIEW, CREATE_MATERIALIZED_VIEW, CREATE_TABLE, INSERT, SELECT, DROP

## Runtimes

### Python (`runtimes/python/`)

Primary user-facing API in `rtbot_sql/__init__.py`:

```python
from rtbot_sql import RtBotSql

sql = RtBotSql()
sql.execute("CREATE STREAM sensors (temperature DOUBLE, device_id TEXT)")
sql.execute("""
  CREATE MATERIALIZED VIEW stats AS
    SELECT temperature,
           MOVING_AVERAGE(temperature, 50) AS avg_temp,
           MOVING_STD(temperature, 50) AS std_temp
    FROM sensors
""")

# Zero-copy fast path: pass timestamps + per-column numpy arrays directly.
sql.insert_buffer("sensors", timestamps_ns, value_columns)

# Convenience path that accepts a pandas DataFrame.
sql.insert_dataframe("sensors", dataframe)

result = sql.execute("SELECT * FROM stats WHERE ...")
```

Key classes:
- `RtBotSql` (`runtime.py`) -- main user-facing API
- `InMemoryCatalog` -- Python-side schema registry
- `InMemoryStreamStore` -- stores input data for replay / backfill
- `LocalPipelineRunner` (`pipeline_runner.py`) -- compiles the current catalog via `compile_session_program`, instantiates a single `NativeSessionPipeline` (pybind11), backfills it from the stream store, and dispatches incoming rows
- `PipelineOutput` -- decoded session output frames (via the shared `BatchDecoder`)

C++ bindings via pybind11 (`_rtbot_sql_native`): exposes `NativeSessionPipeline`, `compile_session`, and a zero-copy `feed_buffer` taking numpy arrays via `py::buffer.request()`. Legacy `NativePipeline.feed_batch` / `feed_buffer` / `run` / `InputVectorMessage` have been removed.

### CLI (`apps/cli/`)

```bash
dist/bin/apps/cli/rtbot-sql "CREATE TABLE orders (id DOUBLE, price DOUBLE)"
dist/bin/apps/cli/rtbot-sql --file examples/queries/bollinger.sql --catalog examples/catalog.json
echo "SELECT ... FROM trades" | dist/bin/apps/cli/rtbot-sql --catalog examples/catalog.json
```

### Browser/WASM (`runtimes/browser/`)

Emscripten-compiled WASM interface. Exposes `compileSessionJson(catalogJson)` which returns the same `SessionCompilationResult` shape as the native C++ API.

### Java (`runtimes/java/`)

JNI bindings with JDK21. `RtBotSqlRuntime` is session-only: it caches `SessionViewInfo` at deploy time, feeds rows via `feedPipelineBufferSession(port_index, …)` (binary frame), and persists state under `SESSION_STATE_KEY` for serialize/restore. Legacy per-view cascade + `feedPipelineBatchI1` / `feedPipelineBufferI1` bindings have been removed. Backfill-from-store rewires a freshly deployed session to replay historical input.

### Redis (`runtimes/redis/` and `apps/redis-ext/`)

Redis module integration.

## Testing

20 unit test files in `tests/unit/`, 5 integration test files in `tests/integration/`.

| Test file | Module | Size |
|-----------|--------|------|
| `expression_test.cpp` | expression compilation | ~47KB |
| `group_by_test.cpp` | GROUP BY compilation | ~50KB |
| `e2e_runtime_test.cpp` | full pipeline E2E | ~131KB |
| `parser_test.cpp` | AST parsing | - |
| `catalog_test.cpp` | schema registry | - |
| `scope_test.cpp` | name resolution | - |
| `function_test.cpp` | function compilation | - |
| `select_test.cpp` | SELECT projection | - |
| `where_test.cpp` | WHERE clause | - |
| `having_test.cpp` | HAVING clause | - |
| `tier_test.cpp` | tier classification | - |
| `plan_test.cpp` | query planning | - |
| `evaluator_test.cpp` | TIER2 expression evaluation | - |
| `alias_expander_test.cpp` | alias expansion | - |

Cross-runtime: `test_python.py` validates Python runtime.

## SQL dialect and limitations

**Supported statements**: CREATE STREAM/TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW, INSERT, SELECT, DROP, DELETE.

**Supported**: arithmetic (+, -, *, /), comparisons (>, <, =, !=, BETWEEN), logical (AND, OR, NOT), CASE expressions, GROUP BY (with hash-based composite keys), HAVING, ORDER BY (requires LIMIT), column aliases.

**Not supported**: subqueries, CTEs, window functions (SQL:2003 OVER syntax), DISTINCT, stream JOINs, string/text operations (TEXT encoded as dictionary IDs internally).

**Data model**: all values stored as doubles. TEXT columns use dictionary encoding (string -> sequential double ID per column).

## Example queries

In `examples/queries/`:
- `bollinger.sql` -- Bollinger bands with MOVING_AVERAGE and MOVING_STD
- `three_level_chain.sql` -- view chaining (trades -> v1 -> v2 -> v3)
- `instrument_stats.sql` -- per-instrument aggregation
- `live_stats.sql` -- running statistics

Sample catalog (`examples/catalog.json`):
```json
{
  "streams": {
    "trades": ["instrument_id", "price", "quantity", "account_id"]
  }
}
```

## Operator types generated

The compiler produces these rtbot operator types in the JSON graph:
Input (one per base stream in session mode, multi-port), VectorExtract, VectorProject, VectorCompose, Join, Addition, Subtraction, Multiplication, Division, CumulativeSum, CountNumber, Average, MovingAverage, StandardDeviation, FIR, IIR, Resample, PeakDetect, FusedExpression / FusedExpressionVector (bytecode-optimized arithmetic + projection, from `libs/fuse/` in rtbot), KeyedPipeline (hash-based partitioned sub-graphs).

Session-mode programs do **not** emit `Output` operators — each materialized view terminal is attached to a Program-owned `Collector` sink so the runtime can read its output frames directly.

## Documentation

- `README.md` -- quick start, supported SQL, project structure
- `docs/reference/overview.md` -- architecture overview
- `docs/reference/programming-patterns.md` -- usage patterns
- `docs/sql-reference.md` -- SQL language reference
