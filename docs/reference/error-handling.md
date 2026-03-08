---
title: Error Handling
sidebar_position: 7
---

# Error Handling

RtBot SQL reports errors at compile time. The compiler catches most issues before any data is processed, and reports all detectable issues in an `errors` list rather than stopping at the first one.

## Compilation errors

When a SQL statement fails to compile, the result contains an `errors` array. Each error has:

- **`message`** — a human-readable description of the problem
- **`line`** — the line number in the SQL text (1-based)
- **`column`** — the column position (1-based)

In Python, compilation errors raise a `SqlError` exception:

```python
from rtbot_sql import RtBotSql, SqlError

sql = RtBotSql()
sql.execute("CREATE STREAM trades (price DOUBLE PRECISION)")

try:
    sql.execute("SELECT unknown_column FROM trades")
except SqlError as e:
    print(e)            # Human-readable message
    print(e.errors)     # List of CompilationError objects
```

In TypeScript, check the `errors` array on the result:

```typescript
const result = rt.execute("SELECT unknown_column FROM trades");
if (result.errors.length > 0) {
  console.error(result.errors);
}
```

## Common compile-time errors

### Unknown column

```sql
SELECT nonexistent FROM trades
-- Error: column "nonexistent" not found in stream "trades"
```

The column name doesn't match any column in the source stream or view. Check the stream schema with `explain()` or review your `CREATE STREAM` statement.

### Unknown function

```sql
SELECT MEDIAN(price) FROM trades
-- Error: unknown function "median"
```

The function isn't supported by RtBot SQL. See [Functions](/docs/reference/rtbot-sql/functions) for the complete list of supported functions.

### Wrong argument count

```sql
SELECT MOVING_AVERAGE(price) FROM trades
-- Error: MOVING_AVERAGE requires 2 arguments (expression, window_size)
```

Windowed functions require both an expression and a window size parameter.

### Unknown source

```sql
SELECT price FROM nonexistent_stream
-- Error: unknown source "nonexistent_stream"
```

The stream, view, or table referenced in FROM hasn't been created. Create it first with `CREATE STREAM` or `CREATE MATERIALIZED VIEW`.

### Window size must be a constant

```sql
SELECT MOVING_AVERAGE(price, quantity) FROM trades
-- Error: window size must be a constant integer
```

The window size parameter for windowed functions must be a literal integer, not a column reference or expression.

### Missing GROUP BY

Non-aggregated columns in the SELECT list require a GROUP BY clause when aggregate functions are present.

### Unsupported syntax

Some SQL features (e.g., `DISTINCT`, subqueries) are not yet supported and will produce a compile-time error.

### Unbounded stream read

`SELECT * FROM stream` without `LIMIT` is invalid because streams are unbounded. Add a `LIMIT` clause.

### Aggregate in WHERE

Aggregate functions cannot be used in WHERE. Use HAVING instead.

## Runtime errors

- **Drop blocked by dependencies** — cannot drop a view that other views depend on. Remove dependents first in topological order.
- **Duplicate entity creation** — creating a stream/view/table with an existing name.
- **Writes to removed targets** — inserting into a dropped stream.

## Using explain for debugging

The `explain()` method (Python) or `compile()` static method (TypeScript) lets you inspect the compilation result without executing:

```python
sql = RtBotSql()
sql.execute("CREATE STREAM trades (price DOUBLE, quantity DOUBLE)")

info = sql.explain("""
  CREATE MATERIALIZED VIEW stats AS
    SELECT MOVING_AVERAGE(price, 20) AS ma
    FROM trades
""")

print(info["statement_type"])   # CREATE_MATERIALIZED_VIEW
print(info["field_map"])        # {'ma': 0}
print(info["source_streams"])   # ['trades']
print(info["view_type"])        # SCALAR
print(info["errors"])           # [] if compilation succeeded
```

## Using validate for syntax checking

In TypeScript, use the static `validate` method to check SQL syntax without needing a catalog:

```typescript
const result = RtBotSql.validate("SELECT * FROM trades", wasmModule);
console.log(result.valid);    // true or false
console.log(result.errors);   // syntax errors, if any
```

Note that validation only checks syntax. Semantic errors (unknown columns, unknown sources) require a catalog and are caught during full compilation.

## Operational guidance

1. Resolve compile-time errors first — they indicate invalid semantics.
2. For drop conflicts, remove dependents in topological order.
3. Keep view naming explicit to make dependency chains obvious.
4. Verify each stage independently with `SELECT` before composing downstream views.
