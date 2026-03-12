---
title: Statements
sidebar_position: 2
---

# Statements

RtBot SQL supports eight statement types. Each compiles differently and produces a different effect on the catalog and runtime.

## CREATE STREAM

Declares an input data source. This tells the system what columns your incoming data has.

```sql
CREATE STREAM trades (
  instrument_id DOUBLE PRECISION,
  price         DOUBLE PRECISION,
  quantity      DOUBLE PRECISION
)
```

- All columns are `DOUBLE PRECISION` (RtBot operates on numeric data)
- No data is stored — this only registers a schema in the catalog
- You can also use `CREATE TABLE` syntax for this purpose (they are interchangeable for stream declaration)

**Effect:** Adds a stream schema to the catalog. No program is compiled.

## CREATE MATERIALIZED VIEW

Defines a persistent streaming computation. This is the primary statement for building real-time pipelines.

```sql
CREATE MATERIALIZED VIEW bollinger AS
  SELECT instrument_id,
         price,
         MOVING_AVERAGE(price, 20)                          AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20) AS upper_band,
         MOVING_AVERAGE(price, 20) - 2 * MOVING_STD(price, 20) AS lower_band
  FROM trades
  GROUP BY instrument_id
```

When created, the compiler:
1. Parses and analyzes the SELECT query
2. Compiles it into an RtBot operator graph
3. Deploys the graph as a live pipeline
4. Registers the view in the catalog with its output schema

From this point on, every row inserted into the source stream flows through the pipeline automatically and produces output rows in the materialized view.

**Key properties:**
- The pipeline updates incrementally — each message is processed in constant time
- Output is queryable via SELECT
- Other views can read from this view's output (view chaining)
- With `GROUP BY`, each unique key gets an independent pipeline instance

**Effect:** Compiles a program, deploys it, registers the view.

## CREATE VIEW

Defines an ephemeral (non-materialized) query. The computation is not deployed — it runs from scratch each time you SELECT from it.

```sql
CREATE VIEW live_stats AS
  SELECT instrument_id,
         SUM(quantity) AS vol,
         COUNT(*)      AS cnt
  FROM trades
  GROUP BY instrument_id
```

Use plain views for ad-hoc analysis queries. Use materialized views for real-time pipelines.

**Effect:** Registers the view definition in the catalog. No program is deployed.

## CREATE TABLE

Creates a mutable key-value store with a changelog stream. Tables support INSERT, UPDATE, and DELETE operations.

```sql
CREATE TABLE thresholds (
  instrument_id DOUBLE PRECISION PRIMARY KEY,
  max_price     DOUBLE PRECISION,
  alert_level   DOUBLE PRECISION
)
```

Tables differ from streams in important ways:
- Tables have a `PRIMARY KEY` that uniquely identifies rows
- Tables support mutation (updates and deletes)
- Tables expose a changelog stream that views can subscribe to
- Tables are stored in memory and queryable via SELECT

**Effect:** Creates the table schema, registers it in the catalog, creates a changelog stream.

## INSERT

Adds rows to a stream or table.

```sql
INSERT INTO trades VALUES (1, 100.5, 50)
```

For streams, INSERT appends a single row. In the Python runtime, `insert_dataframe` is the preferred method for bulk data loading.

For tables, INSERT adds or updates a row based on the primary key.

**Effect:** Appends data. If the target stream feeds a materialized view, the data flows through the pipeline automatically.

## SELECT

Queries data from a stream, view, or table.

```sql
SELECT * FROM bollinger LIMIT 100
```

SELECT execution uses a tiered system:

| Tier | Name | When used | How it works |
|------|------|-----------|-------------|
| TIER1 | READ | Direct query on a stream, table, or materialized view | Reads available output rows directly. Fastest. |
| TIER2 | SCAN | Query with WHERE filter on output rows | Reads and filters available output rows. |
| TIER3 | EPHEMERAL | Query with computations (functions, expressions) | Compiles an ephemeral pipeline, feeds source data through it, returns results. |

The compiler determines the tier automatically based on the query structure.

**Effect:** Returns result rows. In Python, returns a pandas DataFrame.

## SUBSCRIBE

Subscribes to a stream for live output. Available in the TypeScript runtime.

```sql
SUBSCRIBE bollinger
```

**Effect:** Registers a callback that fires for each new message on the stream.

## DROP

Removes a stream, view, or table from the catalog.

```sql
DROP MATERIALIZED VIEW bollinger
```

If the view had a deployed pipeline, it is destroyed. If other views depend on the dropped entity, they may become invalid.

**Effect:** Removes from catalog, destroys pipeline if applicable.

## DELETE

Removes rows from a table.

```sql
DELETE FROM thresholds WHERE instrument_id = 42
```

**Effect:** Removes the row and emits a changelog event.
