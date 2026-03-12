---
title: Execution Model
sidebar_position: 6
---

# Execution Model

RtBot SQL compiles queries into operator graphs that execute incrementally. This page explains how programs run, how state is managed, and how the three runtimes differ.

## Message flow

When a new row is inserted into a stream, it flows through any materialized views that depend on that stream:

1. The row enters the operator graph at the entry operator
2. Each operator reads its input, updates its internal state, and emits output
3. Output flows to connected downstream operators
4. When the graph's output operators emit, the result is appended to the view's output stream
5. If downstream views depend on this view, the output propagates to them

This happens for every message. There is no batching, no micro-batching, no scheduling. Each message is a single traversal of the graph.

## Incremental execution

Every operator in the graph maintains internal state and updates it in constant time per message:

- `MovingAverage(N)` maintains a circular buffer of N values. On each message, it adds the new value, drops the oldest, and recomputes the average in O(1).
- `StandardDeviation(N)` maintains running sums and sum-of-squares over the window. O(1) per message.
- `CumulativeSum` maintains a single running total. O(1) per message.
- `Count` maintains a single counter. O(1) per message.

This means pipeline throughput is independent of window size. `MOVING_AVERAGE(price, 10)` and `MOVING_AVERAGE(price, 10000)` process each message equally fast.

## Per-key isolation

`GROUP BY` compiles to a keyed architecture:

1. Route each message by its key value
2. Instantiate a per-key subgraph on first observation
3. Update only the affected key's state

Memory scales with the number of active keys multiplied by per-key state size.

## Expression de-duplication

Repeated expressions compile once and are reused. For example:

```sql
SELECT MOVING_AVERAGE(price, 20) AS mid_band,
       MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20) AS upper_band
```

The `MOVING_AVERAGE(price, 20)` is computed by one operator instance, and its output is shared across both select projections.

## View types

The compiler classifies each materialized view as one of three types:

### SCALAR

A view without GROUP BY. There is one pipeline instance processing all incoming messages.

```sql
CREATE MATERIALIZED VIEW global_stats AS
  SELECT SUM(quantity) AS total, COUNT(*) AS count
  FROM trades
```

### KEYED

A view with GROUP BY. Each unique key value gets an independent pipeline instance with its own state.

```sql
CREATE MATERIALIZED VIEW per_instrument AS
  SELECT instrument_id, MOVING_AVERAGE(price, 20) AS ma
  FROM trades
  GROUP BY instrument_id
```

### TOPK

A specialized view type for ranking queries with ORDER BY ... LIMIT.

## SELECT execution tiers

When you run a SELECT query, the runtime chooses an execution strategy:

### TIER1_READ

Direct read from the output stream. Used when the query is a simple projection on a materialized view, table, or stream.

```sql
SELECT * FROM bollinger LIMIT 10
```

No computation is needed — the output rows have already been produced by the pipeline. This is the fastest tier.

### TIER2_SCAN

Filtered read. Used when the query filters output rows with a WHERE clause.

```sql
SELECT * FROM bollinger WHERE instrument_id = 42
```

Scans available output rows and applies the filter. No pipeline compilation needed.

### TIER3_EPHEMERAL

Ad-hoc computation. Used when the query requires computation that hasn't been materialized.

```sql
SELECT MOVING_AVERAGE(price, 50) FROM trades LIMIT 100
```

The runtime compiles an ephemeral pipeline, feeds the source data through it, and returns the results. The pipeline is destroyed after the query completes.

## State serialization

Every RtBot operator supports state serialization (collect/restore). This enables:

- **Checkpointing** — save pipeline state to disk, resume after restart
- **Migration** — move a running pipeline between environments
- **Replication** — clone a pipeline's state for horizontal scaling

In the Redis runtime, state persistence is handled automatically via Redis's persistence mechanisms.

## Runtime characteristics

| Property | Browser (WASM) | Python (Native) | Redis (Module) |
|----------|---------------|-----------------|----------------|
| Compilation | In-browser WASM | Native C++ extension | Native C++ |
| Execution | Web Worker (WASM) | Native C++ pipeline | Native C++ |
| State storage | In-memory (JS) | In-memory (Python) | Redis data structures |
| Persistence | None (session-only) | None (session-only) | Redis persistence |
| Throughput | ~10K msg/sec | ~15-60K msg/sec | ~100K+ msg/sec |
| Best for | Prototyping, demos | Notebooks, backtesting | Production |

## Determinism guarantees

Given:

1. Same input data (including event-time sequence)
2. Same compiled graph
3. Same floating-point platform behavior

the system yields identical outputs — regardless of when the data arrives, how fast it arrives, or which runtime executes the program.

This property supports audit reproducibility, cross-environment consistency, and reliable testing.
