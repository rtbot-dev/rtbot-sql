---
title: Execution Model
sidebar_position: 6
---

# Execution Model

## Incremental semantics

RTBot SQL queries are maintained continuously. Incoming messages update state directly.

- `SUM` adds one value
- `COUNT` increments one counter
- window functions update bounded buffers
- no full historical recomputation per message

## Per-key isolation

`GROUP BY` compiles to a keyed architecture:

1. route by key
2. instantiate per-key subgraph on first observation
3. update only the affected key state

Memory scales with active keys and per-key state size.

## Expression de-duplication

Repeated expressions compile once and are reused.

Example:

```sql
MOVING_AVERAGE(price, 20)
```

reused in select projections and HAVING conditions is represented by one operator instance.

## Determinism guarantees

Given:

1. same input data (including event-time sequence)
2. same compiled graph
3. same floating-point platform behavior

the system yields identical outputs.

This property supports audit reproducibility and cross-environment consistency.
