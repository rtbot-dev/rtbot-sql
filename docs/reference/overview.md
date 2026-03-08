---
title: Overview
sidebar_position: 1
---

# RTBot SQL Overview

RTBot SQL compiles SQL into incremental RTBot operator graphs. Results are maintained continuously as new messages arrive.

## Core model

- **Incremental by default:** no batch recomputation for materialized views.
- **Event-time deterministic:** same input data and program produce same output.
- **Streaming-first:** `CREATE TABLE` defines a stream schema (append-only messages).
- **Numeric type system:** all values are IEEE 754 doubles.

## Entity types

- **Stream** via `CREATE TABLE ...` (without primary key)
- **Reference table** via `CREATE TABLE ... PRIMARY KEY`
- **Materialized view** via `CREATE MATERIALIZED VIEW ...`
- **Virtual view** via `CREATE VIEW ...` (inlined composition)

## Query execution tiers for `SELECT`

| Tier | Pattern | Execution |
|------|---------|-----------|
| Tier 1 | `SELECT ... FROM view LIMIT n` | Direct read of maintained output |
| Tier 2 | `SELECT ... FROM source WHERE ... LIMIT n` | Scan + stateless filter |
| Tier 3 | `SELECT ... GROUP BY ...` | Ephemeral pipeline + replay |

Use the sections below for complete statement syntax, function behavior, execution guarantees, and patterns.
