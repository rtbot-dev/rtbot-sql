---
title: Error Handling
sidebar_position: 7
---

# Error Handling

The compiler reports all detectable issues in an `errors` list rather than stopping at the first issue.

## Compile-time errors

Typical categories:

- unknown stream/view/table
- unknown column
- missing `GROUP BY` for non-aggregated select columns
- invalid function arity
- unsupported syntax (for example, `DISTINCT`)
- unbounded stream read (`SELECT * FROM stream` without `LIMIT`)
- `ORDER BY` without `LIMIT`
- aggregate function in `WHERE`
- parse errors

## Runtime errors

Typical categories:

- drop blocked by dependencies
- duplicate entity creation
- writes to removed/nonexistent targets

## Operational guidance

1. Resolve compile-time errors first; they indicate invalid semantics.
2. For drop conflicts, remove dependents in topological order.
3. Keep view naming explicit to make dependency chains obvious.
