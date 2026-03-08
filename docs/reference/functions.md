---
title: Functions
sidebar_position: 4
---

# Functions

## Aggregate functions

- `SUM(expr)` -> running cumulative sum
- `COUNT(*)` -> running count
- `AVG(expr)` -> running average (`SUM/COUNT`)
- `MIN(expr)`, `MAX(expr)` -> cross-key aggregate reads

## Window functions

- `MOVING_AVERAGE(expr, n)`
- `MOVING_SUM(expr, n)`
- `MOVING_COUNT(n)`
- `STDDEV(expr, n)` (`MOVING_STD` alias)
- `MOVING_MIN(expr, n)`
- `MOVING_MAX(expr, n)`

All are incremental and maintain bounded state per key.

## Signal-processing functions

- `FIR(expr, ARRAY[c0, c1, ...])`
- `IIR(expr, ARRAY[a0, ...], ARRAY[b0, ...])`
- `RESAMPLE(expr, interval)`
- `PEAK_DETECT(expr, n)`

## Math functions

Stateless scalar functions include:

- `ABS`, `FLOOR`, `CEIL`/`CEILING`, `ROUND`
- `LN`/`LOG`, `LOG10`, `EXP`
- `SIN`, `COS`, `TAN`, `SIGN`
- `POWER(expr, n)` (constant exponent)

## Expressions and operators

### Arithmetic

- `+`, `-`, `*`, `/`

Compiler optimization:
- scalar operators when one side is constant
- constant folding when both sides are constant

### Comparison

- `>`, `<`, `>=`, `<=`, `=`, `!=`

Scalar vs sync compare operators are chosen by whether operands are constant or stream endpoints.

### Logical

- `AND`, `OR`, `NOT`

## CASE expression

```sql
CASE
  WHEN price > 200 THEN 3
  WHEN price > 100 THEN 2
  ELSE 1
END
```

Compiled as mutually exclusive gates and a multiplexer.
