# Limitations

> This project prefers a system that is simple and functional first. Efficiency improvements can come after the system works correctly.

## SQL Limits
- two-table **equi-join only** in v1
- conjunction-only or disjunction-only predicates in v1
- `GROUP BY` on one column only
- `ORDER BY` on one column only
- only `integer` and `string` data types
- only `COUNT(*)`, `SUM`, `MIN`, and `MAX`

The memo mentions these as possible later additions:
- theta-join
- mixed conjunction and disjunction combinations
- composite indexes

## Key and Index Limits
- only single-column primary keys in v1
- only single-column foreign keys in v1
- composite keys may parse but are rejected as unsupported
- only single-column indexes are supported

## NULL and Referential Behavior
- `NULL` is not supported
- `IS NULL` and `IS NOT NULL` are not supported
- parent-row changes use `RESTRICT`
- no cascade behavior in v1

## Optimizer and Execution Limits
- no cost-based optimizer
- no sort-merge join
- no dynamic join algorithm selection
- nested loop join remains in the executor as a fallback path

## Input / UI Limits
- interactive CLI is the main input mode
- file loading is planned later, not part of the first minimal implementation
