# Query Processing

## SELECT Pipeline
`SELECT` follows this flow:
1. parse
2. bind / validate
3. build logical plan
4. rewrite / optimize
5. choose physical operators
6. execute
7. format result and timing

## Logical Planning
The planner builds a query tree for operations such as:
- table scan
- index scan
- selection
- projection
- join
- sort
- aggregate / group by
- having

## Rule-Based Optimization
The optimizer is rule-based.

Main rewrites:
- predicate normalization
- split `AND` conjuncts
- keep `OR` intact unless a rewrite is clearly safe
- push single-table selections down
- push projections down
- identify join predicates
- choose index scan when obviously applicable

What is not implemented:
- no cost-based optimization
- no dynamic join algorithm selection
- no sort-merge join

## Execution Model
The execution engine uses a materialized model:
- each operator produces all output rows before the parent continues
- operators have a fixed output schema
- `SELECT` returns rows
- DDL and non-`SELECT` DML return status output and affected row count when relevant
- execution time is measured for the whole statement

## Physical Operators
Implemented operators:
- **Table Scan**: reads all rows of a table
- **Index Scan**: reads rows through a single-column index
- **Selection**: filters rows using a Boolean predicate
- **Projection**: keeps only needed columns
- **Join**: combines rows from two inputs using a hash equi-join for the currently supported inner join shape, with nested loop join kept as a fallback operator
- **Sort**: orders rows for `ORDER BY`
- **Aggregate / Group By**: groups rows and computes aggregate values
- **HAVING**: filters grouped results

Supported aggregates:
- `COUNT(*)`
- `SUM(int_column)`
- `MIN(int_column)`
- `MAX(int_column)`

## Aggregate Rules
- `HAVING` requires `GROUP BY`
- every non-aggregated expression in `SELECT` must appear in `GROUP BY`
- `HAVING` may reference grouping columns or aggregate expressions
- aliases may be used if defined

## Example Query Tree
For:

```sql
SELECT name
FROM Students
WHERE age = 20
ORDER BY name;
```

The query tree is:

```text
Project(name)
  Sort(name)
    Selection(age = 20)
      TableScan(Students)
```

Execution order:
1. scan table rows
2. filter rows with `age = 20`
3. sort by `name`
4. keep only the `name` column
