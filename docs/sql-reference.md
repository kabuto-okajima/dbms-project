# SQL Reference

## Supported Statements
DDL:
- `CREATE TABLE`
- `DROP TABLE`
- `CREATE INDEX`
- `DROP INDEX`

DML:
- `INSERT`
- `DELETE`
- `UPDATE`
- `SELECT`

## CREATE TABLE
Supported constraint shapes:
- inline single-column primary key
- table-level single-column primary key
- inline single-column `REFERENCES`
- table-level single-column `FOREIGN KEY`
- default FK actions or explicit `RESTRICT`

Flow:
1. parse table definition
2. validate names, types, PK/FK shape, and referenced table/column existence
3. reject unsupported definitions
4. write catalog metadata
5. return success

If validation fails, nothing is written.

## DROP TABLE
Flow:
1. check target table exists
2. check that no other table still references it by FK
3. remove table rows
4. remove table indexes
5. remove catalog metadata
6. return success

If FK restriction fails, nothing is removed.

## CREATE INDEX
Supported index shapes:
- single-column secondary index
- single-column unique secondary index

Flow:
1. validate table and column
2. validate index definition
3. scan existing rows
4. build index entries
5. make the index visible only if build succeeds
6. return success

If build fails, the index is not kept.

Unique secondary indexes reject duplicate keys while building from existing rows, on `INSERT`, and on `UPDATE`.

## DROP INDEX
Flow:
1. validate target index exists
2. remove index storage
3. remove index metadata
4. return success

If removal fails, the index remains unchanged.

## INSERT
Current shape:
- one `VALUES` row per statement
- integer and string literal values only
- optional explicit column list is supported
- explicit column lists may reorder values into schema order
- partial, duplicate-column, and unknown-column inserts are rejected

Flow:
1. validate target table and input values
2. check type compatibility
3. check PK uniqueness
4. check FK existence
5. allocate and store the row
6. update indexes
7. return success and affected row count

If any step fails, no row or index change is kept.

## DELETE
Flow:
1. identify rows matching the predicate
2. check FK `RESTRICT` rules for parent-row deletion
3. delete rows
4. remove related index entries
5. return success and affected row count

If any step fails, nothing is deleted.

`DELETE` without `WHERE` is allowed.

## UPDATE
Current shape:
- one target table
- one column assignment per statement
- integer and string literal assignment values only

Flow:
1. identify rows matching the predicate
2. compute new row values
3. validate types
4. check PK uniqueness if PK changes
5. check FK validity if FK changes
6. check parent-row `RESTRICT` rules if a referenced key changes
7. write updated rows
8. update affected indexes
9. return success and affected row count

If any step fails, old rows remain unchanged.

`UPDATE` without `WHERE` is allowed.

## SELECT
Supported select-list shapes:
- `*`
- column references
- integer and string literals
- supported aggregate expressions
- aliases for output columns

Flow:
1. validate tables, columns, predicates, grouping, aggregates, and aliases
2. reject unsupported queries
3. build logical plan
4. apply rule-based rewrites
5. choose physical operators
6. execute query tree
7. format rows and return timing

Table aliases are supported. When a table has an alias, the alias is the visible qualifier. This also allows alias-based self-joins.

`SELECT *` is supported through the `*` select-list shape.

## Constraint Rules
- duplicate tuples are allowed unless a PK is violated
- PK is unique and non-null
- FK is enforced immediately
- parent delete/update uses `RESTRICT`
- no cascade behavior in v1
- index maintenance happens on insert, delete, and update
