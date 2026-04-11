# Statements

## Supported Statements
### DDL
- `CREATE TABLE`
- `DROP TABLE`
- `CREATE INDEX`
- `DROP INDEX`

### DML
- `INSERT`
- `DELETE`
- `UPDATE`
- `SELECT`

## General Rules
- each SQL statement is all-or-nothing
- if validation, constraint checks, row updates, catalog updates, or index updates fail, the statement fails
- a parsed statement may still be rejected later as unsupported
- execution time is measured for the full top-level statement

## CREATE TABLE
Flow:
1. parse and bind table definition
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
Flow:
1. validate table and column
2. validate index definition
3. scan existing rows
4. build index entries
5. make the index visible only if build succeeds
6. return success

If build fails, the index is not kept.

## DROP INDEX
Flow:
1. validate target index exists
2. remove index storage
3. remove index metadata
4. return success

If removal fails, the index remains unchanged.

## INSERT
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
Flow:
1. validate tables, columns, predicates, grouping, aggregates, and aliases
2. reject unsupported queries
3. build logical plan
4. apply rule-based rewrites
5. choose physical operators
6. execute query tree
7. format rows and return timing

`SELECT *` is supported.

## Constraint Rules
- duplicate tuples are allowed unless a PK is violated
- PK is unique and non-null
- FK is enforced immediately
- parent delete/update uses `RESTRICT`
- no cascade behavior in v1
- index maintenance happens on insert, delete, and update

## Transaction Boundary
- each top-level statement runs in one `bbolt` transaction
- `SELECT` uses a read-only transaction
- DDL and write DML use a write transaction
- catalog updates, row updates, RID allocation, constraint checks, and index updates happen in the same statement transaction
- a statement becomes visible only after commit
- if any step fails, the whole statement is rolled back
