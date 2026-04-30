# Errors and Transactions

## General Rules
- each SQL statement is all-or-nothing
- if validation, constraint checks, row updates, catalog updates, or index updates fail, the statement fails
- a parsed statement may still be rejected later as unsupported
- execution time is measured for the full top-level statement

## Transaction Boundary
- each top-level statement runs in one `bbolt` transaction
- `SELECT` uses a read-only transaction
- DDL and write DML use a write transaction
- catalog updates, row updates, RID allocation, constraint checks, and index updates happen in the same statement transaction
- a statement becomes visible only after commit
- if any step fails, the whole statement is rolled back

## Error Categories
The implementation uses shared error categories so higher layers and tests can identify the kind of failure.

Current categories:
- `invalid definition`
- `already exists`
- `not found`
- `type mismatch`
- `constraint violation`
