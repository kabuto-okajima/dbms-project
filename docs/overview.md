# Overview

This project is a small DBMS implemented in Go.

It supports a compact SQL subset, stores data in one `bbolt` database file, and executes each top-level SQL statement atomically.

## Supported SQL Surface
Supported statement families:
- `CREATE TABLE`
- `DROP TABLE`
- `CREATE INDEX`
- `DROP INDEX`
- `INSERT`
- `DELETE`
- `UPDATE`
- `SELECT`

Supported `SELECT` features:
- single-table queries
- two-table equi-join, including alias-based self-join
- comparison predicates in `WHERE`
- conjunction-only or disjunction-only predicates
- `GROUP BY` on one column
- `HAVING`
- `ORDER BY` on one or more scalar keys, with `ASC` or `DESC`
- `COUNT(*)`, `SUM`, `MIN`, and `MAX`
- column, literal, aggregate, and `*` select-list items

Supported types:
- integer
- string

A statement may parse successfully but still be rejected later as unsupported.

## Main Flow
1. Input Manager
2. Parser
3. Binder / Semantic Analyzer
4. Logical Plan Builder
5. Rule-Based Optimizer
6. Physical Plan Builder
7. Execution Engine
8. Formatter + Timer

## Component Roles
### Query Input Manager
Accepts SQL from the user. An interactive CLI is implemented. It reads until a semicolon completes the statement, supports multi-line input, and exits on `exit` or `quit` when no statement is in progress.

### Parser
Reads SQL text, checks syntax, and produces a parser AST. The project uses the Vitess SQL parser. The parser may accept more SQL than the system actually supports.

### Binder / Semantic Analyzer
Checks meaning after parsing for DML queries. It resolves tables, columns, and aliases; checks types and aggregate legality; and separates invalid SQL from valid-but-unsupported SQL. DDL shape and PK/FK reference validation are handled by parser/catalog code paths.

### DDL
Handles schema-level statements such as creating and dropping tables or indexes. It updates catalog metadata and checks whether definitions are allowed.

### DML
Handles tuple-level statements such as insert, delete, update, and select. It applies constraints, maintains indexes, and drives query execution for `SELECT`.

### Optimizer
Applies simple rule-based rewrites. It improves the logical plan without using a cost-based model.

### Execution Engine
Runs the physical plan and returns rows or status output. Execution time is measured for the full top-level statement. For the currently supported two-table inner equi-join shape, the executor lowers to a hash join, while nested loop join remains available as a fallback physical operator for future expansion.

### Storage Structures
Stores catalog data, table rows, and index data in one persistent database file.

### Formatter
Renders `SELECT` output as an ASCII table with row count and elapsed time. DDL and write-DML statements return a status message plus elapsed time.
