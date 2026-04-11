# Architecture

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

Supported `SELECT` features in the baseline memo:
- single-table queries
- two-table equi-join
- comparison predicates in `WHERE`
- conjunction-only or disjunction-only predicates
- `GROUP BY` (one column)
- `HAVING`
- `ORDER BY` (one column, `ASC`/`DESC`)
- `COUNT(*)`, `SUM`, `MIN`, `MAX`

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
Accepts SQL from the user. An interactive CLI is implemented.

### Parser
Reads SQL text, checks syntax, and produces a parser AST. The project uses the Vitess SQL parser. The parser may accept more SQL than the system actually supports.

### Binder / Semantic Analyzer
Checks meaning after parsing. It resolves tables, columns, and aliases; checks types and aggregate legality; validates PK/FK references; and separates invalid SQL from valid-but-unsupported SQL.

### DDL
(Data Definition Language)  
Handles schema-level statements such as creating and dropping tables or indexes. It updates catalog metadata and checks whether definitions are allowed.

### DML
(Data Manipulation Language)  
Handles tuple-level statements such as insert, delete, update, and select. It applies constraints, maintains indexes, and drives query execution for `SELECT`.

### Optimizer
Applies simple rule-based rewrites. It improves the logical plan without using a cost-based model.

### Execution Engine
Runs the physical plan and returns rows or status output. Execution time is measured for the full top-level statement.

### Storage Structures
Stores catalog data, table rows, and index data in one persistent database file.
