# Relational DBMS in Go

This repository serves as an archive of a database management system built from scratch, focusing on clear ownership across components and a complete end-to-end execution path.
The project supports a compact SQL subset, stores data in a single bbolt database file, and executes each top-level statement atomically.

## Features
- Interactive SQL CLI with multi-line statement input
- Persistent storage using `bbolt`
- Catalog support for tables, primary keys, foreign keys, and secondary indexes
- Basic DDL and DML statement execution
- SQL processing pipeline: input, parsing, binding, logical planning,
  rule-based optimization, physical planning, execution, and formatting
- Table output with row counts and execution timing

## Supported SQL

Statement families:

- `CREATE TABLE`
- `DROP TABLE`
- `CREATE INDEX`
- `DROP INDEX`
- `INSERT`
- `DELETE`
- `UPDATE`
- `SELECT`

`SELECT` supports single-table queries, two-table equi-joins, predicates,
grouping, basic aggregates, ordering, aliases, and `SELECT *`.

See [docs/sql-reference.md](docs/sql-reference.md) for the full supported SQL
surface and limitations.

## Requirements

- Go 1.25.7 or newer

## Getting Started

Run the CLI with the default database file:

```sh
go run .
```

Use a custom database file:

```sh
go run . ./data.db
```

Run the test suite:

```sh
go test ./...
```

## CLI

The CLI prompt is `db> `. Statements are read until a terminating semicolon.
Use `exit` or `quit` to leave the shell when no partial statement is in
progress.

Example:

```sql
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Ada');
SELECT * FROM users;
```

## Documentation

- [Overview](docs/overview.md)
- [SQL Reference](docs/sql-reference.md)
- [Query Engine](docs/query-engine.md)
- [Storage, Catalog, and Indexes](docs/storage-catalog-indexes.md)
- [CLI and Output](docs/cli-and-output.md)
- [Errors and Transactions](docs/errors-and-transactions.md)
- [Development](docs/development.md)
- [Limitations](docs/limitations.md)
