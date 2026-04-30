# Development

## Repository Structure
```text
dbms-project/
├── README.md
├── main.go
│
├── internal/
│   ├── app/
│   │   └── app.go
│   │
│   ├── input/
│   │   └── cli.go
│   │
│   ├── parser/
│   │   ├── parser.go
│   │   └── supported.go
│   │
│   ├── binder/
│   │   └── binder.go
│   │
│   ├── catalog/
│   │   └── catalog.go
│   │
│   ├── storage/
│   │   ├── bbolt.go
│   │   ├── rid.go
│   │   └── row_codec.go
│   │
│   ├── planner/
│   │   ├── build.go
│   │   ├── logical.go
│   │   └── optimizer.go
│   │
│   ├── executor/
│   │   ├── build.go
│   │   ├── execute.go
│   │   ├── index_selection.go
│   │   └── physical.go
│   │
│   ├── statement/
│   │   ├── create_table.go
│   │   ├── drop_table.go
│   │   ├── create_index.go
│   │   ├── drop_index.go
│   │   ├── insert.go
│   │   ├── delete.go
│   │   ├── update.go
│   │   └── select.go
│   │
│   ├── format/
│   │   └── format.go
│   │
│   └── shared/
│       ├── types.go
│       └── errors.go
│
└── docs/
    ├── cli-and-output.md
    ├── development.md
    ├── errors-and-transactions.md
    ├── limitations.md
    ├── overview.md
    ├── query-engine.md
    ├── sql-reference.md
    └── storage-catalog-indexes.md
```

## app/
Controls the overall flow of the program.
It takes the SQL input, sends it through the main steps of processing, chooses the right statement logic, and returns the final result.

## input/
Handles how the user gives SQL to the system.
For this project, it mainly means reading queries from the CLI.

## parser/
Turns raw SQL text into a parsed SQL structure.
Its job is only syntax-level parsing, not deeper validation.

## binder
Checks whether the parsed SQL actually makes sense.
It resolves tables and columns, checks aliases and aggregates, and rejects unsupported queries.

## catalog/
Stores and manages schema metadata.
This includes information about tables, columns, primary keys, foreign keys, and indexes.

## storage/
Handles low-level data storage.
It is responsible for bbolt access, row encoding, and RID management.

## planner/
Builds the query plan.
It creates the logical plan and applies simple optimization rules.

## executor/
Builds and runs the physical plan.
It performs the actual query operations such as scan, index scan, filter, join, sort, and aggregate.
Index metadata lives in the catalog, index maintenance lives with write statements, and index selection lives in the executor.

## statement/
Implements each SQL statement.
This is where the main behavior for CREATE, DROP, INSERT, DELETE, UPDATE, and SELECT is organized.

## format/
Handles output formatting.
It prepares query results, status messages, and execution timing in a readable form.

## shared/
Keeps small common definitions used across the project.
Mainly shared types and error definitions.

## docs/
Reserved for project documentation.
