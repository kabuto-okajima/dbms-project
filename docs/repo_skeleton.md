## repository structure
```
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
│   ├── index/
│   │   └── index.go
│   │
│   ├── planner/
│   │   ├── logical.go
│   │   ├── optimizer.go
│   │   └── physical.go
│   │
│   ├── executor/
│   │   ├── executor.go
│   │   └── operators.go
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

## index/
Handles index-related logic.
It builds indexes, updates them when data changes, and uses them during query processing.

## planner/
Builds the query plan.
It creates the logical plan, applies simple optimization rules, and turns the result into a physical execution plan.

## executor/
Runs the physical plan.
It performs the actual query operations such as scan, filter, join, sort, and aggregate.

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
