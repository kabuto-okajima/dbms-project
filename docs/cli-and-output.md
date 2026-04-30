# CLI and Output

## CLI Input
The main interface is an interactive CLI.

- the default database path is `dbms.db`
- passing one command-line argument changes the database path
- the primary prompt is `db> `
- the continuation prompt is `...> `
- input is accumulated until the current statement ends with `;`
- `exit` or `quit` exits only when no partial statement is in progress

## Output
`SELECT` results are rendered as an ASCII table.

The table output includes:
- column headers
- row values
- row count
- elapsed statement time

DDL and write-DML statements render:
- a status message, or `ok` if no message is present
- elapsed statement time

Errors are printed to stderr as:

```text
Error: <message>
```
