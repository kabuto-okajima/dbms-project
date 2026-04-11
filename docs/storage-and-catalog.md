# Storage and Catalog

## Storage Choice
The system uses the `bbolt` library as the storage layer.

It stores:
- system catalog buckets
- table data buckets
- index buckets

There is one database file for the whole system.

## Catalog Metadata
Logical metadata:
- `TABLES(table_name, table_bucket, next_rid)`
- `COLUMNS(table_name, column_name, type, ordinal_position)`
- `PRIMARY_KEYS(table_name, column_name)`
- `FOREIGN_KEYS(table_name, column_name, ref_table, ref_column, on_delete, on_update)`
- `INDEXES(index_name, table_name, column_name, is_unique, index_bucket)`

These are implemented as internal buckets inside the same database file.

## Bucket Layout
Example bucket names:
- `catalog/TABLES`
- `catalog/COLUMNS`
- `catalog/PRIMARY_KEYS`
- `catalog/FOREIGN_KEYS`
- `catalog/INDEXES`
- `table/Students`
- `index/idx_students_age`
- `index/pk_students_id`

## Row Storage
Each row has a stable RID.

- table bucket format: `RID -> encoded row`
- RID format: per-table `uint64`
- RIDs come from `TABLES.next_rid`
- RIDs are encoded as big-endian bytes
- RIDs are not reused in v1

Example:
- `RID=1 -> [1, "Alice", 20, 10]`
- `RID=2 -> [2, "Bob", 21, 10]`

## Index Model
Only single-column indexes are supported in the baseline memo.

Each index maps:
- `key -> list of RIDs`

Example:
- `20 -> [RID 1, RID 5, RID 9]`
- `21 -> [RID 2, RID 8]`

For unique indexes, the RID list always has one value.

## Index Rules
- normal indexes allow duplicate keys
- primary-key-backed indexes are unique
- `INSERT` adds the RID under the new key
- `DELETE` removes the RID from the key
- `UPDATE` removes the old RID entry and adds the new one
- if a RID list becomes empty, the index key is removed

## CREATE INDEX Behavior
If a user creates an index on a table that already has rows:
1. scan the existing rows
2. build the index entries
3. make the index visible only if the build succeeds

If the build fails, the index is not kept.

## When Indexes Are Used
Index scan is used only for simple predicates on one indexed column:
- `=`
- `>`
- `>=`
- `<`
- `<=`

`!=` may still fall back to table scan.

## NULL Handling
`NULL` is not supported in v1.
Statements using `NULL`, `IS NULL`, or `IS NOT NULL` are rejected as unsupported.
