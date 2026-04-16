package catalog

import (
	"bytes"
	"encoding/json"
	"sort"

	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

const (
	TablesBucket      = "catalog/TABLES"
	ColumnsBucket     = "catalog/COLUMNS"
	PrimaryKeysBucket = "catalog/PRIMARY_KEYS"
	ForeignKeysBucket = "catalog/FOREIGN_KEYS"
	IndexesBucket     = "catalog/INDEXES"
)

// Manager is the catalog entry point.
//
// Later, this type will own schema-related operations such as:
// - creating and dropping tables
// - creating and dropping indexes
// - reading table metadata for binder / DML / planner use
// - allocating the next RID for a table
type Manager struct{}

func NewManager() *Manager {
	return &Manager{}
}

// TableRecord matches:
// TABLES(table_name, table_bucket, next_rid)
type TableRecord struct {
	TableName   string
	TableBucket string
	NextRID     uint64
}

// ColumnRecord matches:
// COLUMNS(table_name, column_name, type, ordinal_position)
type ColumnRecord struct {
	TableName       string
	ColumnName      string
	Type            shared.DataType
	OrdinalPosition int
}

// PrimaryKeyRecord matches:
// PRIMARY_KEYS(table_name, column_name)
type PrimaryKeyRecord struct {
	TableName  string
	ColumnName string
}

// ForeignKeyRecord matches:
// FOREIGN_KEYS(table_name, column_name, ref_table, ref_column, on_delete, on_update)
type ForeignKeyRecord struct {
	TableName  string
	ColumnName string
	RefTable   string
	RefColumn  string
	OnDelete   string
	OnUpdate   string
}

// IndexRecord matches:
// INDEXES(index_name, table_name, column_name, is_unique, index_bucket)
type IndexRecord struct {
	IndexName   string
	TableName   string
	ColumnName  string
	IsUnique    bool
	IndexBucket string
}

// TableMetadata is the assembled view of one table schema.
//
// The raw catalog buckets split metadata across several logical tables.
// This struct is the shape higher layers will usually want to read.
type TableMetadata struct {
	Name        string
	TableBucket string
	NextRID     uint64
	Columns     []shared.ColumnDefinition
	PrimaryKey  []string
	ForeignKeys []shared.ForeignKeyDefinition
}

// IndexMetadata is the assembled view of one index definition.
type IndexMetadata struct {
	Name        string
	TableName   string
	ColumnName  string
	IsUnique    bool
	IndexBucket string
}

// ReferencingForeignKeyMetadata describes one child-table FK that points at a target table.
// This is used for FK RESTRICT checks when deleting rows from the parent table.
type ReferencingForeignKeyMetadata struct {
	TableName  string
	ColumnName string
	RefTable   string
	RefColumn  string
}

// Bootstrap ensures the internal catalog buckets exist.
func (m *Manager) Bootstrap(tx *storage.Tx) error {
	for _, bucketName := range []string{
		TablesBucket,
		ColumnsBucket,
		PrimaryKeysBucket,
		ForeignKeysBucket,
		IndexesBucket,
	} {
		// Create the bucket if it does not already exist.
		if err := tx.CreateBucket(bucketName); err != nil {
			return err
		}
	}

	return nil
}

// CreateTable writes the catalog metadata for one table definition.
func (m *Manager) CreateTable(tx *storage.Tx, def shared.TableDefinition) error {
	if err := m.Bootstrap(tx); err != nil {
		return err
	}
	if err := m.validateCreateTableDefinition(tx, def); err != nil {
		return err
	}

	existing, err := tx.Get(TablesBucket, []byte(def.Name))
	if err != nil {
		return err
	}
	if existing != nil {
		return shared.NewError(shared.ErrAlreadyExists, "create table: table %q already exists", def.Name)
	}

	tableBucket := tableBucketName(def.Name)
	if err := tx.CreateBucket(tableBucket); err != nil {
		return err
	}

	// 1. Write the TABLES entry first.
	tableRecord := TableRecord{
		TableName:   def.Name,
		TableBucket: tableBucket,
		NextRID:     1,
	}
	if err := putJSON(tx, TablesBucket, []byte(def.Name), tableRecord); err != nil {
		return err
	}

	// 2. Then write the dependent metadata rows.
	for i, column := range def.Columns {
		columnRecord := ColumnRecord{
			TableName:       def.Name,
			ColumnName:      column.Name,
			Type:            column.Type,
			OrdinalPosition: i,
		}
		if err := putJSON(tx, ColumnsBucket, compositeKey(def.Name, column.Name), columnRecord); err != nil {
			return err
		}
	}

	for _, columnName := range def.PrimaryKey {
		pkRecord := PrimaryKeyRecord{
			TableName:  def.Name,
			ColumnName: columnName,
		}
		if err := putJSON(tx, PrimaryKeysBucket, compositeKey(def.Name, columnName), pkRecord); err != nil {
			return err
		}
	}

	for _, fk := range def.ForeignKeys {
		fkRecord := ForeignKeyRecord{
			TableName:  def.Name,
			ColumnName: fk.ColumnName,
			RefTable:   fk.RefTable,
			RefColumn:  fk.RefColumn,
			OnDelete:   "RESTRICT",
			OnUpdate:   "RESTRICT",
		}
		if err := putJSON(tx, ForeignKeysBucket, compositeKey(def.Name, fk.ColumnName), fkRecord); err != nil {
			return err
		}
	}

	return nil
}

// CreateIndex validates the target and writes index metadata plus the physical bucket.
func (m *Manager) CreateIndex(tx *storage.Tx, def shared.IndexDefinition) error {
	if err := m.Bootstrap(tx); err != nil {
		return err
	}
	if err := m.validateCreateIndexDefinition(tx, def); err != nil {
		return err
	}

	table, err := m.GetTable(tx, def.TableName)
	if err != nil {
		return err
	}
	columnOrdinal, err := ColumnOrdinal(table, def.ColumnName)
	if err != nil {
		return err
	}

	indexBucket := indexBucketName(def.Name)
	if err := tx.CreateBucket(indexBucket); err != nil {
		return err
	}

	entries, err := m.buildIndexEntries(tx, table, columnOrdinal, def.IsUnique)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		payload, err := storage.EncodeRIDList(entry.RIDs)
		if err != nil {
			return err
		}
		if err := tx.Put(indexBucket, entry.Key, payload); err != nil {
			return err
		}
	}

	record := IndexRecord{
		IndexName:   def.Name,
		TableName:   def.TableName,
		ColumnName:  def.ColumnName,
		IsUnique:    def.IsUnique,
		IndexBucket: indexBucket,
	}
	return putJSON(tx, IndexesBucket, indexCatalogKey(def.Name), record)
}

// GetTable loads the assembled metadata view for one table.
// It is not reading the table’s actual row data.
// It is reading the system catalog entries that describe the table.
func (m *Manager) GetTable(tx *storage.Tx, tableName string) (*TableMetadata, error) {
	// Start from the base TABLES row, which tells us the table bucket and RID state.
	data, err := tx.Get(TablesBucket, []byte(tableName))
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, shared.NewError(shared.ErrNotFound, "get table: table %q does not exist", tableName)
	}

	var tableRecord TableRecord
	if err := json.Unmarshal(data, &tableRecord); err != nil {
		return nil, err
	}

	metadata := &TableMetadata{
		Name:        tableRecord.TableName,
		TableBucket: tableRecord.TableBucket,
		NextRID:     tableRecord.NextRID,
	}

	type orderedColumn struct {
		definition shared.ColumnDefinition
		ordinal    int
	}

	var orderedColumns []orderedColumn
	columnsPrefix := compositePrefix(tableName)
	// COLUMNS uses composite keys (i.e., a table has multiple columns), so we scan the bucket and keep only this table's rows.
	if err := tx.ForEach(ColumnsBucket, func(key, value []byte) error {
		if !bytes.HasPrefix(key, columnsPrefix) {
			return nil
		}

		var record ColumnRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return err
		}

		orderedColumns = append(orderedColumns, orderedColumn{
			definition: shared.ColumnDefinition{
				Name: record.ColumnName,
				Type: record.Type,
			},
			ordinal: record.OrdinalPosition,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	// Rebuild the original schema order before returning the assembled metadata.
	sort.Slice(orderedColumns, func(i, j int) bool {
		return orderedColumns[i].ordinal < orderedColumns[j].ordinal
	})
	for _, column := range orderedColumns {
		metadata.Columns = append(metadata.Columns, column.definition)
	}

	pkPrefix := compositePrefix(tableName)
	// PRIMARY_KEYS is also split across rows, so we gather all matching entries.
	if err := tx.ForEach(PrimaryKeysBucket, func(key, value []byte) error {
		if !bytes.HasPrefix(key, pkPrefix) {
			return nil
		}

		var record PrimaryKeyRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return err
		}

		metadata.PrimaryKey = append(metadata.PrimaryKey, record.ColumnName)
		return nil
	}); err != nil {
		return nil, err
	}

	fkPrefix := compositePrefix(tableName)
	// FOREIGN_KEYS is turned back into the shared schema-facing FK shape here.
	if err := tx.ForEach(ForeignKeysBucket, func(key, value []byte) error {
		if !bytes.HasPrefix(key, fkPrefix) {
			return nil
		}

		var record ForeignKeyRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return err
		}

		metadata.ForeignKeys = append(metadata.ForeignKeys, shared.ForeignKeyDefinition{
			ColumnName: record.ColumnName,
			RefTable:   record.RefTable,
			RefColumn:  record.RefColumn,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return metadata, nil
}

// GetIndex loads one index definition from the INDEXES catalog bucket.
func (m *Manager) GetIndex(tx *storage.Tx, indexName string) (*IndexMetadata, error) {
	data, err := tx.Get(IndexesBucket, []byte(indexName))
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, shared.NewError(shared.ErrNotFound, "get index: index %q does not exist", indexName)
	}

	var record IndexRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}

	return &IndexMetadata{
		Name:        record.IndexName,
		TableName:   record.TableName,
		ColumnName:  record.ColumnName,
		IsUnique:    record.IsUnique,
		IndexBucket: record.IndexBucket,
	}, nil
}

// ListIndexesByTable loads all index metadata rows for one table.
func (m *Manager) ListIndexesByTable(tx *storage.Tx, tableName string) ([]IndexMetadata, error) {
	var indexes []IndexMetadata

	if err := tx.ForEach(IndexesBucket, func(_ []byte, value []byte) error {
		var record IndexRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return err
		}
		if record.TableName != tableName {
			return nil
		}

		indexes = append(indexes, IndexMetadata{
			Name:        record.IndexName,
			TableName:   record.TableName,
			ColumnName:  record.ColumnName,
			IsUnique:    record.IsUnique,
			IndexBucket: record.IndexBucket,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].Name < indexes[j].Name
	})

	return indexes, nil
}

// ListReferencingForeignKeys finds foreign keys in other tables that point at the target table.
func (m *Manager) ListReferencingForeignKeys(tx *storage.Tx, tableName string) ([]shared.ForeignKeyDefinition, error) {
	var foreignKeys []shared.ForeignKeyDefinition

	if err := tx.ForEach(ForeignKeysBucket, func(_ []byte, value []byte) error {
		var record ForeignKeyRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return err
		}
		if record.RefTable != tableName {
			return nil
		}

		foreignKeys = append(foreignKeys, shared.ForeignKeyDefinition{
			ColumnName: record.ColumnName,
			RefTable:   record.RefTable,
			RefColumn:  record.RefColumn,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return foreignKeys, nil
}

// ListReferencingForeignKeyMetadata finds foreign keys in other tables that point at the target table, returning the child-table metadata needed for FK RESTRICT checks.
func (m *Manager) ListReferencingForeignKeyMetadata(tx *storage.Tx, tableName string) ([]ReferencingForeignKeyMetadata, error) {
	var foreignKeys []ReferencingForeignKeyMetadata

	if err := tx.ForEach(ForeignKeysBucket, func(_ []byte, value []byte) error {
		var record ForeignKeyRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return err
		}
		if record.RefTable != tableName {
			return nil
		}

		foreignKeys = append(foreignKeys, ReferencingForeignKeyMetadata{
			TableName:  record.TableName,
			ColumnName: record.ColumnName,
			RefTable:   record.RefTable,
			RefColumn:  record.RefColumn,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return foreignKeys, nil
}

// DropIndex removes index storage and catalog metadata for one index.
func (m *Manager) DropIndex(tx *storage.Tx, indexName string) error {
	if err := m.Bootstrap(tx); err != nil {
		return err
	}

	indexMeta, err := m.GetIndex(tx, indexName)
	if err != nil {
		return err
	}

	if err := tx.DeleteBucket(indexMeta.IndexBucket); err != nil {
		return err
	}

	if err := tx.Delete(IndexesBucket, indexCatalogKey(indexName)); err != nil {
		return err
	}

	return nil
}

// DropTable removes catalog metadata for one table.
func (m *Manager) DropTable(tx *storage.Tx, tableName string) error {
	if err := m.Bootstrap(tx); err != nil {
		return err
	}

	referencingFKs, err := m.ListReferencingForeignKeys(tx, tableName)
	if err != nil {
		return err
	}
	if len(referencingFKs) > 0 {
		return shared.NewError(shared.ErrConstraintViolation, "drop table: table %q is still referenced by a foreign key", tableName)
	}

	metadata, err := m.GetTable(tx, tableName)
	if err != nil {
		return err
	}

	indexes, err := m.ListIndexesByTable(tx, tableName)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		if err := m.DropIndex(tx, index.Name); err != nil {
			return err
		}
	}

	// Remove the physical table bucket first because it belongs only to this table.
	if err := tx.DeleteBucket(metadata.TableBucket); err != nil {
		return err
	}

	// Remove the main TABLES row.
	if err := tx.Delete(TablesBucket, []byte(tableName)); err != nil {
		return err
	}

	// Remove all dependent metadata rows for this table.
	for _, column := range metadata.Columns {
		if err := tx.Delete(ColumnsBucket, compositeKey(tableName, column.Name)); err != nil {
			return err
		}
	}

	for _, columnName := range metadata.PrimaryKey {
		if err := tx.Delete(PrimaryKeysBucket, compositeKey(tableName, columnName)); err != nil {
			return err
		}
	}

	for _, fk := range metadata.ForeignKeys {
		if err := tx.Delete(ForeignKeysBucket, compositeKey(tableName, fk.ColumnName)); err != nil {
			return err
		}
	}

	return nil
}

// NextRID allocates the next row id for a table.
func (m *Manager) NextRID(tx *storage.Tx, tableName string) (storage.RID, error) {
	if err := m.Bootstrap(tx); err != nil {
		return 0, err
	}

	// Read the current TABLES row so we can return the old next_rid
	// and persist the incremented value back into the catalog.
	data, err := tx.Get(TablesBucket, []byte(tableName))
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, shared.NewError(shared.ErrNotFound, "next rid: table %q does not exist", tableName)
	}

	var record TableRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return 0, err
	}

	// Return the current next_rid as the stable RID for this new row,
	nextRID := storage.RID(record.NextRID)
	// and increment the next_rid in the catalog for the next allocation.
	record.NextRID++

	if err := putJSON(tx, TablesBucket, []byte(tableName), record); err != nil {
		return 0, err
	}

	return nextRID, nil
}

func putJSON(tx *storage.Tx, bucketName string, key []byte, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return tx.Put(bucketName, key, data)
}

func compositeKey(left, right string) []byte {
	return []byte(left + "\x00" + right)
}

func compositePrefix(left string) []byte {
	return []byte(left + "\x00")
}

type builtIndexEntry struct {
	Key  []byte
	RIDs []storage.RID
}

// buildIndexEntries scans all existing rows in a table and prepare the in-memory index contents before writing them to the index bucket
// index entry: index key -> list of RIDs
func (m *Manager) buildIndexEntries(tx *storage.Tx, table *TableMetadata, columnOrdinal int, unique bool) ([]builtIndexEntry, error) {
	entryMap := map[string]*builtIndexEntry{}

	if err := tx.ForEach(table.TableBucket, func(key, value []byte) error {
		row, err := storage.DecodeRow(value)
		if err != nil {
			return err
		}
		if columnOrdinal < 0 || columnOrdinal >= len(row) {
			return shared.NewError(shared.ErrInvalidDefinition, "create index: row is missing indexed column %q", table.Columns[columnOrdinal].Name)
		}

		indexKey, err := storage.EncodeIndexKey(row[columnOrdinal])
		if err != nil {
			return err
		}
		mapKey := string(indexKey)
		entry, ok := entryMap[mapKey]
		if !ok {
			entry = &builtIndexEntry{Key: indexKey}
			entryMap[mapKey] = entry
		}
		if unique && len(entry.RIDs) > 0 {
			return shared.NewError(shared.ErrConstraintViolation, "create index: duplicate key found while building unique index %q", table.Columns[columnOrdinal].Name)
		}
		entry.RIDs = append(entry.RIDs, storage.DecodeRID(key))
		return nil
	}); err != nil {
		return nil, err
	}

	entries := make([]builtIndexEntry, 0, len(entryMap))
	for _, entry := range entryMap {
		entries = append(entries, *entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key) < string(entries[j].Key)
	})
	return entries, nil
}

func tableHasColumn(table *TableMetadata, columnName string) bool {
	_, err := ColumnOrdinal(table, columnName)
	return err == nil
}

func indexBucketName(indexName string) string {
	return "index/" + indexName
}

func indexCatalogKey(indexName string) []byte {
	return []byte(indexName)
}

func primaryKeyIndexName(tableName, columnName string) string {
	return "pk_" + tableName + "_" + columnName
}

func tableBucketName(tableName string) string {
	return "table/" + tableName
}
func (m *Manager) validateCreateIndexDefinition(tx *storage.Tx, def shared.IndexDefinition) error {
	if def.Name == "" {
		return shared.NewError(shared.ErrInvalidDefinition, "create index: index name is required")
	}
	if def.TableName == "" {
		return shared.NewError(shared.ErrInvalidDefinition, "create index: table name is required")
	}
	if def.ColumnName == "" {
		return shared.NewError(shared.ErrInvalidDefinition, "create index: column name is required")
	}

	existing, err := tx.Get(IndexesBucket, indexCatalogKey(def.Name))
	if err != nil {
		return err
	}
	if existing != nil {
		return shared.NewError(shared.ErrAlreadyExists, "create index: index %q already exists", def.Name)
	}

	table, err := m.GetTable(tx, def.TableName)
	if err != nil {
		return err
	}
	if !tableHasColumn(table, def.ColumnName) {
		return shared.NewError(shared.ErrNotFound, "create index: column %q does not exist on table %q", def.ColumnName, def.TableName)
	}

	indexes, err := m.ListIndexesByTable(tx, def.TableName)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		if index.ColumnName == def.ColumnName {
			return shared.NewError(shared.ErrAlreadyExists, "create index: table %q already has an index on column %q", def.TableName, def.ColumnName)
		}
	}

	return nil
}

func (m *Manager) validateCreateTableDefinition(tx *storage.Tx, def shared.TableDefinition) error {
	if def.Name == "" {
		return shared.NewError(shared.ErrInvalidDefinition, "create table: table name is required")
	}
	if len(def.Columns) == 0 {
		return shared.NewError(shared.ErrInvalidDefinition, "create table: table %q must have at least one column", def.Name)
	}

	columnNames := make(map[string]struct{}, len(def.Columns))
	for _, column := range def.Columns {
		if column.Name == "" {
			return shared.NewError(shared.ErrInvalidDefinition, "create table: column name is required")
		}
		if _, exists := columnNames[column.Name]; exists {
			return shared.NewError(shared.ErrInvalidDefinition, "create table: duplicate column %q", column.Name)
		}
		columnNames[column.Name] = struct{}{}

		switch column.Type {
		case shared.TypeInteger, shared.TypeString:
		default:
			return shared.NewError(shared.ErrInvalidDefinition, "create table: unsupported type %q for column %q", column.Type, column.Name)
		}
	}

	// For simplicity, we only support single-column primary keys in v1.
	if len(def.PrimaryKey) > 1 {
		return shared.NewError(shared.ErrInvalidDefinition, "create table: composite primary keys are unsupported in v1")
	}

	for _, columnName := range def.PrimaryKey {
		if _, exists := columnNames[columnName]; !exists {
			return shared.NewError(shared.ErrInvalidDefinition, "create table: primary key column %q does not exist", columnName)
		}
	}

	for _, fk := range def.ForeignKeys {
		if fk.ColumnName == "" {
			return shared.NewError(shared.ErrInvalidDefinition, "create table: foreign key column name is required")
		}
		if _, exists := columnNames[fk.ColumnName]; !exists {
			return shared.NewError(shared.ErrInvalidDefinition, "create table: foreign key column %q does not exist", fk.ColumnName)
		}

		refTable, err := m.GetTable(tx, fk.RefTable)
		if err != nil {
			return shared.NewError(shared.ErrNotFound, "create table: referenced table %q does not exist", fk.RefTable)
		}
		if !tableHasColumn(refTable, fk.RefColumn) {
			return shared.NewError(shared.ErrNotFound, "create table: referenced column %q does not exist on table %q", fk.RefColumn, fk.RefTable)
		}
	}

	return nil
}
