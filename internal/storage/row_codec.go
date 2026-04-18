package storage

import (
	"encoding/json"

	"dbms-project/internal/shared"
)

// Value is one stored cell inside a row.
//
// We keep this explicit instead of using interface{} so row contents stay
// aligned with the project's limited SQL type system.
type Value struct {
	Type         shared.DataType `json:"type"`
	IntegerValue int64           `json:"integer_value,omitempty"`
	StringValue  string          `json:"string_value,omitempty"`
}

// Row is the stored row payload written into a table bucket value.
type Row []Value

func NewIntegerValue(v int64) Value {
	return Value{
		Type:         shared.TypeInteger,
		IntegerValue: v,
	}
}

func NewStringValue(v string) Value {
	return Value{
		Type:        shared.TypeString,
		StringValue: v,
	}
}

// EncodeRow turns one row into bytes for storage.
func EncodeRow(row Row) ([]byte, error) {
	return json.Marshal(row)
}

// DecodeRow turns stored bytes back into one row.
func DecodeRow(data []byte) (Row, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var row Row
	if err := json.Unmarshal(data, &row); err != nil {
		return nil, err
	}

	return row, nil
}

// EncodeIndexKey turns one cell value into a stable index key representation.
func EncodeIndexKey(value Value) ([]byte, error) {
	return json.Marshal(value)
}

// DecodeIndexKey turns a stored index key back into one typed cell value.
func DecodeIndexKey(data []byte) (Value, error) {
	if len(data) == 0 {
		return Value{}, nil
	}

	var value Value
	if err := json.Unmarshal(data, &value); err != nil {
		return Value{}, err
	}

	return value, nil
}

// EncodeRIDList turns an RID list into bytes for index bucket storage.
func EncodeRIDList(rids []RID) ([]byte, error) {
	return json.Marshal(rids)
}

// DecodeRIDList turns stored index payload bytes back into an RID list.
func DecodeRIDList(data []byte) ([]RID, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var rids []RID
	if err := json.Unmarshal(data, &rids); err != nil {
		return nil, err
	}

	return rids, nil
}
