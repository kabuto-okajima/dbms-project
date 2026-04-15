package storage

import "encoding/binary"

// RID is the stable row identifier stored as a per-table uint64.
type RID uint64

// EncodeRID stores an RID in big-endian byte order.
func EncodeRID(rid RID) []byte {
	buf := make([]byte, 8) // -> []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(buf, uint64(rid))
	return buf
}

// DecodeRID reads an RID from big-endian bytes.
func DecodeRID(data []byte) RID {
	if len(data) < 8 {
		return 0
	}

	return RID(binary.BigEndian.Uint64(data))
}
