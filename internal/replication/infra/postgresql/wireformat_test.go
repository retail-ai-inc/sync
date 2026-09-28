package postgresql

import (
	"encoding/binary"
)

// The pgoutput wire format, so the reader can be driven with the same bytes
// PostgreSQL would send it.
//
// These outlived the rewrite: they are the reason the decode can be tested at
// all, and they were the only part of the old test file that did not depend on
// the shape that has gone.

func u32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func u16(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func beginBytes(finalLSN uint64) []byte {
	out := []byte{'B'}
	out = append(out, u64(finalLSN)...)
	out = append(out, u64(0)...) // commit timestamp
	out = append(out, u32(7)...) // xid
	return out
}

func commitBytes(commitLSN, endLSN uint64) []byte {
	out := []byte{'C', 0}
	out = append(out, u64(commitLSN)...)
	out = append(out, u64(endLSN)...)
	out = append(out, u64(0)...) // commit timestamp
	return out
}

func relationBytes(relID uint32, namespace, name string, columns ...string) []byte {
	out := []byte{'R'}
	out = append(out, u32(relID)...)
	out = append(out, append([]byte(namespace), 0)...)
	out = append(out, append([]byte(name), 0)...)
	out = append(out, 'd') // replica identity: default
	out = append(out, u16(uint16(len(columns)))...)
	for _, c := range columns {
		out = append(out, 1) // flagged as part of the key
		out = append(out, append([]byte(c), 0)...)
		out = append(out, u32(25)...) // text
		out = append(out, u32(0xFFFFFFFF)...)
	}
	return out
}

// tupleBytes encodes a tuple body: text columns for non-nil values, NULL
// otherwise.
func tupleBytes(values ...*string) []byte {
	out := u16(uint16(len(values)))
	for _, v := range values {
		if v == nil {
			out = append(out, 'n')
			continue
		}
		out = append(out, 't')
		out = append(out, u32(uint32(len(*v)))...)
		out = append(out, []byte(*v)...)
	}
	return out
}

func insertBytes(relID uint32, values ...*string) []byte {
	out := []byte{'I'}
	out = append(out, u32(relID)...)
	out = append(out, 'N')
	out = append(out, tupleBytes(values...)...)
	return out
}

func deleteBytes(relID uint32, values ...*string) []byte {
	out := []byte{'D'}
	out = append(out, u32(relID)...)
	out = append(out, 'O') // old tuple, full row
	out = append(out, tupleBytes(values...)...)
	return out
}

func updateBytes(relID uint32, oldValues, newValues []*string) []byte {
	out := []byte{'U'}
	out = append(out, u32(relID)...)
	if oldValues != nil {
		out = append(out, 'O')
		out = append(out, tupleBytes(oldValues...)...)
	}
	out = append(out, 'N')
	out = append(out, tupleBytes(newValues...)...)
	return out
}
