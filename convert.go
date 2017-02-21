package boltcluster

import "encoding/binary"

// Itob convert Integer to uint64 BigEndian byte sequence
func Itob(v int, btyeLength int) []byte {
	b := make([]byte, btyeLength)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// Itob8 convert Integer to 8 byte length uint64 BigEndian byte sequence
func Itob8(v int) []byte {
	return Itob(v, 8)
}

// Itob16 convert Integer to 8 byte length uint64 BigEndian byte sequence
func Itob16(v int) []byte {
	return Itob(v, 16)
}

// Itob32 convert Integer to 8 byte length uint64 BigEndian byte sequence
func Itob32(v int) []byte {
	return Itob(v, 32)
}

// Stob converts string to byte sequence
func Stob(s string) []byte {
	return []byte(s)
}
