package boltcluster

import "encoding/binary"

// Itob convert Integer to uint64 BigEndian byte sequence
func Itob8(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// Itob16 convert Integer to 8 byte length uint64 BigEndian byte sequence
func Itob16(v int) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint16(b, uint16(v))
	return b
}

// Itob32 convert Integer to 8 byte length uint64 BigEndian byte sequence
func Itob32(v int) []byte {
	b := make([]byte, 32)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

// Itob32 convert Integer to 8 byte length uint64 BigEndian byte sequence
func Itob64(v int) []byte {
	b := make([]byte, 64)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// Stob converts string to byte sequence
func Stob(s string) []byte {
	return []byte(s)
}

// Btoi convert btye slice into int
func Btoi(bytes []byte) int {
	return int(binary.BigEndian.Uint64(bytes))
}

// Btoui convert btye slice into uint
func Btoui(bytes []byte) uint {
	return uint(binary.BigEndian.Uint64(bytes))
}

// Btoui16 convert btye slice into uint16
func Btoui16(bytes []byte) uint16 {
	return binary.BigEndian.Uint16(bytes)
}

// Btoui32 convert btye slice into uint32
func Btoui32(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}

// Btoui64 convert btye slice into uint64
func Btoui64(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}

// Copy will copy a btye slice, this function is useful,
// when you want to use values from boltdb in outside of the transaction view
func Copy(bytes []byte) []byte {
	newBytes := make([]byte, 0, cap(bytes))
	return append(newBytes, bytes...)
}
