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
