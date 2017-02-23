package boltcluster_test

import (
	"bytes"
	"testing"

	"github.com/LxDB/boltcluster"
	. "github.com/LxDB/testing"
)

func TestItob8(t *testing.T) {
	bs := boltcluster.Itob8(5)

	if !bytes.Equal(bs, []byte{0, 0, 0, 0, 0, 0, 0, 5}) {
		t.Log("transformation failed")
		t.Log(bs)
		t.Fail()
	}
}

func TestItob16(t *testing.T) {
	bs := boltcluster.Itob16(5)

	if !bytes.Equal(bs, []byte{0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Log("transformation failed")
		t.Log(bs)
		t.Fail()
	}
}

func TestItob32(t *testing.T) {
	bs := boltcluster.Itob32(5)

	if !bytes.Equal(bs, []byte{0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Log("transformation failed")
		t.Log(bs)
		t.Fail()
	}
}

func TestItob64(t *testing.T) {
	bs := boltcluster.Itob64(5)

	if !bytes.Equal(bs, []byte{0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Log("transformation failed")
		t.Log(bs)
		t.Fail()
	}
}

func TestStob(t *testing.T) {
	bs := boltcluster.Stob("hello")

	if !bytes.Equal(bs, []byte{104, 101, 108, 108, 111}) {
		t.Log("transformation failed")
		t.Log(bs)
		t.Fail()
	}
}

func TestStobUTF8Safe(t *testing.T) {
	bs := boltcluster.Stob(`⌘`)

	if string(bs) != `⌘` {
		t.Log("transformation failed")
		t.Log(bs)
		t.Fail()
	}
}

func TestBtoi(t *testing.T) {
	intValue := boltcluster.Btoi([]byte{0, 0, 0, 0, 0, 0, 0, 5})

	if intValue != 5 {
		t.Log("transformation failed")
		t.Log(intValue)
		t.Fail()
	}
}

func TestBtoui(t *testing.T) {
	intValue := boltcluster.Btoui([]byte{0, 0, 0, 0, 0, 0, 0, 5})

	if intValue != uint(5) {
		t.Log("transformation failed")
		t.Log(intValue)
		t.Fail()
	}
}

func TestBtoui16(t *testing.T) {
	intValue := boltcluster.Btoui16(boltcluster.Itob16(5))

	if intValue != uint16(5) {
		t.Log("transformation failed")
		t.Log(intValue)
		t.Fail()
	}
}

func TestBtoui32(t *testing.T) {
	intValue := boltcluster.Btoui32(boltcluster.Itob32(5))

	if intValue != uint32(5) {
		t.Log("transformation failed")
		t.Log(intValue)
		t.Fail()
	}
}

func TestBtoui64(t *testing.T) {
	intValue := boltcluster.Btoui64(boltcluster.Itob64(5))

	if intValue != uint64(5) {
		t.Log("transformation failed")
		t.Log(intValue)
		t.Fail()
	}
}

func TestCopy(t *testing.T) {
	slice := boltcluster.Copy([]byte{0, 0, 0, 0, 0, 0, 0, 5})

	if !TestEqBytes(slice, []byte{0, 0, 0, 0, 0, 0, 0, 5}) {
		t.Log("transformation failed")
		t.Log(slice)
		t.Fail()
	}
}
