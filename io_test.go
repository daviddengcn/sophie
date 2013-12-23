package sophie

import (
	"fmt"
	"testing"

	"github.com/daviddengcn/go-assert"
	"github.com/daviddengcn/go-villa"
)

func readWrite(t *testing.T, sa, sb Sophier, outBytes int) {
	var buf villa.ByteSlice
	assert.NoErrorf(t, fmt.Sprintf("readWrite(%v): sa.WriteTo failed: %%v",
		sa), sa.WriteTo(&buf))

	if outBytes >= 0 {
		assert.Equals(t, fmt.Sprintf("readWrite(%v): buf.Len", sa), len(buf),
			outBytes)
	}

	assert.NoErrorf(t, fmt.Sprintf("readWrite(%v): sb.ReadFrom failed: %%v",
		sa), sb.ReadFrom(&buf, len(buf)))
}

func TestBasicSophieTypes(t *testing.T) {
	// Test of nt32
	var i32a, i32b Int32
	i32a = 1234
	readWrite(t, &i32a, &i32b, 4)
	assert.Equals(t, "i32b", i32b, i32a)

	i32a = -1234
	readWrite(t, &i32a, &i32b, 4)
	assert.Equals(t, "i32b", i32b, i32a)

	// Test of String, and ByteArray
	var sa, sb String
	sa = ""
	readWrite(t, &sa, &sb, 1)
	assert.Equals(t, "sb", sb, sa)

	sa = "Hello"
	readWrite(t, &sa, &sb, 6)
	assert.Equals(t, "sb", sb, sa)

	sa = ""
	for len(sa) < 127 {
		sa += "a"
	}
	readWrite(t, &sa, &sb, 128)
	assert.Equals(t, "sb", sb, sa)
	sa = ""

	for len(sa) < 128 {
		sa += "a"
	}
	readWrite(t, &sa, &sb, 130)
	assert.Equals(t, "sb", sb, sa)
}

func TestVInt(t *testing.T) {
	var via, vib VInt
	via = 0
	readWrite(t, &via, &vib, 1)
	assert.Equals(t, "vib", vib, via)
	via = 127
	readWrite(t, &via, &vib, 1)
	assert.Equals(t, "vib", vib, via)
	via = 128
	readWrite(t, &via, &vib, 2)
	assert.Equals(t, "vib", vib, via)
	via = 0X3FFE
	readWrite(t, &via, &vib, 2)
	assert.Equals(t, "vib", vib, via)
	via = 0X4002
	readWrite(t, &via, &vib, 3)
	assert.Equals(t, "vib", vib, via)
	via = 0X1FF2FF
	readWrite(t, &via, &vib, 3)
	assert.Equals(t, "vib", vib, via)
	via = 0X20FF01
	readWrite(t, &via, &vib, 4)
	assert.Equals(t, "vib", vib, via)
	via = 0X0FFFFF01
	readWrite(t, &via, &vib, 4)
	assert.Equals(t, "vib", vib, via)
	via = 0X10000005
	readWrite(t, &via, &vib, 5)
	assert.Equals(t, "vib", vib, via)
	via = 0X7FFFFFF01
	readWrite(t, &via, &vib, 5)
	assert.Equals(t, "vib", vib, via)
	via = 0X800000005
	readWrite(t, &via, &vib, 6)
	assert.Equals(t, "vib", vib, via)
}

func TestRawVInt(t *testing.T) {
	var rvia, rvib RawVInt
	rvia = 0
	readWrite(t, &rvia, &rvib, 0)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 1
	readWrite(t, &rvia, &rvib, 1)
	rvia = 0xFF
	readWrite(t, &rvia, &rvib, 1)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0x100
	readWrite(t, &rvia, &rvib, 2)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0XFFFE
	readWrite(t, &rvia, &rvib, 2)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0X10001
	readWrite(t, &rvia, &rvib, 3)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0XFFFFFF
	readWrite(t, &rvia, &rvib, 3)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0X1000000
	readWrite(t, &rvia, &rvib, 4)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0XFFFFFFFF
	readWrite(t, &rvia, &rvib, 4)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0X100000000
	readWrite(t, &rvia, &rvib, 5)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0XFFFFFFFFFF
	readWrite(t, &rvia, &rvib, 5)
	assert.Equals(t, "rvib", rvib, rvia)
	rvia = 0X10000000000
	readWrite(t, &rvia, &rvib, 6)
	assert.Equals(t, "rvib", rvib, rvia)
}