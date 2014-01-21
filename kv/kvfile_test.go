package kv

import (
	"fmt"
	"testing"

	"github.com/daviddengcn/go-assert"
	"github.com/daviddengcn/go-villa"
	"github.com/daviddengcn/sophie"
)

func TestBasic(t *testing.T) {
	fn := sophie.LocalFsPath("./test.kv")
	defer villa.Path(fn.Path).Remove()

	keys := []sophie.String{
		"abc", "def",
	}
	vals := []sophie.VInt{
		2, 2013,
	}

	writer, err := NewWriter(fn)
	assert.NoErrorf(t, "NewWriter: %v", err)

	for i, key := range keys {
		val := vals[i]
		assert.NoErrorf(t, "Collect: %v", writer.Collect(key, val))
	}
	assert.NoErrorf(t, "writer.Close()", writer.Close())

	reader, err := NewReader(fn)
	assert.NoErrorf(t, "NewReader: %v", err)

	var key sophie.String
	var val sophie.VInt
	for i := 0; ; i++ {
		err := reader.Next(&key, &val)
		if err == sophie.EOF {
			break
		}
		assert.NoErrorf(t, "reader.Next: %v", err)
		assert.Equals(t, fmt.Sprintf("key[%d]", i), key, keys[i])
		assert.Equals(t, fmt.Sprintf("val[%d]", i), val, vals[i])
	}

	assert.NoErrorf(t, "reader.Close()", reader.Close())
}
