package sophie

import (
	"fmt"
	"testing"

	"github.com/daviddengcn/go-assert"
	"github.com/daviddengcn/go-villa"
)

func TestBasic(t *testing.T) {
	fn := FsPath{Fs: LocalFS, Path: "./test.kv"}
	defer villa.Path(fn.Path).Remove()

	keys := []String{
		"abc", "def",
	}
	vals := []VInt{
		2, 2013,
	}

	writer, err := NewKVWriter(fn)
	assert.NoErrorf(t, "NewKVWriter: %v", err)

	for i, key := range keys {
		val := vals[i]
		assert.NoErrorf(t, "Collect: %v", writer.Collect(key, val))
	}
	assert.NoErrorf(t, "writer.Close()", writer.Close())

	reader, err := NewKVReader(fn)
	assert.NoErrorf(t, "NewKVReader: %v", err)

	var key String
	var val VInt
	for i := 0; ; i++ {
		err := reader.Next(&key, &val)
		if err == EOF {
			break
		}
		assert.NoErrorf(t, "reader.Next: %v", err)
		assert.Equals(t, fmt.Sprintf("key[%d]", i), key, keys[i])
		assert.Equals(t, fmt.Sprintf("val[%d]", i), val, vals[i])
	}

	assert.NoErrorf(t, "reader.Close()", reader.Close())
}
