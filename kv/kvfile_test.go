package kv

import (
	"fmt"
	"testing"

	"github.com/golangplus/testing/assert"

	"github.com/daviddengcn/go-villa"
	"github.com/daviddengcn/sophie"
)

func TestReaderWriter(t *testing.T) {
	fn := sophie.LocalFsPath("./test.kv")
	defer villa.Path(fn.Path).Remove()

	keys := []sophie.String{
		"abc", "def",
	}
	vals := []sophie.VInt{
		2, 2013,
	}

	writer, err := NewWriter(fn)
	assert.NoError(t, err)

	for i, key := range keys {
		val := vals[i]
		assert.NoError(t, writer.Collect(key, val))
	}
	assert.NoError(t, writer.Close())

	reader, err := NewReader(fn)
	assert.NoError(t, err)

	var key sophie.String
	var val sophie.VInt
	for i := 0; ; i++ {
		err := reader.Next(&key, &val)
		if err == sophie.EOF {
			break
		}
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("key[%d]", i), key, keys[i])
		assert.Equal(t, fmt.Sprintf("val[%d]", i), val, vals[i])
	}

	assert.NoError(t, reader.Close())
}

func TestReadAsByteOffsWriteByteOffs(t *testing.T) {
	fn := sophie.LocalFsPath("./test.kv")
	defer villa.Path(fn.Path).Remove()

	keyLens := []int{
		1, 2, 3, 4,
	}
	valLens := []int{
		5, 6, 7, 8,
	}
	assert.Equal(t, "len(keyLens)", len(keyLens), len(valLens))

	var keyOffs, keyEnds, valOffs, valEnds []int
	off := 0
	for i, keyLen := range keyLens {
		valLen := valLens[i]

		keyOffs = append(keyOffs, off)
		off += keyLen
		keyEnds = append(keyEnds, off)

		valOffs = append(valOffs, off)
		off += valLen
		valEnds = append(valEnds, off)
	}
	buffer := make([]byte, off)
	for i := range buffer {
		buffer[i] = byte(i)
	}
	assert.NoError(t, WriteByteOffs(fn, buffer, keyOffs, keyEnds, valOffs, valEnds))

	readBuffer, readKeyOffs, readKeyEnds, readValOffs, readValEnds, err := ReadAsByteOffs(fn)
	assert.NoError(t, err)
	assert.Equal(t, "len(keyOffs)", len(readKeyOffs), len(keyLens))
	assert.Equal(t, "len(keyEnds)", len(readKeyEnds), len(keyLens))
	assert.Equal(t, "len(valOffs)", len(readValOffs), len(keyLens))
	assert.Equal(t, "len(valEnds)", len(readValEnds), len(keyLens))
	for i, keyLen := range keyLens {
		valLen := valLens[i]

		assert.Equal(t, fmt.Sprintf("keyLen[%d]", i), readKeyEnds[i]-readKeyOffs[i], keyLen)
		assert.Equal(t, fmt.Sprintf("valLen[%d]", i), readValEnds[i]-readValOffs[i], valLen)

		assert.StringEqual(t, fmt.Sprintf("key[%d]", i), readBuffer[readKeyOffs[i]:readKeyEnds[i]], buffer[keyOffs[i]:keyEnds[i]])
		assert.StringEqual(t, fmt.Sprintf("val[%d]", i), readBuffer[readValOffs[i]:readValEnds[i]], buffer[valOffs[i]:valEnds[i]])
	}
}

func TestWriteByteOffs_DiffLength(t *testing.T) {
	assert.Error(t, WriteByteOffs(sophie.FsPath{}, nil, nil, []int{1}, []int{1, 2}, []int{1, 2, 3}))
}
