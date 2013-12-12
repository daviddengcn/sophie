package sophie

import (
	"bufio"
	"bytes"
	"os"

	"github.com/daviddengcn/go-villa"
)

type FilePath struct {
	Fs   string
	Path villa.Path
}

type bufioWriterBuffer struct {
	*bufio.Writer
}

func (b bufioWriterBuffer) Close() error {
	return b.Writer.Flush()
}

type ClosableBuffer struct {
	bytes.Buffer
}

func (buf ClosableBuffer) Close() error {
	return nil
}

type KVWriter struct {
	file   *os.File
	writer bufioWriterBuffer
	objBuf ClosableBuffer
}

func NewKVFile(fn FilePath) (*KVWriter, error) {
	file, err := fn.Path.Create()
	if err != nil {
		return nil, err
	}
	return &KVWriter{
		file:   file,
		writer: bufioWriterBuffer{bufio.NewWriter(file)},
	}, nil
}

func (kvw *KVWriter) Close() error {
	if err := kvw.writer.Flush(); err != nil {
		return err
	}
	return kvw.file.Close()
}

func (kvw *KVWriter) Collect(key, val SophieWriter) error {
	// write key
	kvw.objBuf.Reset()
	key.WriteTo(&kvw.objBuf)
	if err := VInt(kvw.objBuf.Len()).WriteTo(&kvw.writer); err != nil {
		return err
	}
	if _, err := kvw.writer.Write(kvw.objBuf.Bytes()); err != nil {
		return err
	}
	// write val
	kvw.objBuf.Reset()
	val.WriteTo(&kvw.objBuf)
	if err := VInt(kvw.objBuf.Len()).WriteTo(&kvw.writer); err != nil {
		return err
	}
	if _, err := kvw.writer.Write(kvw.objBuf.Bytes()); err != nil {
		return err
	}
	// success
	return nil
}
