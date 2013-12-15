package sophie

import (
	"bufio"
	"bytes"
	"io"
	"os"

	"github.com/daviddengcn/go-villa"
)

type FilePath struct {
	Fs   string
	Path villa.Path
}

/*
	KVFile format:
	vint(key-len) key vint(val-len) val
*/
type KVWriter struct {
	file   *os.File
	writer *bufio.Writer
	objBuf bytes.Buffer
}

func NewKVWriter(fn FilePath) (*KVWriter, error) {
	file, err := fn.Path.Create()
	if err != nil {
		return nil, err
	}
	return &KVWriter{
		file:   file,
		writer: bufio.NewWriter(file),
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
	if err := VInt(kvw.objBuf.Len()).WriteTo(kvw.writer); err != nil {
		return err
	}
	if _, err := kvw.writer.Write(kvw.objBuf.Bytes()); err != nil {
		return err
	}
	// write val
	kvw.objBuf.Reset()
	val.WriteTo(&kvw.objBuf)
	if err := VInt(kvw.objBuf.Len()).WriteTo(kvw.writer); err != nil {
		return err
	}
	if _, err := kvw.writer.Write(kvw.objBuf.Bytes()); err != nil {
		return err
	}
	// success
	return nil
}

type KVReader struct {
	file   *os.File
	reader *bufio.Reader
}

func NewKVReader(fn FilePath) (*KVReader, error) {
	file, err := fn.Path.Open()
	if err != nil {
		return nil, err
	}
	return &KVReader{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

func (kvr *KVReader) Close() error {
	return kvr.file.Close()
}

func (kvr *KVReader) Next(key, val SophieReader) error {
	var l VInt
	if err := (&l).ReadFrom(kvr.reader); err != nil {
		if err == io.EOF {
			return EOF
		}
		return err
	}
	if err := key.ReadFrom(kvr.reader); err != nil {
		if err == io.EOF {
			return EOF
		}
		return err
	}

	if err := (&l).ReadFrom(kvr.reader); err != nil {
		if err == io.EOF {
			return EOF
		}
		return err
	}
	if err := val.ReadFrom(kvr.reader); err != nil {
		if err == io.EOF {
			return EOF
		}
		return err
	}
	return nil
}
