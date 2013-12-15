package sophie

import (
	"bytes"
	"errors"
	"io"
)

var (
	ErrBadFormat = errors.New("bad kv format")
)

/*
	KVFile format:
	vint(key-len) key vint(val-len) val
*/
type KVWriter struct {
	writer WriteCloser
	objBuf bytes.Buffer
}

func NewKVWriter(fp FsPath) (*KVWriter, error) {
	writer, err := fp.Fs.Create(fp.Path)
	if err != nil {
		return nil, err
	}
	return &KVWriter{
		writer: writer,
	}, nil
}

func (kvw *KVWriter) Close() error {
	return kvw.writer.Close()
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

type CountedReadCloser struct {
	Pos int64
	ReadCloser
}

func (r *CountedReadCloser) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.Pos += int64(n)
	return n, err
}

func (r *CountedReadCloser) ReadByte() (c byte, err error) {
	c, err = r.ReadCloser.ReadByte()
	if err != nil {
		return c, err
	}
	r.Pos++
	return c, nil
}

func CountReadCloser(reader ReadCloser) *CountedReadCloser {
	return &CountedReadCloser{
		ReadCloser: reader,
	}
}

type KVReader struct {
	reader *CountedReadCloser
}

func NewKVReader(fp FsPath) (*KVReader, error) {
	reader, err := fp.Fs.Open(fp.Path)
	if err != nil {
		return nil, err
	}
	return &KVReader{
		reader: CountReadCloser(reader),
	}, nil
}

func (kvr *KVReader) Close() error {
	return kvr.reader.Close()
}

func (kvr *KVReader) Next(key, val SophieReader) error {
	var l VInt
	if err := (&l).ReadFrom(kvr.reader); err != nil {
		if err == io.EOF {
			return EOF
		}
		return err
	}
	posEnd := kvr.reader.Pos + int64(l)
	if err := key.ReadFrom(kvr.reader); err != nil {
		if err == io.EOF {
			return ErrBadFormat
		}
		return err
	}
	if kvr.reader.Pos != posEnd {
		return ErrBadFormat
	}

	if err := (&l).ReadFrom(kvr.reader); err != nil {
		if err == io.EOF {
			return ErrBadFormat
		}
		return err
	}
	posEnd = kvr.reader.Pos + int64(l)
	if err := val.ReadFrom(kvr.reader); err != nil {
		if err == io.EOF {
			return ErrBadFormat
		}
		return err
	}
	if kvr.reader.Pos != posEnd {
		return ErrBadFormat
	}
	return nil
}
