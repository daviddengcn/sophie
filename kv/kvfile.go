/*
Package kv supporting read and write of a simple file formating for Sophie, which
stores key-value pairs.

KVFile format:
  vint(key-len) key vint(val-len) val
*/
package kv

import (
	"io"

	"github.com/golangplus/bytes"
	"github.com/golangplus/errors"

	"github.com/daviddengcn/go-villa"
	"github.com/daviddengcn/sophie"
)

// kv.Writer is a struct for generating a kv file.
// *kv.Writer implements the sophie.CollectCloser interface.
type Writer struct {
	writer sophie.WriteCloser
	objBuf bytesp.Slice
}

// NewWriter returns a *kv.Writer for writing a kv file at the specified FsPath.
func NewWriter(fp sophie.FsPath) (*Writer, error) {
	writer, err := fp.Fs.Create(fp.Path)
	if err != nil {
		return nil, errorsp.WithStacks(err)
	}

	return &Writer{
		writer: writer,
	}, nil
}

// io.Closer interface
func (kvw *Writer) Close() error {
	return kvw.writer.Close()
}

// sophie.CollectCloser interface
func (kvw *Writer) Collect(key, val sophie.SophieWriter) error {
	// write key
	kvw.objBuf.Reset()
	key.WriteTo(&kvw.objBuf)
	if err := sophie.VInt(len(kvw.objBuf)).WriteTo(kvw.writer); err != nil {
		return err
	}
	if _, err := kvw.writer.Write([]byte(kvw.objBuf)); err != nil {
		return err
	}
	// write val
	kvw.objBuf.Reset()
	val.WriteTo(&kvw.objBuf)
	if err := sophie.VInt(len(kvw.objBuf)).WriteTo(kvw.writer); err != nil {
		return err
	}
	if _, err := kvw.writer.Write([]byte(kvw.objBuf)); err != nil {
		return err
	}
	// success
	return nil
}

type countedReadCloser struct {
	Pos int64
	sophie.ReadCloser
}

func (r *countedReadCloser) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.Pos += int64(n)
	return n, err
}

func (r *countedReadCloser) ReadByte() (c byte, err error) {
	c, err = r.ReadCloser.ReadByte()
	if err != nil {
		return c, err
	}
	r.Pos++
	return c, nil
}

func (r *countedReadCloser) Skip(n int64) (int64, error) {
	if n1, err := r.ReadCloser.Skip(n); err != nil {
		return n1, err
	}
	r.Pos += n
	return n, nil
}

func countReadCloser(reader sophie.ReadCloser) *countedReadCloser {
	return &countedReadCloser{
		ReadCloser: reader,
	}
}

// kv.Reader is a struct for reading a kv file.
type Reader struct {
	reader countedReadCloser
}

// NewReader returns a *Reader for reading the kv file at the specified FsPath.
func NewReader(fp sophie.FsPath) (*Reader, error) {
	reader, err := fp.Fs.Open(fp.Path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		reader: countedReadCloser{ReadCloser: reader},
	}, nil
}

// io.Closer interface
func (kvr *Reader) Close() error {
	return kvr.reader.Close()
}

// Next fetches next key/val pair
func (kvr *Reader) Next(key, val sophie.SophieReader) error {
	var l sophie.VInt
	if err := (&l).ReadFrom(&kvr.reader, -1); err != nil {
		return errorsp.WithStacksAndMessage(err, "reading key length failed")
	}
	posEnd := kvr.reader.Pos + int64(l)
	if err := key.ReadFrom(&kvr.reader, int(l)); err != nil {
		if errorsp.Cause(err) == io.EOF {
			return errorsp.WithStacksAndMessage(io.ErrUnexpectedEOF, "Unexpected EOF reading key")
		}
		return errorsp.WithStacksAndMessage(err, "reading key %v failed", key)
	}
	if kvr.reader.Pos != posEnd {
		return errorsp.WithStacksAndMessage(sophie.ErrBadFormat, "PosEnd wrong after reading key(len = %d) %v: exp %d, act %d", l, key, posEnd, kvr.reader.Pos)
	}

	if err := (&l).ReadFrom(&kvr.reader, -1); err != nil {
		if errorsp.Cause(err) == io.EOF {
			return errorsp.WithStacksAndMessage(io.ErrUnexpectedEOF, "Unexpected EOF reading val length for key %v", key)
		}
		return err
	}
	posEnd = kvr.reader.Pos + int64(l)
	if err := val.ReadFrom(&kvr.reader, int(l)); err != nil {
		if errorsp.Cause(err) == io.EOF {
			return errorsp.WithStacksAndMessage(io.ErrUnexpectedEOF, "Unexpected EOF reading val for key %v", key)
		}
		return errorsp.WithStacksAndMessage(err, "reading value for key %v failed", key)
	}
	if kvr.reader.Pos != posEnd {
		return errorsp.WithStacksAndMessage(sophie.ErrBadFormat, "PosEnd wrong after reading key %v, value %v: exp %d, act %d",
			key, val, posEnd, kvr.reader.Pos)
	}
	return nil
}

// ReadAsByteOffs reads a kv file as a slice of buffer and some int slices
// of key offsets, key ends, value offsets, and value ends.
func ReadAsByteOffs(fp sophie.FsPath) (buffer bytesp.Slice, keyOffs, keyEnds, valOffs, valEnds villa.IntSlice, err error) {
	fi, err := fp.Stat()
	if err != nil {
		return nil, nil, nil, nil, nil, errorsp.WithStacks(err)
	}

	reader, err := fp.Open()
	if err != nil {
		return nil, nil, nil, nil, nil, errorsp.WithStacks(err)
	}
	defer reader.Close()

	buffer = make(bytesp.Slice, fi.Size())
	if n, err := reader.Read(buffer); n != len(buffer) || err != nil {
		if err != nil {
			return nil, nil, nil, nil, nil, errorsp.WithStacksAndMessage(err, "n = %d", n)
		}
		return nil, nil, nil, nil, nil, errorsp.NewWithStacks("Expected %d bytes, but only read %d bytes", len(buffer), n)
	}
	buf := countReadCloser(bytesp.NewPSlice(buffer))
	for buf.Pos < int64(len(buffer)) {
		var l sophie.VInt
		if err := (&l).ReadFrom(buf, -1); err != nil {
			return nil, nil, nil, nil, nil, errorsp.WithStacksAndMessage(sophie.ErrBadFormat, "failed to read key-lenth: %v", err)
		}
		keyOffs = append(keyOffs, int(buf.Pos))
		if _, err := buf.Skip(int64(l)); err != nil {
			return nil, nil, nil, nil, nil, errorsp.WithStacksAndMessage(sophie.ErrBadFormat, "failed to skip key: %v", err)
		}
		keyEnds = append(keyEnds, int(buf.Pos))
		if err := (&l).ReadFrom(buf, -1); err != nil {
			return nil, nil, nil, nil, nil, errorsp.WithStacksAndMessage(sophie.ErrBadFormat, "failed to read value-lenth: %v", err)
		}
		valOffs = append(valOffs, int(buf.Pos))
		if _, err := buf.Skip(int64(l)); err != nil {
			return nil, nil, nil, nil, nil, errorsp.WithStacksAndMessage(sophie.ErrBadFormat, "failed to read value: %v", err)
		}
		valEnds = append(valEnds, int(buf.Pos))
	}
	return
}

// WriteByteOffs generates a kv file with key-value pairs represented as a
// slice of buffer and some int slices of key offsets, key ends, value offsets,
// and value ends.
func WriteByteOffs(fp sophie.FsPath, buffer []byte, keyOffs, keyEnds, valOffs, valEnds []int) error {
	if len(keyOffs) != len(keyEnds) || len(keyOffs) != len(valOffs) || len(keyOffs) != len(valEnds) {
		return errorsp.NewWithStacks("length of keyOffs(%d), keyEnds(%d), valOffs(%d) and valEnds(%d) must be the same",
			len(keyOffs), len(keyEnds), len(valOffs), len(valEnds))
	}
	writer, err := fp.Create()
	if err != nil {
		return errorsp.WithStacks(err)
	}
	defer writer.Close()

	for i, keyOff := range keyOffs {
		keyEnd, valOff, valEnd := keyEnds[i], valOffs[i], valEnds[i]
		if err := sophie.VInt(keyEnd - keyOff).WriteTo(writer); err != nil {
			return errorsp.WithStacks(err)
		}
		if _, err := writer.Write(buffer[keyOff:keyEnd]); err != nil {
			return errorsp.WithStacks(err)
		}
		if err := sophie.VInt(valEnd - valOff).WriteTo(writer); err != nil {
			return errorsp.WithStacks(err)
		}
		if _, err := writer.Write(buffer[valOff:valEnd]); err != nil {
			return errorsp.WithStacks(err)
		}
	}
	return nil
}
