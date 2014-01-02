package sophie

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/daviddengcn/go-villa"
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

func (r *CountedReadCloser) Skip(n int64) (int64, error) {
	if n1, err := r.ReadCloser.Skip(n); err != nil {
		return n1, err
	}
	r.Pos += n
	return n, nil
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

// Next fetches next key/val pair
func (kvr *KVReader) Next(key, val SophieReader) error {
	var l VInt
	if err := (&l).ReadFrom(kvr.reader, -1); err != nil {
		if err == io.EOF {
			return EOF
		}
		log.Printf("Failed to read key-lenth: %v", err)
		return err
	}
	posEnd := kvr.reader.Pos + int64(l)
	if err := key.ReadFrom(kvr.reader, int(l)); err != nil {
		if err == io.EOF {
			log.Printf("Unexpected EOF reading key")
			return ErrBadFormat
		}
		log.Printf("Reading key failed: %v", err)
		return err
	}
	if kvr.reader.Pos != posEnd {
		log.Printf("PosEnd wrong after reading key(len = %d) %v: exp %d, act %d",
			l, key, posEnd, kvr.reader.Pos)
		return ErrBadFormat
	}

	if err := (&l).ReadFrom(kvr.reader, -1); err != nil {
		if err == io.EOF {
			log.Printf("Error format of val length for key %v", key)
			return ErrBadFormat
		}
		return err
	}
	posEnd = kvr.reader.Pos + int64(l)
	if err := val.ReadFrom(kvr.reader, int(l)); err != nil {
		if err == io.EOF {
			log.Printf("Unexpected EOF reading val for key %v", key)
			return ErrBadFormat
		}
		log.Printf("Reading value for key %v failed: %v", key, err)
		return err
	}
	if kvr.reader.Pos != posEnd {
		log.Printf("PosEnd wrong after reading key %v, value %v: exp %d, act %d",
			key, val, posEnd, kvr.reader.Pos)
		return ErrBadFormat
	}
	return nil
}

func ReadAsByteOffs(fp FsPath) (buffer villa.ByteSlice,
	keyOffs, keyEnds, valOffs, valEnds villa.IntSlice, err error) {
	fi, err := fp.Stat()
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	reader, err := fp.Open()
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	defer reader.Close()

	buffer = make([]byte, fi.Size())
	if n, err := reader.Read(buffer); n != len(buffer) || err != nil {
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		return nil, nil, nil, nil, nil, errors.New(fmt.Sprintf(
			"Expected %d bytes, but only read %d bytes", len(buffer), n))
	}
	buf := CountReadCloser(villa.NewPByteSlice(buffer))
	for buf.Pos < int64(len(buffer)) {
		var l VInt
		if err := (&l).ReadFrom(buf, -1); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		keyOffs = append(keyOffs, int(buf.Pos))
		if _, err := buf.Skip(int64(l)); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		keyEnds = append(keyEnds, int(buf.Pos))
		if err := (&l).ReadFrom(buf, -1); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		valOffs = append(valOffs, int(buf.Pos))
		if _, err := buf.Skip(int64(l)); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		valEnds = append(valEnds, int(buf.Pos))
	}
	return
}

func WriteByteOffs(fp FsPath, buffer []byte,
	keyOffs, keyEnds, valOffs, valEnds []int) error {
	writer, err := fp.Create()
	if err != nil {
		return err
	}
	defer writer.Close()

	for i, keyOff := range keyOffs {
		keyEnd, valOff, valEnd := keyEnds[i], valOffs[i], valEnds[i]
		if err := VInt(keyEnd - keyOff).WriteTo(writer); err != nil {
			return err
		}
		if _, err := writer.Write(buffer[keyOff:keyEnd]); err != nil {
			return err
		}
		if err := VInt(valEnd - valOff).WriteTo(writer); err != nil {
			return err
		}
		if _, err := writer.Write(buffer[valOff:valEnd]); err != nil {
			return err
		}
	}
	// fmt.Println("WriteByteOffs", fp.Path, len(buffer))
	return nil
}
