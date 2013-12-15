package sophie

import (
	"fmt"
	"io"
)

type Reader interface {
	io.Reader
	io.ByteReader
}

type ReadCloser interface {
	Reader
	io.Closer
}

type Writer interface {
	io.Writer
	io.ByteWriter
}

type WriteCloser interface {
	Writer
	io.Closer
}

type SophieReader interface {
	ReadFrom(r Reader) error
}
type SophieWriter interface {
	WriteTo(w Writer) error
}

type Sophier interface {
	SophieReader
	SophieWriter
}

// *SInt32 implements Sophie interface
type Int32 int32

func (i Int32) WriteTo(w Writer) error {
	arr := [4]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	_, err := w.Write(arr[:])
	return err
}

func (i *Int32) ReadFrom(r Reader) error {
	var arr [4]byte
	n, err := r.Read(arr[:])
	if n < 4 {
		return err
	}

	*i = Int32(arr[0]) | Int32(arr[1])<<8 | Int32(arr[2])<<16 |
		Int32(arr[3])<<24
	return nil
}

func (i *Int32) Val() int32 {
	return int32(*i)
}

// *SVInt implements Sophie interface and serializing as a vint
type VInt uint64

func (i VInt) WriteTo(w Writer) error {
	var arr [10]byte
	n := 0
	for i > 0x7f {
		arr[n] = byte(i&0x7f) | 0x80
		n++
		i >>= 7
	}
	arr[n] = byte(i)
	n++
	_, err := w.Write(arr[:n])
	return err
}

func (i *VInt) ReadFrom(r Reader) error {
	var v VInt
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	v = VInt(b & 0x7f)
	for n := VInt(7); b&0x80 != 0; n += 7 {
		b, err = r.ReadByte()
		if err != nil {
			return err
		}
		v |= VInt(b&0x7f) << n
	}
	*i = v
	return nil
}

func (i *VInt) Val() int64 {
	return int64(*i)
}

func (i *VInt) String() string {
	return fmt.Sprint(*i)
}

type ByteArray []byte

func (ba ByteArray) WriteTo(w Writer) error {
	if err := VInt(len(ba)).WriteTo(w); err != nil {
		return err
	}
	_, err := w.Write(ba)
	return err
}

func (ba *ByteArray) ReadFrom(r Reader) error {
	var l VInt
	if err := l.ReadFrom(r); err != nil {
		return err
	}
	if VInt(cap(*ba)) < l {
		*ba = make(ByteArray, l)
	}

	if VInt(len(*ba)) != l {
		*ba = (*ba)[:l]
	}

	_, err := r.Read(*ba)
	return err
}

// *SString implements Sophie interface
type String string

func (s String) WriteTo(w Writer) error {
	return ByteArray(s).WriteTo(w)
}

func (s *String) ReadFrom(r Reader) error {
	var ba ByteArray
	if err := ba.ReadFrom(r); err != nil {
		return err
	}

	*s = String(ba)

	return nil
}

func (s *String) String() string {
	return string(*s)
}
func (s *String) Val() string {
	return string(*s)
}

type Null struct{}

func (Null) WriteTo(w Writer) error {
	return nil
}

func (Null) ReadFrom(r Reader) error {
	return nil
}
