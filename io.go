package sophie

import (
	"fmt"
	"io"
)

type Reader interface {
	io.Reader
	io.ByteReader
	// Skip n bytes
	Skip(n int64) (int64, error)
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
	/**
	 * @param len the number of bytes to read. -1 means unknown
	 */
	ReadFrom(r Reader, l int) error
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

func (i *Int32) ReadFrom(r Reader, l int) error {
	if l != -1 && l != 4 {
		return ErrBadFormat
	}
	
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

func (i *VInt) ReadFrom(r Reader, l int) error {
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

// *SVInt implements Sophie interface and serializing as a vint
type RawVInt uint64

func (i RawVInt) WriteTo(w Writer) error {
	var arr [8]byte
	n := 0
	for i != 0 {
		arr[n] = byte(i&0xff)
		n++
		i >>= 8
	}
	_, err := w.Write(arr[:n])
	return err
}

func (i *RawVInt) ReadFrom(r Reader, l int) error {
	if l < 0 {
		return ErrBadFormat
	}
	var v RawVInt
	n := RawVInt(0)
	for ; l > 0; l -- {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		v |= (RawVInt(b) & 0xff) << n
		n += 8
	}
	*i = v
	return nil
}

func (i *RawVInt) Val() int64 {
	return int64(*i)
}

func (i *RawVInt) String() string {
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

func (ba *ByteArray) ReadFrom(r Reader, l int) error {
	var sz VInt
	if err := sz.ReadFrom(r, -1); err != nil {
		return err
	}
	if VInt(cap(*ba)) < sz {
		*ba = make(ByteArray, sz)
	}

	if VInt(len(*ba)) != sz {
		*ba = (*ba)[:sz]
	}

	_, err := r.Read(*ba)
	return err
}

type RawByteArray []byte

func (ba RawByteArray) WriteTo(w Writer) error {
	_, err := w.Write(ba)
	return err
}

func (ba *RawByteArray) ReadFrom(r Reader, sz int) error {
	if sz < 0 {
		return ErrBadFormat
	}
	if cap(*ba) < sz {
		*ba = make(RawByteArray, sz)
	}

	if len(*ba) != sz {
		*ba = (*ba)[:sz]
	}

	_, err := r.Read(*ba)
	return err
}

// *String implements Sophie interface
type String string

func (s String) WriteTo(w Writer) error {
	return ByteArray(s).WriteTo(w)
}

func (s *String) ReadFrom(r Reader, l int) error {
	var ba ByteArray
	if err := ba.ReadFrom(r, l); err != nil {
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

// *RawString implements Sophie interface
type RawString string

func (s RawString) WriteTo(w Writer) error {
	return RawByteArray(s).WriteTo(w)
}

func (s *RawString) ReadFrom(r Reader, l int) error {
	var ba RawByteArray
	if err := ba.ReadFrom(r, l); err != nil {
		return err
	}

	*s = RawString(ba)

	return nil
}

func (s *RawString) String() string {
	return string(*s)
}
func (s *RawString) Val() string {
	return string(*s)
}

type Null struct{}

func (Null) WriteTo(w Writer) error {
	return nil
}

func (Null) ReadFrom(r Reader, l int) error {
	if l != -1 && l != 0 {
		return ErrBadFormat
	}
	return nil
}
