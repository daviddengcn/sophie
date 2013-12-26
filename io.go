package sophie

import (
	"fmt"
	"io"
	"log"
	"time"
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

const UNKNOWN_LEN = -1

type SophieReader interface {
	/**
	 * @param len the number of bytes to read. UNKNOWN_LEN(-1) means unknown length
	 */
	ReadFrom(r Reader, l int) error
}
type SophieWriter interface {
	WriteTo(w Writer) error
}

// Sophier is a basic data structure for serialization
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
	if l != UNKNOWN_LEN && l != 4 {
		return ErrBadFormat
	}

	var arr [4]byte
	if _, err := io.ReadFull(r, arr[:]); err != nil {
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
type VInt int

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
	for n := uint(7); b & 0x80 != 0; n += 7 {
		if b, err = r.ReadByte(); err != nil {
			return err
		}
		v |= VInt(b & 0x7f) << n
	}
	*i = v
	return nil
}

func (i *VInt) Val() int {
	return int(*i)
}

func (i *VInt) String() string {
	return fmt.Sprint(*i)
}

// *SVInt implements Sophie interface and serializing as a vint
type RawVInt int

func (i RawVInt) WriteTo(w Writer) error {
	var arr [8]byte
	n := 0
	for i != 0 {
		arr[n] = byte(i & 0xff)
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
	n := uint(0)
	for ; l > 0; l-- {
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

func (i *RawVInt) Val() int {
	return int(*i)
}

func (i *RawVInt) String() string {
	return fmt.Sprint(*i)
}

type ByteSlice []byte

func (ba ByteSlice) WriteTo(w Writer) error {
	if err := VInt(len(ba)).WriteTo(w); err != nil {
		return err
	}
	_, err := w.Write(ba)
	return err
}

func (ba *ByteSlice) ReadFrom(r Reader, l int) error {
	var sz VInt
	if err := sz.ReadFrom(r, UNKNOWN_LEN); err != nil {
		return err
	}
	if VInt(cap(*ba)) < sz {
		*ba = make(ByteSlice, sz)
	}

	if VInt(len(*ba)) != sz {
		*ba = (*ba)[:sz]
	}

	_, err := io.ReadFull(r, *ba)
	return err
}

type RawByteSlice []byte

func (ba RawByteSlice) WriteTo(w Writer) error {
	_, err := w.Write(ba)
	return err
}

func (ba *RawByteSlice) ReadFrom(r Reader, sz int) error {
	if sz < 0 {
		log.Printf("RawByteSlice expecting a size by get %d", sz)
		return ErrBadFormat
	}
	if cap(*ba) < sz {
		*ba = make(RawByteSlice, sz)
	}

	if len(*ba) != sz {
		*ba = (*ba)[:sz]
	}

	n, err := io.ReadFull(r, *ba)
	if n != sz {
		log.Printf("RawByteSlice.ReadFrom: exp %d bytes act %d: %v", sz, n, err)
	}
	return err
}

// *String implements Sophie interface
type String string

func (s String) WriteTo(w Writer) error {
	return ByteSlice(s).WriteTo(w)
}

func (s *String) ReadFrom(r Reader, l int) error {
	var ba ByteSlice
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

func ReadString(r Reader) (s String, err error) {
	err = s.ReadFrom(r, UNKNOWN_LEN)
	return
}

func WriteStringSlice(w Writer, sl []string) error {
	if err := VInt(len(sl)).WriteTo(w); err != nil {
		return err
	}
	for _, s := range sl {
		if err := String(s).WriteTo(w); err != nil {
			return err
		}
	}
	return nil
}

func ReadStringSlice(r Reader, sl *[]string) (err error) {
	var l VInt
	if err := l.ReadFrom(r, -1); err != nil {
		return err
	}
	if cap(*sl) < int(l) {
		*sl = make([]string, l)
	} else {
		*sl = (*sl)[:l]
	}
	for i := range *sl {
		if err := (*String)(&(*sl)[i]).ReadFrom(r, -1); err != nil {
			return err
		}
	}
	
	return nil
}

// *RawString implements Sophie interface
type RawString string

func (s RawString) WriteTo(w Writer) error {
	return RawByteSlice(s).WriteTo(w)
}

func (s *RawString) ReadFrom(r Reader, l int) error {
	var ba RawByteSlice
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

var NULL Null = Null{}

func (Null) WriteTo(w Writer) error {
	return nil
}

func (Null) ReadFrom(r Reader, l int) error {
	if l != UNKNOWN_LEN && l != 0 {
		return ErrBadFormat
	}
	return nil
}

type Time time.Time

func (t Time) WriteTo(w Writer) error {
	bytes, err := time.Time(t).MarshalBinary()
	if err != nil {
		return err
	}
	return ByteSlice(bytes).WriteTo(w)
}

func (t *Time) ReadFrom(r Reader, l int) error {
	var bytes ByteSlice
	if err := (&bytes).ReadFrom(r, l); err != nil {
		return err
	}
	return ((*time.Time)(t)).UnmarshalBinary(bytes)
}
