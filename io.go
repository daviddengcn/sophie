/*
Package sophie provides an raw mechanism for serializing data.

It aims at more efficiency than other serialization methods because of the
following reasons:

* Maximum of reusing objects, allocation and GC are avoided
* No reflections

Since the serialization is flexible, one can also make some trade-offs between
efficiency and convinience. E.g., if the data structure may be changed in the
future, in the ReadFrom/WriteTo, God codec can be used to provide future
compatibility.


Sub packages:
  mr  MapReduce library
  kv  A file format storing key-value pairs.
*/
package sophie

import (
	"errors"
	"fmt"
	"io"
	"log"
	"time"
)

var (
	// Returned if some Sophie file is found to have bad format.
	ErrBadFormat = errors.New("Bad Sophie format")
	// End of Sophie file.
	EOF = errors.New("EOF")
)

// sophie.Reader is an interface extended from io.Reader + io.ByteReader
type Reader interface {
	io.Reader
	io.ByteReader
	// Skip skips n bytes, returns the number of actually skipped
	Skip(n int64) (int64, error)
}

// sophie.ReadCloser is sohpie.Reader + io.Closer
type ReadCloser interface {
	Reader
	io.Closer
}

// sophie.Writer is io.Writer + io.ByteWriter
type Writer interface {
	io.Writer
	io.ByteWriter
}

// sophie.WriteCloser is sophie.Writer + io.Closer
type WriteCloser interface {
	Writer
	io.Closer
}

// Iterator is an interface for iterating Sophier kv pairs.
type Iterator interface {
	Next(key, val SophieReader) error
}

// sohpie.IterateCloser is Iterator + io.Closer
type IterateCloser interface {
	Iterator
	io.Closer
}

// Collector is an interface for collecting Sophie key-value pairs.
type Collector interface {
	Collect(key, val SophieWriter) error
}

// CollectCloser is Collector + io.Closer
type CollectCloser interface {
	Collector
	io.Closer
}

// The constants for unknown length.
// @see SophieReader.ReadFrom
const UNKNOWN_LEN = -1

// SophieReader is the interface for some data structure that can reads fields
// from a Reader. The data in the Reader could have known length.
// For all predefined Sophies with prefix Raw, the length must be specified.
type SophieReader interface {
	// ReadFrom reads fields from a Reader. If l is not UNKNOWN_LEN, it can be
	// used to determine the border of the serialized data.
	// @param l  the number of bytes to read. UNKNOWN_LEN(-1) means unknown
	//           length
	ReadFrom(r Reader, l int) error
}

// SophieWriter is the interface for some data structure that can write fields
// to a Writer.
type SophieWriter interface {
	// WriteTo writes fields to the Writer
	WriteTo(w Writer) error
}

// Sophier is a basic data structure for serialization.
// It is SophieReader + SophieWriter
type Sophier interface {
	SophieReader
	SophieWriter
}

// *Int32 implements Sophie interface
type Int32 int32

// Returns a new instance of *Int32 as a Sophier
func NewInt32() Sophier {
	return new(Int32)
}

// SophieWriter interface
func (i Int32) WriteTo(w Writer) error {
	arr := [4]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	_, err := w.Write(arr[:])
	return err
}

// SophieReader interface
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

// *VInt implements Sophie interface and serializing as a vint
type VInt int

// Returns a new instace of *VInt as a Sophier
func NewVInt() Sophier {
	return new(VInt)
}

// SophieWriter interface
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

// SophieReader interface
func (i *VInt) ReadFrom(r Reader, l int) error {
	var v VInt
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	v = VInt(b & 0x7f)
	for n := uint(7); b&0x80 != 0; n += 7 {
		if b, err = r.ReadByte(); err != nil {
			return err
		}
		v |= VInt(b&0x7f) << n
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

// *RawVInt implements Sophie interface and serializing as a vint.
// It assumes the length to be known.
type RawVInt int

// Returns a new instance of *RawVInt as a Sophier
func NewRawVInt() Sophier {
	return new(RawVInt)
}

// SophieWriter interface
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

// SophieReader interface
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

// *ByteSlice implements Sophier interface.
type ByteSlice []byte

// Returns a new instance of *ByteSlice as a Sophier
func NewByteSlice() Sophier {
	return new(ByteSlice)
}

// SophieWriter interface
func (ba ByteSlice) WriteTo(w Writer) error {
	if err := VInt(len(ba)).WriteTo(w); err != nil {
		return err
	}
	_, err := w.Write(ba)
	return err
}

// SophieReader interface
func (ba *ByteSlice) ReadFrom(r Reader, l int) error {
	var sz VInt
	if err := sz.ReadFrom(r, UNKNOWN_LEN); err != nil {
		return err
	}
	*ba = make(ByteSlice, sz)

	_, err := io.ReadFull(r, *ba)
	return err
}

// *RawByteSlice implements Sophier interface. It encodes byte-slice assuming
// the length of buffer will be known when decoding.
type RawByteSlice []byte

// Returns a new instance of *RawByteSlice as a Sophier
func NewRawByteSlice() Sophier {
	return new(RawByteSlice)
}

// SophieWriter interface
func (ba RawByteSlice) WriteTo(w Writer) error {
	_, err := w.Write(ba)
	return err
}

// SophieReader interface
func (ba *RawByteSlice) ReadFrom(r Reader, sz int) error {
	if sz < 0 {
		log.Printf("RawByteSlice expecting a size by get %d", sz)
		return ErrBadFormat
	}
	*ba = make(RawByteSlice, sz)

	n, err := io.ReadFull(r, *ba)
	if n != sz {
		log.Printf("RawByteSlice.ReadFrom: exp %d bytes act %d: %v", sz, n, err)
	}
	return err
}

// *String implements Sophie interface
type String string

// Returns a new instance of *String as a Sophier
func NewString() Sophier {
	return new(String)
}

// SophieWriter interface
func (s String) WriteTo(w Writer) error {
	return ByteSlice(s).WriteTo(w)
}

// SophieReader interface
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

// A helper function that reads a String from a Reader.
func ReadString(r Reader) (s String, err error) {
	err = s.ReadFrom(r, UNKNOWN_LEN)
	return
}

// A helper function that writes a slice of Strings to a Writer. Serialized data
// can be read by ReadStringSlice function.
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

// A helper function that reads a slice String from a Reader. The data was
// serialized by WriteStringSlice function.
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

// *RawString implements Sophie interface. It assumes the length to be known.
type RawString string

// Returns a new instance of *RawByteSlice as a Sophier
func NewRawString() Sophier {
	return new(RawString)
}

// SophieWriter interface.
func (s RawString) WriteTo(w Writer) error {
	return RawByteSlice(s).WriteTo(w)
}

// SophieReader interface
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

// Null is an empty data structure implementing Sophie interface.
type Null struct{}

// NULL is a variable of type Null.
var NULL Null = Null{}

func ReturnNULL() Sophier {
	return NULL
}

// SophieWriter interface
func (Null) WriteTo(w Writer) error {
	return nil
}

// SophieReader interface
func (Null) ReadFrom(r Reader, l int) error {
	if l != UNKNOWN_LEN && l != 0 {
		return ErrBadFormat
	}
	return nil
}

// Time is a time.Time, and *Time implements Sophie interface.
type Time time.Time

// Returns a new instance of *Time as a Sophier
func NewTime() Sophier {
	return new(Time)
}

// SophieWriter interface
func (t Time) WriteTo(w Writer) error {
	bytes, err := time.Time(t).MarshalBinary()
	if err != nil {
		return err
	}
	return ByteSlice(bytes).WriteTo(w)
}

// SophieReader interface
func (t *Time) ReadFrom(r Reader, l int) error {
	var bytes ByteSlice
	if err := (&bytes).ReadFrom(r, l); err != nil {
		return err
	}
	return ((*time.Time)(t)).UnmarshalBinary(bytes)
}
