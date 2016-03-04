package mr

import (
	"github.com/daviddengcn/sophie"
)

// a struct implementing OnlyMapper with funcs.
type OnlyMapperStruct struct {
	NewKeyF func() sophie.Sophier
	NewValF func() sophie.Sophier
	MapF    func(key, val sophie.SophieWriter, c []sophie.Collector) error
	MapEndF func(c []sophie.Collector) error
}

// OnlyMapper interface
func (oms *OnlyMapperStruct) NewKey() sophie.Sophier {
	if oms.NewKeyF != nil {
		return oms.NewKeyF()
	}
	// sophie.RawByteSlice is a Sophier that can read any Sophie data.
	return new(sophie.RawByteSlice)
}

// OnlyMapper interface
func (oms *OnlyMapperStruct) NewVal() sophie.Sophier {
	if oms.NewValF != nil {
		return oms.NewValF()
	}
	return new(sophie.RawByteSlice)
}

// OnlyMapper interface
func (oms *OnlyMapperStruct) Map(key, val sophie.SophieWriter,
	c []sophie.Collector) error {
	if oms.MapF != nil {
		return oms.MapF(key, val, c)
	}
	return nil
}

// OnlyMapper interface
func (oms *OnlyMapperStruct) MapEnd(c []sophie.Collector) error {
	if oms.MapEndF != nil {
		return oms.MapEndF(c)
	}
	return nil
}

// A MapperStruct implementing Mapper by funcs.
type MapperStruct struct {
	// Func for Mapper.NewKey
	NewKeyF func() sophie.Sophier
	// Func for Mapper.NewVal
	NewValF func() sophie.Sophier
	// Func for Mapper.Map
	MapF func(key, val sophie.SophieWriter, c PartCollector) error
	// Func for Mapper.MapEnd
	MapEndF func(c PartCollector) error
}

// Mapper interface
func (ms *MapperStruct) NewKey() sophie.Sophier {
	if ms.NewKeyF != nil {
		return ms.NewKeyF()
	}
	return new(sophie.RawByteSlice)
}

// Mapper interface
func (ms *MapperStruct) NewVal() sophie.Sophier {
	if ms.NewValF != nil {
		return ms.NewValF()
	}
	return new(sophie.RawByteSlice)
}

// Mapper interface
func (ms *MapperStruct) Map(key, val sophie.SophieWriter,
	c PartCollector) error {

	if ms.MapF != nil {
		return ms.MapF(key, val, c)
	}

	return nil
}

// Mapper interface
func (ms *MapperStruct) MapEnd(c PartCollector) error {
	if ms.MapEndF != nil {
		return ms.MapEndF(c)
	}
	return nil
}

// OutputStruct is a struct whose pointer implements Output interface.
type OutputStruct struct {
	CollectorF func(int) (sophie.CollectCloser, error)
}

func (o *OutputStruct) Collector(i int) (sophie.CollectCloser, error) {
	if o.CollectorF == nil {
		return sophie.NullCollectCloser, nil
	}
	return o.CollectorF(i)
}

// A helper variable with an Output returning the NullCollectCloser
var NullOutput = &OutputStruct{}

// A struct implementing Reducer interface by funcs.
type ReducerStruct struct {
	NewKeyF func() sophie.Sophier
	NewValF func() sophie.Sophier
	ReduceF func(key sophie.SophieWriter, nextVal SophierIterator,
		c []sophie.Collector) error
	ReduceEndF func(c []sophie.Collector) error
}

// Reducer interface
func (rs *ReducerStruct) NewKey() sophie.Sophier {
	if rs.NewKeyF != nil {
		return rs.NewKeyF()
	}
	return new(sophie.RawByteSlice)
}

// Reducer interface
func (rs *ReducerStruct) NewVal() sophie.Sophier {
	if rs.NewValF != nil {
		return rs.NewValF()
	}
	return new(sophie.RawByteSlice)
}

// Reducer interface
func (rs *ReducerStruct) Reduce(key sophie.SophieWriter,
	nextVal SophierIterator, c []sophie.Collector) error {

	if rs.ReduceF != nil {
		return rs.ReduceF(key, nextVal, c)
	}
	return nil
}

// Reducer interface
func (rs *ReducerStruct) ReduceEnd(c []sophie.Collector) error {
	if rs.ReduceEndF != nil {
		return rs.ReduceEndF(c)
	}
	return nil
}

// A struct implementing the Input interface by funcs.
type InputStruct struct {
	PartCountF func() (int, error)
	IteratorF  func(index int) (sophie.IterateCloser, error)
}

// Input interface
func (is *InputStruct) PartCount() (int, error) {
	if is.PartCountF != nil {
		return is.PartCountF()
	}
	return 0, nil
}

// Input interface
func (is *InputStruct) Iterator(index int) (sophie.IterateCloser, error) {
	if is.IteratorF != nil {
		return is.IteratorF(index)
	}
	return &sophie.IterateCloserStruct{}, nil
}
