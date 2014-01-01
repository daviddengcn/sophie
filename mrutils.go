package sophie

// EmptyClose is a helper type defining an empty Close method
type EmptyClose struct{}

func (EmptyClose) Close() error {
	return nil
}

// EmptyOnlyMapper is a helper type defining an empty MapEnd method for
// OnlyMapper
type EmptyOnlyMapper struct{}

func (EmptyOnlyMapper) MapEnd(c []Collector) error {
	return nil
}

// a function type implementing OnlyMapperFactory interface
type OnlyMapperFactoryFunc func(src, part int) OnlyMapper

func (f OnlyMapperFactoryFunc) NewMapper(src, part int) OnlyMapper {
	return f(src, part)
}

// a helper struct for implementing OnlyMapper with closures.
type OnlyMapperStruct struct {
	NewKeyFunc func() Sophier
	NewValFunc func() Sophier
	MapFunc    func(key, val SophieWriter, c []Collector) error
	MapEndFunc func(c []Collector) error
}

// OnlyMapper.NewKey
func (oms *OnlyMapperStruct) NewKey() Sophier {
	if oms.NewKeyFunc != nil {
		return oms.NewKeyFunc()
	}
	return new(RawByteSlice)
}

// OnlyMapper.NewVal
func (oms *OnlyMapperStruct) NewVal() Sophier {
	if oms.NewValFunc != nil {
		return oms.NewValFunc()
	}
	return new(RawByteSlice)
}

// OnlyMapper.Map
func (oms *OnlyMapperStruct) Map(key, val SophieWriter, c []Collector) error {
	if oms.MapFunc != nil {
		return oms.MapFunc(key, val, c)
	}
	return nil
}

// OnlyMapper.MapEnd
func (oms *OnlyMapperStruct) MapEnd(c []Collector) error {
	if oms.MapEnd != nil {
		return oms.MapEndFunc(c)
	}
	return nil
}

type EmptyMapper struct{}

func (EmptyMapper) MapEnd(c PartCollector) error {
	return nil
}

type MapperStruct struct {
	NewKeyFunc func() Sophier
	NewValFunc func() Sophier
	MapFunc    func(key, val SophieWriter, c PartCollector) error
	MapEndFunc func(c PartCollector) error
}

func (ms *MapperStruct) NewKey() Sophier {
	if ms.NewKeyFunc != nil {
		return ms.NewKeyFunc()
	}
	return new(RawByteSlice)
}
func (ms *MapperStruct) NewVal() Sophier {
	if ms.NewValFunc != nil {
		return ms.NewValFunc()
	}
	return new(RawByteSlice)
}
func (ms *MapperStruct) Map(key, val SophieWriter, c PartCollector) error {
	if ms.MapFunc != nil {
		return ms.MapFunc(key, val, c)
	}

	return nil
}
func (ms *MapperStruct) MapEnd(c PartCollector) error {
	if ms.MapEndFunc != nil {
		return ms.MapEndFunc(c)
	}
	return nil
}

// a function type implementing MapperFactory interface
type MapperFactoryFunc func(src, part int) Mapper

func (f MapperFactoryFunc) NewMapper(src, part int) Mapper {
	return f(src, part)
}

type nullCollectCloser struct {
	EmptyClose
}

//
func (nullCollectCloser) Collect(key, val SophieWriter) error {
	return nil
}

// A helper variable with a CollectCloser ignoring every thing collecting to it
var NullCollectCloser = nullCollectCloser{}

type nullOutput struct {
}

func (nullOutput) Collector(index int) (CollectCloser, error) {
	return NullCollectCloser, nil
}

// A helper variable with an Output returning the NullCollectCloser
var NullOutput = nullOutput{}

type ReducerStruct struct {
	NewKeyFunc func() Sophier
	NewValFunc func() Sophier
	ReduceFunc func(key SophieWriter, nextVal SophierIterator,
		c []Collector) error
	ReduceEndFunc func(c []Collector) error
}

func (rs *ReducerStruct) NewKey() Sophier {
	if rs.NewKeyFunc != nil {
		return rs.NewKeyFunc()
	}
	return new(RawByteSlice)
}

func (rs *ReducerStruct) NewVal() Sophier {
	if rs.NewValFunc != nil {
		return rs.NewValFunc()
	}
	return new(RawByteSlice)
}

func (rs *ReducerStruct) Reduce(key SophieWriter, nextVal SophierIterator,
	c []Collector) error {
	if rs.ReduceFunc != nil {
		return rs.ReduceFunc(key, nextVal, c)
	}
	return nil
}

func (rs *ReducerStruct) ReduceEnd(c []Collector) error {
	if rs.ReduceEndFunc != nil {
		return rs.ReduceEndFunc(c)
	}
	return nil
}

// EmptyReducer is a helper type defining some empty implementation of a Reducer
type EmptyReducer struct{}

func (EmptyReducer) ReduceEnd(c []Collector) error {
	return nil
}

// a function type implementing ReducerFactory interface
type ReducerFactoryFunc func(part int) Reducer

func (f ReducerFactoryFunc) NewReducer(part int) Reducer {
	return f(part)
}

type IterateCloserStruct struct {
	NextFunc   func(key, val SophieReader) error
	CloserFunc func() error
}

func (ics *IterateCloserStruct) Next(key, val SophieReader) error {
	if ics.NextFunc != nil {
		return ics.NextFunc(key, val)
	}
	return EOF
}

func (ics *IterateCloserStruct) Close() error {
	if ics.CloserFunc != nil {
		return ics.CloserFunc()
	}
	return nil
}

type InputStruct struct {
	PartCountFunc func() (int, error)
	IteratorFunc  func(index int) (IterateCloser, error)
}

func (is *InputStruct) PartCount() (int, error) {
	if is.PartCountFunc != nil {
		return is.PartCountFunc()
	}
	return 0, nil
}

func (is *InputStruct) Iterator(index int) (IterateCloser, error) {
	if is.IteratorFunc != nil {
		return is.IteratorFunc(index)
	}
	return &IterateCloserStruct{}, nil
}
