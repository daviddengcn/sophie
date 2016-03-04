package sophie

// EmptyClose is a helper type defining an empty Close method
type EmptyClose struct{}

// io.Closer interface
func (EmptyClose) Close() error {
	return nil
}

type nullCollectCloser struct {
	EmptyClose
}

func (nullCollectCloser) Collect(key, val SophieWriter) error {
	return nil
}

// A helper variable with a CollectCloser ignoring every thing collecting to it
var NullCollectCloser = nullCollectCloser{}

// A struct implementing IterateCloser by funcs.
type IterateCloserStruct struct {
	NextF   func(key, val SophieReader) error
	CloserF func() error
}

// Iterator interface
func (ics *IterateCloserStruct) Next(key, val SophieReader) error {
	if ics.NextF != nil {
		return ics.NextF(key, val)
	}
	return EOF
}

// io.Closer struct.
func (ics *IterateCloserStruct) Close() error {
	if ics.CloserF != nil {
		return ics.CloserF()
	}
	return nil
}

// A func type implementing Collector interface.
// A nil func is a no-op Collector.
type CollectorF func(key, val SophieWriter) error

// Collector interface.
func (c CollectorF) Collect(key, val SophieWriter) error {
	if c == nil {
		return nil
	}
	return c(key, val)
}

// CollectCloserStruct is a struct whose pointer implements CollectCloser interface
type CollectCloserStruct struct {
	CollectF func(SophieWriter, SophieWriter) error
	CloseF   func() error
}

func (c *CollectCloserStruct) Collect(k, v SophieWriter) error {
	if c.CollectF == nil {
		return nil
	}
	return c.CollectF(k, v)
}

func (c *CollectCloserStruct) Close() error {
	if c.CloseF == nil {
		return nil
	}
	return c.CloseF()
}
