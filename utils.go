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
type CollectorF func(key, val SophieWriter) error

// Collector interface.
func (c CollectorF) Collect(key, val SophieWriter) error {
	return c(key, val)
}
