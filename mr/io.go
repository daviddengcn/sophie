package mr

import (
	"github.com/daviddengcn/sophie"
)

// A collector that collects kv pairs to a specified part.
type PartCollector interface {
	CollectTo(part int, key, val sophie.SophieWriter) error
}

// Input represents a specified input source for a mr job.
type Input interface {
	// PartCount returns the number partitions.
	PartCount() (int, error)
	// index range [0, PartCount())
	Iterator(index int) (sophie.IterateCloser, error)
}

// Output represents a specified output destination for a mr job.
type Output interface {
	// Collector generates a sophie.CollectCloser for collecting kv pairs.
	// index is an interger indicating the index to some partition.
	Collector(index int) (sophie.CollectCloser, error)
}
