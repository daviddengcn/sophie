package mr

import (
	"errors"
	"io"
	"log"

	"github.com/daviddengcn/sophie"
	"github.com/golangplus/errors"
)

var (
	// end of map, an error returned by a Mapper/OnlyMapper.Map indicating a
	// stop of continuing mapping
	EOM = errors.New("EOM")
)

// OnlyMapper is an interface defining the map actions for MapOnlyJob
type OnlyMapper interface {
	// NewKey returns a new instance of key for reading from Source
	NewKey() sophie.Sophier
	// NewVal returns a new instance of value for reading from Source
	NewVal() sophie.Sophier
	// Make a map action for a key/val pair, collecting results to c.
	// NOTE the key-value pairs will be reused on next call to Map, so don't
	// make a deep copy if you want to save the contents.
	// If sophie.EOM is returned the mapping is stopped (as sucess).
	// If other non-nil error is returned, the job is aborted as failure.
	// @param c  the slice of Collectors. Same length as Source.
	Map(key, val sophie.SophieWriter, c []sophie.Collector) error
	// Make a map action at final stage, collecting results to c
	// @param c  the slice of Collectors. Same length as Source.
	MapEnd(c []sophie.Collector) error
}

// MapOnlyJob is a job with a mapping step only.
type MapOnlyJob struct {
	// The slice of Inputs
	Source []Input

	// The factory for OnlyMappers
	NewMapperF func(src, part int) OnlyMapper

	// The slice of Outputs
	Dest []Output
}

// Runs the job.
// If some of the mapper failed, one of the error is returned.
func (job *MapOnlyJob) Run() error {
	if job.NewMapperF == nil {
		return errors.New("MapOnlyJob: NewMapperF undefined!")
	}
	if job.Source == nil {
		return errors.New("MapOnlyJob: Source undefined!")
	}
	totalPart := 0
	endss := make([][]chan error, 0, len(job.Source))
	for i := range job.Source {
		partCount, err := job.Source[i].PartCount()
		if err != nil {
			return err
		}
		ends := make([]chan error, 0, partCount)
		for part := 0; part < partCount; part++ {
			end := make(chan error, 1)
			ends = append(ends, end)
			go func(i, part, totalPart int, end chan error) {
				end <- func() error {
					mapper := job.NewMapperF(i, part)
					key, val := mapper.NewKey(), mapper.NewVal()
					cs := make([]sophie.Collector, 0, len(job.Dest))
					for _, dst := range job.Dest {
						c, err := dst.Collector(totalPart)
						if err != nil {
							return errorsp.WithStacksAndMessage(err, "open collector for source %d part %d failed", i, part)
						}
						defer c.Close()
						cs = append(cs, c)
					}
					iter, err := job.Source[i].Iterator(part)
					if err != nil {
						return errorsp.WithStacksAndMessage(err, " open source %d part %d failed", i, part)
					}
					defer iter.Close()

					for {
						if err := iter.Next(key, val); err != nil {
							if errorsp.Cause(err) != io.EOF {
								return errorsp.WithStacksAndMessage(err, "next failed")
							}
							break
						}
						if err := mapper.Map(key, val, cs); err != nil {
							if errorsp.Cause(err) == EOM {
								log.Print("EOM returned, exit early")
								break
							}
							return errorsp.WithStacksAndMessage(err, "mapping %v %v failed", key, val)
						}
					}
					return errorsp.WithStacksAndMessage(mapper.MapEnd(cs), "map end failed")
				}()
			}(i, part, totalPart, end)
			totalPart++
		}
		endss = append(endss, ends)
	}
	var errReturned error
	for _, ends := range endss {
		for part, end := range ends {
			log.Printf("Waiting for mapper %d...", part)
			if err := <-end; err != nil {
				log.Printf("Error returned for part %d: %v", part, err)
				errReturned = err
			}
			log.Printf("No error for mapper %d...", part)
		}
	}
	return errReturned
}
