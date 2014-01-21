package mr

import (
	"errors"
	"fmt"
	"log"

	"github.com/daviddengcn/sophie"
)

// The mapping stage in MrJob.
type Mapper interface {
	// NewKey returns a new instance of the key Sophier object.
	NewKey() sophie.Sophier
	// NewVal returns a new instance of the value Sophier object.
	NewVal() sophie.Sophier
	// Map converts the input kv pair to what the Reducer expect and send to
	// the PartCollector.
	Map(key, val sophie.SophieWriter, c PartCollector) error
	// MapEnd is invoked after a partition of the Input is mapped.
	MapEnd(c PartCollector) error
}

// MapperFactory is a factory of genearting Mapper instances.
type MapperFactory interface {
	// Returns a Mapper of specified indexes in the Source and parts.
	NewMapper(src, part int) Mapper
}

// An interator for fetching a list of Sophiers. If sophie.EOF is returned as
// the error, no further Sophiers are avaiable.
type SophierIterator func() (sophie.Sophier, error)

// The reducing stage in MrJob.
type Reducer interface {
	// NewKey returns a new instance of the key Sophier object for reducing.
	NewKey() sophie.Sophier
	// NewVal returns a new instance of the value Sophier object for reducing.
	NewVal() sophie.Sophier
	// to get all values:
	//   for {
	//	 	val, err := nextVal()
	//   	if err == sophie.EOF {
	//   		break;
	//   	}
	//      if err != nil {
	//   		return err;
	//   	}
	//      ...
	//   }
	Reduce(key sophie.SophieWriter, nextVal SophierIterator,
		c []sophie.Collector) error
	// MapEnd is invoked after a partition of the reducing kv pairs is reduced.
	ReduceEnd(c []sophie.Collector) error
}

// ReducerFactory is the factory generating instances of Reducers
type ReducerFactory interface {
	// Returns a Reducer for a specified partion
	NewReducer(part int) Reducer
}

// An MrJob contains a mapping step and a reducing step. In reducing step, kv
// pairs are sorted by keys, and values of a key are reduced using the Reducer.
type MrJob struct {
	// The factory for Mappers
	MapFactory MapperFactory
	// The factory for Reducers
	RedFactory ReducerFactory

	// The Sorter that sorts kv pairs mapped by Mappers and provides
	// SophierIterator for Reducers.
	Sorter Sorter

	// The source Inputs
	Source []Input
	// The destination Outputs
	Dest []Output
}

// Runs the MrJob.
// If Sorter is not specified, MemSorters is used.
func (job *MrJob) Run() error {
	if job.MapFactory == nil {
		return errors.New("MrJob: MapFactory undefined!")
	}
	if job.RedFactory == nil {
		return errors.New("MrJob: RedFactory undefined!")
	}
	if job.Source == nil {
		return errors.New("MrJob: Source undefined!")
	}

	/*
	 * Map
	 */
	sorters := job.Sorter
	if sorters == nil {
		log.Println("Sorter not specified, using MemSorters...")
		sorters = NewMemSorters()
	}

	log.Println("Start mapping...")
	endss := make([][]chan error, 0, len(job.Source))
	totalPart := 0
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
					c, err := sorters.NewPartCollector(totalPart)
					if err != nil {
						return err
					}
					mapper := job.MapFactory.NewMapper(i, part)
					key, val := mapper.NewKey(), mapper.NewVal()
					iter, err := job.Source[i].Iterator(part)
					if err != nil {
						return err
					}
					defer iter.Close()

					for {
						if err := iter.Next(key, val); err != nil {
							if err == sophie.EOF {
								break
							}
							return err
						}

						if err := mapper.Map(key, val, c); err != nil {
							return err
						}
					}
					return mapper.MapEnd(c)
				}()
			}(i, part, totalPart, end)
			totalPart++
		}

		endss = append(endss, ends)
	}

	for _, ends := range endss {
		for _, end := range ends {
			err := <-end
			if err != nil {
				return err
			}
		}
	}
	if err := sorters.ClosePartCollectors(); err != nil {
		fmt.Printf("sorters.ClosePartCollectors(): %v", err)
	}
	log.Printf("Map ends, begin to reduce")

	var ends []chan error
	parts := sorters.ReduceParts()
	for _, part := range parts {
		end := make(chan error, 1)
		ends = append(ends, end)
		go func(part int, end chan error) {
			end <- func() error {
				it, err := sorters.NewReduceIterator(part)
				if err != nil {
					return err
				}
				cs := make([]sophie.Collector, 0, len(job.Dest))
				for _, dst := range job.Dest {
					c, err := dst.Collector(part)
					if err != nil {
						return err
					}
					defer c.Close()
					cs = append(cs, c)
				}
				reducer := job.RedFactory.NewReducer(part)
				return it.Iterate(cs, reducer)
			}()
		}(part, end)
	}

	for _, end := range ends {
		err := <-end
		if err != nil {
			return err
		}
	}
	log.Println("Reduce ends.")

	return nil
}
