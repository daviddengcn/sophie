package sophie

import (
	"errors"
	"fmt"
	"io"
	"log"
)

var (
	EOF = errors.New("EOF")
	// end of map, an error returned by a Mapper/OnlyMapper.Map indicating a stop
	// of continuing mapping
	EOM = errors.New("EOM")
)

type Iterator interface {
	Next(key, val SophieReader) error
}

type IterateCloser interface {
	Iterator
	io.Closer
}

type Collector interface {
	Collect(key, val SophieWriter) error
}

type CollectCloser interface {
	Collector
	io.Closer
}

type PartCollector interface {
	CollectTo(part int, key, val SophieWriter) error
}

// OnlyMapper is an interface defining the map actions for MapOnlyJob
type OnlyMapper interface {
	// NewKey returns a new instance of key
	NewKey() Sophier
	// NewVal returns a new instance of value
	NewVal() Sophier
	// Make a map action for a key/val pair, collecting results to c
	// If sophie.EOM is returned the mapping is stopped (as sucess).
	// If other non-nil error is returned, the job is aborted as failure.
	Map(key, val SophieWriter, c []Collector) error
	// Make a map action at final stage, collecting results to c
	MapEnd(c []Collector) error
}

// The factory interface for generating OnlyMappers
type OnlyMapperFactory interface {
	// New an OnlyMapper for a particular part and source index
	NewMapper(src, part int) OnlyMapper
}

// Input represents a specified input source
type Input interface {
	PartCount() (int, error)
	// index range [0, PartCount())
	Iterator(index int) (IterateCloser, error)
}

type Output interface {
	Collector(index int) (CollectCloser, error)
}

type MapOnlyJob struct {
	// The factory for OnlyMappers
	MapFactory OnlyMapperFactory

	Source []Input
	Dest   []Output
}

func (job *MapOnlyJob) Run() error {
	if job.MapFactory == nil {
		return errors.New("MapOnlyJob: MapFactory undefined!")
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
					mapper := job.MapFactory.NewMapper(i, part)
					key, val := mapper.NewKey(), mapper.NewVal()
					cs := make([]Collector, 0, len(job.Dest))
					for _, dst := range job.Dest {
						c, err := dst.Collector(totalPart)
						if err != nil {
							return err
						}
						defer c.Close()
						cs = append(cs, c)
					}
					iter, err := job.Source[i].Iterator(part)
					if err != nil {
						return err
					}
					defer iter.Close()

					for {
						if err := iter.Next(key, val); err != nil {
							if err != EOF {
								return err
							}
							break
						}

						if err := mapper.Map(key, val, cs); err != nil {
							if err == EOM {
								break
							}
							return err
						}
					}

					return mapper.MapEnd(cs)
				}()
			}(i, part, totalPart, end)
		}
		totalPart++
		endss = append(endss, ends)
	}

	for _, ends := range endss {
		for part, end := range ends {
			log.Printf("Waiting for mapper %d...", part)
			if err := <-end; err != nil {
				// FIXME return after all ends
				log.Printf("Error returned for part %d: %v", part, err)
				return err
			}
			log.Printf("No error for mapper %d...", part)
		}
	}

	return nil
}

type Mapper interface {
	NewKey() Sophier
	NewVal() Sophier
	Map(key, val SophieWriter, c PartCollector) error
	MapEnd(c PartCollector) error
}

// MapperFactory is a factory of genearting Mapper instances.
type MapperFactory interface {
	// Returns a Mapper of specified indexes in the Source and parts.
	NewMapper(src, part int) Mapper
}

type SophierIterator func() (Sophier, error)

type Reducer interface {
	NewKey() Sophier
	NewVal() Sophier
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
	Reduce(key SophieWriter, nextVal SophierIterator, c []Collector) error
	ReduceEnd(c []Collector) error
}

type ReducerFactory interface {
	NewReducer(part int) Reducer
}

type MrJob struct {
	MapFactory MapperFactory
	RedFactory ReducerFactory

	Sorter Sorter

	Source []Input
	Dest   []Output
}

type ReduceIterator interface {
	Iterate(c []Collector, r Reducer) error
}

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
		sorters = &MemSorters{
			sorters: make(map[int]*MemSorter),
		}
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
							if err == EOF {
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
		}

		endss = append(endss, ends)
		totalPart++
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
				cs := make([]Collector, 0, len(job.Dest))
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
