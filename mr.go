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
	Map(key, val SophieWriter, c Collector) error
	// Make a map action at final stage, collecting results to c
	MapEnd(c Collector) error
}

// The factory interface for generating OnlyMappers
type OnlyMapperFactory interface {
	// New an OnlyMapper for a particular partition
	NewMapper(part int) OnlyMapper
}

type Input interface {
	PartCount() (int, error)
	Iterator(index int) (IterateCloser, error)
}

type Output interface {
	Collector(index int) (CollectCloser, error)
}

type MapOnlyJob struct {
	// The factory for OnlyMappers
	MapFactory OnlyMapperFactory

	Source Input
	Dest   Output
}

func (job *MapOnlyJob) Run() error {
	if job.MapFactory == nil {
		return errors.New("MapOnlyJob: MapFactory undefined!")
	}
	if job.Source == nil {
		return errors.New("MapOnlyJob: Source undefined!")
	}
	if job.Dest == nil {
		return errors.New("MapOnlyJob: Dest undefined!")
	}
	
	partCount, err := job.Source.PartCount()
	if err != nil {
		return err
	}

	ends := make([]chan error, 0, partCount)
	for part := 0; part < partCount; part++ {
		end := make(chan error, 1)
		ends = append(ends, end)
		go func(part int, end chan error) {
			end <- func() error {
				mapper := job.MapFactory.NewMapper(part)
				key, val := mapper.NewKey(), mapper.NewVal()
				c, err := job.Dest.Collector(part)
				if err != nil {
					return err
				}
				iter, err := job.Source.Iterator(part)
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
		
					if err := mapper.Map(key, val, c); err != nil {
						if err == EOM {
							break
						}
						return err
					}
				}
				
				return mapper.MapEnd(c)
			}()
		}(part, end)
	}
	
	for part, end := range ends {
		if err := <- end; err != nil {
			log.Printf("Error returned for part %d: %v", part, err)
			return err
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

type EmptyMapper struct{}

func (EmptyMapper) MapEnd(c PartCollector) error {
	return nil
}

type MapperFactory interface {
	NewMapper(part int) Mapper
}

type SophierIterator func() (Sophier, error)

type Reducer interface {
	NewKey() Sophier
	NewVal() Sophier
	Reduce(key SophieWriter, nextVal SophierIterator, c Collector) error
	ReduceEnd(c Collector) error
}

type EmptyReducer struct{}

func (EmptyReducer) ReduceEnd(c Collector) error {
	return nil
}

type ReducerFactory interface {
	NewReducer(part int) Reducer
}

type singleReducerFactory struct {
	Reducer
}

func (self singleReducerFactory) NewReducer(part int) Reducer {
	return self.Reducer
}
func SingleReducerFactory(reducer Reducer) ReducerFactory {
	return singleReducerFactory{reducer}
}

type MrJob struct {
	MapFactory MapperFactory
	RedFactory ReducerFactory

	Sorter Sorter

	Source Input
	Dest   Output
}

type ReduceIterator interface {
	Iterate(c Collector, r Reducer) error
}

func (job *MrJob) Run() error {
	/*
	 * Map
	 */
	partCount, err := job.Source.PartCount()
	if err != nil {
		return err
	}

	sorters := job.Sorter
	if sorters == nil {
		fmt.Println("Using memStorters...")
		sorters = &MemSorters{
			sorters: make(map[int]*MemSorter),
		}
	}

	ends := make([]chan error, 0, partCount)

	for part := 0; part < partCount; part++ {
		end := make(chan error, 1)
		ends = append(ends, end)
		go func(part int, end chan error) {
			end <- func() error {
				c, err := sorters.NewPartCollector(part)
				if err != nil {
					return err
				}
				mapper := job.MapFactory.NewMapper(part)
				key, val := mapper.NewKey(), mapper.NewVal()
				iter, err := job.Source.Iterator(part)
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
		}(part, end)
	}

	for _, end := range ends {
		err := <-end
		if err != nil {
			return err
		}
	}
	if err := sorters.ClosePartCollectors(); err != nil {
		fmt.Printf("sorters.ClosePartCollectors(): %v", err)
	}
	fmt.Printf("Map ends, begin to reduce\n")

	ends = ends[:0]
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
				c, err := job.Dest.Collector(part)
				if err != nil {
					return err
				}
				defer c.Close()
				reducer := job.RedFactory.NewReducer(part)
				return it.Iterate(c, reducer)
			}()
		}(part, end)
	}

	for _, end := range ends {
		err := <-end
		if err != nil {
			return err
		}
	}

	return nil
}
