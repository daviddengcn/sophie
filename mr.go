package sophie

import (
	"errors"
	"fmt"
	"io"
)

var (
	EOF = errors.New("EOF")
)

type EmptyClose struct{}

func (EmptyClose) Close() error {
	return nil
}

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

type OnlyMapper interface {
	NewKey() Sophier
	NewVal() Sophier
	Map(key, val SophieWriter, c Collector) error
	MapEnd(c Collector) error
}

type EmptyOnlyMapper struct{}

func (EmptyOnlyMapper) MapEnd(c Collector) error {
	return nil
}

type Input interface {
	PartCount() (int, error)
	Iterator(index int) (IterateCloser, error)
}

type Output interface {
	Collector(index int) (CollectCloser, error)
}

type MapOnlyJob struct {
	// TODO change to OnlyMapperFactory
	Mapper OnlyMapper

	Source Input
	Dest   Output
}

func (job *MapOnlyJob) Run() error {
	partCount, err := job.Source.PartCount()
	if err != nil {
		return err
	}

	mapper := job.Mapper
	key, val := mapper.NewKey(), mapper.NewVal()

	for i := 0; i < partCount; i++ {
		c, err := job.Dest.Collector(i)
		if err != nil {
			return err
		}
		iter, err := job.Source.Iterator(i)
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
				return err
			}
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

type singleMapperFactory struct {
	Mapper
}

func (self singleMapperFactory) NewMapper(part int) Mapper {
	return self.Mapper
}

func SingleMapperFactory(mapper Mapper) MapperFactory {
	return singleMapperFactory{mapper}
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

	ends := make([]chan error, partCount)

	for i := 0; i < partCount; i++ {
		ends[i] = make(chan error, 1)
		c, err := sorters.NewPartCollector(i)
		if err != nil {
			// FIXME
			return err
		}
		go func(part int, end chan error, c PartCollector) {
			end <- func() error {
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
				mapper.MapEnd(c)
				fmt.Printf("Iterator %d finished\n", part)
				return nil
			}()
		}(i, ends[i], c)
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
