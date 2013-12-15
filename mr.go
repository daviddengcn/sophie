package sophie

import (
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/daviddengcn/go-villa"
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

func (m EmptyMapper) MapEnd(c PartCollector) error {
	return nil
}

type SophierIterator func() (Sophier, error)

type Reducer interface {
	NewKey() Sophier
	NewVal() Sophier
	Reduce(key SophieWriter, nextVal SophierIterator, c Collector) error
}

type MrJob struct {
	Mapper  Mapper
	Reducer Reducer

	Source Input
	Dest   Output

	TempDir villa.Path
}

type MemSorter struct {
	Buffer  villa.ByteSlice
	KeyOffs villa.IntSlice
	ValOffs villa.IntSlice
	ValEnds villa.IntSlice
}

func (ms *MemSorter) Len() int {
	return len(ms.KeyOffs)
}

func bytesCmp(a, b []byte) int {
	for i := range a {
		if i >= len(b) {
			return 1
		}
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	// equal
	return 0
}

func (ms *MemSorter) Less(i, j int) bool {
	si := ms.Buffer[ms.KeyOffs[i]:ms.ValOffs[i]]
	sj := ms.Buffer[ms.KeyOffs[j]:ms.ValOffs[j]]
	return bytesCmp(si, sj) <= 0
}

func (ms *MemSorter) Swap(i, j int) {
	ms.KeyOffs.Swap(i, j)
	ms.ValOffs.Swap(i, j)
	ms.ValEnds.Swap(i, j)
}

func (ms *MemSorter) Iterate(c Collector, r Reducer) error {
	if len(ms.KeyOffs) == 0 {
		// nothing to iterate
		return nil
	}

	key, val := r.NewKey(), r.NewVal()
	nextKey := r.NewKey()
	idx := 0
	keyBuf := ms.Buffer[ms.KeyOffs[idx]:ms.ValOffs[idx]]
	key.ReadFrom(&keyBuf)
	for idx < len(ms.KeyOffs) {
		valBuf := ms.Buffer[ms.ValOffs[idx]:ms.ValEnds[idx]]
		val.ReadFrom(&valBuf)
		idx++

		curVal := val

		if err := r.Reduce(key, func() (s Sophier, err error) {
			if curVal == nil {
				return nil, EOF
			}
			s = curVal
			curVal = nil

			if idx < len(ms.KeyOffs) {
				keyBuf0 := ms.Buffer[ms.KeyOffs[idx-1]:ms.ValOffs[idx-1]]
				keyBuf := ms.Buffer[ms.KeyOffs[idx]:ms.ValOffs[idx]]
				if bytesCmp(keyBuf0, keyBuf) == 0 {
					// same key
					valBuf := ms.Buffer[ms.ValOffs[idx]:ms.ValEnds[idx]]
					val.ReadFrom(&valBuf)
					idx++

					curVal = val
				}
				nextKey.ReadFrom(&keyBuf)
			}
			return
		}, c); err != nil {
			return err
		}
		// nextKey stores the key of the current idx
		key, nextKey = nextKey, key
	}

	return nil
}

type MemSorters map[int]*MemSorter

func (ms MemSorters) CollectTo(part int, key, val SophieWriter) error {
	sorter, ok := ms[part]
	if !ok {
		sorter = &MemSorter{}
		ms[part] = sorter
	}
	sorter.KeyOffs.Add(len(sorter.Buffer))
	key.WriteTo(&sorter.Buffer)
	sorter.ValOffs.Add(len(sorter.Buffer))
	val.WriteTo(&sorter.Buffer)
	sorter.ValEnds.Add(len(sorter.Buffer))

	return nil
}

func (job *MrJob) Run() error {
	/*
	 * Map
	 */
	partCount, err := job.Source.PartCount()
	if err != nil {
		return err
	}

	mapper := job.Mapper
	key, val := mapper.NewKey(), mapper.NewVal()

	sorters := make(MemSorters)

	for i := 0; i < partCount; i++ {
		iter, err := job.Source.Iterator(i)
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

			if err := mapper.Map(key, val, sorters); err != nil {
				return err
			}
		}
	}

	fmt.Printf("Map ends, begin to reduce\n")

	for part, sorter := range sorters {
		fmt.Printf("Sorting part %d: %d entries\n", part, len(sorter.KeyOffs))
		sort.Sort(sorter)
		fmt.Printf("Sorted: %v\n", sorter.KeyOffs)
		c, err := job.Dest.Collector(part)
		if err != nil {
			return err
		}
		defer c.Close()
		sorter.Iterate(c, job.Reducer)
	}

	return nil
}
