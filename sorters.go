package sophie

import (
	"fmt"
	"sort"
	"sync"

	"github.com/daviddengcn/go-villa"
)

type Sorter interface {
	NewPartCollector(inPart int) (PartCollector, error)
	ClosePartCollectors() error
	ReduceParts() []int
	NewReduceIterator(part int) (ReduceIterator, error)
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
	if len(b) > len(a) {
		return -1
	}
	// equal
	return 0
}

/*
 * MemSorters
 */

type MemSorter struct {
	sync.Mutex
	Buffer  villa.ByteSlice
	KeyOffs villa.IntSlice
	ValOffs villa.IntSlice
	ValEnds villa.IntSlice
}

func (ms *MemSorter) Len() int {
	return len(ms.KeyOffs)
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

func (ms *MemSorter) Iterate(c []Collector, r Reducer) error {
	key, val := r.NewKey(), r.NewVal()
	for idx := 0; idx < len(ms.KeyOffs); {
		keyBuf := ms.Buffer[ms.KeyOffs[idx]:ms.ValOffs[idx]]
		if err := key.ReadFrom(&keyBuf, len(keyBuf)); err != nil {
			return err
		}

		curVal := idx
		idx++

		valIter := func() (Sophier, error) {
			if curVal < 0 {
				// not values for this key, return EOF
				return nil, EOF
			}
			// fetch value
			valBuf := ms.Buffer[ms.ValOffs[curVal]:ms.ValEnds[curVal]]
			if err := val.ReadFrom(&valBuf, len(valBuf)); err != nil {
				return nil, err
			}
			curVal = -1

			if idx < len(ms.KeyOffs) {
				keyBuf0 := ms.Buffer[ms.KeyOffs[idx-1]:ms.ValOffs[idx-1]]
				keyBuf := ms.Buffer[ms.KeyOffs[idx]:ms.ValOffs[idx]]
				if bytesCmp(keyBuf0, keyBuf) == 0 {
					// same key, prepare next value
					curVal = idx
					idx++
				}
			}
			return val, nil
		}

		if err := r.Reduce(key, valIter, c); err != nil {
			return err
		}
		// iterate to end in case the reducer doesn't
		for curVal >= 0 {
			if _, err := valIter(); err != nil {
				if err != EOF {
					return err
				}
			}
		}
	}

	r.ReduceEnd(c)

	return nil
}

type MemSorters struct {
	sync.RWMutex
	sorters map[int]*MemSorter
}

func (ms *MemSorters) CollectTo(part int, key, val SophieWriter) error {
	ms.RLock()
	sorter, ok := ms.sorters[part]
	ms.RUnlock()
	if !ok {
		ms.Lock()
		sorter, ok = ms.sorters[part]
		if !ok {
			sorter = &MemSorter{}
			ms.sorters[part] = sorter
		}
		ms.Unlock()
	}
	sorter.Lock()
	defer sorter.Unlock()

	sorter.KeyOffs.Add(len(sorter.Buffer))
	key.WriteTo(&sorter.Buffer)
	sorter.ValOffs.Add(len(sorter.Buffer))
	val.WriteTo(&sorter.Buffer)
	sorter.ValEnds.Add(len(sorter.Buffer))

	return nil
}

func (ms *MemSorters) NewPartCollector(int) (PartCollector, error) {
	return ms, nil
}

func (*MemSorters) ClosePartCollectors() error {
	return nil
}

func (ms *MemSorters) ReduceParts() []int {
	parts := make([]int, 0, len(ms.sorters))
	for part, _ := range ms.sorters {
		parts = append(parts, part)
	}
	return parts
}

func (ms *MemSorters) NewReduceIterator(part int) (ReduceIterator, error) {
	sorter := ms.sorters[part]
	sort.Sort(sorter)
	return sorter, nil
}

/*
 * FileSorter
 */

type mapOut struct {
	sync.Mutex
	rawPath FsPath
	writer  *KVWriter
	reader  *KVReader
}

func (mo *mapOut) Collect(key, val SophieWriter) error {
	mo.Lock()
	defer mo.Unlock()

	// fmt.Println("Collect", key, val)

	return mo.writer.Collect(key, val)
}

func sophieCmp(a, b Sophier) int {
	var bufA, bufB villa.ByteSlice
	a.WriteTo(&bufA)
	b.WriteTo(&bufB)
	return bytesCmp(bufA, bufB)
}

func (mo *mapOut) Iterate(c []Collector, r Reducer) error {
	key, val := r.NewKey(), r.NewVal()
	err := mo.reader.Next(key, val)
	if err != nil {
		if err == EOF {
			// empty input
			return nil
		}
		return err
	}

	nextKey, nextVal := r.NewKey(), r.NewVal()
	for {
		curVal := val
		valIter := func() (s Sophier, err error) {
			if curVal == nil {
				return nil, EOF
			}
			s, curVal = curVal, nil

			err = mo.reader.Next(nextKey, nextVal)
			if err != nil {
				if err != EOF {
					return s, err
				}
				// all key/val read
				nextKey, nextVal = nil, nil
			}
			if nextKey != nil && sophieCmp(key, nextKey) == 0 {
				curVal = nextVal
				val, nextVal = nextVal, val
			}
			return s, nil
		}
		if err := r.Reduce(key, valIter, c); err != nil {
			return err
		}
		// r.Reduce could return before iterating all values
		for curVal != nil {
			if _, err := valIter(); err != nil {
				if err != EOF {
					return err
				}
			}
		}
		if nextKey == nil {
			break
		}
		key, nextKey = nextKey, key
		val, nextVal = nextVal, val
	}

	r.ReduceEnd(c)

	return nil
}

type FileSorter struct {
	sync.RWMutex
	TmpFolder FsPath
	mapOuts   map[int]*mapOut
	sortToken chan bool
}

func NewFileSorter(TmpFolder FsPath) *FileSorter {
	sortToken := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		sortToken <- true
	}
	TmpFolder.Join("mapOut").Remove()
	TmpFolder.Join("sorted").Remove()
	return &FileSorter{
		TmpFolder: TmpFolder,
		mapOuts:   make(map[int]*mapOut),
		sortToken: sortToken,
	}
}

func (fs *FileSorter) CollectTo(part int, key, val SophieWriter) error {
	fs.RLock()
	mo, ok := fs.mapOuts[part]
	fs.RUnlock()
	if !ok {
		fs.Lock()
		mo, ok = fs.mapOuts[part]
		if !ok {
			fldMapOut := fs.TmpFolder.Join("mapOut")
			fldMapOut.Mkdir(0755)
			path := fldMapOut.Join(fmt.Sprintf("part-%05d", part))

			writer, err := NewKVWriter(path)
			if err != nil {
				fs.Unlock()
				return err
			}
			mo = &mapOut{rawPath: path, writer: writer}
			fs.mapOuts[part] = mo
		}
		fs.Unlock()
	}
	return mo.Collect(key, val)
}

func (fs *FileSorter) NewPartCollector(int) (PartCollector, error) {
	return fs, nil
}

func (fs *FileSorter) ClosePartCollectors() (err error) {
	for _, mo := range fs.mapOuts {
		if e := mo.writer.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (fs *FileSorter) ReduceParts() []int {
	parts := make([]int, 0, len(fs.mapOuts))
	for part, _ := range fs.mapOuts {
		parts = append(parts, part)
	}
	// fmt.Println("FileSorter ReduceParts", parts)
	return parts
}

type offsSorter struct {
	Buffer           villa.ByteSlice
	KeyOffs, KeyEnds villa.IntSlice
	ValOffs, ValEnds villa.IntSlice
}

func (os *offsSorter) Len() int {
	return len(os.KeyOffs)
}

func (os *offsSorter) Less(i, j int) bool {
	si := os.Buffer[os.KeyOffs[i]:os.KeyEnds[i]]
	sj := os.Buffer[os.KeyOffs[j]:os.ValEnds[j]]
	return bytesCmp(si, sj) <= 0
}

func (os *offsSorter) Swap(i, j int) {
	os.KeyOffs.Swap(i, j)
	os.KeyEnds.Swap(i, j)
	os.ValOffs.Swap(i, j)
	os.ValEnds.Swap(i, j)
}

func (fs *FileSorter) NewReduceIterator(part int) (ReduceIterator, error) {
	mo := fs.mapOuts[part]
	<-fs.sortToken
	defer func() {
		fs.sortToken <- true
	}()

	// read
	var os offsSorter
	var err error
	os.Buffer, os.KeyOffs, os.KeyEnds, os.ValOffs, os.ValEnds, err =
		ReadAsByteOffs(mo.rawPath)
	if err != nil {
		return nil, err
	}
	// sort
	sort.Sort(&os)
	// save
	fldSorted := fs.TmpFolder.Join("sorted")
	fldSorted.Mkdir(0755)
	redIn := fldSorted.Join(fmt.Sprintf("part-%05d", part))
	if err := WriteByteOffs(redIn, os.Buffer, os.KeyOffs, os.KeyEnds,
		os.ValOffs, os.ValEnds); err != nil {
		return nil, err
	}

	mo.reader, err = NewKVReader(redIn)
	if err != nil {
		return nil, err
	}

	return mo, nil
}
