package sophie

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/daviddengcn/go-assert"
	//	"github.com/daviddengcn/go-villa"
)

type linesIter struct {
	EmptyClose
	pos   int
	lines []string
}

func (iter *linesIter) Next(key, val SophieReader) error {
	if iter.pos >= len(iter.lines) {
		return EOF
	}
	*(key.(*RawString)) = RawString(iter.lines[iter.pos])
	iter.pos++
	return nil
}

type linesInput []string

func (lines linesInput) PartCount() (int, error) {
	return 1, nil
}

func (lines linesInput) Iterator(int) (IterateCloser, error) {
	return &linesIter{lines: lines}, nil
}

type LinesCounterMapper struct {
	EmptyOnlyMapper
	EmptyClose

	intList []Int32
}

func (lcm *LinesCounterMapper) Collector(index int) (CollectCloser, error) {
	return lcm, nil
}

func (lcm *LinesCounterMapper) Collect(key, val SophieWriter) error {
	if i, ok := key.(Int32); ok {
		lcm.intList = append(lcm.intList, i)
	}
	if i, ok := val.(Int32); ok {
		lcm.intList = append(lcm.intList, i)
	}
	return nil
}

func (lcm *LinesCounterMapper) NewKey() Sophier {
	return new(RawString)
}
func (lcm *LinesCounterMapper) NewVal() Sophier {
	return Null{}
}
func (lcm *LinesCounterMapper) Map(key, val SophieWriter, c []Collector) error {
	//fmt.Printf("Mapping (%v, %v) ...\n", key, val)
	c[0].Collect(Int32(1), Null{})
	return nil
}

const (
	WORDS = `
Writer implements buffering for an io.Writer object.
If an error occurs writing to a Writer, no more data will be
accepted and all subsequent writes will return the error.
After all data has been written, the client should call the 
Flush method to guarantee all data has been forwarded to the
underlying io.Writer.
At least eight people are dead after a bizarre mix of weather across the 
country spawned tornadoes, ice storms and record-setting warmth this weekend.
Four of the deaths involved two vehicle accidents in Kentucky. Three people 
drowned in the Rolling Fork River near New Hope when a car drove into the water.
Two people escaped but were hospitalized with hypothermia.
"Water was out of the banks, considerably up onto the roadway area. They ran 
their vehicle into the water. Two of the folks were exiting the vehicle as the 
swift water started pushing the vehicle downstream. The other three occupants 
of the vehicle were unable to exit," said Joe Prewitt, with Nelson County 
Emergency Management.
111`
)

func TestMapOnly(t *testing.T) {
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper LinesCounterMapper

	job := MapOnlyJob{
		MapFactory: OnlyMapperFactoryFunc(func(src, part int) OnlyMapper{
			return &mapper
		}),
		
		Source: []Input{lines},
		Dest:   []Output{&mapper},
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())
	assert.Equals(t, "len(dest)", len(mapper.intList), len(lines))
}

type WordCountMapper struct {
	EmptyMapper
}

func (wcm *WordCountMapper) NewKey() Sophier {
	return new(RawString)
}
func (wcm *WordCountMapper) NewVal() Sophier {
	return Null{}
}

func (wcm *WordCountMapper) Map(key, val SophieWriter, c PartCollector) error {
	//fmt.Printf("WordCountMapper (%v, %v) ...\n", key, val)
	line := *(key.(*RawString))
	words := strings.Split(string(line), " ")
	for _, word := range words {
		if len(word) == 0 {
			continue
		}
		word = strings.ToLower(word)
		//fmt.Printf("CollectTo %v\n", word)
		c.CollectTo(int(word[0]), RawString(word), RawVInt(1))
		//		c.CollectTo(0, RawString(word), RawVInt(1))
	}
	return nil
}

type WordCountReducer struct {
	EmptyClose
	EmptyReducer
	sync.Mutex
	counts map[string]int
}

func (wc *WordCountReducer) NewKey() Sophier {
	return new(RawString)
}

func (wc *WordCountReducer) NewVal() Sophier {
	return new(RawVInt)
}

func (wc *WordCountReducer) Reduce(key SophieWriter, nextVal SophierIterator,
	c []Collector) error {
	// fmt.Printf("Reducing %v\n", key)
	var count RawVInt
	for {
		val, err := nextVal()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}
		// fmt.Println("WordCountReducer.Reduce", key, val)
		count += *(val.(*RawVInt))
	}

	// fmt.Println("WordCountReducer.Reduce c.Collect", key, count)

	return c[0].Collect(key, count)
}

func (wc *WordCountReducer) Collect(key, val SophieWriter) error {
	wc.Lock()
	defer wc.Unlock()

	wc.counts[key.(*RawString).Val()] = int(val.(RawVInt))
	//	fmt.Printf("Result %v: %v\n", key, val)
	return nil
}

func (wc *WordCountReducer) Collector(index int) (CollectCloser, error) {
	return wc, nil
}

func statWords(text string) map[string]int {
	cnts := make(map[string]int)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		words := strings.Split(line, " ")
		for _, word := range words {
			if word == "" {
				continue
			}
			word = strings.ToLower(word)
			cnts[word] = cnts[word] + 1
		}
	}
	return cnts
}

func assertMapEquals(t *testing.T, act, exp map[string]int) {
	assert.Equals(t, "count", len(act), len(exp))
	for k, v := range exp {
		assert.Equals(t, "count of "+k, act[k], v)
	}
}

func TestMapReduce(t *testing.T) {
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper WordCountMapper
	reducer := WordCountReducer{counts: make(map[string]int)}

	job := MrJob{
		MapFactory: MapperFactoryFunc(func(src, part int) Mapper {
			return &mapper
		}),
		RedFactory: ReducerFactoryFunc(func(part int) Reducer {
			return &reducer
		}),
		Source:     []Input{lines},
		Dest:       []Output{&reducer},
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())

	expCnts := statWords(WORDS)
	// fmt.Println(reducer.counts)
	// fmt.Println(expCnts)

	assertMapEquals(t, reducer.counts, expCnts)
}

func TestMRFromFile(t *testing.T) {
	fmt.Println("TestMRFromFile starts")
	fpRoot := FsPath{
		Fs:   LocalFS,
		Path: ".",
	}

	mrin := fpRoot.Join("mrin")
	mrin.Mkdir(0755)

	mrtmp := fpRoot.Join("tmp")

	/*
	 * Prepare input
	 */
	var inF *KVWriter = nil
	index := 0
	lines := strings.Split(WORDS, "\n")
	for i, line := range lines {
		if i%3 == 0 {
			if inF != nil {
				assert.NoErrorf(t, "inF.Close: %v", inF.Close())
				index++
			}
			var err error
			inF, err = NewKVWriter(mrin.Join(fmt.Sprintf("part-%05d", index)))
			assert.NoErrorf(t, "NewKVWriter: %v", err)
		}

		assert.NoErrorf(t, "inF.Collect", inF.Collect(RawString(line), Null{}))
	}
	if inF != nil {
		assert.NoErrorf(t, "inF.Close: %v", inF.Close())
	}

	mrout := fpRoot.Join("mrout")
	assert.NoErrorf(t, "Remove mrout: %v", mrout.Remove())

	/*
	 * MrJob
	 */
	var mapper WordCountMapper
	reducer := WordCountReducer{counts: make(map[string]int)}

	job := MrJob{
		Source:     []Input{KVDirInput(mrin)},
		MapFactory: MapperFactoryFunc(func(src, part int) Mapper {
			return &mapper
		}),

		RedFactory: ReducerFactoryFunc(func(part int) Reducer {
			return &reducer
		}),
		Dest:       []Output{KVDirOutput(mrout)},

		Sorter: NewFileSorter(mrtmp),
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())

	/*
	 * Check result
	 */

	resIn := KVDirInput(mrout)
	n, err := resIn.PartCount()
	assert.NoErrorf(t, "resIn.PartCount(): %v", err)
	var word RawString
	var cnt RawVInt
	actCnts := make(map[string]int)
	for i := 0; i < n; i++ {
		iter, err := resIn.Iterator(i)
		assert.NoErrorf(t, "resIn.Iterator: %v", err)
		for {
			err := iter.Next(&word, &cnt)
			if err == EOF {
				break
			}
			assert.NoErrorf(t, "iter.Next: %v", err)
			actCnts[string(word)] = int(cnt)
		}
	}

	expCnts := statWords(WORDS)
	// fmt.Println(expCnts)
	// fmt.Println(actCnts)

	assertMapEquals(t, actCnts, expCnts)
	fmt.Println("TestMRFromFile ends")
}


// a mapper that CollectTo 0 a (RawString("part"), VInt(self)) pair at MapEnd.
type partMapper int

func (partMapper) NewKey() Sophier {
	return NULL
}

func (partMapper) NewVal() Sophier {
	return NULL
}

func (partMapper) Map(key, val SophieWriter, c PartCollector) error {
	return nil
}

func (pm partMapper) MapEnd(c PartCollector) error {
	return c.CollectTo(0, RawString("part"), VInt(pm))
}

// an Input with specified part number but no entries for each part
type emptyInput int

func (ei emptyInput) PartCount() (int, error) {
	return int(ei), nil
}

func (ei emptyInput) Iterator(index int) (IterateCloser, error) {
	return ei, nil
}

func (ei emptyInput) Next(key, val SophieReader) error {
	return EOF
}

func (ei emptyInput) Close() error {
	return nil
}

// reducer
type intsetReducer map[VInt]bool

func (intsetReducer) NewKey() Sophier {
	return new(RawString)
}

func (intsetReducer) NewVal() Sophier {
	return new(VInt)
}

func (st intsetReducer) Reduce(key SophieWriter, nextVal SophierIterator,
c []Collector) error {
	keyStr := key.(*RawString).String()
	if keyStr != "part" {
		return errors.New(`Key should be "part"`)
	}
	for {
		val, err := nextVal()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}
		
		part := *val.(*VInt)
		if st[part] {
			return errors.New(fmt.Sprintf("Duplicated value: %v", part))
		}
		st[part] = true
	}
	return nil
}

func (st intsetReducer) ReduceEnd(c []Collector) error {
	return nil
}

func TestReduceValues(t *testing.T) {
	job := MrJob {
		MapFactory: MapperFactoryFunc(func(src, part int) Mapper {
			return partMapper(part)
		}),
		RedFactory: ReducerFactoryFunc(func(part int) Reducer {
			return make(intsetReducer)
		}),
		
		Source: []Input{
			emptyInput(2),
		},
	}
	assert.NoErrorf(t, "job.Run failed: %v", job.Run())
}
