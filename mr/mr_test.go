package mr

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/daviddengcn/go-assert"
	"github.com/daviddengcn/sophie"
	"github.com/daviddengcn/sophie/kv"
)

type linesIter struct {
	sophie.EmptyClose
	pos   int
	lines []string
}

func (iter *linesIter) Next(key, val sophie.SophieReader) error {
	if iter.pos >= len(iter.lines) {
		return sophie.EOF
	}
	*(key.(*sophie.RawString)) = sophie.RawString(iter.lines[iter.pos])
	iter.pos++
	return nil
}

type linesInput []string

func (lines linesInput) PartCount() (int, error) {
	return 1, nil
}

func (lines linesInput) Iterator(int) (sophie.IterateCloser, error) {
	return &linesIter{lines: lines}, nil
}

type LinesCounterMapper struct {
	sophie.EmptyClose
	intList []sophie.Int32
}

func (lcm *LinesCounterMapper) Collector(index int) (sophie.CollectCloser, error) {
	return lcm, nil
}

func (lcm *LinesCounterMapper) Collect(key, val sophie.SophieWriter) error {
	if i, ok := key.(sophie.Int32); ok {
		lcm.intList = append(lcm.intList, i)
	}
	if i, ok := val.(sophie.Int32); ok {
		lcm.intList = append(lcm.intList, i)
	}
	return nil
}

func (lcm *LinesCounterMapper) NewKey() sophie.Sophier {
	return new(sophie.RawString)
}
func (lcm *LinesCounterMapper) NewVal() sophie.Sophier {
	return sophie.Null{}
}
func (lcm *LinesCounterMapper) Map(key, val sophie.SophieWriter,
	c []sophie.Collector) error {

	//fmt.Printf("Mapping (%v, %v) ...\n", key, val)
	c[0].Collect(sophie.Int32(1), sophie.Null{})
	return nil
}
func (lcm *LinesCounterMapper) MapEnd(c []sophie.Collector) error {
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
	fmt.Println(">>> TestMapOnly")
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper LinesCounterMapper

	job := MapOnlyJob{
		NewMapperF: func(src, part int) OnlyMapper {
			return &mapper
		},

		Source: []Input{lines},
		Dest:   []Output{&mapper},
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())
	assert.Equals(t, "len(dest)", len(mapper.intList), len(lines))
}

type WordCountMapper struct{}

func (wcm *WordCountMapper) NewKey() sophie.Sophier {
	return new(sophie.RawString)
}
func (wcm *WordCountMapper) NewVal() sophie.Sophier {
	return sophie.Null{}
}
func (wcm *WordCountMapper) MapEnd(c PartCollector) error {
	return nil
}

func (wcm *WordCountMapper) Map(key, val sophie.SophieWriter, c PartCollector) error {
	//fmt.Printf("WordCountMapper (%v, %v) ...\n", key, val)
	line := *(key.(*sophie.RawString))
	words := strings.Split(string(line), " ")
	for _, word := range words {
		if len(word) == 0 {
			continue
		}
		word = strings.ToLower(word)
		//fmt.Printf("CollectTo %v\n", word)
		c.CollectTo(int(word[0]), sophie.RawString(word), sophie.RawVInt(1))
		//		c.CollectTo(0, RawString(word), RawVInt(1))
	}
	return nil
}

type WordCountReducer struct {
	sophie.EmptyClose
	sync.Mutex
	counts map[string]int
}

func (wc *WordCountReducer) NewKey() sophie.Sophier {
	return new(sophie.RawString)
}

func (wc *WordCountReducer) NewVal() sophie.Sophier {
	return new(sophie.RawVInt)
}

func (wc *WordCountReducer) Reduce(key sophie.SophieWriter,
	nextVal SophierIterator, c []sophie.Collector) error {

	// fmt.Printf("Reducing %v\n", key)
	var count sophie.RawVInt
	for {
		val, err := nextVal()
		if err == sophie.EOF {
			break
		}
		if err != nil {
			return err
		}
		// fmt.Println("WordCountReducer.Reduce", key, val)
		count += *(val.(*sophie.RawVInt))
	}

	// fmt.Println("WordCountReducer.Reduce c.Collect", key, count)

	return c[0].Collect(key, count)
}
func (wc *WordCountReducer) ReduceEnd(c []sophie.Collector) error {
	return nil
}

func (wc *WordCountReducer) Collect(key, val sophie.SophieWriter) error {
	wc.Lock()
	defer wc.Unlock()

	wc.counts[key.(*sophie.RawString).Val()] = int(val.(sophie.RawVInt))
	//	fmt.Printf("Result %v: %v\n", key, val)
	return nil
}

func (wc *WordCountReducer) Collector(index int) (sophie.CollectCloser, error) {
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
	fmt.Println(">>> TestMapReduce")
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper WordCountMapper
	reducer := WordCountReducer{counts: make(map[string]int)}

	job := MrJob{
		Source: []Input{lines},
		NewMapperF: func(src, part int) Mapper {
			return &mapper
		},
		NewReducerF: func(part int) Reducer {
			return &reducer
		},
		Dest: []Output{&reducer},
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())

	expCnts := statWords(WORDS)
	// fmt.Println(reducer.counts)
	// fmt.Println(expCnts)

	assertMapEquals(t, reducer.counts, expCnts)
}

func TestMRFromFile(t *testing.T) {
	fmt.Println(">>> TestMRFromFile")
	fpRoot := sophie.LocalFsPath(".")

	mrin := fpRoot.Join("mrin")
	mrin.Mkdir(0755)

	mrtmp := fpRoot.Join("tmp")

	/*
	 * Prepare input
	 */
	var inF *kv.Writer = nil
	index := 0
	lines := strings.Split(WORDS, "\n")
	for i, line := range lines {
		if i%3 == 0 {
			if inF != nil {
				assert.NoErrorf(t, "inF.Close: %v", inF.Close())
				index++
			}
			var err error
			inF, err = kv.NewWriter(mrin.Join(fmt.Sprintf("part-%05d", index)))
			assert.NoErrorf(t, "NewKVWriter: %v", err)
		}

		assert.NoErrorf(t, "inF.Collect",
			inF.Collect(sophie.RawString(line), sophie.Null{}))
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
		Source: []Input{kv.DirInput(mrin)},
		NewMapperF: func(src, part int) Mapper {
			return &mapper
		},

		Sorter: NewFileSorter(mrtmp),

		NewReducerF: func(part int) Reducer {
			return &reducer
		},
		Dest: []Output{kv.DirOutput(mrout)},
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())

	/*
	 * Check result
	 */
	resIn := kv.DirInput(mrout)
	n, err := resIn.PartCount()
	assert.NoErrorf(t, "resIn.PartCount(): %v", err)
	var word sophie.RawString
	var cnt sophie.RawVInt
	actCnts := make(map[string]int)
	for i := 0; i < n; i++ {
		iter, err := resIn.Iterator(i)
		assert.NoErrorf(t, "resIn.Iterator: %v", err)
		for {
			err := iter.Next(&word, &cnt)
			if err == sophie.EOF {
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
}

func TestReduceValues(t *testing.T) {
	fmt.Println(">>> TestReduceValues")
	/*
	 * Source are of two parts with nothing in each, but at each mapend, a pair
	 * of <"part", <part>> is collected. So the reducer will check whether a key
	 * of "part" with two different values are reduced.
	 */
	job := MrJob{
		Source: []Input{
			&InputStruct{
				PartCountF: func() (int, error) {
					return 2, nil
				},
			},
		},

		NewMapperF: func(src, part int) Mapper {
			return &MapperStruct{
				MapEndF: func(c PartCollector) error {
					return c.CollectTo(0, sophie.RawString("part"),
						sophie.VInt(part))
				},
			}
		},

		NewReducerF: func(part int) Reducer {
			st := make(map[sophie.VInt]bool)
			return &ReducerStruct{
				NewKeyF: sophie.NewRawString,
				NewValF: sophie.NewVInt,

				ReduceF: func(key sophie.SophieWriter,
					nextVal SophierIterator, c []sophie.Collector) error {

					keyStr := string(*key.(*sophie.RawString))
					if keyStr != "part" {
						return errors.New(`Key should be "part"`)
					}
					for {
						val, err := nextVal()
						if err == sophie.EOF {
							break
						}
						if err != nil {
							return err
						}

						part := *val.(*sophie.VInt)
						if st[part] {
							t.Errorf("Duplicated value: %v", part)
						}
						st[part] = true
					}
					return nil
				},
			}
		},
	}
	assert.NoErrorf(t, "job.Run failed: %v", job.Run())
}
