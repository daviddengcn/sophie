package sophie

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/daviddengcn/go-assert"
	"github.com/daviddengcn/go-villa"
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
	*(key.(*String)) = String(iter.lines[iter.pos])
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
	return new(String)
}
func (lcm *LinesCounterMapper) NewVal() Sophier {
	return Null{}
}
func (lcm *LinesCounterMapper) Map(key, val SophieWriter, c Collector) error {
	//fmt.Printf("Mapping (%v, %v) ...\n", key, val)
	c.Collect(Int32(1), Null{})
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
111`
)

func TestMapOnly(t *testing.T) {
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper LinesCounterMapper

	job := MapOnlyJob{
		Mapper: &mapper,
		Source: lines,
		Dest:   &mapper,
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())
	assert.Equals(t, "len(dest)", len(mapper.intList), len(lines))
}

type WordCountMapper struct {
	EmptyMapper
}

func (wcm *WordCountMapper) NewKey() Sophier {
	return new(String)
}
func (wcm *WordCountMapper) NewVal() Sophier {
	return Null{}
}

func (wcm *WordCountMapper) Map(key, val SophieWriter, c PartCollector) error {
	//fmt.Printf("WordCountMapper (%v, %v) ...\n", key, val)
	line := *(key.(*String))
	words := strings.Split(string(line), " ")
	for _, word := range words {
		if len(word) == 0 {
			continue
		}
		word = strings.ToLower(word)
		//fmt.Printf("CollectTo %v\n", word)
		c.CollectTo(int(word[0]), String(word), VInt(1))
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
	return new(String)
}

func (wc *WordCountReducer) NewVal() Sophier {
	return new(VInt)
}

func (wc *WordCountReducer) Reduce(key SophieWriter, nextVal SophierIterator,
	c Collector) error {
	//	fmt.Printf("Reducing %v\n", key)
	var count VInt
	for {
		val, err := nextVal()
		if err != nil {
			break
		}
		count += *(val.(*VInt))
	}

	return c.Collect(key, count)
}

func (wc *WordCountReducer) Collect(key, val SophieWriter) error {
	wc.Lock()
	defer wc.Unlock()

	wc.counts[key.(*String).Val()] = int(val.(VInt))
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
		assert.Equals(t, "count of "+k, v, act[k])
	}
}

func TestMapReduce(t *testing.T) {
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper WordCountMapper
	reducer := WordCountReducer{counts: make(map[string]int)}

	job := MrJob{
		MapFactory: SingleMapperFactory(&mapper),
		RedFactory: SingleReducerFactory(&reducer),
		Source:  lines,
		Dest:    &reducer,
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())

	expCnts := statWords(WORDS)
	fmt.Println(reducer.counts)
	fmt.Println(expCnts)

	assertMapEquals(t, reducer.counts, expCnts)
}

func TestMRFromFile(t *testing.T) {
	mrinPath := villa.Path("./mrin")
	mrinPath.Mkdir(0755)

	mroutPath := villa.Path("./mrout")

	mrin := FsPath{
		Fs:   LocalFS,
		Path: mrinPath.S(),
	}

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

		assert.NoErrorf(t, "inF.Collect", inF.Collect(String(line), Null{}))
	}
	if inF != nil {
		assert.NoErrorf(t, "inF.Close: %v", inF.Close())
	}

	var mapper WordCountMapper
	reducer := WordCountReducer{counts: make(map[string]int)}

	job := MrJob{
		Source: KVDirInput{
			Fs:   LocalFS,
			Path: mrinPath.S(),
		},
		MapFactory: SingleMapperFactory(&mapper),

		RedFactory: SingleReducerFactory(&reducer),
		Dest: KVDirOutput{
			Fs:   LocalFS,
			Path: mroutPath.S(),
		},
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())

	resIn := KVDirInput{
		Fs:   LocalFS,
		Path: mroutPath.S(),
	}
	n, err := resIn.PartCount()
	assert.NoErrorf(t, "resIn.PartCount(): %v", err)
	var word String
	var cnt VInt
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
	fmt.Println(expCnts)
	fmt.Println(actCnts)

	assertMapEquals(t, actCnts, expCnts)
}
