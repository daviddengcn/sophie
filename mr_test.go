package sophie

import (
	"strings"
	"sync"
	"testing"

	"github.com/daviddengcn/go-assert"

	"fmt"
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

func TestMapReduce(t *testing.T) {
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper WordCountMapper
	reducer := WordCountReducer{counts: make(map[string]int)}

	job := MrJob{
		Mapper:  &mapper,
		Reducer: &reducer,
		Source:  lines,
		Dest:    &reducer,
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())

	expCnts := statWords(WORDS)
	fmt.Println(reducer.counts)
	fmt.Println(expCnts)

	assert.Equals(t, "count", len(reducer.counts), len(expCnts))
	for k, v := range expCnts {
		assert.Equals(t, "count of "+k, v, reducer.counts[k])
	}
}
