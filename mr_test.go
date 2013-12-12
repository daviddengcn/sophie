package sophie

import (
	"errors"
	"strings"
	"testing"

	"github.com/daviddengcn/go-assert"

	"fmt"
)

type linesIter struct {
	pos   int
	lines []string
}

func (iter *linesIter) Next(key, val SophieReader) error {
	if iter.pos >= len(iter.lines) {
		return errors.New("EOF")
	}
	*(key.(*String)) = String(iter.lines[iter.pos])
	iter.pos++
	return nil
}

func (iter *linesIter) Close() error {
	return nil
}

type linesInput []string

func (lines linesInput) PartCount() (int, error) {
	return 1, nil
}

func (lines linesInput) Iterator(int) (Iterator, error) {
	return &linesIter{lines: lines}, nil
}

type LinesCounterMapper struct {
	intList []Int32
}

func (lcm *LinesCounterMapper) Collector(index int) (Collector, error) {
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
func (lcm *LinesCounterMapper) Map(key, val Sophier, c Collector) error {
	fmt.Printf("Mapping (%v, %v) ...\n", key, val)
	c.Collect(Int32(1), Null{})
	return nil
}

func (lcm *LinesCounterMapper) Close() error {
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
}

func (wcm *WordCountMapper) NewKey() Sophier {
	return new(String)
}
func (wcm *WordCountMapper) NewVal() Sophier {
	return Null{}
}

func (wcm *WordCountMapper) Map(key, val Sophier, c PartCollector) error {
	fmt.Printf("WordCountMapper (%v, %v) ...\n", key, val)
	line := *(key.(*String))
	words := strings.Split(string(line), " ")
	for _, word := range words {
		if len(word) == 0 {
			continue
		}
		word = strings.ToLower(word)
		fmt.Printf("CollectTo %v\n", word)
		c.CollectTo(int(word[0]), String(word), VInt(1))
	}
	return nil
}

type WordCountReducer struct {
}

func (wc *WordCountReducer) NewKey() Sophier {
	return new(String)
}

func (wc *WordCountReducer) NewVal() Sophier {
	return new(VInt)
}

func (wc *WordCountReducer) Reduce(key Sophier, nextVal SophierIterator, c Collector) error {
	fmt.Printf("Reducing %v\n", key)
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
	fmt.Printf("Result %v: %v\n", key, val)
	return nil
}

func (wc *WordCountReducer) Collector(index int) (Collector, error) {
	return wc, nil
}

func (wc *WordCountReducer) Close() error {
	return nil
}

func TestMapReduce(t *testing.T) {
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper WordCountMapper
	var reducer WordCountReducer

	job := MrJob{
		Mapper:  &mapper,
		Reducer: &reducer,
		Source:  lines,
		Dest:    &reducer,
	}

	assert.NoErrorf(t, "RunJob: %v", job.Run())
}
