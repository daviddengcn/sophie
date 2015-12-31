package mr

import (
	"fmt"
	"sort"
	"testing"

	"github.com/daviddengcn/sophie"
	"github.com/golangplus/testing/assert"
)

func checkSorter(t *testing.T, s Sorter) {
	var cs [3]PartCollector
	for i := range cs {
		var err error
		cs[i], err = s.NewPartCollector(i)
		assert.NoError(t, err)
	}
	inData := [][]int{
		{0, 1, 6, 6},
		{1, 3, 1},
		{2, 2, 3, 6},
	}
	outData := [...][]string{
		0: {"0", "3", "3", "6", "6", "6"},
		1: {"1", "1", "1"},
		2: {"2", "2"},
	}
	for i, list := range inData {
		for _, v := range list {
			assert.NoError(t, cs[i].CollectTo(v%3, sophie.VInt(v), sophie.String(fmt.Sprint(v))))
		}
	}
	assert.NoError(t, s.ClosePartCollectors())
	parts := s.ReduceParts()
	sort.Ints(parts)
	assert.Equal(t, "parts", parts, []int{0, 1, 2})
	for _, part := range parts {
		it, err := s.NewReduceIterator(part)
		if !assert.NoError(t, err) {
			return
		}
		ReduceEndFCalled := false
		outVls := make([]string, 0)
		assert.NoError(t, it.Iterate([]sophie.Collector{
			sophie.CollectorF(func(key, val sophie.SophieWriter) error {
				assert.StringEqual(t, "val", val, key)
				outVls = append(outVls, val.(*sophie.String).Val())
				return nil
			}),
		}, &ReducerStruct{
			NewKeyF: sophie.NewVInt,
			NewValF: sophie.NewString,
			ReduceF: func(key sophie.SophieWriter, nextVal SophierIterator, c []sophie.Collector) error {
				assert.Equal(t, "len(c)", len(c), 1)
				for {
					val, err := nextVal()
					if err == sophie.EOF {
						break
					}
					if err != nil {
						t.Errorf("nextVal() failed: %v", err)
						return err
					}
					c[0].Collect(key, val)
				}
				return nil
			},
			ReduceEndF: func(c []sophie.Collector) error {
				ReduceEndFCalled = true
				assert.Equal(t, "len(c)", len(c), 1)
				return nil
			},
		}))
		assert.Should(t, ReduceEndFCalled, "ReduceEndF not called!")
		assert.Equal(t, "outVls", outVls, outData[part])
	}
}

func TestMemSorter(t *testing.T) {
	fmt.Println(">>> TestMemSorter")
	s := NewMemSorters()
	checkSorter(t, s)
}

func TestFileSorter(t *testing.T) {
	fmt.Println(">>> TestFileSorter")
	fpRoot := sophie.LocalFsPath(".")
	s := NewFileSorter(fpRoot.Join("tmp"))
	checkSorter(t, s)
}
