package mr

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/golangplus/errors"
	"github.com/golangplus/testing/assert"

	"github.com/daviddengcn/sophie"
)

func TestMapOnly(t *testing.T) {
	lines := linesInput(strings.Split(WORDS, "\n"))

	var mapper LinesCounterMapper

	job := MapOnlyJob{
		NewMapperF: func(src, part int) OnlyMapper {
			return &mapper
		},
		Source: []Input{lines},
		Dest:   []Output{&mapper},
	}

	assert.NoError(t, job.Run())
	assert.Equal(t, "len(dest)", len(mapper.intList), len(lines))
}

func TestMapOnly_EarlyExit(t *testing.T) {
	inputClosed := false
	collected := 0
	collectorClosed := false
	job := MapOnlyJob{
		Source: []Input{&InputStruct{
			PartCountF: func() (int, error) { return 1, nil },
			IteratorF: func(i int) (sophie.IterateCloser, error) {
				if i != 0 {
					t.Errorf("index out of range: %d", i)
					return nil, fmt.Errorf("index out of range: %d", i)
				}
				// A Source generated numbers from 0 to numberSource - 1. Key is the number and value is it's string representation.
				n := 0
				return &sophie.IterateCloserStruct{
					NextF: func(key, val sophie.SophieReader) error {
						if n >= 10 {
							return errorsp.WithStacks(io.EOF)
						}
						*(key.(*sophie.VInt)) = sophie.VInt(n)
						*(val.(*sophie.RawString)) = sophie.RawString(fmt.Sprint(n))
						n++
						return nil
					},
					CloserF: func() error {
						inputClosed = true
						return nil
					},
				}, nil
			},
		}},
		NewMapperF: func(src, part int) OnlyMapper {
			n := 0
			return &OnlyMapperStruct{
				NewKeyF: func() sophie.Sophier { return new(sophie.VInt) },
				NewValF: func() sophie.Sophier { return new(sophie.RawString) },
				MapF: func(key, val sophie.SophieWriter, c []sophie.Collector) error {
					if err := c[0].Collect(key, val); err != nil {
						return err
					}
					n++
					if n == 5 {
						return errorsp.WithStacks(EOM)
					}
					return nil
				},
			}
		},
		Dest: []Output{&OutputStruct{
			CollectorF: func(i int) (sophie.CollectCloser, error) {
				if i != 0 {
					t.Errorf("index out of range: %d", i)
					return nil, fmt.Errorf("index out of range: %d", i)
				}
				return &sophie.CollectCloserStruct{
					CollectF: func(_, _ sophie.SophieWriter) error {
						collected++
						return nil
					},
					CloseF: func() error {
						collectorClosed = true
						return nil
					},
				}, nil
			},
		}},
	}
	assert.NoError(t, job.Run())
	assert.True(t, "inputClosed", inputClosed)
	assert.Equal(t, "collected", collected, 5)
	assert.True(t, "collectorClosed", collectorClosed)
}
