/*
Package mr provides a local concurrent computing model(MapReduce) using Sophie
serialization.

A simple word count example is like this:

	job := MrJob {
		Source: []Input{...},

		NewMapperF: func(src, part int) Mapper {
			return &MapperStruct {
				NewKeyF: sophie.NewString,
				NewValF: sophie.ReturnNULL,
				MapperF: func(key, val sophie.SophieWriter, c PartCollector) error {
					line := key.(*RawString).String()
					words := strings.Split(line, " ")
					for _, word: range words {
						c.CollectTo(0, sophie.RawString(word), sophie.VInt(1))
					}
				},
			}
		},

		NewReducerF: func(part int) Reducer {
			return &ReducerStruct {
				NewKeyF: sophie.NewRawString,
				NewValF: sophie.NewVInt,
				ReducerF: func((key sophie.SophieWriter, nextVal SophierIterator,
					c []sophie.Collector) error {
					var count sophie.VInt
					for {
						val, err := nextVal()
						if err == sophie.EOF {
							break
						}
						if err != nil {
							return err
						}
						count += val.(*sophie.VInt).Val()
					}
					return c[0].Collect(key, count)
				},
			}
		},

		Dest: []Output{...},
	}
	
	if err := job.Run(); err != nil {
		log.Fatalf("job.Run failed: %v", err)
	}

One can also use MapOnlyJob for simple jobs.
*/
package mr

import (
	"errors"
	"log"

	"github.com/daviddengcn/sophie"
)

// The mapping stage in MrJob.
type Mapper interface {
	// NewKey returns a new instance of the key Sophier object.
	NewKey() sophie.Sophier
	// NewVal returns a new instance of the value Sophier object.
	NewVal() sophie.Sophier
	// Map converts the input kv pair to what the Reducer expect and send to
	// the PartCollector.
	Map(key, val sophie.SophieWriter, c PartCollector) error
	// MapEnd is invoked after a partition of the Input is mapped.
	MapEnd(c PartCollector) error
}

// An interator for fetching a list of Sophiers. If sophie.EOF is returned as
// the error, no further Sophiers are avaiable.
type SophierIterator func() (sophie.Sophier, error)

// The reducing stage in MrJob.
type Reducer interface {
	// NewKey returns a new instance of the key Sophier object for reducing.
	NewKey() sophie.Sophier
	// NewVal returns a new instance of the value Sophier object for reducing.
	NewVal() sophie.Sophier
	// to get all values:
	//   for {
	//	 	val, err := nextVal()
	//   	if err == sophie.EOF {
	//   		break;
	//   	}
	//      if err != nil {
	//   		return err;
	//   	}
	//      ...
	//   }
	Reduce(key sophie.SophieWriter, nextVal SophierIterator,
		c []sophie.Collector) error
	// MapEnd is invoked after a partition of the reducing kv pairs is reduced.
	ReduceEnd(c []sophie.Collector) error
}

// An MrJob contains a mapping step and a reducing step. In reducing step, kv
// pairs are sorted by keys, and values of a key are reduced using the Reducer.
type MrJob struct {
	// The factory for Mappers
	NewMapperF func(src, part int) Mapper
	// The factory for Reducers
	NewReducerF func(part int) Reducer

	// The Sorter that sorts kv pairs mapped by Mappers and provides
	// SophierIterator for Reducers.
	Sorter Sorter

	// The source Inputs
	Source []Input
	// The destination Outputs
	Dest []Output
}

// Runs the MrJob.
// If Sorter is not specified, MemSorters is used.
func (job *MrJob) Run() error {
	if job.NewMapperF == nil {
		return errors.New("MrJob: MapFactory undefined!")
	}
	if job.NewReducerF == nil {
		return errors.New("MrJob: RedFactory undefined!")
	}
	if job.Source == nil {
		return errors.New("MrJob: Source undefined!")
	}

	/*
	 * Map
	 */
	sorters := job.Sorter
	if sorters == nil {
		log.Println("Sorter not specified, using MemSorters...")
		sorters = NewMemSorters()
	}

	log.Println("Start mapping...")
	endss := make([][]chan error, 0, len(job.Source))
	totalPart := 0
	for i := range job.Source {
		partCount, err := job.Source[i].PartCount()
		if err != nil {
			return err
		}

		ends := make([]chan error, 0, partCount)
		for part := 0; part < partCount; part++ {
			end := make(chan error, 1)
			ends = append(ends, end)
			go func(i, part, totalPart int, end chan error) {
				end <- func() error {
					c, err := sorters.NewPartCollector(totalPart)
					if err != nil {
						return err
					}
					mapper := job.NewMapperF(i, part)
					key, val := mapper.NewKey(), mapper.NewVal()
					iter, err := job.Source[i].Iterator(part)
					if err != nil {
						return err
					}
					defer iter.Close()

					for {
						if err := iter.Next(key, val); err != nil {
							if err == sophie.EOF {
								break
							}
							return err
						}

						if err := mapper.Map(key, val, c); err != nil {
							return err
						}
					}
					return mapper.MapEnd(c)
				}()
			}(i, part, totalPart, end)
			totalPart++
		}

		endss = append(endss, ends)
	}

	for _, ends := range endss {
		for _, end := range ends {
			err := <-end
			if err != nil {
				return err
			}
		}
	}
	if err := sorters.ClosePartCollectors(); err != nil {
		log.Printf("sorters.ClosePartCollectors(): %v", err)
	}
	log.Printf("Map ends, begin to reduce")

	var ends []chan error
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
				cs := make([]sophie.Collector, 0, len(job.Dest))
				for _, dst := range job.Dest {
					c, err := dst.Collector(part)
					if err != nil {
						return err
					}
					defer c.Close()
					cs = append(cs, c)
				}
				reducer := job.NewReducerF(part)
				return it.Iterate(cs, reducer)
			}()
		}(part, end)
	}

	for _, end := range ends {
		err := <-end
		if err != nil {
			return err
		}
	}
	log.Println("Reduce ends.")

	return nil
}
