package kv

import (
	"fmt"

	"github.com/golangplus/errors"

	"github.com/daviddengcn/sophie"
)

/*
	A folder with KV Files as an mr.Input
*/
type DirInput sophie.FsPath

// mr.Input interface
func (in DirInput) PartCount() (int, error) {
	infos, err := in.Fs.ReadDir(in.Path)
	if err != nil {
		return 0, errorsp.WithStacks(err)
	}

	return len(infos), nil
}

// mr.Input interface
func (in DirInput) Iterator(index int) (sophie.IterateCloser, error) {
	infos, err := in.Fs.ReadDir(in.Path)
	if err != nil {
		return nil, err
	}

	return NewReader(sophie.FsPath(in).Join(infos[index].Name()))
}

/*
	A folder with KV Files as an Output
*/
type DirOutput sophie.FsPath

// mr.Output interface
func (out DirOutput) Collector(index int) (sophie.CollectCloser, error) {
	if err := out.Fs.Mkdir(out.Path, 0755); err != nil {
		return nil, errorsp.WithStacks(err)
	}
	return NewWriter(sophie.FsPath(out).Join(fmt.Sprintf("part-%05d", index)))
}

// Clean removes the folder.
func (out DirOutput) Clean() error {
	return errorsp.WithStacks(sophie.FsPath(out).Remove())
}
