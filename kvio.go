package sophie

import (
	"fmt"
)

/*
	A folder with KV Files as an Input
*/
type KVDirInput FsPath

func (in KVDirInput) PartCount() (int, error) {
	infos, err := in.Fs.ReadDir(in.Path)
	if err != nil {
		return 0, err
	}

	return len(infos), nil
}

func (in KVDirInput) Iterator(index int) (IterateCloser, error) {
	infos, err := in.Fs.ReadDir(in.Path)
	if err != nil {
		return nil, err
	}

	return NewKVReader(FsPath(in).Join(infos[index].Name()))
}

/*
	A folder with KV Files as an Output
*/
type KVDirOutput FsPath

func (out KVDirOutput) Collector(index int) (CollectCloser, error) {
	if err := out.Fs.Mkdir(out.Path, 0755); err != nil {
		return nil, err
	}
	return NewKVWriter(FsPath(out).Join(fmt.Sprintf("part-%05d", index)))
}

func (out KVDirOutput) Clean() error {
	return FsPath(out).Remove()
}