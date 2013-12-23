package sophie

import (
	"bufio"
	"os"

	"github.com/daviddengcn/go-villa"
)

type FileSystem interface {
	Create(fn string) (WriteCloser, error)
	Mkdir(path string, perm os.FileMode) error
	Open(fn string) (ReadCloser, error)
	ReadDir(dir string) ([]os.FileInfo, error)
	Stat(fn string) (os.FileInfo, error)
	Remove(fn string) error
}

type BufferedFileWriter struct {
	file *os.File
	*bufio.Writer
}

func (b BufferedFileWriter) Close() error {
	if err := b.Flush(); err != nil {
		return err
	}
	return b.file.Close()
}

type BufferedFileReader struct {
	file *os.File
	*bufio.Reader
}

func (b BufferedFileReader) Close() error {
	return b.file.Close()
}

func (b BufferedFileReader) Skip(n int64) (int64, error) {
	var arr [1024]byte
	left := n
	for left > 0 {
		l := left
		if l > int64(len(arr)) {
			l = int64(len(arr))
		}
		if _, err := b.Read(arr[:l]); err != nil {
			return n - left, err
		}
		left -= l
	}
	return n, nil
}

type localFileSystem struct {
}

var (
	LocalFS FileSystem = localFileSystem{}
)

func (lfs localFileSystem) Create(fn string) (WriteCloser, error) {
	file, err := villa.Path(fn).Create()
	if err != nil {
		return nil, err
	}
	return BufferedFileWriter{
		file:   file,
		Writer: bufio.NewWriter(file),
	}, nil
}

func (lfs localFileSystem) Open(fn string) (ReadCloser, error) {
	file, err := villa.Path(fn).Open()
	if err != nil {
		return nil, err
	}

	return BufferedFileReader{
		file:   file,
		Reader: bufio.NewReader(file),
	}, nil
}

func (lfs localFileSystem) ReadDir(dir string) ([]os.FileInfo, error) {
	return villa.Path(dir).ReadDir()
}

func (lfs localFileSystem) Mkdir(path string, perm os.FileMode) error {
	return villa.Path(path).MkdirAll(perm)
}

func (lfs localFileSystem) Stat(fn string) (os.FileInfo, error) {
	return villa.Path(fn).Stat()
}

func (lfs localFileSystem) Remove(fn string) error {
	return villa.Path(fn).RemoveAll()
}

type FsPath struct {
	Fs   FileSystem
	Path string
}

func (fp FsPath) Create() (WriteCloser, error) {
	return fp.Fs.Create(fp.Path)
}

func (fp FsPath) Open() (ReadCloser, error) {
	return fp.Fs.Open(fp.Path)
}
func (fp FsPath) ReadDir() ([]os.FileInfo, error) {
	return fp.Fs.ReadDir(fp.Path)
}
func (fp FsPath) Mkdir(perm os.FileMode) error {
	return fp.Fs.Mkdir(fp.Path, perm)
}
func (fp FsPath) Stat() (os.FileInfo, error) {
	return fp.Fs.Stat(fp.Path)
}
func (fp FsPath) Remove() error {
	return fp.Fs.Remove(fp.Path)
}

func (fp FsPath) Join(sub string) FsPath {
	return FsPath{
		Fs:   fp.Fs,
		Path: string(villa.Path(fp.Path).Join(sub)),
	}
}
