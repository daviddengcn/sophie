package sophie

import (
	"bufio"
	"os"

	"github.com/daviddengcn/go-villa"
)

// An interface defining some actions an file-system should have.
type FileSystem interface {
	// Create creates a file of a specified name.
	Create(fn string) (WriteCloser, error)
	// Mkdir makes the directory and its parents if necessary of specifiy path
	// and perm.
	Mkdir(path string, perm os.FileMode) error
	// Open opens a ReadCloser for reading the file of a specified name.
	Open(fn string) (ReadCloser, error)
	// ReadDir reads the FileInfos of the files under a specified directory.
	ReadDir(dir string) ([]os.FileInfo, error)
	// Stat returns the FileInfo of a file/directory of a specified name.
	Stat(fn string) (os.FileInfo, error)
	// Remove deletes a file or a directory(and all its file/directories in it)
	Remove(fn string) error
}

// BufferedFileWriter is a sophie.WriteCloser with buffer.
type BufferedFileWriter struct {
	file *os.File
	*bufio.Writer
}

// sophie.WriteCloser interface
func (b BufferedFileWriter) Close() error {
	if err := b.Flush(); err != nil {
		return err
	}
	return b.file.Close()
}

// BufferedFileReader is a sophie.ReadCloser with buffer
type BufferedFileReader struct {
	file *os.File
	*bufio.Reader
}

// sophie.ReadCloser interface
func (b BufferedFileReader) Close() error {
	return b.file.Close()
}

// sophie.Reader interface
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

type localFileSystem struct{}

var (
	// LocalFS is a FileSystem representing the local file-system.
	LocalFS FileSystem = localFileSystem{}
)

// FileSystem interface
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

// FileSystem interface
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

// FileSystem interface
func (lfs localFileSystem) ReadDir(dir string) ([]os.FileInfo, error) {
	return villa.Path(dir).ReadDir()
}

// FileSystem interface
func (lfs localFileSystem) Mkdir(path string, perm os.FileMode) error {
	return villa.Path(path).MkdirAll(perm)
}

// FileSystem interface
func (lfs localFileSystem) Stat(fn string) (os.FileInfo, error) {
	return villa.Path(fn).Stat()
}

// FileSystem interface
func (lfs localFileSystem) Remove(fn string) error {
	return villa.Path(fn).RemoveAll()
}

// FsPath is a pair of FileSystem and a path
type FsPath struct {
	// The FileSystem
	Fs FileSystem
	// The path
	Path string
}

// LocalFsPath returns an FsPath of the LocalFS and the specified path
func LocalFsPath(path string) FsPath {
	return FsPath{
		Fs:   LocalFS,
		Path: path,
	}
}

// TempDirPath returns the OS temporary dir as the fs path.
func TempDirPath() FsPath {
	return LocalFsPath(os.TempDir())
}

// Calls FileSystem.Create with the path
func (fp FsPath) Create() (WriteCloser, error) {
	return fp.Fs.Create(fp.Path)
}

// Calls FileSystem.Open with the path
func (fp FsPath) Open() (ReadCloser, error) {
	return fp.Fs.Open(fp.Path)
}

// Calls FileSystem.ReadDir with the path
func (fp FsPath) ReadDir() ([]os.FileInfo, error) {
	return fp.Fs.ReadDir(fp.Path)
}

// Calls FileSystem.Mkdir with the path
func (fp FsPath) Mkdir(perm os.FileMode) error {
	return fp.Fs.Mkdir(fp.Path, perm)
}

// Calls FileSystem.Stat with the path
func (fp FsPath) Stat() (os.FileInfo, error) {
	return fp.Fs.Stat(fp.Path)
}

// Calls FileSystem.Remove with the path
func (fp FsPath) Remove() error {
	return fp.Fs.Remove(fp.Path)
}

// Join returns a new FsPath with the same FileSystem and the path joined with
// sub
func (fp FsPath) Join(sub string) FsPath {
	return FsPath{
		Fs:   fp.Fs,
		Path: string(villa.Path(fp.Path).Join(sub)),
	}
}
