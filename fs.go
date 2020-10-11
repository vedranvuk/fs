package fs

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// FileError is a File error.
type FileError struct {
	file *File // file is the File that caused the error.
	err  error // err is the actual error.<
}

// Error implements error.Error() on FileError.
func (fe *FileError) Error() string { return fe.err.Error() }

// File returns the file that was the cause of the error.
func (fe *FileError) File() *File { return fe.file }

// files is a managed map of files.
type files map[string]*File

// file returns an existing or creates a new file under
// specified name if one does not already exist.
//
// When returning a new file its' parrent is set to specified parent.
func (f files) file(parent interface{}, name string, dir bool) *File {
	file, ok := f[name]
	if !ok {
		file = newFile(name, parent, dir)
		f[name] = file
		return file
	}
	return file
}

// delete deletes a *File from files by specified name.
func (f files) delete(name string) { delete(f, name) }

// File defines a file.
type File struct {
	name   string                 // name is the name of the file.
	parent interface{}            // parent is the reference to file's parent which can be a *File or an *Fs.
	dir    bool                   // Directory specifies if this file is a directory.
	meta   map[string]interface{} // meta is file metadata, user storage, anything.
	err    *FileError             // err marks this file as having an error.
	files                         // files is a map of contained file names to Files if this is a Directory.
}

// newFile returns a new *File instance.
func newFile(name string, parent interface{}, dir bool) *File {
	return &File{name: name, parent: parent, dir: dir, files: make(files)}
}

// Name returns File's name.
func (f *File) Name() string { return f.name }

// Parent returns this File's parent.
// If there is no parent, i.e. this file represents the root directory
// in a Fs a nil file is returned.
func (f *File) Parent() *File {
	if file, ok := f.parent.(*File); ok {
		return file
	}
	return nil
}

// IsDir returns if this file is marked as a directory.
func (f *File) IsDir() bool { return f.dir }

// GetMeta returns user set meta information by specified key.s
func (f *File) GetMeta(key string) interface{} { return f.meta[key] }

// SetMeta sets user meta information val under specified key.
func (f *File) SetMeta(key string, val interface{}) { f.meta[key] = val }

// Error returns File error if any.
func (f *File) Error() error { return f.err }

// Count returns child item count.
func (f *File) Count() int { return len(f.files) }

// Fs returns the Fs this File belongs to.
func (f *File) Fs() *Fs {
	curr := f.parent
	for {
		if fs, ok := curr.(*Fs); ok {
			return fs
		}
		if file, ok := curr.(*File); ok {
			curr = file.parent
		} else {
			panic("bug")
		}
	}
}

// ErrParentNotDir is returned when a child file is added to a file that is
// not a directory.
var ErrParentNotDir = errors.New("fs: parent is not a directory")

// makeFile creates a file in self which must be a directory.
// If this File is not a directory, makeFile file will have a ErrParentNotDir set.
func (f *File) makeFile(name string) (file *File) {

	file = f.file(f, name, true)
	if !f.dir {
		file.err = &FileError{file: file, err: ErrParentNotDir}
	}

	fs := f.Fs()
	path := file.Path(false)
	if _, exists := fs.removelist[path]; exists {
		delete(fs.removelist, path)
	}

	return
}

// NewDir creates a new directory inside this file. See makeFile.
func (f *File) NewDir(name string) (file *File) {
	file = f.makeFile(name)
	file.dir = true
	return
}

// NewFile creates a new file inside this file, see makeFile.
func (f *File) NewFile(name string) *File { return f.makeFile(name) }

// leftPathElem returns the first element of the path and the rest.
// If s is empty returns empty dir and empty rest.
// If s begins with a slash returns empty dir and everything as else.
// It is a helper for Get.
func leftPathElem(s string) (dir, rest string) {
	s = strings.TrimSpace(s)
	i := strings.Index(s, "/")
	if i < 0 {
		return s, ""
	}
	if i == 0 {
		return "", s[1:]
	}
	return s[:i], s[i+1:]
}

// ErrInvalidName is returned when an invalid name is specified.
var ErrInvalidName = errors.New("fs: invalid name")

// Get returns a file by name where name can be a name of a file in this
// directory, a path to a file in a subdirectory or a rooted path which
// gets evaluated from Fs root directory.
//
// If directory is true and the files along the path do not exist, the last
// element of the path will be created as a directory instead of a file.
//
// Get can return an os.ErrNotExists if an invalid path is specified.
// An invalid path can result only if an element of the specified path
// already exists in the Fs and is a file instead of a directory.
//
// Get returns ErrInvalidName if name is empty or invalid.
func (f *File) Get(name string, directory bool) (*File, error) {
	dir, rest := leftPathElem(name)
	if dir == "" {
		if rest == "" {
			return nil, ErrInvalidName
		}
		return f.Fs().Get(rest, directory)
	}
	if rest != "" {
		return f.file(f, dir, true).Get(rest, directory)
	}
	return f.file(f, dir, directory), nil
}

// Path returns the path to this file, including the File name. If absolute is
// specified it returns an absolute path to the file, otherwise a path relative
// to the Fs root directory.
func (f *File) Path(absolute bool) string {
	if file, ok := f.parent.(*File); ok {
		return path.Join(file.Path(absolute), f.name)
	}
	if fs, ok := f.parent.(*Fs); ok {
		if absolute {
			return filepath.Join(fs.abs, f.name)
		}
		return "/" + f.name
	}
	return ""
}

// Exists checks if the file exists on disk.
// If an error occurs it is returned with an invalid value of file's existence.
func (f *File) Exists() (bool, error) {
	_, err := os.Stat(f.Path(true))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Touch creates this file on disk and returns nil on success.
// It creates an empty file or an empty directory, depending.
// All directories along the path are created.
// If overwrite is specified, silently overwrites the file, otherwise
// returns an ErrExists. ANy other error is returned and the op may have
// completed partially.
func (f *File) Touch(overwrite bool) error {
	if f.err != nil {
		return f.err
	}
	p := f.Path(true)
	if f.dir {
		if err := os.MkdirAll(p, 0755); err != nil {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	flags := os.O_CREATE | os.O_TRUNC | os.O_RDWR
	if !overwrite {
		flags = flags | os.O_EXCL
	}
	file, err := os.OpenFile(p, flags, 0644)
	if err != nil {
		return err
	}
	return file.Close()
}

// Remove deletes this file from disk.
// It does so recursively if it is a directory.
// Returns os.ErrNotExists if file did not exist on disk prior to the call.
// Returns any other error if one occurs.
func (f *File) Remove() error {
	if fs, ok := f.parent.(*Fs); ok {
		fs.File = *newFile("/", fs, true)
		return os.RemoveAll(fs.abs)
	}
	return os.Remove(f.Path(true))
}

// Delete deletes this file from its' parent.
func (f *File) Delete() {
	if file, ok := f.parent.(*File); ok {
		file.delete(f.name)
	}
	if fs, ok := f.parent.(*Fs); ok {
		fs.delete(f.name)
	}
	f.Fs().removelist[f.Path(false)] = f
}

// ErrIncompatibleStructure is returned when mirroring a file to a different Fs
// that already has a structure which is incompatible with source Fs structure.
// Usually, this occurs when a target contains a same named file as its source
// directory so target file cannot contain children.
var ErrIncompatibleStructure = errors.New("fs: target fs structure not compatible")

// Mirror mirrors the file to a target Fs at the same relative path.
//
// If File is a directory the directory is created in the target Fs. If it has
// children, they are copied as well. If File is a file it is copied to target.
// In all cases, directories along the path to the File are created in the
// target Fs.
//
// If the target Fs has an existing structure that differs from the source in
// that the target has existing files coresponding to paths of files being
// mirrored but are of different type (i.e. file/dir) an error is returned.
//
// If any other error occurs it is returned.
func (f *File) Mirror(target *Fs, overwrite bool) error {

	tgtfile, err := target.Get(f.Path(false), f.IsDir())
	if err != nil {
		return err
	}
	if err := tgtfile.Touch(overwrite); err != nil {
		return err
	}

	infile, err := os.OpenFile(f.Path(true), os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer infile.Close()

	outfile, err := os.OpenFile(tgtfile.Path(true), os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer outfile.Close()

	buf := make([]byte, 4096)
	reader := bufio.NewReader(infile)
	writer := bufio.NewWriter(outfile)

	for loop := true; loop; {
		n, err := reader.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			loop = false
		}
		if _, err = writer.Write(buf[:n]); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	return nil
}

// ReadWriteSeekCloser combines io.Seeker and io.ReadWriteCloser.
type ReadWriteSeekCloser interface {
	io.Seeker
	io.ReadWriteCloser
}

// ErrFileIsDirectory is returned when trying to open a directory.
var ErrFileIsDirectory = errors.New("fs: file is a directory")

// Open opens an underlying file in read/write mode if it is not a directory.
// If the file does not yet exist it is created.
// If truncate is specified, file is cleared on open.
// If an error occurs it is returned.
func (f *File) Open(truncate bool) (ReadWriteSeekCloser, error) {
	if f.IsDir() {
		return nil, ErrFileIsDirectory
	}
	flags := os.O_CREATE | os.O_RDWR
	if truncate {
		flags = flags | os.O_TRUNC
	}
	file, err := os.OpenFile(f.Path(true), flags, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// walkFunc is the Fs traversal function.
// Passes current *File being examined, returning false stops enumeration.
type walkFunc func(*File) bool

// Walk walks the files sorted by name in ascending order.
// It walks the complete tree.
func (f *File) Walk(fn walkFunc) {
	names := make([]string, 0, len(f.files))
	for key := range f.files {
		names = append(names, key)
	}
	sort.Strings(names)
	for _, name := range names {
		file := f.files[name]
		if !fn(file) {
			break
		}
		file.Walk(fn)
	}
}

// Fs defines a reflection of a filesystem rooted at a directory on disk.
type Fs struct {
	root       string // root is the root directory where fs will act.
	abs        string // abs is the absolute path of root.
	removelist map[string]*File
	File
}

// Root returns Fs's root folder as set on construction.
func (fs *Fs) Root() string { return fs.root }

// Abs returns absolute path of Fs root.
func (fs *Fs) Abs() string { return fs.abs }

// parse is the implementation of Parse.
func (fs *Fs) parse(file *File, path string) error {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		var f *File
		if fi.IsDir() {
			f = file.NewDir(fi.Name())
			if err := fs.parse(f, filepath.Join(path, fi.Name())); err != nil {
				return err
			}
		} else {
			f = file.NewFile(fi.Name())
		}
	}
	return nil
}

// ErrRootNotSet is returned by Parse() if root is not set.
var ErrRootNotSet = errors.New("fs: root not set")

// Parse parses root and reflects it in self replacing current Fs structure.
// Returns an error if one occurs.
func (fs *Fs) Parse() error {
	if fs.root == "" {
		return ErrRootNotSet
	}
	if _, err := os.Stat(fs.abs); err != nil {
		return err
	}
	fs.files = make(files)
	return fs.parse(&fs.File, fs.abs)
}

func indentString(depth int) (s string) {
	for i := 0; i < depth; i++ {
		s += "  "
	}
	return s
}

func printFiles(f files, indent int) (result string) {

	names := make([]string, 0, len(f))
	for name := range f {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		result += fmt.Sprintf("%s %s\n", indentString(indent), f[name].Name())
		if len(f[name].files) > 0 {
			indent++
			result += printFiles(f[name].files, indent)
			indent--
		}
	}
	return
}

// String implements Stringer.
func (fs *Fs) String() string { return printFiles(fs.files, 0) }

// Flush commits current Fs structure to disk or returns an error if one
// occurs. It creates all directories along the path to touched files.
//
// If overwrite is specified, existing files are overwritten.
//
// If remove is specified removes files from disk that were deleted
// from Fs since the last call to Flush.
func (fs *Fs) Flush(overwrite, remove bool) (err error) {

	defer func() {
		fs.removelist = make(map[string]*File)
	}()

	fs.Walk(func(file *File) bool {
		if err = file.Touch(overwrite); err != nil {
			return false
		}
		return true
	})

	if remove {
		flist := make([]string, 0, len(fs.removelist))
		for key := range fs.removelist {
			flist = append(flist, key)
		}
		sort.Strings(flist)
		for _, relpath := range flist {
			path := filepath.Join(fs.abs, relpath)
			if err := os.RemoveAll(path); err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
		}
	}

	return
}

// new returns a new *File rooted at root or an error.
func new(root string) (fs *Fs, err error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	fs = &Fs{root: root, abs: abs, removelist: make(map[string]*File)}
	fs.File = *newFile("/", fs, true)
	return
}

// At returns a Fs rooted at specified root directory.
// No actions are executed on the resulting Fs.
func At(root string) (*Fs, error) { return new(root) }

// copyFileRecursive recursively copies from from to to.
func copyFileRecursive(from, to *File) {
	for name, file := range from.files {
		var nf *File
		if file.IsDir() {
			nf = to.NewDir(name)
		} else {
			nf = to.NewFile(name)
		}
		if file.Count() > 0 {
			copyFileRecursive(file, nf)
		}
	}
}

// From returns a new Fs instance rooted at specified root and having
// the structure of specified fs.
func From(fs *Fs, root string) (*Fs, error) {
	newfs, err := At(root)
	if err != nil {
		return nil, err
	}
	copyFileRecursive(&fs.File, &newfs.File)
	return fs, nil
}

// Parse parses a root directory and returns a Fs reflecting its'
// subdirectory structure or an error if one occured.
func Parse(root string) (*Fs, error) {
	p, err := new(root)
	if err != nil {
		return nil, err
	}
	if err := p.Parse(); err != nil {
		return nil, err
	}
	return p, nil
}
