package fs

import (
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

var (
	// ErrParentNotDir is returned when a child file is being added to a file
	// that is not a directory.
	ErrParentNotDir = errors.New("fs: parent is not a directory")
	// ErrInvalidName is returned when an invalid or empty name is specified.
	ErrInvalidName = errors.New("fs: invalid name")
	// ErrIncompatibleStructure is returned when mirroring a file to a different Fs
	// that already has a structure which is incompatible with source Fs structure.
	// Usually, this occurs when a target contains a same named file as its source
	// directory so target file cannot contain children.
	ErrIncompatibleStructure = errors.New("fs: target fs structure not compatible")
	// ErrFileIsDirectory is returned when calling Open on a directory.
	ErrFileIsDirectory = errors.New("fs: file is a directory")
	// ErrRootNotSet is returned by Parse() if root is not set.
	ErrRootNotSet = errors.New("fs: root not set")
)

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

// IsDir returns if this file is marked as a directory.
func (f *File) IsDir() bool { return f.dir }

// GetMeta returns user set meta information by specified key.s
func (f *File) GetMeta(key string) interface{} { return f.meta[key] }

// SetMeta sets user meta information val under specified key.
func (f *File) SetMeta(key string, val interface{}) { f.meta[key] = val }

// Count returns child item count.
func (f *File) Count() int { return len(f.files) }

// Files returns files contained by this file if it is a directory.
// Otherwise, always returns an empty slice.
func (f *File) Files() []*File {
	res := make([]*File, 0, len(f.files))
	for _, file := range f.files {
		if !file.IsDir() {
			res = append(res, file)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name() < res[j].Name()
	})
	return res
}

// FileNames returns names of files contained by this file if it is a directory.
// Otherwise, always returns an empty slice.
func (f *File) FileNames() []string {
	res := make([]string, 0, len(f.files))
	for _, file := range f.files {
		if !file.IsDir() {
			res = append(res, file.Name())
		}
	}
	sort.Strings(res)
	return res
}

// Dirs returns directories contained by this file if it is a directory.
// Otherwise, always returns an empty slice.
func (f *File) Dirs() []*File {
	res := make([]*File, 0, len(f.files))
	for _, file := range f.files {
		if file.IsDir() {
			res = append(res, file)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name() < res[j].Name()
	})
	return res
}

// DirNames returns names of dirs contained by this file if it is a directory.
// Otherwise, always returns an empty slice.
func (f *File) DirNames() []string {
	res := make([]string, 0, len(f.files))
	for _, file := range f.files {
		if file.IsDir() {
			res = append(res, file.Name())
		}
	}
	sort.Strings(res)
	return res
}

// children is the implementation of Children.
func (f *File) children(depth, curr int, absolute bool, result *[]string) {
	if depth >= 0 && curr > depth {
		return
	}
	for _, file := range f.files {
		*result = append(*result, file.Path(absolute))
		file.children(depth, curr+1, absolute, result)
	}
}

// Children returns this File's children up to specified depth where 0 is the
// direct children of file. A negative depth imposes no limit.
// If absolute is specified returns absolute paths, otherwise relative to root.
func (f *File) Children(depth int, absolute bool) []string {
	var result []string
	f.children(depth, 0, absolute, &result)
	sort.Strings(result)
	return result
}

// makeFile creates a file in self which must be a directory.
// If this File is not a directory an ErrParentNotDir is returned.
func (f *File) makeFile(name string, directory bool) (*File, error) {

	file := f.file(f, name, directory)
	if !f.dir {
		return nil, ErrParentNotDir
	}

	fs := f.Fs()
	path := file.Path(false)
	if _, exists := fs.removelist[path]; exists {
		delete(fs.removelist, path)
	}

	return file, nil
}

// NewDir creates a new directory inside this file. See makeFile.
func (f *File) NewDir(name string) (*File, error) { return f.makeFile(name, true) }

// NewFile creates a new file inside this file, see makeFile.
func (f *File) NewFile(name string) (*File, error) { return f.makeFile(name, false) }

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

// Mirror mirrors the file to a target Fs at the same relative path.
//
// If File is a directory the directory is created in the target Fs. If it has
// children and children is true, they are copied as well. If File is a file it
// is copied to target. In all cases, directories along the path to the File
// are created in the target Fs.
//
// If the target Fs has an existing structure that differs from the source in
// that the target has existing files coresponding to paths of files being
// mirrored but are of different type (i.e. file/dir) an error is returned.
//
// If any other error occurs it is returned.
func (f *File) Mirror(target *Fs, overwrite, children bool) error {

	tgtfile, err := target.Get(f.Path(false), f.IsDir())
	if err != nil {
		return err
	}
	if err := tgtfile.Touch(overwrite); err != nil {
		return err
	}

	if !f.IsDir() {
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

		if _, err := io.Copy(outfile, infile); err != nil {
			return err
		}
	}

	if children {
		for _, file := range f.files {
			if err := file.Mirror(target, overwrite, children); err != nil {
				return err
			}
		}
	}

	return nil
}

// ReadWriteSeekCloser combines io.Seeker and io.ReadWriteCloser.
type ReadWriteSeekCloser interface {
	io.Seeker
	io.ReadWriteCloser
}

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
		var err error
		if fi.IsDir() {
			f, err = file.NewDir(fi.Name())
			if err != nil {
				return err
			}
			if err := fs.parse(f, filepath.Join(path, fi.Name())); err != nil {
				return err
			}
		} else {
			f, err = file.NewFile(fi.Name())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Parse parses root and reflects it in self replacing current Fs structure.
// Returns an error if one occurs.
func (fs *Fs) Parse() error {
	if fs.root == "" {
		return ErrRootNotSet
	}
	abs, err := filepath.Abs(fs.root)
	if err != nil {
		return err
	}
	fs.abs = abs
	if _, err := os.Stat(fs.abs); err != nil {
		return err
	}
	fs.files = make(files)
	return fs.parse(&fs.File, fs.abs)
}

// indentString builds an indent string for printFiles.
func indentString(depth int) string {
	b := make([]byte, 0, depth)
	for i := 0; i < depth; i++ {
		b = append(b, ' ')
	}
	return string(b)
}

// filesString returns files as string.
func filesString(f files, indent int) (result string) {

	names := make([]string, 0, len(f))
	for name := range f {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		result += fmt.Sprintf("%s %s\n", indentString(indent), f[name].Name())
		if len(f[name].files) > 0 {
			indent++
			result += filesString(f[name].files, indent)
			indent--
		}
	}
	return
}

// String implements Stringer.
func (fs *Fs) String() string { return filesString(fs.files, 0) }

// Flush commits current Fs structure to disk or returns an error if one
// occurs. It creates all directories along the path to touched files.
//
// If overwrite is specified, existing files are overwritten.
//
// If remove is specified removes files from disk that were deleted
// from Fs since the last call to Flush.
//
// If operation fails mid flight, any files created up to error
// are not removed.
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
	if err != nil {
		return
	}

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

// newFs returns a newFs *Fs rooted at root or an error.
func newFs(root string) (fs *Fs, err error) {
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
func At(root string) (*Fs, error) { return newFs(root) }

// copyFileRecursive recursively copies from from to to.
func copyFileRecursive(from, to *File) {
	for name, file := range from.files {
		var nf *File
		if file.IsDir() {
			nf, _ = to.NewDir(name)
		} else {
			nf, _ = to.NewFile(name)
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
	p, err := newFs(root)
	if err != nil {
		return nil, err
	}
	if err := p.Parse(); err != nil {
		return nil, err
	}
	return p, nil
}
