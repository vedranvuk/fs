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
	// ErrExists is returned when a Descriptor with specified name already exists.
	ErrExists = errors.New("fs: descriptor exists")
	// ErrInvalidName is returned when an invalid or empty name is specified.
	ErrInvalidName = errors.New("fs: invalid name")
	// ErrInvalidPath is returned when an invalid path is specified.
	// Invalid path in fs is a path that specifies an element in a path
	// that is a File instead of a Directory.
	ErrInvalidPath = errors.New("fs: invalid path")
	// ErrDirNotEmpty is returned when a Directory Descriptor that contains
	// children is being removed and a recursive operation was not requested.
	ErrDirNotEmpty = errors.New("fs: cannot remove directory descriptor, contains children")
	// ErrParentNotDir is returned when a child file is being added to a file
	// that is not a directory.
	ErrParentNotDir = errors.New("fs: parent is not a directory")
	// ErrOpenDirectory is returned when calling Open on a directory.
	ErrOpenDirectory = errors.New("fs: cannot open a directory")
	// ErrRootNotSet is returned by Parse() if root is not set.
	ErrRootNotSet = errors.New("fs: root not set")
	// ErrRootParentTraversal is returned whan specifying a path towards the
	// parent directory of a Fs root.
	ErrRootParentTraversal = errors.New("fs: traversing to root parent")
	// ErrIncompatibleStructure is returned when mirroring a file to a different Fs
	// that already has a structure which is incompatible with source Fs structure.
	// Usually, this occurs when a target contains a same named file as its source
	// directory so target file cannot contain children.
	ErrIncompatibleStructure = errors.New("fs: target fs structure not compatible")
)

// descriptorMap is a map of descriptor names to descriptor instances.
// It implements several methods for common operations.
type descriptorMap map[string]*Descriptor

// get always returns a descriptor under specified name and true if it returned
// an existing descriptor or false if it created a new one.
//
// If descriptor did not exist it will be created under specified name, under
// specified parent and it will be a dir if dir was true.
func (dm descriptorMap) get(name string, parent interface{}, dir bool) (*Descriptor, bool) {
	file, ok := dm[name]
	if !ok {
		file = newDescriptor(name, parent, dir)
		dm[name] = file
		return file, false
	}
	return file, true
}

// delete deletes a *Descriptor from descriptorMap by specified name.
func (dm descriptorMap) delete(name string) { delete(dm, name) }

// len returns count of entries in the descriptorMap.
func (dm descriptorMap) len() int { return len(dm) }

// validateDescriptorName validates a Descriptor returning ErrInvalidName if
// the specified name is empty or invalid because it contains reserved or
// invalid characters.
func validateDescriptorName(name string) error {
	if name == "" || name == "//" {
		return ErrInvalidName
	}
	return nil
}

// Descriptor defines a file descriptor which can represent both a file
// and a directory. Once a Descriptor is created its' type (file/directory)
// cannot be changed.
//
// Issuing directory related operations on a Descriptor which is a File
// instead of a Directory will usually return errors.
type Descriptor struct {
	// name is the name of the Descriptor.
	// Descriptors are kept in a descriptorMap and names of Descriptors in a
	// descriptorMap are unique and case-sensitive.
	name string
	// parent is the reference to Descriptor's parent which can be
	// either another *Descriptor or a *Fs if this Descriptor is the
	// root directory in a *Fs instance.
	parent interface{}
	// dir specifies if this Descriptor is a directory.
	dir bool
	// meta is the Descriptor metadata, provided for user storage.
	meta map[string]interface{}
	// descriptorMap is a map of contained Descriptors if this
	// Descriptor is a Directory.
	descriptorMap
}

// newDescriptor returns a new *Descriptor instance.
func newDescriptor(name string, parent interface{}, dir bool) *Descriptor {
	return &Descriptor{name: name, parent: parent, dir: dir, descriptorMap: make(descriptorMap)}
}

// newChild creates a child Descriptor in self which must be a Directory.
// If this Descriptor is not a Directory an ErrParentNotDir is returned.
//
// If a Descriptor under specified name already exists it is returned along
// with ErrExists.
//
// If an invalid name is specified returns a nil *Descriptor and ErrInvalidName.
//
// Directory specifies if the Descriptor should be created as a Directory.
func (d *Descriptor) newChild(name string, directory bool) (*Descriptor, error) {

	if err := validateDescriptorName(name); err != nil {
		return nil, ErrInvalidName
	}

	file, existed := d.get(name, d, directory)
	if !d.dir {
		return nil, ErrParentNotDir
	}

	fs := d.Fs()
	path := file.Path(false)
	if _, exists := fs.removeList[path]; exists {
		delete(fs.removeList, path)
	}

	if existed {
		return file, ErrExists
	}
	return file, nil
}

// NewDirectory creates a new Directory Descriptor inside this Descriptor.
// See newChild for details.
func (d *Descriptor) NewDirectory(name string) (*Descriptor, error) { return d.newChild(name, true) }

// NewFile creates a new File Descriptor inside this Descriptor.
// See newChild for details.
func (d *Descriptor) NewFile(name string) (*Descriptor, error) { return d.newChild(name, false) }

// Name returns Descriptor's name.
func (d *Descriptor) Name() string { return d.name }

// Parent returns this Descriptor's parent *Descriptor.
// If there is no parent, i.e. this Descriptor represents the root Directory
// in a Fs a nil Descriptor is returned.
func (d *Descriptor) Parent() *Descriptor {
	if file, ok := d.parent.(*Descriptor); ok {
		return file
	}
	return nil
}

// Fs returns the Fs this Descriptor belongs to.
func (d *Descriptor) Fs() *Fs {
	curr := d.parent
	for {
		if fs, ok := curr.(*Fs); ok {
			return fs
		}
		if file, ok := curr.(*Descriptor); ok {
			curr = file.parent
			continue
		}
		panic("fs: bug: cannot find Descriptor's Fs")
	}
}

// leftPathElem is a helper to Get that returns the first (leftmost) element
// of the path and the rest.
//
// If s is empty returns empty dir and empty rest.
// If s begins with a slash returns empty dir and everything as else, minus its'
// leading frontslash.
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

// Get returns a Descriptor by specified name where name can be a name of a
// Descriptor in this Directory Descriptor, a path relative to this Descriptor
// or a rooted path that is rooted at the root directory of the Fs that manages
// this Descriptor.
//
// If directory is true and the Descriptor specified by name needs to be
// created, it is created as a Directory instead of a File.
//
// Returns ErrInvalidName if name is empty or invalid.
//
// Returns an ErrInvalidPath if an element of the path specified by name
// already exists in the Fs, is a File instead of a Directory and does not
// have children.
//
// Returns ErrRootParentTraversal if name specifies a path that leads to a
// directory outside of Fs' root directory.
//
// If Get returns an error and a non-nil Descriptor, the Descriptor should be
// used exclusively for purposes of examining the error and will be a
// Descriptor that caused the error.
func (d *Descriptor) Get(name string, directory bool) (*Descriptor, error) {

	// Special case for Fs root.
	if name == "//" {
		return &d.Fs().Descriptor, nil
	}

	if err := validateDescriptorName(name); err != nil {
		return nil, ErrInvalidName
	}

	dir, rest := leftPathElem(name)

	// Redirrect rooted paths to Fs root.
	if dir == "" {
		if rest == "" {
			return nil, ErrInvalidName
		}
		return d.Fs().Get(rest, directory)
	}

	// Handle dot-names.
	switch dir {
	case ".":
		return d, nil
	case "..":
		if parent := d.Parent(); parent != nil {
			return parent.Get(rest, directory)
		}
		return nil, ErrRootParentTraversal
	}

	if !d.IsDirectory() {
		return d, ErrInvalidPath
	}

	desc, _ := d.get(dir, d, directory)
	if rest != "" {
		return desc.Get(rest, directory)
	}
	return desc, nil
}

// IsDirectory returns if this Descriptor is a Directory.
func (d *Descriptor) IsDirectory() bool { return d.dir }

// GetMeta returns meta information by specified key.
func (d *Descriptor) GetMeta(key string) interface{} { return d.meta[key] }

// SetMeta sets meta information val under specified key.
func (d *Descriptor) SetMeta(key string, val interface{}) { d.meta[key] = val }

// Count returns this Descriptor's child Descriptor count.
func (d *Descriptor) Count() int { return len(d.descriptorMap) }

// Files returns files contained by this Descriptor if it is a Directory.
// Otherwise, always returns an empty slice.
func (d *Descriptor) Files() []*Descriptor {
	res := make([]*Descriptor, 0, len(d.descriptorMap))
	for _, file := range d.descriptorMap {
		if !file.IsDirectory() {
			res = append(res, file)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name() < res[j].Name()
	})
	return res
}

// FileNames returns names of files contained by this Descriptor if it is a
// directory. Otherwise, always returns an empty slice.
func (d *Descriptor) FileNames(namesonly, absolute bool) []string {
	res := make([]string, 0, len(d.descriptorMap))
	for _, file := range d.descriptorMap {
		if !file.IsDirectory() {
			if namesonly {
				res = append(res, file.Name())
			} else {
				res = append(res, file.Path(absolute))
			}
		}
	}
	sort.Strings(res)
	return res
}

// Directories returns Directories contained by this Descriptor if it is a
// directory. Otherwise, always returns an empty slice.
func (d *Descriptor) Directories() []*Descriptor {
	res := make([]*Descriptor, 0, len(d.descriptorMap))
	for _, file := range d.descriptorMap {
		if file.IsDirectory() {
			res = append(res, file)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name() < res[j].Name()
	})
	return res
}

// DirectoryNames returns names of Directories contained by this Descriptor if
// it is a directory. Otherwise, always returns an empty slice.
func (d *Descriptor) DirectoryNames(namesonly, absolute bool) []string {
	res := make([]string, 0, len(d.descriptorMap))
	for _, file := range d.descriptorMap {
		if file.IsDirectory() {
			if namesonly {
				res = append(res, file.Name())
			} else {
				res = append(res, file.Path(absolute))
			}
		}
	}
	sort.Strings(res)
	return res
}

// children is the implementation of Children.
func (d *Descriptor) children(depth, curr int, absolute bool, result *[]string) {
	if depth >= 0 && curr > depth {
		return
	}
	for _, file := range d.descriptorMap {
		*result = append(*result, file.Path(absolute))
		file.children(depth, curr+1, absolute, result)
	}
}

// Children returns this Descriptor's children up to specified depth where 0 is
// the direct children of thr Descriptor. A negative depth imposes no limit.
// If absolute is specified returns absolute paths, otherwise relative to root.
func (d *Descriptor) Children(depth int, absolute bool) []string {
	var result []string
	d.children(depth, 0, absolute, &result)
	sort.Strings(result)
	return result
}

// Path returns the path to this Descriptor, including the Descriptor name. If
// absolute is specified it returns an absolute path to the Descriptor,
// otherwise a path relative to the Fs root directory.
func (d *Descriptor) Path(absolute bool) string {
	if file, ok := d.parent.(*Descriptor); ok {
		return path.Join(file.Path(absolute), d.name)
	}
	if fs, ok := d.parent.(*Fs); ok {
		if absolute {
			return filepath.Join(fs.abs, d.name)
		}
		return "/" + d.name
	}
	return ""
}

// Exists checks if the Descriptor exists on disk and returns the truth and a
// nil error on success.
// If an error occurs it is returned with an invalid value of file's existence.
func (d *Descriptor) Exists() (bool, error) {
	_, err := os.Stat(d.Path(true))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Touch creates this Descriptor on disk and returns nil on success.
// It creates an empty File or an empty Directory, depending on descriptor type.
// All directories along the path are created.
// If overwrite is specified, silently overwrites the Descriptor, otherwise
// returns an os.ErrExists. Any other error is returned and the op may have
// completed partially.
func (d *Descriptor) Touch(overwrite bool) error {
	p := d.Path(true)
	if d.dir {
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

// Remove deletes this Descriptor from disk. It does so recursively if the
// Descriptor is a directory and recursive is specified.
// Returns os.ErrNotExists if file did not exist on disk prior to the call.
// Returns any other error if one occurs.
func (d *Descriptor) Remove(recursive bool) error {
	if fs, ok := d.parent.(*Fs); ok {
		fs.Descriptor = *newDescriptor("/", fs, true)
		if recursive {
			return os.RemoveAll(fs.abs)
		}
		return os.Remove(fs.abs)
	}
	if recursive {
		return os.RemoveAll(d.Path(true))
	}
	return os.Remove(d.Path(true))
}

// Delete deletes this Descriptor from its' parent. If Descriptor is a
// Directory and recursive is specified it removes Descriptors recursively,
// otherwise returns an error if Descriptor contains children.
func (d *Descriptor) Delete(recursive bool) error {
	if file, ok := d.parent.(*Descriptor); ok {
		if file.Count() > 0 && !recursive {
			return ErrDirNotEmpty
		}
		file.delete(d.name)
	}
	if fs, ok := d.parent.(*Fs); ok {
		if fs.Count() > 0 && !recursive {
			return ErrDirNotEmpty
		}
		fs.delete(d.name)
	}

	d.Walk(func(desc *Descriptor) bool {
		d.Fs().removeList[desc.Path(false)] = desc
		return true
	}, true)
	d.Fs().removeList[d.Path(false)] = d
	return nil
}

// From mirrors a Descriptor from specified source Fs to a Descriptor in this
// Fs at the same relative path.
//
// If copy is specified copies the underlying files of the source Fs to this Fs.
// (This is pšotentially a VERY long operation.)
// If overwrite is specified silently overwrites existing files in this Fs.
// If recursive is specified it recursively copies Descriptors from source.
//
// If an error occurs it is returned. If the operation fails mid-flight there
// may be files left over from an unfinished operation.
func (d *Descriptor) From(source *Fs, copy, overwrite, recursive bool) error {

	srcfile, err := source.Get(d.Path(false), d.IsDirectory())
	if err != nil {
		return err
	}
	if err := d.Touch(overwrite); err != nil {
		return err
	}

	if copy && !srcfile.IsDirectory() {
		infile, err := os.OpenFile(srcfile.Path(true), os.O_RDONLY, os.ModePerm)
		if err != nil {
			return err
		}
		defer infile.Close()

		outfile, err := os.OpenFile(d.Path(true), os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer outfile.Close()

		if _, err := io.Copy(outfile, infile); err != nil {
			return err
		}
	}

	if recursive {
		for _, file := range srcfile.descriptorMap {
			newfile, err := d.Get(file.Path(false), file.IsDirectory())
			if err != nil {
				return err
			}
			if err := newfile.From(source, copy, overwrite, recursive); err != nil {
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

// Open opens an underlying Descriptor in read/write mode if it is a File.
// If the File does not yet exist on disk it is created.
// If truncate is specified, File is cleared on open.
//
// Returns ErrOpenDirectory if Descriptor is a Directory or any other error
// if it occurs.
//
// Caller is responsible for closing the returned ReadWriteSeekCloser.
func (d *Descriptor) Open(truncate bool) (ReadWriteSeekCloser, error) {
	if d.IsDirectory() {
		return nil, ErrOpenDirectory
	}
	flags := os.O_CREATE | os.O_RDWR
	if truncate {
		flags = flags | os.O_TRUNC
	}
	file, err := os.OpenFile(d.Path(true), flags, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// walkFunc is the Descriptor traversal function prototype. It passes current
// Descriptor being traversed to the WalkFunc which must return true to
// continue enumeration or false to stop it.
type walkFunc func(*Descriptor) bool

// Walk walks the files sorted by name in ascending order.
// It walks the complete tree and does it recursively if recursive is specified.
func (d *Descriptor) Walk(fn walkFunc, recursive bool) {
	names := make([]string, 0, len(d.descriptorMap))
	for key := range d.descriptorMap {
		names = append(names, key)
	}
	sort.Strings(names)
	for _, name := range names {
		file := d.descriptorMap[name]
		if !fn(file) {
			break
		}
		if recursive {
			file.Walk(fn, recursive)
		}
	}
}

// removeList maps a name of a removed Descriptor to a Descriptor.
type removeList map[string]*Descriptor

// Fs defines a filesystem rooted at a directory on disk.
//
// It embeds a Directory Descriptor that represents the Fs root directory.
type Fs struct {
	// root is the root directory which Fs manages.
	root string
	// abs is the absolute path of root.
	abs string
	// removeList is a list of Descriptors removed from Fs since
	// last call to Flush.
	removeList
	// Descriptor is the DIrectory Descriptor representing Fs root.
	Descriptor
}

// Root returns Fs's root folder as set on construction.
func (fs *Fs) Root() string { return fs.root }

// Abs returns absolute path of Fs root.
func (fs *Fs) Abs() string { return fs.abs }

// parse is the implementation of Parse.
func (fs *Fs) parse(file *Descriptor, path string) error {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		var f *Descriptor
		var err error
		if fi.IsDir() {
			f, err = file.NewDirectory(fi.Name())
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
	fs.descriptorMap = make(descriptorMap)
	return fs.parse(&fs.Descriptor, fs.abs)
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
func filesString(desc *Descriptor, indent int) (result string) {

	names := make([]string, 0, desc.Count())
	names = append(names, desc.DirectoryNames(true, false)...)
	names = append(names, desc.FileNames(true, false)...)

	for _, name := range names {
		result += fmt.Sprintf("%s %s\n", indentString(indent), name)
		if child, ok := desc.descriptorMap[name]; ok {
			indent++
			result += filesString(child, indent)
			indent--
		}
	}
	return
}

// String implements Stringer.
func (fs *Fs) String() string { return filesString(&fs.Descriptor, 0) }

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
		fs.removeList = make(removeList)
	}()

	fs.Walk(func(file *Descriptor) bool {
		if err = file.Touch(overwrite); err != nil {
			return false
		}
		return true
	}, true)
	if err != nil {
		return
	}

	if remove {

		type pair struct {
			path string
			desc *Descriptor
		}
		list := make([]*pair, 0, len(fs.removeList))
		for key, val := range fs.removeList {
			list = append(list, &pair{key, val})
		}
		sort.Slice(list, func(i, j int) bool {
			return list[i].path < list[j].path
		})
		for i := len(list) - 1; i >= 0; i-- {
			if err := os.Remove(filepath.Join(fs.abs, list[i].path)); err != nil {
				return err
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
	fs = &Fs{root: root, abs: abs, removeList: make(removeList)}
	fs.Descriptor = *newDescriptor("/", fs, true)
	return
}

// At returns a Fs rooted at specified root directory.
// No actions are executed on the resulting Fs.
func At(root string) (*Fs, error) { return newFs(root) }

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

// From returns a new Fs instance rooted at specified root and having
// the structure of specified source fs. If an error occurs returns a nil Fs
// and an error.
// See Descriptor.From for details on other parameters.
func From(root string, source *Fs, copy, overwrite, recursive bool) (*Fs, error) {
	p, err := At(root)
	if err != nil {
		return nil, err
	}
	if err := p.From(source, copy, overwrite, recursive); err != nil {
		return nil, err
	}
	return p, nil
}
