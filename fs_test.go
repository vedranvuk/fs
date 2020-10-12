package fs

import (
	"fmt"
	"testing"
)

func TestFs(t *testing.T) {
	fs, err := At("testdata")
	if err != nil {
		t.Fatal(err)
	}
	fs.Get("/test1/file1", false)
	fs.Get("/test1/file2", false)
	fs.Get("/test1/file3", false)

	fs.Get("/test2/file1", false)
	fs.Get("/test2/file2", false)
	fs.Get("/test2/file3", false)

	fs.Get("/test3/file1", false)
	fs.Get("/test3/file2", false)
	fs.Get("/test3/file3", true)

	fmt.Println(fs.Children(-1, false))
	fmt.Println(fs.Children(-1, true))

	fmt.Println(fs)

	if err := fs.Flush(true, false); err != nil {
		t.Fatal(err)
	}

	file, err := fs.Get("test3", true)
	if err != nil {
		t.Fatal(err)
	}
	file.Delete()

	if err := fs.Flush(true, true); err != nil {
		t.Fatal(err)
	}

	fmt.Println(fs)

	if err := fs.Remove(); err != nil {
		t.Fatal(err)
	}
}

func TestMirror(t *testing.T) {
	oldfs, err := At("oldfs")
	if err != nil {
		t.Fatal(err)
	}
	oldfs.Get("/dir1/file1", false)
	oldfs.Get("/dir1/file2", false)
	oldfs.Get("/dir1/file3", false)

	if err := oldfs.Flush(true, false); err != nil {
		t.Fatal(err)
	}

	f, err := oldfs.Get("/dir1/file2", false)
	if err != nil {
		t.Fatal(err)
	}
	rwsc, err := f.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rwsc.Write([]byte("Hello World!")); err != nil {
		t.Fatal(err)
	}
	if err := rwsc.Close(); err != nil {
		t.Fatal(err)
	}

	newfs, err := At("newfs")
	if err != nil {
		t.Fatal(err)
	}

	f, err = oldfs.Get("/dir1", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Mirror(newfs, true, true); err != nil {
		t.Fatal(err)
	}

	if err := newfs.Remove(); err != nil {
		t.Fatal(err)
	}
	if err := oldfs.Remove(); err != nil {
		t.Fatal(err)
	}
}
