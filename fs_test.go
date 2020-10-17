package fs

import (
	"fmt"
	"os"
	"testing"
)

func init() {
	os.RemoveAll("test/data")
	os.RemoveAll("test/mirrordata")

	fs, err := At("test/data")
	if err != nil {
		panic(err)
	}
	fs.NewFile("/abc/file1.ext")
	fs.NewFile("/abc/file2.ext")
	fs.NewFile("/abc/file3.ext")
	fs.NewFile("/abc/def/file1.ext")
	fs.NewFile("/abc/def/file2.ext")
	fs.NewFile("/abc/def/file3.ext")
	fs.NewFile("/def/file1.ext")
	fs.NewFile("/def/file2.ext")
	fs.NewFile("/def/file3.ext")
	fs.NewFile("/ghi/file1.ext")
	fs.NewFile("/ghi/file2.ext")
	fs.NewFile("/ghi/file3.ext")

	if err := fs.Flush(true, false); err != nil {
		panic(err)
	}
}

func TestFs(t *testing.T) {
	fs, err := Parse("test/data")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := fs.Get("..", true); err != ErrRootParentTraversal {
		t.Fatal("Get(..) failed")
	}

	file, err := fs.Get(".", true)
	if err != nil {
		t.Fatal(err)
	}
	if file != &fs.Descriptor {
		t.Fatal("Get failed")
	}

	file, err = fs.Get("/abc/file1.ext", true)
	if err != nil {
		t.Fatal(err)
	}

	if exists, err := file.Exists(); err != nil {
		t.Fatal(err)
	} else {
		if !exists {
			t.Fatal("Exists failed")
		}
	}

	rwsc, err := file.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	rwsc.Write([]byte("Hello World!"))
	if err := rwsc.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := file.Get("../../../..", true); err != ErrRootParentTraversal {
		t.Fatal(err)
	}

	mirrorfs, err := From("test/mirrordata", fs, true, true, true)
	if err != nil {
		t.Fatal(err)
	}

	fs.Walk(func(desc *Descriptor) bool {
		if _, err := mirrorfs.Get(desc.Path(false), false); err != nil {
			t.Fatal(err)
		}
		return true
	}, true)

	if err := mirrorfs.Delete(true); err != nil {
		t.Fatal(err)
	}

	if err := mirrorfs.Flush(true, true); err != nil {
		t.Fatal(err)
	}

	fmt.Println(fs)

	if err := fs.Remove(true); err != nil {
		t.Fatal(err)
	}
}
