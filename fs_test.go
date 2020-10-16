package fs

import (
	"testing"
)

func init() {
	fs, err := At("test/data")
	if err != nil {
		panic(err)
	}
	fs.NewFile("/A/AA.file")
	fs.NewFile("/A/AB.file")
	fs.NewFile("/A/AC.file")
	fs.NewFile("/A/AA.file")
	fs.NewFile("/A/AB.file")
	fs.NewFile("/A/AC.file")
	fs.NewFile("/A/AA/AAA/AAAA.file")
	fs.NewFile("/A/AA/AAA/AAAB.file")
	fs.NewFile("/A/AA/AAA/AAAC.file")
	fs.NewFile("/B/BA/BAA/BAAA.file")

	if err := fs.Flush(true, false); err != nil {
		panic(err)
	}
}

func TestNames(t *testing.T) {
	fs, err := Parse("test/data")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := fs.Get("..", true); err != ErrRootParentTraversal {
		t.Fatal("hacked!")
	}

	file, err := fs.Get(".", true)
	if err != nil {
		t.Fatal(err)
	}
	if file != &fs.Descriptor {
		t.Fatal("fail")
	}

	file, err = fs.Get("/folder1/folder1sub1/folder1sub1file1", true)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.Get("../../..", true); err != ErrInvalidName {
		t.Fatal(err)
	}

	if _, err := file.Get("../../../..", true); err != ErrRootParentTraversal {
		t.Fatal(err)
	}
}

func TestMirror(t *testing.T) {
	fs, err := Parse("test/data")
	if err != nil {
		t.Fatal(err)
	}

	mirrorfs, err := At("test/mirrordata")
	if err != nil {
		t.Fatal(err)
	}

	if err := fs.Mirror(mirrorfs, true, true); err != nil {
		t.Fatal(err)
	}

	if err := mirrorfs.Flush(true, false); err != nil {
		t.Fatal(err)
	}

	fs.Walk(func(desc *Descriptor) bool {
		if _, err := mirrorfs.Get(desc.Path(false), false); err != nil {
			t.Fatal(err)
		}
		return true
	}, true)

	if err := mirrorfs.Remove(true); err != nil {
		t.Fatal(err)
	}
}

func TestFlush(t *testing.T) {
	fs, err := Parse("test/data")
	if err != nil {
		t.Fatal(err)
	}

	mirrorfs, err := At("test/mirrordata")
	if err != nil {
		t.Fatal(err)
	}

	if err := fs.Mirror(mirrorfs, true, true); err != nil {
		t.Fatal(err)
	}

	if err := mirrorfs.Flush(true, false); err != nil {
		t.Fatal(err)
	}

	if err := mirrorfs.Delete(true); err != nil {
		t.Fatal(err)
	}

	if err := mirrorfs.Flush(true, true); err != nil {
		t.Fatal(err)
	}

	if err := mirrorfs.Remove(true); err != nil {
		t.Fatal(err)
	}
}
