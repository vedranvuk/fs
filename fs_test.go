package fs

import (
	"fmt"
	"testing"
)

func TestTest(t *testing.T) {

	fs, err := Parse("test/data")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(fs)
}
