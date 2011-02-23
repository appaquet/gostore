package fs_test

import (
	"gostore/services/fs"
	"testing"
)

func BenchmarkParsing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fs.NewPath("/home/test/toto/../toto/tata/./")
	}
}

func TestParsing(t *testing.T) {
	path := fs.NewPath("/")
	parent := path.ParentPath()
	if path.String() != "/" {
		t.Errorf("1) Expected path of / != %s\n", path)
	}
	if parent.String() != "/" {
		t.Errorf("2) Expected parent of / is / != %s\n", parent)
	}

	path = fs.NewPath("/home")
	parent = path.ParentPath()
	if path.String() != "/home" {
		t.Errorf("3) Expected path of /home != %s\n", path)
	}
	if parent.String() != "/" {
		t.Errorf("4) Expected parent of /home is / != %s\n", parent)
	}

	path = fs.NewPath("/home/test")
	parent = path.ParentPath()
	if path.String() != "/home/test" {
		t.Errorf("5) Expected path of /home/test != %s\n", path)
	}
	if parent.String() != "/home" {
		t.Errorf("6) Expected parent of /home/test is /home != %s\n", parent)
	}

	path = fs.NewPath("/home/test/")
	parent = path.ParentPath()
	if path.String() != "/home/test" {
		t.Errorf("7) Expected path of /home/test != %s\n", path)
	}
	if parent.String() != "/home" {
		t.Errorf("8) Expected parent of /home/test is /home != %s\n", parent)
	}

	path = fs.NewPath("/home/test/..")
	parent = path.ParentPath()
	if path.String() != "/home" {
		t.Errorf("9) Expected path of /home != %s\n", path)
	}
	if parent.String() != "/" {
		t.Errorf("10) Expected parent of /home is / != %s\n", parent)
	}

	path = fs.NewPath("/home/..")
	parent = path.ParentPath()
	if path.String() != "/" {
		t.Errorf("11) Expected path of / != %s\n", path)
	}
	if parent.String() != "/" {
		t.Errorf("12) Expected parent of / is / != %s\n", parent)
	}

	path = fs.NewPath("/..")
	parent = path.ParentPath()
	if path.String() != "/" {
		t.Errorf("13) Expected path of / != %s\n", path)
	}
	if parent.String() != "/" {
		t.Errorf("14) Expected parent of / is / != %s\n", parent)
	}

	path = fs.NewPath("/.")
	parent = path.ParentPath()
	if path.String() != "/" {
		t.Errorf("15) Expected path of / != %s\n", path)
	}
	if parent.String() != "/" {
		t.Errorf("16) Expected parent of /. is / != %s\n", parent)
	}

	path = fs.NewPath("/home/.")
	parent = path.ParentPath()
	if path.String() != "/home" {
		t.Errorf("17) Expected path of /home != %s\n", path)
	}
	if parent.String() != "/" {
		t.Errorf("18) Expected parent of /home is / != %s\n", parent)
	}

	path = fs.NewPath("/home/./test")
	parent = path.ParentPath()
	if path.String() != "/home/test" {
		t.Errorf("19) Expected path of /home/test != %s\n", path)
	}
	if parent.String() != "/home" {
		t.Errorf("20) Expected parent of /home/test is /home != %s\n", parent)
	}

	path = fs.NewPath("/home/./test/..")
	parent = path.ParentPath()
	if path.String() != "/home" {
		t.Errorf("21) Expected path of /home != %s\n", path)
	}
	if parent.String() != "/" {
		t.Errorf("22) Expected parent of /home/./test/.. is / != %s\n", parent)
	}

	path = fs.NewPath("/home/./test/../test/tata/../toto/./")
	parent = path.ParentPath()
	if path.String() != "/home/test/toto" {
		t.Errorf("23) Expected path of /home/test/toto != %s\n", path)
	}
	if parent.String() != "/home/test" {
		t.Errorf("24) Expected parent of /home/test/toto is /home/test != %s\n", parent)
	}
}


func TestChildPath(t *testing.T) {
	path := fs.NewPath("/home")
	child := path.ChildPath("test")
	if child.String() != "/home/test" {
		t.Errorf("1) Wrong child path: %s %s\n", child, path.Parts)
	}

	path = fs.NewPath("/")
	child = path.ChildPath("home")
	if child.String() != "/home" {
		t.Errorf("2) Wrong child path: %s %s\n", child, path.Parts)
	}
}

func TestBaseName(t *testing.T) {
	path := fs.NewPath("/home")
	if path.BaseName() != "home" {
		t.Errorf("1) BaseName should be 'home' but received '%s'\n", path.BaseName())
	}

	path = fs.NewPath("/home/..")
	if path.BaseName() != "" {
		t.Errorf("2) BaseName should be '' but received '%s'\n", path.BaseName())
	}

	path = fs.NewPath("/")
	if path.BaseName() != "" {
		t.Errorf("3) BaseName should be '' but received '%s'\n", path.BaseName())
	}

	path = fs.NewPath("/..")
	if path.BaseName() != "" {
		t.Errorf("4) BaseName should be '' but received '%s'\n", path.BaseName())
	}

	path = fs.NewPath("/home/test/..")
	if path.BaseName() != "home" {
		t.Errorf("5) BaseName should be 'home' but received '%s'\n", path.BaseName())
	}
}

func TestIsRoot(t *testing.T) {
	path := fs.NewPath("/home")
	if path.IsRoot() {
		t.Errorf("1) /home should not considered root\n")
	}

	path = fs.NewPath("/")
	if !path.IsRoot() {
		t.Errorf("2) / should be considered root: '%s'\n", path.Parts[0])
	}

	path = fs.NewPath("/test/..")
	if !path.IsRoot() {
		t.Errorf("3) /test/.. should be considered root\n")
	}
}
