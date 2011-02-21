package main_test

import (
	"testing"
	"gostore/services/fs"
	"gostore/log"
	"bytes"
	"time"
)

// TODO: Remove those sleep when locking will be implemented

func TestDeleteNonRecursive(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestDeleteNonRecursive...")

	path := fs.NewPath("/tests/delete/nonrec/level1")
	byts := []byte("data")
	buf := bytes.NewBuffer(byts)
	tc.nodes[0].Fss.Write(path, int64(len(byts)), "", buf, nil)

	time.Sleep(50000000)

	exists, _ := tc.nodes[2].Fss.Exists(path, nil)
	if !exists {
		t.Errorf("1) Created file should exists\n")
	}

	tc.nodes[0].Fss.Delete(path, false, nil)

	exists, _ = tc.nodes[2].Fss.Exists(path, nil)
	if exists {
		t.Errorf("2) Deleted file shouldn't exists\n")
	}
}

func TestDeleteNonRecursiveMultipleLevel(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestDeleteNonRecursiveMultipleLevel...")

	byts := []byte("data")
	buf := bytes.NewBuffer(byts)
	tc.nodes[0].Fss.Write(fs.NewPath("/tests/delete/nonrecml/level1/level2/level3"), int64(len(byts)), "", buf, nil)

	time.Sleep(10000000)

	err := tc.nodes[0].Fss.Delete(fs.NewPath("/tests/delete/nonrecml/level1"), false, nil)

	if err != fs.ErrorNotEmpty {
		t.Errorf("1) Should get an error if trying to delete a multiple level file")
	}

	exists, _ := tc.nodes[2].Fss.Exists(fs.NewPath("/tests/delete/nonrecml/level1/level2/level3"), nil)
	if !exists {
		t.Errorf("2) Should not be deleted since it was a multilevel with non recursive\n")
	}
}


func TestDeleteParentHierarchie(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestDeleteParentHierarchie...")

	path := fs.NewPath("/tests/delete/hierarchie/child1")
	byts := []byte("child1")
	buf := bytes.NewBuffer(byts)
	tc.nodes[2].Fss.Write(path, int64(len(byts)), "", buf, nil)

	time.Sleep(100000000)

	tc.nodes[5].Fss.Delete(path, false, nil)

	time.Sleep(100000000)

	children, _ := tc.nodes[6].Fss.Children(path.ParentPath(), nil)
	for _, child := range children {
		if child.Name == "child1" {
			t.Errorf("1) File hasn't been removed from parent children")
		}
	}
}

func TestDeleteRecursive(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestDeleteRecursive...")

	path := fs.NewPath("/tests/delete/rec/level1-a/level2-a")
	byts := []byte("data")
	buf := bytes.NewBuffer(byts)
	tc.nodes[8].Fss.Write(path, int64(len(byts)), "", buf, nil)

	path = fs.NewPath("/tests/delete/rec/level1-a/level2-b")
	byts = []byte("data")
	buf = bytes.NewBuffer(byts)
	tc.nodes[0].Fss.Write(path, int64(len(byts)), "", buf, nil)

	path = fs.NewPath("/tests/delete/rec/level1-b/level2-a")
	byts = []byte("data")
	buf = bytes.NewBuffer(byts)
	tc.nodes[1].Fss.Write(path, int64(len(byts)), "", buf, nil)

	path = fs.NewPath("/tests/delete/rec/level1-b/level2-b")
	byts = []byte("data")
	buf = bytes.NewBuffer(byts)
	tc.nodes[4].Fss.Write(path, int64(len(byts)), "", buf, nil)

	path = fs.NewPath("/tests/delete/rec/level1-c/level2-a")
	byts = []byte("data")
	buf = bytes.NewBuffer(byts)
	tc.nodes[2].Fss.Write(path, int64(len(byts)), "", buf, nil)

	path = fs.NewPath("/tests/delete/rec/level1-c/level2-b")
	byts = []byte("data")
	buf = bytes.NewBuffer(byts)
	tc.nodes[6].Fss.Write(path, int64(len(byts)), "", buf, nil)

	time.Sleep(10000000)

	// 1 level with recursive on
	tc.nodes[5].Fss.Delete(fs.NewPath("/tests/delete/rec/level1-a/level2-a"), true, nil)
	exists, _ := tc.nodes[1].Fss.Exists(fs.NewPath("/tests/delete/rec/level1-a/level2-a"), nil)
	if exists {
		t.Errorf("1) Deleted file shouldn't exists\n")
	}

	// 2 level with recursive on
	tc.nodes[3].Fss.Delete(fs.NewPath("/tests/delete/rec/level1-a"), true, nil)
	exists, _ = tc.nodes[4].Fss.Exists(fs.NewPath("/tests/delete/rec/level1-a"), nil)
	if exists {
		t.Errorf("2) Deleted file shouldn't exists\n")
	}
	exists, _ = tc.nodes[5].Fss.Exists(fs.NewPath("/tests/delete/rec/level1-a/level2-b"), nil)
	if exists {
		t.Errorf("3) Deleted file shouldn't exists\n")
	}

	// 3 level with recursive on
	tc.nodes[3].Fss.Delete(fs.NewPath("/tests/delete/rec"), true, nil)
	exists, _ = tc.nodes[4].Fss.Exists(fs.NewPath("/tests/delete/rec"), nil)
	if exists {
		t.Errorf("4) Deleted file shouldn't exists\n")
	}
	exists, _ = tc.nodes[5].Fss.Exists(fs.NewPath("/tests/delete/rec/level1-c"), nil)
	if exists {
		t.Errorf("5) Deleted file shouldn't exists\n")
	}
	exists, _ = tc.nodes[5].Fss.Exists(fs.NewPath("/tests/delete/rec/level1-c/level2-a"), nil)
	if exists {
		t.Errorf("6) Deleted file shouldn't exists\n")
	}
	exists, _ = tc.nodes[5].Fss.Exists(fs.NewPath("/tests/delete/rec/level1-c/level2-b"), nil)
	if exists {
		t.Errorf("6) Deleted file shouldn't exists\n")
	}
}
