package main_test

import (
	"testing"
	"gostore/services/fs"
	"gostore/log"
	"bytes"
)


func TestChildrenList(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestChildrenList...")

	// Read write test
	byts := []byte("child1 data of size 22")
	buf := bytes.NewBuffer(byts)
	tc.nodes[0].Fss.Write(fs.NewPath("/tests/hierarchie/childrenlist/child1"), int64(len(byts)), "application/json", buf, nil)

	byts = []byte("child2")
	buf = bytes.NewBuffer(byts)
	tc.nodes[1].Fss.Write(fs.NewPath("/tests/hierarchie/childrenlist/child2"), int64(len(byts)), "text/html", buf, nil)
	byts = []byte("child3")
	buf = bytes.NewBuffer(byts)
	tc.nodes[3].Fss.Write(fs.NewPath("/tests/hierarchie/childrenlist/child3"), int64(len(byts)), "text/css", buf, nil)
	byts = []byte("otherchildrenlist")
	buf = bytes.NewBuffer(byts)
	tc.nodes[3].Fss.Write(fs.NewPath("/tests/hierarchie/otherchildrenlist"), int64(len(byts)), "", buf, nil)

	// Check third level
	children, err := tc.nodes[5].Fss.Children(fs.NewPath("/tests/hierarchie/childrenlist"), nil)
	if err != nil {
		t.Errorf("1) Children returned an an error for: %s\n", err)
	}
	if len(children) != 3 {
		t.Errorf("2) Children didn't return 3 children: %d\n", len(children))
	}

	one, two, three := false, false, false
	for _, child := range children {
		switch child.Name {
		case "child1":
			if child.MimeType != "application/json" {
				t.Errorf("3) child1 should have a type application/json: %s\n", child.MimeType)
			}

			if child.Size != 22 {
				t.Errorf("4) child1 should have a size of 22: %d\n", child.Size)
			}

			one = true
		case "child2":
			two = true

			if child.MimeType != "text/html" {
				t.Errorf("5) child2 should have a type text/html: %s\n", child.MimeType)
			}

			if child.Size != 6 {
				t.Errorf("6) child2 should have a size of 6: %d\n", child.Size)
			}

		case "child3":
			three = true
		default:
			t.Errorf("7) Received invalid child: %s\n", child)
		}
	}
	if !one || !two || !three {
		t.Errorf("8) One or more children missing. Received: %s\n", children)
	}

	// Check second level
	children, err = tc.nodes[8].Fss.Children(fs.NewPath("/tests/hierarchie"), nil)
	if err != nil {
		t.Errorf("9) Children returned an an error for: %s\n", err)
	}
	if len(children) != 2 {
		t.Errorf("10) Children didn't return 3 children: %d\n", len(children))
	}
	one, two, three = false, false, false
	for _, child := range children {
		switch child.Name {
		case "childrenlist":
			one = true
		case "otherchildrenlist":
			two = true
		default:
			t.Errorf("11) Received invalid child: %s\n", child)
		}
	}
	if !one || !two {
		t.Errorf("12) One or more children missing. Received: %s\n", children)
	}

	// Check first level
	children, err = tc.nodes[4].Fss.Children(fs.NewPath("/tests"), nil)
	if err != nil {
		t.Errorf("13) Children returned an an error for: %s\n", err)
	}
	if len(children) < 1 {
		t.Errorf("14) Children didn't return at least 1: %d\n", len(children))
	}
	one = false
	for _, child := range children {
		if child.Name == "hierarchie" {
			one = true
		}
	}
	if !one {
		t.Errorf("15) Didn't receive 'hierarchie' child. Received: %s\n", children)
	}
}
