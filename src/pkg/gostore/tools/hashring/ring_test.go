// Author: Andre-Philippe Paquet
// Date: November 2010

package hashring_test

import (
	"gostore/tools/hashring"
	"testing"
)


func TestAddElement(t *testing.T) {
	ring := hashring.NewRing()

	ring.AddElement(hashring.NewElement("bcd", "bcd"))
	ring.AddElement(hashring.NewElement("abc", "abc"))
	ring.AddElement(hashring.NewElement("fgh", "fgh"))
	ring.AddElement(hashring.NewElement("def", "def"))
	ring.AddElement(hashring.NewElement("ijk", "ijk"))
	ring.AddElement(hashring.NewElement("jkl", "jkl"))
	ring.AddElement(hashring.NewElement("ghi", "ghi"))

	node := ring.ResolveString("abb")
	if node == nil || node.Hash != "abc" {
		t.Errorf("1) abb should resolve to abc: %s", node)
	}

	node = ring.ResolveString("dfh")
	if node == nil || node.Hash != "fgh" {
		t.Errorf("2) dfh should resolve to fgh: %s", node)
	}

	node = ring.ResolveString("zzz")
	if node == nil || node.Hash != "abc" {
		t.Errorf("3) zzz should resolve to abc: %s", node)
	}

	node = ring.ResolveString("lmo")
	if node == nil || node.Hash != "abc" {
		t.Errorf("4) lmo should resolve to abc: %s", node)
	}

	ring.AddElement(hashring.NewElement("xyz", "xyz"))

	node = ring.ResolveString("lmo")
	if node == nil || node.Hash != "xyz" {
		t.Errorf("5) lmo should resolve to xyz: %s", node)
	}
}

func TestRemoveElement(t *testing.T) {
	ring := hashring.NewRing()

	ring.AddElement(hashring.NewElement("bcd", "bcd"))
	ring.AddElement(hashring.NewElement("abc", "abc"))
	ring.AddElement(hashring.NewElement("fgh", "fgh"))
	ring.AddElement(hashring.NewElement("def", "def"))
	ring.AddElement(hashring.NewElement("ijk", "ijk"))
	ring.AddElement(hashring.NewElement("jkl", "jkl"))
	ring.AddElement(hashring.NewElement("ghi", "ghi"))

	ring.RemoveElement(hashring.NewElement("fgh", "fgh"))
	node := ring.ResolveString("efg")
	if node == nil || node.Hash != "ghi" {
		t.Errorf("1) efg should resolve to fgh: %s", node)
	}

	ring.RemoveElement(hashring.NewElement("jkl", "jkl"))
	node = ring.ResolveString("jaa")
	if node == nil || node.Hash != "abc" {
		t.Errorf("2) jaa should resolve to abc: %s", node)
	}

	ring.RemoveElement(hashring.NewElement("abc", "abc"))
	node = ring.ResolveString("jaa")
	if node == nil || node.Hash != "bcd" {
		t.Errorf("3) jaa should resolve to bcd: %s", node)
	}

	node = ring.ResolveString("aaa")
	if node == nil || node.Hash != "bcd" {
		t.Errorf("4) aaa should resolve to bcd: %s", node)
	}
}
