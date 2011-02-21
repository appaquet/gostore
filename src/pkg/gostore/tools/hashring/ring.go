// Author: Andre-Philippe Paquet
// Date: November 2010

package hashring

import (
	"container/list"
)

type HashRing struct {
	list *list.List
}

func NewRing() *HashRing {
	ring := new(HashRing)
	ring.list = list.New()
	return ring
}


// TODO: Rename hash by hash
func (r *HashRing) resolveElement(hash string) *list.Element {
	curelem := r.list.Front()
	if curelem == nil {
		return curelem
	}

	curringelem := (curelem.Value).(*Element)
	if hash <= curringelem.Hash {
		return curelem
	}

	if hash > r.list.Back().Value.(*Element).Hash {
		return nil
	}

	for curelem != nil && hash > curringelem.Hash {
		nextelem := curelem.Next()

		if nextelem == nil || hash <= (nextelem.Value).(*Element).Hash {
			return nextelem
		} else {
			curelem = nextelem
			curringelem = (nextelem.Value).(*Element)
		}
	}

	return nil
}

func (r *HashRing) AddElement(ringelem *Element) {
	var elem *list.Element
	elemhash := ringelem.Hash

	nextelem := r.resolveElement(elemhash)
	if nextelem == nil {
		if r.list.Front() != nil {
			elem = r.list.PushBack(ringelem)
		} else {
			elem = r.list.PushFront(ringelem)
		}
	} else {
		elem = r.list.InsertBefore(ringelem, nextelem)
	}

	ringelem.elem = elem
	ringelem.ring = r
}

func (r *HashRing) FirstElement() *Element {
	front := r.list.Front()
	if front != nil {
		return front.Value.(*Element)
	}
	
	return nil
}

func (r *HashRing) RemoveElement(ringelem *Element) {
	hash := ringelem.Hash
	nextelem := r.resolveElement(hash)

	if nextelem.Value.(*Element).Hash == hash {
		r.list.Remove(nextelem)
	}
}


func (r *HashRing) ResolveString(hash string) *Element {
	nextelem := r.resolveElement(hash)

	if nextelem == nil {
		if r.list.Front() != nil {
			return r.list.Front().Value.(*Element)
		} else {
			return nil
		}
	}

	return nextelem.Value.(*Element)
}


type Element struct {
	Hash  string
	Value interface{}
	elem  *list.Element
	ring  *HashRing
}

func NewElement(hash string, value interface{}) *Element {
	re := new(Element)
	re.Value = value
	re.Hash = hash
	return re
}

func (re *Element) Next() *Element {
	next := re.elem.Next()
	if next != nil {
		return next.Value.(*Element)
	}

	return re.ring.list.Front().Value.(*Element)
}
