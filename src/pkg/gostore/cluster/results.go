// Author: Andre-Philippe Paquet
// Date: November 2010

package cluster

import (
	"container/vector"
)

// Nodes resolving collection
type ResolveResult struct {
	token       string
	nodes       vector.Vector
	onlineCount int
}

// Add a node into results
func (r *ResolveResult) Add(node *Node) bool {
	// check if its there first
	for _, aNode := range r.nodes {
		if node.Id == (aNode.(*Node)).Id {
			return false
		}
	}

	r.nodes.Push(node)

	return true
}

func (r *ResolveResult) Count() int { return len(r.nodes) }

func (r *ResolveResult) Get(index int) *Node { return r.nodes.At(index).(*Node) }

func (r *ResolveResult) GetOnline(index int) *Node {
	cur := 0
	for i := 0; i < r.Count(); i++ {
		node := r.Get(i)
		if node.Status == Status_Online {
			if cur == index {
				return r.Get(i)
			} else {
				cur++
			}
		}
	}

	return nil
}

func (r *ResolveResult) Iter() chan *Node {
	c := make(chan *Node)
	go func() {
		for _, node := range r.nodes {
			c <- node.(*Node)
		}
		close(c)
	}()
	return c
}

func (r *ResolveResult) IterOnline() chan *Node {
	c := make(chan *Node)
	go func() {
		for _, vNode := range r.nodes {
			node := vNode.(*Node)
			if node.Status == Status_Online {
				c <- node
			}
		}
		close(c)
	}()
	return c
}

func (r *ResolveResult) InOnlineNodes(node *Node) bool {
	for i := 0; i < r.Count(); i++ {
		cur := r.Get(i)
		if cur.Status == Status_Online && cur.Equals(node) {
			return true
		}
	}

	return false
}

func (r *ResolveResult) GetFirst() *Node {
	if r.Count() > 0 {
		return r.Get(0)
	}

	return nil
}

func (r *ResolveResult) IsFirst(node *Node) bool {
	if r.Count() > 0 && r.Get(0).Equals(node) {
		return true
	}

	return false
}
