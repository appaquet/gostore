// Author: Andre-Philippe Paquet
// Date: November 2010

package cluster

// Collection of nodes of the cluster
type Nodes struct {
	nodes map[uint16]*Node
}

func newNodes() *Nodes {
	n := new(Nodes)
	n.nodes = make(map[uint16]*Node)
	return n
}

// Returns a node of the cluster
func (n *Nodes) Get(index uint16) *Node {
	node, exists := n.nodes[index]
	if !exists {
		return nil
	}

	return node
}

// Set the node at the given position
func (n *Nodes) Add(node *Node) {
	n.nodes[node.Id] = node
}

// Remove all nodes
func (n *Nodes) Empty() {
	n.nodes = make(map[uint16]*Node)
}

// Returns the number of nodes currently in the cluster
func (n *Nodes) Count() uint16 {
	return uint16(len(n.nodes))
}

// Returns a channel that can be used to iterate over
// nodes of the cluster
func (n *Nodes) Iter() chan *Node {
	c := make(chan *Node)
	go func() {
		for _, v := range n.nodes {
			c <- v
		}
		close(c)
	}()
	return c
}

// Returns a slice of nodes
func (n *Nodes) Slice() []Node {
	ret := make([]Node, len(n.nodes))
	for i, node := range n.nodes {
		ret[i] = *node
	}
	return ret
}
