// Author: Andre-Philippe Paquet
// Date: November 2010

package cluster

import (
	"crypto/md5"
	"gostore/log"
	"fmt"
	"gostore/tools/hashring"
)

// Consistent hashing ring part of a cluster and having a 
// replication factor representing the number node returned
// when resolving a key
type Ring struct {
	id        uint8
	ring      *hashring.HashRing
	repFactor int
}

// Returns a new cluster ring
func NewRing(repFactor int) *Ring {cr := new(Ring)
	cr.ring = hashring.NewRing()
	     cr.repFactor = repFactor
	return cr
}

// Adds a node to the current ring
func (cr *Ring) AddNode(token string, node *Node) {
	cr.ring.AddElement(hashring.NewElement(token, node))
}

// Return string representatin of the ring
func (cr *Ring) String() string {
	nodes := ""
	first := cr.ring.FirstElement()
	if first != nil {
		nodes += fmt.Sprintf("%s=%s,", first.Hash, first.Value.(*Node).String())
		cur := first.Next()

		for cur != first {
			nodes += fmt.Sprintf("%s=%s,", cur.Hash, cur.Value.(*Node).String())
			cur = cur.Next()
		}
	}

	return fmt.Sprintf("R[%d,{%s}]", cr.id, nodes)
}

// Resolves nodes that are manager for a given key. The key will be
// hashed prior to resolve. Use ResolveHash to use a hash directly.
// The number of nodes returned depends of the replication factor of the 
// ring and the number of nodes in the ring.
//
// Ex: 	if only 1 node is in the ring and using a replicator factor
// 		of 2, only 1 node will be returned
func (cr *Ring) Resolve(key string) *ResolveResult {
	// hash the key
	md5hash := md5.New()
	md5hash.Write([]byte(key))
	sum := fmt.Sprintf("%x", md5hash.Sum())

	return cr.ResolveToken(sum)
}

// Resolves nodes that are manager for a given token. The number of
// nodes returned depends of the replication factor of the ring
// and the number of nodes in the ring.
//
// Ex: 	if only 1 node is in the ring and using a replicator factor
// 		of 2, only 1 node will be returned
func (cr *Ring) ResolveToken(token string) *ResolveResult {
	res := new(ResolveResult)
	res.token = token

	// resolve the key in the ring
	ringElem := cr.ring.ResolveString(token)
	if ringElem == nil {
		log.Fatal("Cluster: Got no element in ring %s for resolving of %s", cr, token)
	}

	// add the first node found
	firstNode := (ringElem.Value).(*Node)
	res.Add(firstNode)

	// iterate until we have the replication factor we want
	var curNode *Node
	for i := res.Count(); res.Count() < cr.repFactor && curNode != firstNode; i++ {
		nextElem := ringElem.Next()

		curNode = (nextElem.Value).(*Node)
		res.Add(curNode)

		ringElem = nextElem
	}

	return res
}
