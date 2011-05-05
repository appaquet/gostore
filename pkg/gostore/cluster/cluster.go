// Author: Andre-Philippe Paquet
// Date: November 2010

// Cluster model package.
// This package stores nodes of the cluster and their ring
// membership.
package cluster

import (
	"gostore"
	"gostore/log"
)

const (
	MAX_NODE_ID = 65535
)


// TODO: replace all uint16 for token by a "Token" struct (from db/Segment)

// Cluster containing nodes, rings and nodes membership to 
// rings.
type Cluster struct {
	Config gostore.Config

	Nodes *Nodes
	Rings *Rings

	MyNode *Node

	Notifier *WatcherNotifier
}

// Returns a new cluster configured using the given configuration
func NewCluster(config gostore.Config) *Cluster {
	cls := new(Cluster)

	cls.Config = config
	cls.Nodes = newNodes()
	cls.Rings = newRingsConfig(config.Rings, config.GlobalRing)
	cls.Notifier = NewWatcherNotifier()

	return cls
}

// Set the local node so that services knows which is the current local node
// in the cluster. The local node is the node that is currently running in this
// instance.
func (c *Cluster) SetMyNode(node *Node) {
	c.MyNode = node
}

// Takes all nodes, get their membership to rings and add them to each rings.
func (c *Cluster) FillRings() {
	for node := range c.Nodes.Iter() {
		nbrings := uint8(len(node.Rings))

		if nbrings > 0 {
			var r uint8
			for r = 0; r < nbrings; r++ {
				nodering := node.Rings[r]
				ring := c.Rings.GetRing(nodering.Ring)
				if ring != nil {
					ring.AddNode(nodering.Token, node)
				}
			}
		}

		// add all nodes to the default ring
		c.Rings.GetGlobalRing().AddNode(node.Hash(), node)
	}
}


// Merge a node into the cluster, notifying any modification
func (c *Cluster) MergeNode(nNode *Node, notify bool) {
	oNode := c.Nodes.Get(nNode.Id)
	if oNode != nil {
		oNode.Address = nNode.Address
		oNode.Adhoc = nNode.Adhoc
		oNode.TcpPort = nNode.TcpPort
		oNode.UdpPort = nNode.UdpPort

		if oNode.Status != nNode.Status {
			switch oNode.Status {
			case Status_Offline:
				if nNode.Status == Status_Joining {
					oNode.Status = Status_Joining
					if notify {
						c.Notifier.NotifyNodeJoining(oNode)
					}
				} else {
					log.Fatal("Invalid node status transition")
				}

			case Status_Joining:
				if nNode.Status == Status_Online {
					oNode.Status = Status_Online
					if notify {
						c.Notifier.NotifyNodeOnline(oNode)
					}

				} else {
					log.Fatal("Invalid node status transition")
				}
			default:
				log.Fatal("Unsupported node status transition")
			}
		}

		// TODO: Rings

	} else {
		c.Nodes.Add(nNode)
		if notify {
			if nNode.Status == Status_Online {
				c.Notifier.NotifyNodeOnline(nNode)
			} else {
				c.Notifier.NotifyNodeJoining(nNode)
			}
		}
	}
}
