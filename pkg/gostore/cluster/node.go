// Author: Andre-Philippe Paquet
// Date: November 2010

package cluster

import (
	"fmt"
	"net"
	"os"
	"crypto/md5"
	"gostore/tools/typedio"
)

const (
	// Node statuses
	Status_Joining = iota
	Status_Connecting
	Status_Online
	Status_Disconnceting
	Status_Offline
	Status_Leaving
)


// Represents a node of a cluster
type Node struct {
	Id    uint16
	Adhoc bool

	Address net.IP
	TcpPort uint16
	UdpPort uint16
	Status  byte

	// Rings in which the node is member
	Rings []NodeRing

	hash     string
	cluster  *Cluster
}

// Node's rings structure
type NodeRing struct {
	Token string
	Ring  uint8
}

// Returns a new node
func NewNode(id uint16, address net.IP, tcpport uint16, udpport uint16) *Node {
	node := new(Node)
	node.Adhoc = false
	node.Id = id
	node.Address = address
	node.TcpPort = tcpport
	node.UdpPort = udpport

	node.Rings = make([]NodeRing, 0)

	return node
}

func NewAdhocNode(address net.IP, tcpport uint16, udpport uint16) *Node {
	node := new(Node)
	node.Adhoc = true
	node.Address = address
	node.TcpPort = tcpport
	node.UdpPort = udpport

	node.Rings = make([]NodeRing, 0)

	return node
}

func NewEmptyNode() *Node {
	node := new(Node)
	node.Rings = make([]NodeRing, 0)
	return node
}

// Returns a string representation of the node
func (n *Node) String() string {
	if n == nil {
		return "nil"
	}

	if n.Adhoc {
		return fmt.Sprintf("N[?,%s,%d,%d]", n.Address, n.TcpPort, n.UdpPort)
	}

	return fmt.Sprintf("N[%d/%s]", n.Id, StatusToString(n.Status))
}

// Returns (and lazy generate) node token
func (n *Node) Hash() string {
	if n.hash == "" {
		hash := md5.New()
		hash.Write([]byte(n.String()))
		n.hash = fmt.Sprintf("%x", hash.Sum())
	}

	return n.hash
}

func (n *Node) ChangeTo(node *Node) {
	node.Address = n.Address
	node.TcpPort = n.TcpPort
	node.UdpPort = n.UdpPort

	if node.Status != n.Status {
		node.Status = n.Status
		// TODO: Call the watcher
	}
}

// Compare to another node and return true in case of equality
func (n *Node) Equals(node *Node) bool {
	if node == nil || n == nil {
		return false
	}

	if n.Adhoc || node.Adhoc {
		if node.Address.String() == n.Address.String() && node.TcpPort == n.TcpPort && node.UdpPort == n.UdpPort {
			return true
		}
	} else {
		if node.Id == n.Id {
			return true
		}
	}

	return false
}

// Add a ring in which the node is member
func (n *Node) AddRing(ringId byte, token string) {
	nr := NodeRing{token, ringId}

	n.Rings = append(n.Rings, nr)
}

// Returns a string representation of the node status
func StatusToString(status byte) string {
	switch status {
	case Status_Online:
		return "U"
	case Status_Offline:
		return "D"
	case Status_Joining:
		return "J"
	}

	return "?"
}


func (n *Node) Serialize(writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint16(n.Id)				// id
	if err != nil {
		return
	}

	err = writer.WriteUint8(n.Status)			// status
	if err != nil {
		return
	}

	err = writer.WriteString(n.Address.String())		// address
	if err != nil {
		return
	}

	err = writer.WriteUint16(n.TcpPort)			// tcp port
	if err != nil {
		return
	}

	err = writer.WriteUint16(n.UdpPort)			// udp port
	if err != nil {
		return
	}

	err = writer.WriteUint8(uint8(len(n.Rings)))		// nb rings
	if err != nil {
		return
	}

	for _, ring := range n.Rings {				// each ring 
		err = writer.WriteUint8(ring.Ring)
		if err != nil {
			return
		}

		err = writer.WriteString(ring.Token)
		if err != nil {
			return
		}
	}

	return nil
}


func (n *Node) Unserialize(reader typedio.Reader) (err os.Error) {
	n.Id, err = reader.ReadUint16()				// id
	if err != nil {
		return err
	}

	n.Status, err = reader.ReadUint8()			// status
	if err != nil {
		return err
	}

	strAddr, err := reader.ReadString()			// address
	if err != nil {
		return err
	}
	n.Address = net.ParseIP(strAddr)

	n.TcpPort, err = reader.ReadUint16()			// tcp port
	if err != nil {
		return err
	}

	n.UdpPort, err = reader.ReadUint16()			// udp port
	if err != nil {
		return err
	}


	nbRings, err := reader.ReadUint8()			// nb rings
	if err != nil {
		return err
	}

	n.Rings = make([]NodeRing, nbRings)
	var i uint8
	for i=0; i<nbRings; i++ {				// each ring
		nodeRing := NodeRing{}

		nodeRing.Ring, err = reader.ReadUint8()
		if err != nil {
			return err
		}

		nodeRing.Token, err = reader.ReadString()
		if err != nil {
			return err
		}
	}

	return nil
}


