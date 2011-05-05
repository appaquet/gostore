package comm

import (
	"gostore/cluster"
	"net"
	"sync"
	"gostore/log"
)

type Pool struct {
	mutex *sync.Mutex

	connections map[int]*Connection
}

func NewPool() *Pool {
	pool := new(Pool)
	pool.mutex = new(sync.Mutex)
	pool.connections = make(map[int]*Connection)

	// TODO: Start connection manager

	return pool
}

func (p *Pool) GetDataConnection(node *cluster.Node) *Connection {
	// TODO: IMPLEMENT THE POOLING

	adr := net.TCPAddr{node.Address, int(node.TcpPort)}
	con, err := net.DialTCP("tcp", nil, &adr) // TODO: should use local address instead of nil (implicitly local)
	if err != nil {
		log.Error("NETPOOL: Couldn't create a connection\n", err)
		return nil
	}

	abcon := net.Conn(con)
	connection := NewConnection(p, P_TCP, D_Outbound, abcon)
	return connection
}

func (p *Pool) GetMsgConnection(node *cluster.Node) *Connection {
	// TODO: IMPLEMENT THE POOLING

	adr := net.UDPAddr{node.Address, int(node.UdpPort)}
	con, err := net.DialUDP("udp", nil, &adr) // TODO: should use local address instead of nil (implicitly local)
	if err != nil {
		log.Error("NETPOOL: Couldn't create a connection\n", err)
		return nil
	}

	abcon := net.Conn(con)
	connection := NewConnection(p, P_UDP, D_Outbound, abcon)
	return connection
}

func (p *Pool) Release(connection *Connection) {
	// TODO: IMPLEMENT THE POOLING!!
	// TODO: Put the connection in read mode if its inbound
	connection.gocon.Close()
}

func (p *Pool) CloseAll() {
	// TODO: Implement it
}
