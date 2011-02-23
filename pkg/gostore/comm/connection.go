package comm

import (
	"net"
	"fmt"
)

const (
	D_Outbound = iota
	D_Inbound

	P_TCP
	P_UDP
)

type Connection struct {
	gocon net.Conn
	pool  *Pool
	proto int // P_TCP or P_UDP

	direction int // D_OUTBOUN, D_INBOUND
}

func NewConnection(pool *Pool, proto int, direction int, conn net.Conn) *Connection {
	con := new(Connection)
	con.gocon = conn
	con.pool = pool
	con.proto = proto
	con.direction = direction
	return con
}

func (c *Connection) Release() {
	if c.proto == P_TCP {
		c.pool.Release(c)

	} else {
		if c.direction == D_Outbound {
			c.gocon.Close()
		}
	}
}

func (c *Connection) Close() {
	if c.direction == D_Outbound || c.direction == D_Inbound {
		c.Close()
	}
}

func (c *Connection) String() string {
	var proto string
	if c.proto == P_TCP {
		proto = "TCP"
	} else {
		proto = "UDP"
	}

	return fmt.Sprintf("Connection(#%d, %s)", -1, proto)
}
