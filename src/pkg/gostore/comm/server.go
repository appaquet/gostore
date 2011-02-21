package comm

import (
	"net"
	"os"
	"gostore/log"
	"io"
	"bytes"
)

type Server struct {
	tcpaddr *net.TCPAddr
	udpaddr *net.UDPAddr

	tcpsock *net.TCPListener
	udpsock *net.UDPConn

	comm *Comm
}

func NewServer(comm *Comm, tcpaddr *net.TCPAddr, udpaddr *net.UDPAddr) *Server {
	s := new(Server)

	s.comm = comm
	s.tcpaddr = tcpaddr
	s.udpaddr = udpaddr

	s.start()

	return s
}

func (s *Server) start() {
	var err os.Error

	log.Debug("ServiceServer: starting listening tcp socket on %s\n", s.tcpaddr)
	s.tcpsock, err = net.ListenTCP("tcp", s.tcpaddr)
	if err != nil {
		log.Fatal("Couldn't create TCP server listener: %s\n", err)
	}

	go s.acceptTCP()

	log.Debug("ServiceServer: starting listening udp socket on %s\n", s.udpaddr)
	s.udpsock, err = net.ListenUDP("udp", s.udpaddr)
	if err != nil {
		log.Fatal("Couldn't create UDP server listener: %s\n", err)
	}

	go s.acceptUDP()
}

func (s *Server) acceptTCP() {
	for {
		conn, err := s.tcpsock.Accept()
		if s.comm.running {
			if err != nil {
				log.Error("Couldn't accept TCP connexion: %s\n", err)
			}

			go s.handleTCPConnection(conn)
		} else {
			log.Info("Dropping connection because communications have been paused")
		}
	}
}

func (s *Server) handleTCPConnection(conn net.Conn) {
	connection := NewConnection(s.comm.pool, P_TCP, D_Inbound, conn)

	reader := io.Reader(conn)
	msg := s.comm.NewMessage()
	msg.connection = connection
	err := msg.readMessage(reader)

	if err != nil {
		log.Error("Couldn't handle message received from TCP because of errors: %s %s\n", msg, err)
		conn.Close() // Close the connection to make sure we don't cause error
	} else {
		s.comm.handleMessage(msg)
	}
}

func (s *Server) acceptUDP() {
	// Looping for new messages
	for {
		buf := make([]byte, MAX_MSG_SIZE)
		n, adr, err := s.udpsock.ReadFrom(buf)

		if s.comm.running {
			if err != nil {
				log.Error("Error while reading UDP (read %d) from %s: %s\n", n, adr, err)

			} else {
				abcon := net.Conn(s.udpsock)
				connection := NewConnection(s.comm.pool, P_UDP, D_Inbound, abcon)
				read := io.Reader(bytes.NewBuffer(buf))
				msg := s.comm.NewMessage()
				msg.connection = connection
				err := msg.readMessage(read)

				if err != nil {
					log.Error("Couldn't handle message received from UDP because of errors: %s %s\n", msg, err)
				} else {
					go s.comm.handleMessage(msg)
				}
			}
		} else {
			log.Info("Dropping connection because communications have been paused")
		}
	}
}
