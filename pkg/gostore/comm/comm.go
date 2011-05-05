// Cluster communication package
// All services register against this package and use it to communicate
// to the same service on other nodes.
package comm

import (
	"gostore/cluster"
	"net"
	"gostore/log"
	"io"
	"os"
	"sync"
	"bufio"
)

const (
	RESERVED_FUNCTIONS = 2 // Number of reserved functions (response and error)
	FUNC_RESPONSE      = 0
	FUNC_ERROR         = 1

	TRACKER_CLEAN_TIME = 5000 // 5 seconds
	TRACKER_LOOP_SLEEP = 100  // 100 ms
	MAX_MSG_SIZE       = 8000
)

type Comm struct {
	*Services

	// Running flag
	running bool

	// cluster instance
	Cluster *cluster.Cluster

	// Server listening for incoming messages
	server *Server

	// Connections pool
	pool *Pool

	// Message id and its mutex
	seqmutex *sync.Mutex
	seqid    uint16

	// Messages that need tracking
	messageTrackers map[string]*MessageTracker
	trackersMutex   *sync.Mutex
}

func NewComm(cluster *cluster.Cluster) *Comm {
	comm := new(Comm)

	comm.running = true
	comm.Services = newServices()

	mynode := cluster.MyNode
	comm.Cluster = cluster

	tcpaddr := net.TCPAddr{mynode.Address, int(mynode.TcpPort)}
	udpaddr := net.UDPAddr{mynode.Address, int(mynode.UdpPort)}
	comm.server = NewServer(comm, &tcpaddr, &udpaddr)

	comm.pool = NewPool()

	comm.seqmutex = new(sync.Mutex)
	comm.seqid = 1

	// message trackers
	comm.messageTrackers = make(map[string]*MessageTracker)
	comm.trackersMutex = new(sync.Mutex)
	go comm.startMessageTracker()

	return comm
}

func (comm *Comm) Pause() {
	comm.running = false
}

func (comm *Comm) Resume() {
	comm.running = true
}

func (comm *Comm) NewMsgMessage(serviceId byte) *Message {
	msg := newMessage(comm)
	msg.ServiceId = serviceId
	msg.Type = T_MSG
	return msg
}

func (comm *Comm) NewDataMessage(serviceId byte) *Message {
	msg := newMessage(comm)
	msg.ServiceId = serviceId
	msg.Type = T_DATA
	return msg
}

func (comm *Comm) NewMessage() *Message {
	return newMessage(comm)
}


func (comm *Comm) SendNode(node *cluster.Node, message *Message) {
	// resolve function and service names
	message.PrepareSend()

	if node.Equals(comm.Cluster.MyNode) {
		log.Debug("%d: Looping message (%s) locally\n", comm.Cluster.MyNode.Id, message)

		// if this message need a acknowledge, add it to the ack watcher
		if message.Timeout > 0 || message.OnError != nil || message.OnResponse != nil {
			comm.watchMessage(message, node)
		}

		message.SeekZero()
		comm.handleMessage(message)
		message.Release()

	} else {
		// watch message here, because getting a connection may timeout if node is
		// down and trying to open a TCP connection
		if message.Timeout > 0 || message.OnError != nil || message.OnResponse != nil {
			comm.watchMessage(message, node)
		}

		// TODO: Replace by a queue using channel so we don't create too many connections
		go func() {
			// We use TCP if message is more than 8000 bytes (maximum UDP packet size)
			var connection *Connection
			if message.Type == T_MSG && message.TotalSize() < MAX_MSG_SIZE {
				connection = comm.pool.GetMsgConnection(node)
			} else {
				connection = comm.pool.GetDataConnection(node)
			}

			if connection == nil {
				// Report as an error because the tracker should have reported the timeout if its a timeout
				if message.OnError != nil {
					message.OnError(message, os.NewError("Couldn't get TCP connection to node"))
				} else {
					log.Error("Couldn't get a connection for message %s to %s\n", message, node)
				}

				return
			}

			log.Debug("%d: Sending message (%s) to %s via %s\n", comm.Cluster.MyNode.Id, message, node, connection)

			bufwriter := bufio.NewWriter(io.Writer(connection.gocon))
			buffer := io.Writer(bufwriter)

			message.SeekZero()
			err := message.writeMessage(buffer)
			if err != nil {
				log.Error("%d: Got an error writing message %s to socket for %s via %s: %s\n", err, comm.Cluster.MyNode.Id, message, node, connection, err)
			}

			err = bufwriter.Flush()
			if err != nil {
				log.Error("%d: Got an error sending message %s to %s via %s: %s\n", comm.Cluster.MyNode.Id, message, node, connection, err)
				if message.OnError != nil {
					message.OnError(message, err)
				}
			}

			// release the message and connection
			connection.Release()
			message.Release()
		}()
	}
}

func (comm *Comm) SendFirst(res *cluster.ResolveResult, message *Message) {
	node := res.GetFirst()
	if node != nil {
		comm.SendNode(node, message)
		return
	}

	log.Fatal("Couldn't send message (%s) to first (%s). No node found.\n", message, node)
}

func (comm *Comm) SendOne(res *cluster.ResolveResult, message *Message) {
	// TODO: Round robin
	node := res.GetOnline(0)
	if node != nil {
		comm.SendNode(node, message)
		return
	}

	log.Fatal("Couldn't send the message (%s) to one. No node found.\n", message)
}

func (comm *Comm) RespondSource(initialMessage *Message, message *Message) {
	message.SetMiddleNode(comm.Cluster.MyNode)
	message.SetSourceNode(initialMessage.SourceNode())
	message.InitId = initialMessage.Id

	comm.SendNode(initialMessage.SourceNode(), message)
}

func (comm *Comm) RespondMiddle(initialMessage *Message, message *Message) {
	message.SetMiddleNode(comm.Cluster.MyNode)
	message.SetSourceNode(initialMessage.SourceNode())

	message.InitId = initialMessage.Id

	comm.SendNode(initialMessage.MiddleNode(), message)
}

func (comm *Comm) RespondError(initialMessage *Message, error os.Error) {
	initSrc := initialMessage.SourceNode()
	initMiddle := initialMessage.MiddleNode()

	errorMsg := comm.NewMsgMessage(initialMessage.ServiceId)
	errorMsg.FunctionId = FUNC_ERROR
	errorMsg.SetSourceNode(initSrc)
	errorMsg.SetMiddleNode(comm.Cluster.MyNode)
	errorMsg.InitId = initialMessage.Id
	WriteErrorPayload(errorMsg, error)

	if initMiddle != nil && !initMiddle.Equals(initSrc) {
		// respond error to middle 
		comm.SendNode(initialMessage.MiddleNode(), errorMsg)
	} else {
		// respond error to source
		comm.SendNode(initialMessage.SourceNode(), errorMsg)
	}
}

func (comm *Comm) RedirectNode(node *cluster.Node, message *Message) {
	message.SetMiddleNode(comm.Cluster.MyNode)
	comm.SendNode(node, message)
}

func (comm *Comm) RedirectFirst(res *cluster.ResolveResult, message *Message) {
	message.SetMiddleNode(comm.Cluster.MyNode)
	comm.SendFirst(res, message)
}

func (comm *Comm) RedirectOne(res *cluster.ResolveResult, message *Message) {
	message.SetMiddleNode(comm.Cluster.MyNode)
	comm.SendOne(res, message)
}

func (comm *Comm) handleMessage(message *Message) {
	// TODO: We should make sure we don't handle a message twice (since UDP can duplicate packets)

	if message.FunctionId == FUNC_ERROR {
		log.Info("Comm: Received an error: %s\n", message)
	}

	// Check if the message needed an acknowledgement or check if
	// it has an error callback
	handled := false
	if message.SourceNode().Equals(comm.Cluster.MyNode) {
		handled = comm.handleTracker(message)
	}

	// Service #0 is net and there is no implementation yet.
	// Function = RESPONSE should be handled by callback
	if message.ServiceId != 0 && message.FunctionId != FUNC_RESPONSE && !handled {
		serviceWrapper := comm.GetWrapper(message.ServiceId)

		if serviceWrapper.service != nil {
			// if its an error
			if message.FunctionId == FUNC_ERROR {
				err := ReadErrorPayload(message)
				message.SeekZero()
				serviceWrapper.service.HandleUnmanagedError(message, err)
			} else {
				// call the right function
				handled := serviceWrapper.callFunction(message.FunctionId, message)

				if !handled {
					serviceWrapper.service.HandleUnmanagedMessage(message)
				}
			}
		} else {
			log.Error("Comm: Couldn't find service for message %s\n", message)
		}
	}
}

func (comm *Comm) nextSequenceId() uint16 {
	comm.seqmutex.Lock()
	var seq uint16 = comm.seqid
	comm.seqid += 1
	comm.seqmutex.Unlock()
	return seq
}
