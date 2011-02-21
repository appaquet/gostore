package comm

import (
	"io"
	"gostore/cluster"
	"gostore/log"
	"gostore/tools/buffer"
	"gostore/tools/typedio"
	"os"
	"net"
	"fmt"
)

/**************************************************************************************
 * Message packet structure
 * ------------------------------------------------------------------------------------
 * | Id (4) | Flags (see below) (1) | [Initial Id (4)] | Service Id (1) |  MsgSize (4) | 
 * ------------------------------------------------------------------------------------
 *  [DataSize(8)] | SrcNodeInfo (var) | [MiddleNodeInfo (var)] | Function Id (1) |
 * ------------------------------------------------------------------------------------ 
 *  Msg + [Data]
 * ------------------------------------------------------------------------------------
 *
 * Flags:
 *  0x01 - Initial message Id present
 *	0x02 - Is Data (0 = Node DataSize field nor data)
 *	0x04 - Source node is adhoc (IP+UDPPort+TCPPort in header instead of SrcNode field)
 *	0x08 - Has Middle Node
 *	0x10 - Middle node is adhoc (IP+UDPPort+TCPPort in header instead of MiddleNode field)
 */

const (
	T_MSG  = 1
	T_DATA = 2

	prm_has_init_msg_id   = 0x01
	prm_is_data           = 0x02
	prm_src_node_adhoc    = 0x04
	prm_has_middle_node   = 0x08
	prm_middle_node_adhoc = 0x10
)

type Message struct {
	Id     uint16
	InitId uint16

	srcNodeAdhoc   bool
	srcNodeId      uint16
	srcNodeAdr     net.IP
	srcNodeUdpPort uint16
	srcNodeTcpPort uint16

	middleNodePresent bool
	middleNodeAdhoc   bool
	middleNodeId      uint16
	middleNodeAdr     net.IP
	middleNodeUdpPort uint16
	middleNodeTcpPort uint16

	Type       byte // 1=Message or 2=Data
	ServiceId  byte
	FunctionId byte
	Function   string // helper for services

	// message buffer
	Message *buffer.Buffer

	// data
	Data          io.Reader
	DataSize      int64
	DataAutoClose bool

	// associated inbound connection (for releasing)
	connection *Connection

	// tracking handling
	Timeout    int
	Retries    int
	RetryDelay int

	OnTimeout          func(last bool) (retry bool, handled bool)
	LastTimeoutAsError bool
	OnResponse         func(message *Message)
	OnError            func(message *Message, error os.Error)
	Wait               chan bool // used by sender to synchronize send, receive

	comm *Comm
}

func newMessage(comm *Comm) *Message {
	r := new(Message)
	r.comm = comm
	r.Id = comm.nextSequenceId()
	r.SetSourceNode(r.comm.Cluster.MyNode)
	r.middleNodePresent = false
	r.Message = buffer.New()
	r.Wait = make(chan bool, 1)

	return r
}

func (r *Message) Hash() string { return fmt.Sprintf("%d%d", r.Id, r.SourceNode().Hash()) }

func (r *Message) InitHash() string { return fmt.Sprintf("%d%d", r.InitId, r.SourceNode().Hash()) }

func (r *Message) TotalSize() uint64 {
	return uint64(r.Message.Size) + uint64(r.DataSize) + 36
}

func (r *Message) PrepareSend() {
	// Resolve function
	if r.FunctionId == 0 && r.Function != "" {
		r.FunctionId = r.comm.wrappers[r.ServiceId].functionName2Id(r.Function)
	}
}

func (r *Message) readMessage(reader io.Reader) (err os.Error) {
	treader := typedio.NewReader(reader)

	r.Id, err = treader.ReadUint16() // message id
	if err != nil {
		return
	}

	flags, err := treader.ReadUint8() // flags
	if err != nil {
		return
	}

	hasInitId := false
	if flags&prm_has_init_msg_id == prm_has_init_msg_id {
		hasInitId = true
	}

	if flags&prm_is_data == prm_is_data {
		r.Type = T_DATA
	} else {
		r.Type = T_MSG
	}

	if flags&prm_src_node_adhoc == prm_src_node_adhoc {
		r.srcNodeAdhoc = true
	}

	if flags&prm_has_middle_node == prm_has_middle_node {
		r.middleNodePresent = true
	}

	if flags&prm_middle_node_adhoc == prm_middle_node_adhoc {
		r.middleNodeAdhoc = true
	}

	if hasInitId {
		r.InitId, err = treader.ReadUint16() // initial message id
		if err != nil {
			return
		}
	}

	r.ServiceId, err = treader.ReadUint8() // service id
	if err != nil {
		return
	}

	msgSize, err := treader.ReadUint16() // message size
	if err != nil {
		return
	}

	if r.Type == T_DATA {
		r.DataSize, err = treader.ReadInt64() // data size
		if err != nil {
			return
		}
	}

	if r.srcNodeAdhoc { // source node information
		adr, err := treader.ReadString() // addr
		if err != nil {
			return
		}
		r.srcNodeAdr = net.ParseIP(adr)

		r.srcNodeTcpPort, err = treader.ReadUint16() // tcp port
		if err != nil {
			return
		}

		r.srcNodeUdpPort, err = treader.ReadUint16() // udp port
		if err != nil {
			return
		}
	} else {
		r.srcNodeId, err = treader.ReadUint16() // node id
		if err != nil {
			return
		}
	}

	// TODO: Adhoc
	if r.middleNodePresent {
		if r.middleNodeAdhoc {
			adr, err := treader.ReadString() // addr
			if err != nil {
				return
			}
			r.middleNodeAdr = net.ParseIP(adr)

			r.middleNodeTcpPort, err = treader.ReadUint16() // tcp port
			if err != nil {
				return
			}

			r.middleNodeUdpPort, err = treader.ReadUint16() // udp port
			if err != nil {
				return
			}
		} else {
			r.middleNodeId, err = treader.ReadUint16() // node id
			if err != nil {
				return
			}
		}
	}

	r.FunctionId, err = treader.ReadUint8() // function id
	if err != nil {
		return
	}

	// Load message
	r.Message = buffer.NewWithSize(int64(msgSize), false) // message
	n, err := io.Copyn(r.Message, reader, int64(msgSize))
	r.Message.Seek(0, 0)

	if err != nil {
		log.Error("COMM: Got an error reading message from message: %s", err)
		return err
	}

	if n != int64(msgSize) {
		log.Error("COMM: Couldn't read the whole message. Read %d out of %d", n, msgSize)
		return os.NewError("Message truncated")
	}

	// release the connection if its a message
	if r.Type == T_MSG {
		r.Release()
	} else {
		r.Data = reader
	}

	return nil
}

func (r *Message) writeMessage(writer io.Writer) (err os.Error) {
	twriter := typedio.NewWriter(writer)

	err = twriter.WriteUint16(r.Id) // message id
	if err != nil {
		return
	}

	// prepare flags
	var flags byte
	if r.InitId != 0 {
		flags = flags | prm_has_init_msg_id
	}

	if r.Type == T_DATA {
		flags = flags | prm_is_data
	}

	if r.srcNodeAdhoc {
		flags = flags | prm_src_node_adhoc
	}

	if r.middleNodePresent {
		flags = flags | prm_has_middle_node
	}

	if r.middleNodeAdhoc {
		flags = flags | prm_middle_node_adhoc
	}

	err = twriter.WriteUint8(flags) // flags
	if err != nil {
		return
	}

	if r.InitId != 0 {
		err = twriter.WriteUint16(r.InitId) // initial message id
		if err != nil {
			return
		}
	}

	err = twriter.WriteUint8(r.ServiceId) // service id
	if err != nil {
		return
	}

	msgSize := uint16(r.Message.Size)
	err = twriter.WriteUint16(msgSize) // message size
	if err != nil {
		return
	}

	if r.Type == T_DATA {
		twriter.WriteInt64(r.DataSize) // data size
		if err != nil {
			return
		}
	}

	if r.srcNodeAdhoc { // source node information
		err = twriter.WriteString(r.srcNodeAdr.String()) // addr
		if err != nil {
			return
		}

		err = twriter.WriteUint16(r.srcNodeTcpPort) // tcp port
		if err != nil {
			return
		}

		err = twriter.WriteUint16(r.srcNodeUdpPort) // udp port
		if err != nil {
			return
		}

	} else {
		err = twriter.WriteUint16(r.srcNodeId) // node id
		if err != nil {
			return
		}
	}

	if r.middleNodePresent { // middle node information
		if r.middleNodeAdhoc {
			err = twriter.WriteString(r.middleNodeAdr.String()) // addr
			if err != nil {
				return
			}

			err = twriter.WriteUint16(r.middleNodeTcpPort) // tcp port
			if err != nil {
				return
			}

			err = twriter.WriteUint16(r.middleNodeUdpPort) // udp port
			if err != nil {
				return
			}

		} else {
			err = twriter.WriteUint16(r.middleNodeId) // node id
			if err != nil {
				return
			}
		}
	}

	err = twriter.WriteUint8(r.FunctionId) // function id
	if err != nil {
		return
	}

	// Write message
	r.Message.Seek(0, 0)
	w, err := io.Copyn(writer, r.Message, r.Message.Size) // message

	if err != nil {
		log.Error("Couldn't write message message to writer: %s\n", err)
		return err
	}

	if w != int64(msgSize) {
		log.Error("Couldn't write the whole message message to write: written %d out of %d\n", w, msgSize)
		return os.NewError("Message write truncated")
	}

	// Write data
	if r.Type == T_DATA {
		io.Copyn(writer, r.Data, r.DataSize) // data
	}

	return nil
}

func (r *Message) SourceNode() *cluster.Node {
	if r.srcNodeAdhoc {
		return cluster.NewAdhocNode(r.srcNodeAdr, r.srcNodeTcpPort, r.srcNodeUdpPort)
	}

	return r.comm.Cluster.Nodes.Get(r.srcNodeId)
}

func (r *Message) SetSourceNode(node *cluster.Node) {
	if node.Adhoc {
		r.srcNodeAdhoc = true
		r.srcNodeAdr = node.Address
		r.srcNodeTcpPort = node.TcpPort
		r.srcNodeUdpPort = node.UdpPort
	} else {
		r.srcNodeAdhoc = false
		r.srcNodeId = node.Id
	}
}

func (r *Message) SetMiddleNode(node *cluster.Node) {
	if node == nil {
		r.middleNodePresent = false

	} else {
		r.middleNodePresent = true
		if node.Adhoc {
			r.middleNodePresent = true
			r.middleNodeAdhoc = true
			r.middleNodeAdr = node.Address
			r.middleNodeTcpPort = node.TcpPort
			r.middleNodeUdpPort = node.UdpPort
		} else {
			r.middleNodeAdhoc = false
			r.middleNodeId = node.Id
		}
	}
}

func (r *Message) MiddleNode() *cluster.Node {
	if r.middleNodePresent {
		if r.middleNodeAdhoc {
			return cluster.NewAdhocNode(r.middleNodeAdr, r.middleNodeTcpPort, r.middleNodeUdpPort)
		}

		return r.comm.Cluster.Nodes.Get(r.middleNodeId)
	}

	return nil
}


func (r *Message) String() string {
	var typ string
	if r.Type == T_MSG {
		typ = "M"
	} else {
		typ = "D"
	}

	return fmt.Sprintf("M[ID=%d,T=%s,IID=%d,SNOD=%s,MNOD=%s,SRV=%d,FNC=%d,MSZ=%d,DSZ=%d]", r.Id, typ, r.InitId, r.SourceNode(), r.MiddleNode(), r.ServiceId, r.FunctionId, r.Message.Size, r.DataSize)
}

func (r *Message) SeekZero() {
	r.Message.Seek(0, 0)

	if seekable, seek := r.DataIsSeekable(); seekable {
		seek.Seek(0, 0)
	}
}

func (r *Message) DataIsSeekable() (bool, io.ReadSeeker) {
	if j, ok := (r.Data).(io.ReadSeeker); ok {
		return true, j
	}

	return false, nil
}

func (r *Message) Release() {
	if r.connection != nil {
		r.connection.Release()
		r.connection = nil
	}

	if r.DataAutoClose && r.Data != nil {
		if j, ok := (r.Data).(io.Closer); ok {
			j.Close()
		}
	}
}
