package cls

import (
	"gostore/comm"
	"gostore/log"
	"gostore/cluster"
	"gostore/tools/commitlog"
	"gostore"
	"os"
	"fmt"
	"sync"
)

const (
	master_token = "00000000000000000000000000000000"

	state_booting = iota
	state_joining
	state_online
	state_offline
)

var ()

type ClusterService struct {
	comm      *comm.Comm
	serviceId byte

	cluster      *cluster.Cluster
	clusterMutex *sync.Mutex

	// peristence
	commitlog      *commitlog.CommitLog
	clusterVersion int64
	diskVerson     int64

	dataDir     string
	clsDataPath string
	masterRing  byte

	state byte

	sconfig gostore.ConfigService
	config  gostore.Config
}

func NewClusterService(comm *comm.Comm, config gostore.Config, sconfig gostore.ConfigService) *ClusterService {
	cs := new(ClusterService)

	cs.sconfig = sconfig
	cs.config = config
	cs.comm = comm
	cs.cluster = comm.Cluster
	cs.clusterMutex = new(sync.Mutex)
	cs.serviceId = sconfig.Id

	cs.state = state_booting

	if path, ok := sconfig.CustomConfig["DataDir"].(string); ok {
		cs.dataDir = path
	} else {
		log.Fatal("CS: Config 'DataDir' must be specified")
	}
	cs.clsDataPath = fmt.Sprintf("%s/cluster.db", cs.dataDir)

	if masteRing, ok := sconfig.CustomConfig["MasterRing"].(float64); ok {
		cs.masterRing = byte(masteRing)
	} else {
		log.Fatal("CS: Config 'MasterRing' must be specified")
	}

	// peristence
	cs.commitlog = commitlog.New(cs.dataDir)
	cs.commitlog.RegisterMutation(&clusterMutation{cs, []*cluster.Node{}, 0, false})

	return cs
}

func (cs *ClusterService) HandleUnmanagedMessage(msg *comm.Message) {
	log.Error("CS: Got an unmanaged message: %s", msg)
}

func (cs *ClusterService) HandleUnmanagedError(errorMessage *comm.Message, error os.Error) {
	log.Error("CS: Got an unmanaged error: %s", errorMessage)
}

func (cs *ClusterService) Boot() {
	myNode := cs.cluster.MyNode
	log.Debug("%d: Booting cluster service", myNode.Id)

	cs.loadCluster()

	// Contact master or listen for incoming requests if I'm master
	masters := cs.cluster.Rings.GetRing(cs.masterRing).ResolveToken(master_token)
	if masters.IsFirst(myNode) {
		// TODO: We should contact secondary master first!
		myNode.Status = cluster.Status_Online
		cs.state = state_online
		myNode.Adhoc = false

	} else {
		myNode.Status = cluster.Status_Offline
		cs.state = state_offline

		// switch to adhoc, we are not yet in the cluster
		myNode.Adhoc = true

		// contact master
		cs.ContactMaster()
	}
}

func (cs *ClusterService) Stop() {
	cs.state = state_offline
	cs.saveCluster()
}


func (cs *ClusterService) ContactMaster() {
	myNode := cs.cluster.MyNode
	log.Debug("%d: Contacting master...", myNode.Id)

	msg := cs.comm.NewMsgMessage(cs.serviceId)
	msg.Function = "RemoteContactMaster"

	err := cs.cluster.MyNode.Serialize(msg.Message)
	if err != nil {
		log.Fatal("Couldn't marshal my node: %s", err)
	}

	msg.OnResponse = func(msg *comm.Message) {
		// TODO: Merge the cluster we got into our cluster
		myNode.Status = cluster.Status_Online
	}

	// send our node to master
	masters := cs.cluster.Rings.GetRing(cs.masterRing).Resolve(master_token)
	cs.comm.SendFirst(masters, msg)
}

func (cs *ClusterService) RemoteContactMaster(msg *comm.Message) {
	myNode := cs.cluster.MyNode
	log.Debug("%d: Got a ContactMaster request: %s", myNode.Id, msg)

	if cs.state == state_online {
		masters := cs.cluster.Rings.GetRing(cs.masterRing).ResolveToken(master_token)

		// make sure I'm the master, and online
		if myNode.Status == cluster.Status_Online && masters.IsFirst(myNode) {
			node := cluster.NewEmptyNode()
			err := node.Unserialize(msg.Message)
			if err != nil {
				cs.comm.RespondError(msg, os.NewError("Couldn't unmarshal node data"))
				log.Error("Couldn't unmarshal node data: %s", err)
				return
			}

			node.Status = cluster.Status_Online
			cs.cluster.MergeNode(node, true)

			// TODO: Send the cluster back to the node
			resp := cs.comm.NewMsgMessage(cs.serviceId)
			cs.comm.RespondSource(msg, resp)
			//log.Fatal("BLABLA", node.Id)

			// TODO: LOCK SO THAT WE DON'T MAKE IT ONLINE TWICE

			// TODO: Accept the node
			// TODO: Check its rings
			// TODO: Broadcast the change
		} else {
			cs.comm.RedirectFirst(masters, msg)
		}
	}
}
