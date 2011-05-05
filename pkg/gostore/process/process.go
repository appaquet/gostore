package process

import (
	"gostore"
	"net"
	"gostore/cluster"
	"gostore/comm"
	"gostore/services/fs"
	"gostore/services/cls"
	"gostore/log"
)

type Process struct {
	Config  gostore.Config
	Cluster *cluster.Cluster
	Sc      *comm.Comm

	Fss *fs.FsService
	Cls *cls.ClusterService
}

func NewProcess(config gostore.Config) *Process {
	proc := new(Process)
	proc.Config = config

	// Create the cluster instance
	proc.Cluster = cluster.NewCluster(proc.Config)
	nodes := proc.Cluster.Nodes

	// Validate rings
	if len(config.Rings) == 0 {
		log.Fatal("CONFIG: At least one ring must be configured (at least the global)")
	}

	// Generate active nodes
	for _, confnode := range config.Nodes {
		acnode := cluster.NewNode(confnode.NodeId, net.ParseIP(confnode.NodeIP), confnode.TCPPort, confnode.UDPPort)

		for _, confring := range confnode.Rings {
			acnode.AddRing(confring.RingId, confring.Token)
		}

		nodes.Add(acnode)
	}

	// My node
	mynode := nodes.Get(config.CurrentNode)
	proc.Cluster.SetMyNode(mynode)

	// Fill all rings
	proc.Cluster.FillRings()

	// Create services server
	proc.Sc = comm.NewComm(proc.Cluster)

	var oneCls bool
	for _, sconfig := range config.Services {
		switch sconfig.Type {
		case "fs":
			log.Debug("Server: creating file system service\n")
			proc.Fss = fs.NewFsService(proc.Sc, &sconfig)
			proc.Sc.AddService(comm.Service(proc.Fss), sconfig)
			break
		case "cls":
			log.Debug("Server: creating cluster service\n")
			proc.Cls = cls.NewClusterService(proc.Sc, config, sconfig)
			proc.Sc.AddService(comm.Service(proc.Cls), sconfig)
			oneCls = true
			break
		}
	}

	// if no cluster service, make all nodes online
	if !oneCls {
		for node := range nodes.Iter() {
			node.Status = cluster.Status_Online // TODO: Should not be here!
		}
	}

	// boot services
	proc.Sc.BootServices()

	return proc
}
