package main_test

import (
	"gostore"
	"gostore/process"
	"gostore/cluster"
	"gostore/log"
	"fmt"
	"os"
	"time"
)


type TestCluster struct {
	nodes []*process.Process
}

const (
	firstport = 3000
)

var (
	tc *TestCluster
)

func SetupCluster() {
	log.MaxLevel = 10 // Default: 2

	if tc == nil {
		tc = NewTestCluster(10)
	}

	StopAllNodes()
}

func NewTestCluster(nodescount int) *TestCluster {
	tc := new(TestCluster)

	tc.nodes = make([]*process.Process, nodescount)

	return tc
}

func StopAllNodes() {
	for _, proc := range tc.nodes {
		if proc != nil {
			proc.Sc.Pause()
		}
	}
}

func StartNode(id int) {
	var nodes []gostore.ConfigNode
	if id != 0 {
		nodes = make([]gostore.ConfigNode, 2)
	} else {
		nodes = make([]gostore.ConfigNode, 1)
	}

	// make the master
	nodes[0].NodeId = uint16(0)
	nodes[0].NodeIP = "127.0.0.1"
	nodes[0].TCPPort = uint16(firstport + 0*10)
	nodes[0].UDPPort = uint16(firstport + 0*10 + 1)
	nodes[0].Rings = make([]gostore.ConfigNodeRing, 1)
	nodes[0].Rings[0].RingId = 1
	nodes[0].Rings[0].Token = "0000000000000000000000"

	// make myself
	if id != 0 {
		nodes[1].NodeId = uint16(id)
		nodes[1].NodeIP = "127.0.0.1"
		nodes[1].TCPPort = uint16(firstport + id*10)
		nodes[1].UDPPort = uint16(firstport + id*10 + 1)
	}

	rings := make([]gostore.ConfigRing, 2)
	rings[0].Id = 0
	rings[0].ReplicationFactor = 3
	rings[1].Id = 1
	rings[1].ReplicationFactor = 3

	datadir := fmt.Sprintf("data/%d", id)

	conf := new(gostore.Config)

	conf.CurrentNode = uint16(id)
	conf.Nodes = nodes

	conf.Rings = rings

	conf.Services = make([]gostore.ConfigService, 2)

	// add cluster service
	conf.Services[0].Id = 1
	conf.Services[0].Type = "cls"
	conf.Services[0].CustomConfig = make(map[string]interface{})
	conf.Services[0].CustomConfig["DataDir"] = datadir
	conf.Services[0].CustomConfig["MasterRing"] = 1.0

	// add FS
	conf.Services[1].Id = 2
	conf.Services[1].Type = "fs"
	conf.Services[1].CustomConfig = make(map[string]interface{})
	conf.Services[1].CustomConfig["DataDir"] = datadir
	conf.Services[1].CustomConfig["ApiAddress"] = fmt.Sprintf("127.0.0.1:%d", (firstport + id*10 + 2))

	// Clear and create data dir
	os.RemoveAll(datadir)
	os.Mkdir(datadir, 0777)

	tc.nodes[id] = process.NewProcess(*conf)
}

func StopNode(id int) {
	proc := tc.nodes[id]
	if proc != nil {
		//proc.Kill()
	}
}

func WaitOnline(id, maxWait int) os.Error {
	start := time.Seconds()
	for tc.nodes[id].Cluster.MyNode.Status != cluster.Status_Online {
		if time.Seconds()-start > int64(maxWait) {
			return os.NewError("Maximum wait time exceed")
		}

		time.Sleep(1000000) // 1 ms
	}

	return nil
}
