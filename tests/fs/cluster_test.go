package main_test

import (
	"gostore"
	"gostore/process"
	"gostore/log"
	"fmt"
	"os"
)

type TestCluster struct {
	nodes []*process.Process
}


var (
	tc *TestCluster
)

func SetupCluster() {
	log.MaxLevel = 2 // Default: 2

	if tc == nil {
		tc = NewTestCluster(13)
	}
}

func NewTestCluster(nodescount int) *TestCluster {
	tc := new(TestCluster)
	firstport := 20000

	tc.nodes = make([]*process.Process, nodescount)

	nodes := make([]gostore.ConfigNode, nodescount)
	for i := 0; i < nodescount; i++ {
		nodes[i].NodeId = uint16(i)
		nodes[i].NodeIP = "127.0.0.1"
		nodes[i].TCPPort = uint16(firstport + i*10)
		nodes[i].UDPPort = uint16(firstport + i*10 + 1)
	}

	rings := make([]gostore.ConfigRing, 1)
	rings[0].Id = 0
	rings[0].ReplicationFactor = 3

	for i := 0; i < nodescount; i++ {
		datadir := fmt.Sprintf("data/%d", i)

		conf := new(gostore.Config)

		conf.CurrentNode = uint16(i)
		conf.Nodes = nodes

		conf.Rings = rings
		conf.GlobalRing = 0

		conf.Services = make([]gostore.ConfigService, 1)

		// add FS
		conf.Services[0].Id = 2
		conf.Services[0].Type = "fs"
		conf.Services[0].CustomConfig = make(map[string]interface{})
		conf.Services[0].CustomConfig["DataDir"] = datadir
		conf.Services[0].CustomConfig["ApiAddress"] = fmt.Sprintf("127.0.0.1:%d", (firstport + i*10 + 2))

		// Clear and create data dir
		os.RemoveAll(datadir)
		os.Mkdir(datadir, 0777)

		tc.nodes[i] = process.NewProcess(*conf)
	}

	return tc
}

func GetProcessForPath(paths ...string) (resp *process.Process, other *process.Process) {
	var found []bool = make([]bool, len(tc.nodes))

	for a, path := range paths {
		res := tc.nodes[0].Cluster.Rings.GetGlobalRing().Resolve(path)
		for i := 0; i < res.Count(); i++ {
			node := res.Get(i)

			if a == 0 && i == 0 {
				resp = tc.nodes[node.Id]
			}

			found[node.Id] = true
		}
	}

	for i := 0; i < len(tc.nodes); i++ {
		if !found[i] {
			other = tc.nodes[i]
		}
	}

	return resp, other
}
