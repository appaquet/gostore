package main_test

import (
	"testing"
	"gostore/cluster"
	"gostore/log"
)

func TestBoot(t *testing.T) {
	log.Debug("TestBoot")

	SetupCluster()

	// start master
	StartNode(0)

	// start another node
	StartNode(1)


	err := WaitOnline(1, 10)
	if err != nil {
		t.Errorf("1) Got an error: %s", err)
	}

	for i, proc := range tc.nodes {
		if proc != nil {
			if proc.Cluster.Nodes.Get(1) == nil || proc.Cluster.Nodes.Get(1).Status != cluster.Status_Online {
				t.Errorf("2) Node 1 is not online for proc %d", i)
			}
		}
	}
}

func TestPersistence(t *testing.T) {
	log.Debug("TestPersistence")

	SetupCluster()

	//start master
	StartNode(0)

	// starter another node
	StartNode(1)

	WaitOnline(1, 10)

	StopNode(1)

	StartNode(1)
}


