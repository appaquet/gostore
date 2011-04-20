package cls

import (
	"os"
	"gostore/log"
	"gostore/tools/typedio"
	"gostore/cluster"
	"fmt"
	"time"
)


func (cs *ClusterService) loadCluster() {
	cs.clusterMutex.Lock()

	// Load data
	log.Debug("cls: Loading cluster data...")
	stat, err := os.Stat(cs.clsDataPath)
	if err == nil && stat.IsRegular() {
		file, err := os.OpenFile(cs.clsDataPath, os.O_RDONLY, 0777)
		if err == nil {
			typedFile := typedio.NewReader(file)

			cs.clusterVersion, _ = typedFile.ReadInt64()		// cluster version
			cs.diskVerson = cs.clusterVersion


			nbNodes, _ := typedFile.ReadUint16()			// nodes count
			var i uint16
			for i=0; i<nbNodes; i++ {
				node := cluster.NewEmptyNode()
				node.Unserialize(typedFile)

				node.Status = cluster.Status_Offline

				cs.cluster.MergeNode(node, false)	// merge node, doesn't notify
			}

		} else {
			log.Error("cls: Error while opening data file", err)
		}
	}

	// replay commit log
	log.Info("cls: Replaying commit log...")
	cs.commitlog.Replay()

	// TODO: Load cluster data

	cs.clusterMutex.Unlock()
}



func (cs ClusterService) saveCluster() {
	cs.clusterMutex.Lock()

	tempPath := fmt.Sprintf("%s/%d", os.TempDir(), time.Nanoseconds())
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		log.Fatal("Couldn't open temp cluster data file", err)
	}

	typedFile := typedio.NewWriter(file)

	typedFile.WriteInt64(cs.clusterVersion)			// cluster version
	cs.diskVerson = cs.clusterVersion


	typedFile.WriteUint16(cs.cluster.Nodes.Count())		// nodes count
	for node := range cs.cluster.Nodes.Iter() {
		node.Serialize(typedFile)
	}

	file.Close()

	err = os.Rename(tempPath, cs.clsDataPath)
	if err != nil {
		log.Fatal("Couldn't rename %s to %s", tempPath, cs.clsDataPath)
	}

	cs.clusterMutex.Unlock()
}


//
// Mutations on the cluster are simply nodes that are being merged into the
// cluster. 
//
type clusterMutation struct{
	cls		*ClusterService
	nodes		[]*cluster.Node
	version		int64
	notify		bool
}

func (dm *clusterMutation) Unserialize(reader typedio.Reader) {
	dm.version, _ = reader.ReadInt64()
	nbNodes, _ := reader.ReadUint16()
	dm.nodes = make([]*cluster.Node, nbNodes)

	var i uint16
	for i=0; i<nbNodes; i++ {
		node := cluster.NewEmptyNode()
		node.Unserialize(reader)
		dm.nodes[i] = node
	}
}

func (dm *clusterMutation) Serialize(writer typedio.Writer) {
	writer.WriteInt64(dm.version)
	writer.WriteUint16(uint16(len(dm.nodes)))

	for _, node := range dm.nodes {
		node.Serialize(writer)
	}

	// if data were persisted to disk, we don't make the cluster
	// since its not new data
	dm.notify = false
}

func (dm *clusterMutation) Commit() {
}

func (dm *clusterMutation) Execute() {
}
