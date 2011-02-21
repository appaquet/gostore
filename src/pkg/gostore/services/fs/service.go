package fs

import (
	"gostore/comm"
	"gostore/log"
	"gostore/cluster"
	"gostore"
	"os"
	"container/list"
	"sync"
)

const ()

var (
	ErrorFileNotFound = os.NewError("File not found")
	ErrorNotEmpty     = os.NewError("Can't delete path because contains children")
)

type FsService struct {
	comm    *comm.Comm
	cluster *cluster.Cluster
	ring    *cluster.Ring

	sconfig   *gostore.ConfigService
	dataDir   string
	serviceId uint8
	running   bool

	// api
	api        *api
	apiAddress string

	// file headers
	headers *FileHeaders

	// file mutexes
	mutexes  map[string]*sync.Mutex
	mapMutex *sync.Mutex

	// replication queue
	replQueue      *list.List
	replQueueMutex *sync.Mutex
	replForce      bool
}

func NewFsService(comm *comm.Comm, sconfig *gostore.ConfigService) *FsService {
	fss := new(FsService)

	fss.running = true
	fss.sconfig = sconfig
	fss.headers = newFileHeaders(fss)
	fss.comm = comm
	fss.cluster = comm.Cluster
	fss.serviceId = sconfig.Id

	datadir, ok := sconfig.CustomConfig["DataDir"]
	if !ok {
		log.Fatal("FSS: DataDir config should be setted!")
	}
	fss.dataDir = datadir.(string)

	// file mutexes
	fss.mutexes = make(map[string]*sync.Mutex)
	fss.mapMutex = new(sync.Mutex)

	apiAddress, ok := sconfig.CustomConfig["ApiAddress"]
	if !ok {
		log.Fatal("FSS: ApiAddress config should be setted!")
	}
	fss.apiAddress = apiAddress.(string)

	ringid, ok := sconfig.CustomConfig["RingId"]
	if ok {
		fss.ring = fss.cluster.Rings.GetRing(uint8(ringid.(float64)))
	} else {
		fss.ring = fss.cluster.Rings.GetGlobalRing()
	}

	// create the api
	fss.api = createApi(fss)

	// TODO: Start a local timeout tracker
	// TODO: Start a gargage collector

	// start replication watcher
	// TODO: We should be able to start as many watcher as we have core (or we need!)
	fss.replQueue = list.New()
	fss.replQueueMutex = new(sync.Mutex)
	go fss.replicationWatcher()

	return fss
}

func (fss *FsService) HandleUnmanagedMessage(msg *comm.Message) {
	log.Error("FSS: Got an unmanaged message: %s", msg)
}

func (fss *FsService) HandleUnmanagedError(errorMessage *comm.Message, error os.Error) {
	log.Error("FSS: Got an unmanaged error: %s", errorMessage)
}

func (fss *FsService) Boot() {
}

func (fss *FsService) Lock(key string) {
	mutex, found := fss.mutexes[key]
	if !found {
		fss.mapMutex.Lock()

		// must make sure it still doesn't exist
		mutex, found = fss.mutexes[key]
		if !found {
			mutex = new(sync.Mutex)
			fss.mutexes[key] = mutex
		}

		fss.mapMutex.Unlock()
	}

	mutex.Lock()
}

func (fss *FsService) Unlock(key string) {
	mutex, found := fss.mutexes[key]
	if !found {
		log.Error("FSS: Couldn't find file mutex to unlock for key %s\n", key)
		return
	}

	mutex.Unlock()
}
