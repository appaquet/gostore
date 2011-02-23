//
// Embeded database with in-memory index
//
package embdb

import (
	"strconv"
	"sync"
	"time"
	"fmt"
	"gostore/tools/commitlog"
	"os"
	"gostore/log"
)


//
// Database config
//
type Config struct {
	DataPath     string
	DataTempPath string
	LogPath      string
}


//
// Database object
//
type Db struct {
	root      *Root
	config    Config
	commitlog *commitlog.CommitLog
}

func NewDb(config Config) *Db {
	db := new(Db)
	db.config = config
	db.root = &Root{make(map[string]*Namespace), new(sync.Mutex)}

	db.commitlog = commitlog.New(config.LogPath)
	db.commitlog.RegisterMutation(&databaseMutation{db, nil})

	// Load data
	db.loadData()

	// Replay log
	db.replayLog()

	return db
}


func (db *Db) replayLog() {
	log.Info("Replaying commit log... ")
	replayed := db.commitlog.Replay()
	log.Info("%d transaction replayed from commit log", replayed)
}

func (db *Db) loadData() {
	log.Info("Loading database data from disk at path %s...", db.config.DataPath)
	loaded := 0

	dataDirFile, err := os.Open(db.config.DataPath, os.O_RDONLY, 0777)
	if err != nil {
		log.Fatal("Couldn't open data directory: %s", err)
		return
	}

	// namespaces
	nsDirs, err := dataDirFile.Readdir(-1)
	if err != nil {
		log.Fatal("Couldn't read data sub-directories: %s", err)
		return
	}
	for _, nsDir := range nsDirs {
		if nsDir.IsDirectory() {
			nsPath := db.config.DataPath + nsDir.Name + "/"
			nsDirFile, err := os.Open(nsPath, os.O_RDONLY, 0777)
			if err != nil {
				log.Fatal("Couldn't open namespace directory: %s", err)
				return
			}

			// object maps
			omDirs, err := nsDirFile.Readdir(-1)
			if err != nil {
				log.Fatal("Couldn't read namespace directory: %s", err)
				return
			}
			for _, omDir := range omDirs {
				if omDir.IsDirectory() {
					omPath := nsPath + omDir.Name + "/"
					omDirFile, err := os.Open(omPath, os.O_RDONLY, 0777)
					if err != nil {
						log.Fatal("Couldn't open object map directory: %s", err)
						return
					}

					// objects
					objDirs, err := omDirFile.Readdir(-1)
					if err != nil {
						log.Fatal("Couldn't read object map directory: %s", err)
						return
					}
					for _, objDir := range objDirs {
						objPath := omPath + objDir.Name

						// TODO: Should be passing "true" to load only header
						loadObject(objPath, false, db)
						loaded++
					}

					omDirFile.Close()
				}
			}

			nsDirFile.Close()
		}
	}

	dataDirFile.Close()
	log.Info("Loaded %d objects from disk", loaded)
}


func (db *Db) getNextTransactionId() int64 {
	return time.Nanoseconds()
}

//
// Commit "count" transactions, writting modified
// files to disk
//
func (db *Db) Commit(count int) {
	db.commitlog.Commit(count)
}

func (db *Db) Execute(trans *Transaction) (err os.Error) {
	trans.id = db.getNextTransactionId()

	// make sure there aren't error
	err = trans.preRun(db)
	if err != nil {
		return err
	}

	// if the transaction doesn't modify anything, execute directly
	if trans.readOnly {
		trans.run(db)
	} else {
		db.commitlog.Execute(&databaseMutation{db, trans})
	}

	return nil
}

func (db *Db) GetNamespace(namespace string, create bool) (ns *Namespace, parent Container, err os.Error) {
	val, ok := db.root.namespaces[namespace]

	if !ok {
		if create {
			val, ok = db.root.namespaces[namespace]
			if !ok {
				val = &Namespace{make(map[string]*ObjectMap), new(sync.Mutex)}
				db.root.namespaces[namespace] = val
			}
		} else {
			return nil, db.root, os.NewError(fmt.Sprintf("No such namespace: %s", namespace))
		}
	}

	return val, db.root, nil
}

func (db *Db) GetObjectMap(namespace, objmap string, create bool) (om *ObjectMap, parent Container, err os.Error) {
	ns, parent, err := db.GetNamespace(namespace, create)
	if err != nil {
		return nil, parent, err
	}

	val, ok := ns.objectMaps[objmap]

	if !ok {
		if create {
			val, ok = ns.objectMaps[objmap]
			if !ok {
				val = &ObjectMap{make(map[string]*Object), new(sync.Mutex)}
				ns.objectMaps[objmap] = val
			}
		} else {
			return nil, ns, os.NewError(fmt.Sprintf("No such object map: %s/%s", namespace, objmap))
		}
	}

	return val, ns, nil
}


func (db *Db) GetObject(namespace, objmap, objkey string, createUnderlying, createObject bool) (o *Object, parent Container, err os.Error) {
	om, container, err := db.GetObjectMap(namespace, objmap, createUnderlying)
	if err != nil {
		return nil, container, err
	}

	val, ok := om.objects[objkey]

	if !ok {
		if createObject {
			val, ok = om.objects[objkey]
			if !ok {
				val = &Object{0, nil, 0, new(sync.Mutex)}
				om.objects[objkey] = val
			}
		}
	}

	return val, om, nil
}

// TODO: Return error if create is false
func Walk(create bool, data *interface{}, container *interface{}, key []string) {
	// we are at the end
	if len(key) == 0 {
		if *data != nil {
			*container = *data
		}

		*data = *container

		// current object is null and we don't create it, return nil
	} else if *container == nil && !create {
		*data = nil // TODO: Return error


	} else if *container == nil && create {

		integer, err := strconv.Atoi(key[0])
		if err == nil { // array
			arr := make([]interface{}, integer+1)

			var subcontainer interface{}
			Walk(create, data, &subcontainer, key[1:])
			arr[integer] = subcontainer
			*container = arr

		} else { // map

			mmap := make(map[string]interface{})

			var subcontainer interface{}
			if val, found := mmap[key[0]]; found {
				subcontainer = val
			}
			Walk(create, data, &subcontainer, key[1:])
			mmap[key[0]] = subcontainer

			*container = mmap
		}

	} else if arr, ok := (*container).([]interface{}); ok { // container is an array
		integer, err := strconv.Atoi(key[0])
		if err != nil { // array
			return
		}

		// TODO: Support for -1 (last, ...)
		if integer < len(arr) && integer >= 0 {
			var subcontainer interface{} = arr[integer]
			Walk(create, data, &subcontainer, key[1:])
			arr[integer] = subcontainer

		} else if create {
			newarr := make([]interface{}, integer+1)
			copy(newarr, arr)
			arr = newarr
			*container = arr

			var subcontainer interface{}
			Walk(create, data, &subcontainer, key[1:])
			arr[integer] = subcontainer
		}

	} else if mmap, ok := (*container).(map[string]interface{}); ok { // container is a map
		var subcontainer interface{}
		if val, found := mmap[key[0]]; found {
			subcontainer = val
		}
		Walk(create, data, &subcontainer, key[1:])
		mmap[key[0]] = subcontainer
	}
}
