package db

import (
	"strconv"
	"time"
	"fmt"
	"os"
	"sync"
	"gostore/cluster"
	proto "goprotobuf.googlecode.com/hg/proto"
)

var _ = fmt.Errorf
var _ = proto.Bool
var _ = os.NewError
var _ = time.After

//
// Database config
//
type Config struct {
	DataPath string
}


//
// Database
//
// TODO: Replace by shard
type Db struct {
	containers 	map[string]container
	containersMutex	*sync.Mutex

	config Config

	segmentManager   *segmentManager
	viewstateManager *viewStateManager
}

func NewDb(config Config) *Db {
	db := new(Db)
	db.config = config
	db.containers = make(map[string]container)
	db.containersMutex = new(sync.Mutex)

	db.segmentManager = newSegmentManager(config.DataPath, 0, cluster.MAX_NODE_ID)
	db.viewstateManager = newViewStateManager(db)

	return db
}

func (db *Db) createContainer(name string) {
	db.containersMutex.Lock()
	db.containers[name] = newContainer()
	db.containersMutex.Unlock()
}

func (db *Db) getContainer(name string) (container, bool) {
	db.containersMutex.Lock()
	c, f := db.containers[name]
	db.containersMutex.Unlock()

	return c,f
}

func (db *Db) getNextTransactionId() uint64 {
	return uint64(time.Nanoseconds())
}

func (db *Db) Reload() {
	err := db.segmentManager.replayAll(db)
	if err != nil {
		panic(err)
	}

}

func (db *Db) Close() {
	db.segmentManager.closeAll()
}

func (db *Db) Execute(trans *Transaction) (ret *TransactionReturn) {
	var err os.Error

	/*
	bytes, err := proto.Marshal(trans)
	if err != nil {
		panic(fmt.Sprintf("Got an error marshalling: %s", err))
	}

	newtrans := NewEmptyTransaction()
	err = proto.Unmarshal(bytes, newtrans)
	if err != nil {
		panic(fmt.Sprintf("Got an error unmarshalling: %s", err))
	}


	newtrans.Id = proto.Uint64(db.getNextTransactionId())

	token := Token(0) // TODO: Use real token!!
	vs := db.viewstateManager.createViewState(token, true, 0)

	err = newtrans.init()
	if err != nil {
		panic(fmt.Sprintf("Transaction initialisation error: %s", err))
	}

	ret = newtrans.execute(vs)
	bytes, err = proto.Marshal(ret)
	if err != nil {
		panic(fmt.Sprintf("Got an error marshalling: %s", err))
	}

	newret := new(TransactionReturn)
	err = proto.Unmarshal(bytes, newret)
	if err != nil {
		panic(fmt.Sprintf("Got an error unmarshalling: %s", err))
	}
	//*/


	//*
	trans.Id = proto.Uint64(db.getNextTransactionId())
	token := Token(0) // TODO: Use real token!!
	trans.init()
	vs := db.viewstateManager.createViewState(token, true, 0)
	newret := trans.execute(vs)
	//*/


	if newret.Error == nil {
		err = vs.prepareCommit()
		if err != nil {
			vs.rollback()

			newret.Error = &TransactionError{
				Id: proto.Uint32(0), // TODO: ERRNO
				Message: proto.String(err.String()),
			}
			return newret
		}

		err = vs.commit()
		if err != nil {
			newret.Error = &TransactionError{
				Id: proto.Uint32(0), // TODO: ERRNO
				Message: proto.String(err.String()),
			}
			return newret
		}
	}


	return newret

}


func walk(create bool, data *interface{}, container *interface{}, key []string) {
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
			walk(create, data, &subcontainer, key[1:])
			arr[integer] = subcontainer
			*container = arr

		} else { // map

			mmap := make(map[string]interface{})

			var subcontainer interface{}
			if val, found := mmap[key[0]]; found {
				subcontainer = val
			}
			walk(create, data, &subcontainer, key[1:])
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
			walk(create, data, &subcontainer, key[1:])
			arr[integer] = subcontainer

		} else if create {
			newarr := make([]interface{}, integer+1)
			copy(newarr, arr)
			arr = newarr
			*container = arr

			var subcontainer interface{}
			walk(create, data, &subcontainer, key[1:])
			arr[integer] = subcontainer
		}

	} else if mmap, ok := (*container).(map[string]interface{}); ok { // container is a map
		var subcontainer interface{}
		if val, found := mmap[key[0]]; found {
			subcontainer = val
		}
		walk(create, data, &subcontainer, key[1:])
		mmap[key[0]] = subcontainer
	}
}
