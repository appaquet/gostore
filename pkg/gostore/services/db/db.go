package db

import (
	"strconv"
	"time"
	"fmt"
	"gostore/cluster"
	proto "goprotobuf.googlecode.com/hg/proto"
)


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
	containers map[string]container
	config Config

	segmentManager   *segmentManager
	viewstateManager *viewStateManager
}

func NewDb(config Config) *Db {
	db := new(Db)
	db.config = config
	db.containers = make(map[string]container)

	db.segmentManager = newSegmentManager(db, config.DataPath, 0, cluster.MAX_NODE_ID)
	db.viewstateManager = newViewStateManager(db)

	return db
}

func (db *Db) createContainer(name string) {
	db.containers[name] = newContainer()
}


func (db *Db) getNextTransactionId() uint64 {
	return uint64(time.Nanoseconds())
}

func (db *Db) Execute(trans *Transaction) (ret *TransactionReturn) {
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
	vs := db.viewstateManager.newViewState()

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

	return newret

	/*trans.Id = proto.Uint64(db.getNextTransactionId())
	vs := db.viewstateManager.newViewState()
	return trans.execute(vs)*/
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
