package db

import (
	"strconv"
	"time"
	"gostore/cluster"
	proto "goprotobuf.googlecode.com/hg/proto"
)


//
// Database config
//
type Config struct {
	DataPath	string
}


//
// Database object
//
// TODO: Replace by shard
type Db struct {
	root			*Root
	config			Config

	segmentManager		*SegmentManager
	viewstateManager	*ViewStateManager
}

func NewDb(config Config) *Db {
	db := new(Db)
	db.config = config
	db.root = newRoot()

	db.segmentManager = newSegmentManager(db, config.DataPath, 0, cluster.MAX_NODE_ID)
	db.viewstateManager = newViewStateManager()

	return db
}



func (db *Db) getNextTransactionId() uint64 {
	return uint64(time.Nanoseconds())
}

func (db *Db) Execute(trans *Transaction) (ret *TransactionReturn) {
	bytes, err := proto.Marshal(trans)
	if err != nil {
		panic("Got an error marshalling")
	}

	newtrans := NewEmptyTransaction()
	err = proto.Unmarshal(bytes, newtrans)
	if err != nil {
		panic("Got an error unmarshalling")
	}
	

	newtrans.Id = proto.Uint64(db.getNextTransactionId())
	vs := db.viewstateManager.NewViewState()

	ret = newtrans.execute(vs)
	bytes, err = proto.Marshal(ret)
	if err != nil {
		panic("Got an error marshalling")
	}

	newret := new(TransactionReturn)
	err = proto.Unmarshal(bytes, newret)
	if err != nil {
		panic("Got an error unmarshalling")
	}

	return newret

	/*trans.Id = proto.Uint64(db.getNextTransactionId())
	vs := db.viewstateManager.NewViewState()
	return trans.execute(vs)*/
}



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
