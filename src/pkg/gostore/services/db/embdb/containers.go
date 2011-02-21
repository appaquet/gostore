package embdb

import (
	"sync"
	"time"
	"os"
	"fmt"
	"json"
	"gostore/tools/typedio"
	"gostore/log"
	"reflect"
	"sort"
)

//
// Lockable container. A container is a part of the database 
// (namespace, object map, object) that may need to be locked
// if it needs to be modified.
//
// Container hierarchy: Root -> Namespace -> Object map -> Objects
//
type Container interface {
	Lock()
	Unlock()
}

//
// Container array implementing the sort.Interface interface
//
type Containers []Container

func (c Containers) Len() int { return len(c) }

func (c Containers) Less(i, j int) bool {
	return reflect.NewValue(c[i]).Addr() < reflect.NewValue(c[j]).Addr()
}

func (c Containers) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

func (c Containers) Sort() { sort.Sort(c) }


//
// Database root that contains namespace
//
type Root struct {
	namespaces map[string]*Namespace
	mutex      *sync.Mutex
}

func (r *Root) Lock() {
	r.mutex.Lock()
}

func (r *Root) Unlock() {
	r.mutex.Unlock()
}

func (r *Root) String() string {
	return fmt.Sprintf("Root[%s]", r.namespaces)
}

//
// Namespace that contains object maps
//
type Namespace struct {
	objectMaps map[string]*ObjectMap
	mutex      *sync.Mutex
}

func (ns *Namespace) Lock() {
	ns.mutex.Lock()
}

func (ns *Namespace) Unlock() {
	ns.mutex.Unlock()
}

func (ns *Namespace) String() string {
	return fmt.Sprintf("Namespace[%s]", ns.objectMaps)
}


//
// Object map that contains objects
//
type ObjectMap struct {
	objects map[string]*Object
	mutex   *sync.Mutex
}

func (om *ObjectMap) Lock() {
	om.mutex.Lock()
}

func (om *ObjectMap) Unlock() {
	om.mutex.Unlock()
}

func (om *ObjectMap) String() string {
	return fmt.Sprintf("ObjectMap[%s]", om.objects)
}

//
// Object that can contains anything the user can serialize
// into JSON. Data is therefor JSON serializable.
//
type Object struct {
	// doesn't have namespace, objmap, objkey to save memory
	version int64
	data    interface{}
	flags   byte
	mutex   *sync.Mutex
}

const (
	obj_flag_commited byte = 0x01
	obj_flag_deleted  byte = 0x02
)

func (o *Object) String() string {
	return fmt.Sprintf("Object[v=%d, data=%s]", o.version, o.data)
}

func (o *Object) isFlag(flag byte) bool {
	if o.flags&flag == flag {
		return true
	}

	return false
}

func (o *Object) setFlag(flag byte, value bool) {
	if value {
		o.flags = o.flags | flag
	} else {
		o.flags = o.flags | ^flag
	}
}

func (o *Object) incrementVersion(version int64) {
	o.version = version
	o.setFlag(obj_flag_commited, false)
}

func (o *Object) Lock() {
	o.mutex.Lock()
}

func (o *Object) Unlock() {
	o.mutex.Unlock()
}

func (o *Object) save(namespace, objmap, objkey string, db *Db) {
	// TODO: Write in SSTables'ish

	o.Lock()

	// only if the object hasn't been commited to disk yet
	if !o.isFlag(obj_flag_commited) {
		// create new file
		tempPath := fmt.Sprintf("%s/%d", db.config.DataTempPath, time.Nanoseconds())
		file, err := os.Open(tempPath, os.O_WRONLY|os.O_CREATE, 777)
		if err != nil {
			log.Fatal("Couldn't open temp file: %s", err)
			return
		}

		bytes, err := json.Marshal(o.data)
		if err != nil {
			log.Error("Couldn't marshal data for object %s/%s/%s!", namespace, objmap, objkey)
		}

		typedfile := typedio.NewWriter(file)

		typedfile.WriteString(namespace) // namespace
		typedfile.WriteString(objmap)    // objmap
		typedfile.WriteString(objkey)    // objkey
		typedfile.WriteInt64(o.version)  // version

		typedfile.WriteUint32(uint32(len(bytes))) // data length
		file.Write(bytes)                         // data

		file.Close()

		// create directories
		nspath := db.config.DataPath + namespace + "/"
		stat, _ := os.Stat(nspath)
		if stat == nil || !stat.IsDirectory() {
			err := os.Mkdir(nspath, 0777)
			if err != nil {
				log.Fatal("Couldn't create namespace directory %s: %s", nspath, err)
			}
		}

		ompath := nspath + objmap + "/"
		stat, _ = os.Stat(ompath)
		if stat == nil || !stat.IsDirectory() {
			err := os.Mkdir(ompath, 0777)
			if err != nil {
				log.Fatal("Couldn't create objmap directory %s: %s", ompath, err)
			}
		}

		// replace old file 
		objpath := ompath + objkey
		err = os.Rename(tempPath, objpath)
		if err != nil {
			log.Error("Couldn't move final object file from %s to %s: %s", tempPath, objpath, err)
		}

		// set commited flag
		o.setFlag(obj_flag_commited, true)
	}

	o.Unlock()
}

func loadObject(path string, headerOnly bool, db *Db) {
	file, err := os.Open(path, os.O_RDONLY, 0777)
	if err != nil {
		log.Fatal("Couldn't open data file %s: %s", path, err)
		return
	}

	typedfile := typedio.NewReader(file)

	obj := new(Object)
	obj.mutex = new(sync.Mutex)

	namespace, _ := typedfile.ReadString() // namespace
	objmap, _ := typedfile.ReadString()    // objmap
	objkey, _ := typedfile.ReadString()    // objkey

	obj.version, _ = typedfile.ReadInt64() // version

	// TODO: Handle "headerOnly"
	dataLength, _ := typedfile.ReadUint32() // data length
	bytes := make([]byte, dataLength)
	file.Read(bytes)
	err = json.Unmarshal(bytes, &obj.data)
	if err != nil {
		log.Error("Couldn't unmarshal data from %s: %s", path, err)
	}

	om, _, _ := db.GetObjectMap(namespace, objmap, true)
	om.mutex.Lock()
	om.objects[objkey] = obj
	om.mutex.Unlock()
}
