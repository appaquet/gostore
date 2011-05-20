package db

import (
	"os"
	"fmt"
	"strings"
	"gostore/tools/typedio"
	proto "goprotobuf.googlecode.com/hg/proto"
	"sync"
	"gostore/log"
)

var _ = log.Debug

func makeVsKey(token Token, vsId uint32) uint64 {
	return uint64(token)<<32 + uint64(vsId)
}

func implodeObjectKey(containerName, key string) string {
	return containerName + "^" + key // TODO: Make sure we don't have a ^ at the end of containerName or beginning of key
}

func explodeObjectKey(mapKey string) (containerName, key string) {
	p := strings.Split(mapKey, "^", 2)
	return p[0], p[1]
}

//
// Transaction view state manager
//
type viewStateManager struct {
	db	*Db

	vs	map[uint64]*viewState
	vsMutex *sync.Mutex

	nextId	uint32

	lockObjects		chan *viewState
	unlockObjects		chan *viewState
	objectsLocks		map[string]objectLock
}

func newViewStateManager(db *Db) *viewStateManager {
	m := new(viewStateManager)
	m.db = db

	m.vsMutex = new(sync.Mutex)
	m.vs = make(map[uint64]*viewState) // Sync via vsMutex


	m.lockObjects = make(chan *viewState, 1)
	m.unlockObjects = make(chan *viewState, 1)
	m.objectsLocks = make(map[string]objectLock) // Sync via commitor
	go m.lockManagerRoutine()

	return m
}

func (m *viewStateManager) createViewState(token Token, genId bool, id uint32) *viewState {
	m.vsMutex.Lock()
	if genId {
		found := false
		for !found {
			key := makeVsKey(token, m.nextId)
			if _, ok := m.vs[key]; !ok {
				found = true
			} else {
				m.nextId++
			}
		}
		id = m.nextId
	}

	vs := &viewState{
		db: m.db,
		objects: make(map[string]object),
		token: token,
		id: id,
		next: make(chan *viewState, 1),
		wait: make(chan bool, 1),
	}

	key := makeVsKey(token, id)
	m.vs[key] = vs
	m.vsMutex.Unlock()

	if genId {
		// write viewstate creation to disk
		mut := &mutation{
			typ:mut_create_vs,
			vs:id,
		}
		m.db.segmentManager.writeExecuteMutation(vs.token, mut, m.db)
		mut.execute(m.db, false)
	}

	return vs
}

func (m *viewStateManager) deleteViewState(vs *viewState) {
	key := makeVsKey(vs.token, vs.id)
	m.vsMutex.Lock()
	m.vs[key] = nil, false
	m.vsMutex.Unlock()
}

func (m *viewStateManager) getViewState(token Token, id uint32) *viewState {
	m.vsMutex.Lock()
	key := makeVsKey(token, id)
	vs, found := m.vs[key]
	m.vsMutex.Unlock()

	if !found {
		return nil
	}

	return vs
}

func (m *viewStateManager) lockManagerRoutine() {
	var next *viewState

	for {
		if next != nil {
			locked, abort := next.lockObjects()
			if abort || locked {
				next.wait <- true
			}
		}

		select {
		case vs := <- m.lockObjects:
			locked, abort := vs.lockObjects()
			if locked || abort {
				vs.wait <- true
			}

		case vs := <- m.unlockObjects:
			vs.unlockObjects()

			// another vs was waiting for this vs to unlock, push it next in line
			if len(vs.next) > 0 {
				next = <- vs.next
			}
		}

	}
}

func (m *viewStateManager) lockObject(objMapKey string, vs *viewState, readonly bool) (success, abort bool, blockerVs *viewState) {
	lock, found := m.objectsLocks[objMapKey]
	// TODO: Implement readonly locks waiting

	if found {
		// if the lock is for write, we abort (object will be modified!)
		if !lock.readonly {
			success = false
			abort = true
			blockerVs = lock.vs
			return
		}

		// wait for the lock
		success = false
		abort = false
		blockerVs = lock.vs
	} else {
		m.objectsLocks[objMapKey] = objectLock{
			readonly: readonly,
			vs: vs,
		}

		success = true
		abort = false
	}

	return
}

func (m *viewStateManager) unlockObject(objMapKey string, vs *viewState) {
	lock, found := m.objectsLocks[objMapKey]
	if found {
		if lock.vs == vs {
			m.objectsLocks[objMapKey] = objectLock{}, false
		}
	}
}

//
// Object lock imposed by a viewstate
//
type objectLock struct {
	readonly	bool
	vs		*viewState
}







//
// Transaction view state
//
type viewState struct {
	db		*Db

	token		Token
	id		uint32
	segEntry	*segmentEntry

	objects		map[string]object
	hasMutations	bool // TODO: use this to skip prep, commit to disk for readonly vs

	next		chan *viewState
	error		os.Error
	wait		chan bool
}

func (vs *viewState) getObject(containerName, key string) (obj object, err os.Error) { 
	mapkey := implodeObjectKey(containerName, key)
	
	if obj, found := vs.objects[mapkey]; found {
		return obj, nil
	}

	container, found := vs.db.getContainer(containerName)
	if !found {
		return object{}, os.NewError("Container not found")
	}

	obj, _ = container.getObject(key)
	// TODO: if obj is not loaded, load it
	// TODO: add the object to vs.objects

	return
}

func (vs *viewState) setObject(containerName, key string, obj object) {
	mapkey := implodeObjectKey(containerName, key)
	obj.setFlag(obj_flag_new, true) // mark as modified
	vs.objects[mapkey] = obj
}

func (vs *viewState) mutateObject(op mutationOperation) (err os.Error) {
	// TODO: Get object, save initial version
	mut := &mutation{
		typ: mut_obj_op,
		op: op,
		vs: vs.id,
	}
	err = vs.db.segmentManager.writeExecuteMutation(vs.token, mut, vs.db)
	if err != nil {
		return
	}

	err = mut.execute(vs.db, false)
	return
}

func (vs *viewState) prepareCommit() (err os.Error) {
	// locks all objects
	vs.db.viewstateManager.lockObjects <- vs
	<- vs.wait
	if vs.error != nil {
		return vs.error
	}

	return
}



func (vs *viewState) commit() (err os.Error) {
	mut := &mutation{
		typ: mut_commit_vs,
		vs: vs.id,
	}
	err = vs.db.segmentManager.writeExecuteMutation(vs.token, mut, vs.db)
	if err == nil {
		err = mut.execute(vs.db, false)
	}

	vs.db.viewstateManager.unlockObjects <- vs

	return
}

//
// Real execution of the commit (through the mutation)
//
func (vs *viewState) executeCommit() {
	for objMapKey, obj := range vs.objects {
		if obj.isFlag(obj_flag_new) {
			obj.setFlag(obj_flag_new, false)
			containerName, objKey := explodeObjectKey(objMapKey)
			container, _ := vs.db.getContainer(containerName)
			container.setObject(objKey, obj)
		}
	}

	vs.db.viewstateManager.deleteViewState(vs)
}


func (vs *viewState) rollback() (err os.Error) {
	mut := &mutation{
		typ: mut_rollback_vs,
		vs: vs.id,
	}
	err = vs.db.segmentManager.writeExecuteMutation(vs.token, mut, vs.db)
	if err != nil {
		return err
	}

	vs.db.viewstateManager.unlockObjects <- vs

	err = mut.execute(vs.db, false)
	return
}

//
// Real execution of the rollback (through the mutation)
//
func (vs *viewState) executeRollback() {
	vs.db.viewstateManager.deleteViewState(vs)
}




//
// Locks all objects needed by this viewstate (called by lockManager)
//
func (vs *viewState) lockObjects() (locked, abort bool) {
	locked = true
	abort = true
	vsSegmentPosition := vs.segEntry.absolutePosition() // TODO: replace... we should have a way to get position for readonly


	// TODO: Always lock in the same order!!

	for objMapKey, obj := range vs.objects {
		forWrite := obj.isFlag(obj_flag_new)

		// get the lock
		success, abort, blocker := vs.db.viewstateManager.lockObject(objMapKey, vs, !forWrite)
		if abort {
			locked = false
			abort = true
			return locked, abort
		}
		if !success {
			locked = false

			// we are blocked by another vs, wait in line!
			if len(blocker.next) < cap(blocker.next) {
				abort = false
				blocker.next <- vs
			} else {
				// no more room to wait, we abort
				abort = true
			}

			return locked, abort
		}

		containerName, objKey := explodeObjectKey(objMapKey)
		container, found := vs.db.getContainer(containerName)
		if !found {
			locked = false
			abort = true
			vs.error = os.NewError(fmt.Sprintf("No such container: %s", containerName))
			return
		}

		initObj, found := container.getObject(objKey)
		if found {
			objSeg := vs.db.segmentManager.getSegment(initObj.segment)
			objPos := objSeg.toAbsolutePosition(initObj.position)
			
			if objPos > vsSegmentPosition {
				locked = false
				abort = true
				vs.error = os.NewError(fmt.Sprintf("Object %s/%s has changed during transaction", containerName, objKey))
				return
			}
		}
	}

	return locked, abort
}


//
// Unlocks objects needed by this vs (called from lockManager)
//
func (vs *viewState) unlockObjects() {
	// TODO: UNLOCK IN SAME ORDER THAN LOCKS
	for objMapKey, _ := range vs.objects {
		vs.db.viewstateManager.unlockObject(objMapKey, vs) 
	}
}











//
// Database mutation transaction operation
//
type mutationOperation interface {
	getContainer() string
	getKey() string
	mutateObject(obj *object) (err os.Error)
}


//
// Mutation
//
const (
	mut_create_vs = 1
	mut_commit_vs = 2
	mut_rollback_vs = 3
	mut_obj_op = 4
)

type mutation struct {
	seg		*segment
	segEntry	*segmentEntry

	typ	uint16
	vs	uint32

	// mut_obj_op
	op		mutationOperation
}

func (mut *mutation) execute(db *Db, replay bool) (err os.Error) {
	switch mut.typ {

	case mut_create_vs:
		err = mut.executeCreateVs(db, replay)

	case mut_obj_op:
		err = mut.executeObjectOperation(db, replay)

	case mut_commit_vs:
		err = mut.executeCommitVs(db, replay)

	case mut_rollback_vs:
		err = mut.executeRollbackVs(db, replay)

	}

	return
}

func (mut *mutation) executeCreateVs(db *Db, replay bool) (err os.Error) {
	// only create it if we are on replay
	if replay {
		vs := db.viewstateManager.createViewState(mut.segEntry.token, false, mut.vs)
		vs.segEntry = mut.segEntry
	} else {
		vs := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
		vs.segEntry = mut.segEntry
	}
	return nil
}

func (mut *mutation) executeObjectOperation(db *Db, replay bool) (err os.Error) {
	vs := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if vs == nil  {
		log.Warning("Viewstate not found to execute object mutation: token=%d, vs=%d", mut.segEntry.token, mut.vs)
		return os.NewError(fmt.Sprintf("Unknown ViewState to execute object operation on (token=%d, vs=%d)", mut.segEntry.token, mut.vs))
	}

	container := mut.op.getContainer()
	key := mut.op.getKey()

	newObj := object{
		segment: mut.seg.id,
		position: mut.segEntry.relativePosition(),
	}

	// unless we are replaying, we mutate the object
	if !replay {
		curObj, err := vs.getObject(container, key)
		if err != nil {
			return err
		}

		newObj.data = curObj.data // TODO: CLONE DATA!!
		err = mut.op.mutateObject(&newObj)
	}

	vs.setObject(container, key, newObj)
	return
}

func (mut *mutation) executeCommitVs(db *Db, replay bool) (err os.Error) {
	vs := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if vs == nil {
		return os.NewError("Unknown ViewState to commit")
	}

	vs.executeCommit()
	return
}

func (mut *mutation) executeRollbackVs(db *Db, replay bool) (err os.Error) {
	vs := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if vs == nil {
		return os.NewError("Unknown ViewState to rollback")
	}

	vs.executeRollback()
	return
}


func (mut *mutation) serialize(writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint16(mut.typ)					// type
	if err != nil {
		return
	}

	switch mut.typ {

	case mut_create_vs:
	case mut_commit_vs:
	case mut_rollback_vs:
		err = writer.WriteUint32(mut.vs)				// viewstate
		if err != nil {
			return
		}

	case mut_obj_op:
		err = writer.WriteUint16(operation2id(mut.op.(operation)))	// operation id
		if err != nil {
			return
		}

		var bytes []uint8
		bytes, err = proto.Marshal(mut.op)
		if err != nil {
			return
		}

		err = writer.WriteUint32(uint32(len(bytes)))			// operation size
		if err != nil {
			return
		}

		writer.Write(bytes)						// operation
		if err != nil {
			return
		}
	}

	return
}

func mutationUnserialize(reader typedio.Reader) (mut *mutation, err os.Error) {
	mut = &mutation{}

	mut.typ, err = reader.ReadUint16()					// type
	if err != nil {
		return mut, err
	}


	switch mut.typ {

	case mut_create_vs:
	case mut_commit_vs:
	case mut_rollback_vs:
		mut.vs, err = reader.ReadUint32()				// viewstate
		if err != nil {
			return mut, err
		}


	case mut_obj_op:
		opid, err := reader.ReadUint16()				// operation id
		if err != nil {
			return mut, err
		}
		mut.op = id2operation(opid).(mutationOperation)

		var bytes []byte
		var bytecount uint32

		bytecount, err = reader.ReadUint32()				// operation size
		if err != nil {
			return mut, err
		}

		bytes = make([]byte, int(bytecount))
		reader.Read(bytes)						// operation

		err = proto.Unmarshal(bytes, mut.op)
		if err != nil {
			return mut, err
		}

	default:
		return nil, os.NewError(fmt.Sprintf("Unknown mutation type to unserialize: %d", mut.typ))
	}

	return mut, err
}


