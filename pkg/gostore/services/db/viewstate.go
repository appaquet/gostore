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
	return (10000000000*uint64(token)) + uint64(vsId)
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

	prepare		chan *viewState
	commit		chan *viewState
	rollback	chan *viewState

	objectsLocks	map[string]objectLock
}

func newViewStateManager(db *Db) *viewStateManager {
	m := new(viewStateManager)
	m.db = db

	m.vsMutex = new(sync.Mutex)
	m.vs = make(map[uint64]*viewState) // Sync via vsMutex


	m.prepare = make(chan *viewState, 1)
	m.commit = make(chan *viewState, 1)
	m.rollback = make(chan *viewState, 1)
	m.objectsLocks = make(map[string]objectLock) // Sync via commitor
	go m.commitorRoutine()

	return m
}

func (m *viewStateManager) createViewState(token Token, genId bool, id uint32) *viewState {
	m.vsMutex.Lock()
	if genId {
		id = m.generateId(token)
	}

	vs := &viewState{
		db: m.db,
		objects: make(map[string]object),
		token: token,
		id: id,
		next: make(chan *viewState),
		wait: make(chan bool, 100), // TODO: const
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
	}

	return vs
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

func (m *viewStateManager) generateId(token Token) (id uint32) {
	found := false
	for !found {
		key := makeVsKey(token, m.nextId)
		if _, ok := m.vs[key]; !ok {
			found = true
		} else {
			m.nextId++
		}
	}

	return m.nextId
}

func (m *viewStateManager) commitorRoutine() {
	var next *viewState

	for {
		if next != nil {
			prepared, abort := next.executePrepareCommit(m)
			if abort || prepared {
				next.wait <- true
			}
		}

		select {
		case vs := <- m.prepare:
			prepared, abort := vs.executePrepareCommit(m)
			if prepared || abort {
				vs.wait <- true
			}

		case vs := <- m.commit:
			vs.executeCommit(m)

			// another vs was waiting for this vs to commit, push it next in line
			if len(vs.next) > 0 {
				next = <- vs.next
			}

			// remove from map
			key := makeVsKey(vs.token, vs.id)
			m.vsMutex.Lock()
			m.vs[key] = nil, false
			m.vsMutex.Unlock()

			vs.wait <- true

		case vs := <- m.rollback:
			vs.executeRollback(m)
			if len(vs.next) > 0 {
				next = <- vs.next
			}

			// remove from map
			key := makeVsKey(vs.token, vs.id)
			m.vsMutex.Lock()
			m.vs[key] = nil, false
			m.vsMutex.Unlock()

			vs.wait <- true
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


	// TODO: Thread safe!
	container, found := vs.db.containers[containerName]
	if !found {
		return object{}, os.NewError("Container not found")
	}

	obj, _ = container.objects[key]

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

	return
}


func (vs *viewState) prepareCommit() (err os.Error) {
	mut := &mutation{
		typ: mut_prepare_vs,
		vs: vs.id,
	}
	err = vs.db.segmentManager.writeExecuteMutation(vs.token, mut, vs.db)
	if err != nil {
		return err
	}

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
	if err != nil {
		return err
	}

	<- vs.wait
	if vs.error != nil {
		return vs.error
	}

	return
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

	<- vs.wait
	if vs.error != nil {
		return vs.error
	}

	return
}

//
// Execution of the prepare commit (thread safely called from manager)
//
func (vs *viewState) executePrepareCommit(m *viewStateManager) (prepared, abort bool) {
	prepared = true
	abort = true
	vsSegmentPosition := vs.segEntry.absolutePosition()


	// TODO: Always lock in the same order!!

	for objMapKey, obj := range vs.objects {
		forWrite := obj.isFlag(obj_flag_new)

		// get the lock
		success, abort, blocker := m.lockObject(objMapKey, vs, !forWrite)
		if abort {
			prepared = false
			abort = true
			return prepared, abort
		}
		if !success {
			prepared = false

			// we are blocked by another vs, wait in line!
			if len(blocker.next) < cap(blocker.next) {
				abort = false
				blocker.next <- vs
			} else {
				// no more room to wait, we abort
				abort = true
			}

			return prepared, abort
		}


		// TODO: THREAD SAFE!
		containerName, objKey := explodeObjectKey(objMapKey)
		container, found := vs.db.containers[containerName]
		if !found {
			prepared = false
			abort = true
			vs.error = os.NewError(fmt.Sprintf("No such container: %s", containerName))
			return
		}

		initObj, found := container.objects[objKey]
		if found {
			objSeg := vs.db.segmentManager.getSegment(initObj.segment)
			objPos := objSeg.toAbsolutePosition(initObj.position)
			
			if objPos > vsSegmentPosition {
				prepared = false
				abort = true
				vs.error = os.NewError(fmt.Sprintf("Object %s/%s has changed during transaction", containerName, objKey))
				return
			}
		}
	}

	return prepared, abort
}

//
// Execution of the commit (thread safely called from manager)
//
func (vs *viewState) executeCommit(m *viewStateManager) {
	for objMapKey, obj := range vs.objects {
		if obj.isFlag(obj_flag_new) {
			// TODO: Thread safe
			obj.setFlag(obj_flag_new, false)
			containerName, objKey := explodeObjectKey(objMapKey)
			container := vs.db.containers[containerName]
			container.objects[objKey] = obj
		}


		// unlock the object
		m.unlockObject(objMapKey, vs) 
	}
}

//
// Execution of the rollback (thread safely called from manager)
//
func (vs *viewState) executeRollback(m *viewStateManager) {
	for objMapKey, _ := range vs.objects {
		// unlock the object
		m.unlockObject(objMapKey, vs)
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
	mut_prepare_vs = 2
	mut_commit_vs = 3
	mut_rollback_vs = 4
	mut_obj_op = 5
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

	case mut_prepare_vs:
		err = mut.executePrepareVs(db, replay)

	case mut_commit_vs:
		err = mut.executeCommitVs(db, replay)

	case mut_rollback_vs:
		err = mut.executeRollbackVs(db, replay)

	}

	return
}

func (mut *mutation) executeCreateVs(db *Db, replay bool) (err os.Error) {
	// TODO: Return error if the viewstate already exists

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

func (mut *mutation) executePrepareVs(db *Db, replay bool) (err os.Error) {
	vs := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if vs == nil {
		return os.NewError("Unknown ViewState to prepare for commit")
	}

	db.viewstateManager.prepare <- vs

	if replay {
		<-vs.wait
	}

	return
}

func (mut *mutation) executeCommitVs(db *Db, replay bool) (err os.Error) {
	vs := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if vs == nil {
		return os.NewError("Unknown ViewState to commit")
	}

	db.viewstateManager.commit <- vs

	if replay {
		<-vs.wait
	}

	return
}

func (mut *mutation) executeRollbackVs(db *Db, replay bool) (err os.Error) {
	vs := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if vs == nil {
		return os.NewError("Unknown ViewState to rollback")
	}

	db.viewstateManager.rollback <- vs

	if replay {
		<-vs.wait
	}

	return
}


func (mut *mutation) serialize(writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint16(mut.typ)					// type
	if err != nil {
		return
	}

	switch mut.typ {

	case mut_create_vs:
	case mut_prepare_vs:
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
	case mut_prepare_vs:
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


