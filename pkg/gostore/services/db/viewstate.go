package db

import (
	"os"
	"gostore/tools/typedio"
	proto "goprotobuf.googlecode.com/hg/proto"
	"sync"
	"gostore/log"
)

var _ = log.Debug

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

	locks		map[string]viewStateLock
}

type viewStateLock struct {
	readonly	bool
	vs		*viewState
}

func newViewStateManager(db *Db) *viewStateManager {
	m := new(viewStateManager)
	m.vs = make(map[uint64]*viewState)
	m.db = db
	m.vsMutex = new(sync.Mutex)

	m.locks = make(map[string]viewStateLock)

	m.prepare = make(chan *viewState, 1)
	m.commit = make(chan *viewState, 1)
	m.rollback = make(chan *viewState, 1)
	go m.commitorRoutine()

	return m
}

func (m *viewStateManager) makeKey(token Token, vsId uint32) uint64 {
	return (10000000000*uint64(token)) + uint64(vsId)
}

func (m *viewStateManager) generateId(token Token) (id uint32) {
	found := false
	for !found {
		key := m.makeKey(token, m.nextId)
		if _, ok := m.vs[key]; !ok {
			found = true
		} else {
			m.nextId++
		}
	}

	return m.nextId
}

func (m *viewStateManager) createViewState(token Token, genId bool, id uint32) *viewState {
	m.vsMutex.Lock()
	defer m.vsMutex.Unlock()

	vs := &viewState{
		db: m.db,
		objects: make(map[string]object),
		token: token,
		id: id,
		next: make(chan *viewState),
		wait: make(chan bool, 1),
	}

	var key uint64
	if genId {
		key = m.makeKey(token, m.generateId(token))
	} else {
		key = m.makeKey(token, id)
	}
	m.vs[key] = vs

	if genId {
		// write viewstate creation to disk
		mut := &mutation{
			typ:mut_create_vs,
			vs:m.nextId,
		}
		m.db.segmentManager.writeMutation(vs.token, mut)
	}

	return vs
}

func (m *viewStateManager) getViewState(token Token, id uint32) (vs *viewState, found bool) {
	m.vsMutex.Lock()
	defer m.vsMutex.Unlock()
		
	key := m.makeKey(token, id)
	vs, found = m.vs[key]

	return
}

func (m *viewStateManager) commitorRoutine() {
	var next *viewState

	for {
		if next != nil {
			if next.executePrepareCommit() {
				next.wait <- true
			}
		}

		select {
		case vs := <- m.prepare:
			if vs.executePrepareCommit() {
				vs.wait <- true
			}

		case vs := <- m.commit:
			vs.executeCommit()
			if len(vs.next) > 0 {
				next = <- vs.next
			}

			// remove from map
			key := m.makeKey(vs.token, vs.id)
			m.vsMutex.Lock()
			m.vs[key] = nil, false
			m.vsMutex.Unlock()

			vs.wait <- true

		case vs := <- m.rollback:
			vs.executeRollback()
			if len(vs.next) > 0 {
				next = <- vs.next
			}

			// remove from map
			key := m.makeKey(vs.token, vs.id)
			m.vsMutex.Lock()
			m.vs[key] = nil, false
			m.vsMutex.Unlock()

			vs.wait <- true
		}

	}
}










//
// Transaction view state
//
type viewState struct {
	token		Token
	id		uint32

	db		*Db

	objects		map[string]object

	next		chan *viewState
	error		os.Error
	wait		chan bool
}

func (vs *viewState) mapKey(containerName, key string) string {
	return containerName + "_" + key // TODO: Make sure we don't have a _ at the end of containerName or beginning of key
}

func (vs *viewState) getObject(containerName, key string) (obj object, err os.Error) { 
	mapkey := vs.mapKey(containerName, key)

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

	return
}

func (vs *viewState) setObject(containerName, key string, obj object) {
	mapkey := vs.mapKey(containerName, key)
	vs.objects[mapkey] = obj
}



func (vs *viewState) mutateObject(op mutationOperation) (err os.Error) {
	mut := &mutation{
		typ: mut_obj_op,
		op: op,
		vs: vs.id,
	}
	err = vs.db.segmentManager.writeMutation(vs.token, mut)

	return
}

func (vs *viewState) prepareCommit() (err os.Error) {
	mut := &mutation{
		typ: mut_prepare_vs,
		vs: vs.id,
	}
	err = vs.db.segmentManager.writeMutation(vs.token, mut)

	return
}

func (vs *viewState) commit() (err os.Error) {
	mut := &mutation{
		typ: mut_commit_vs,
		vs: vs.id,
	}
	err = vs.db.segmentManager.writeMutation(vs.token, mut)

	return
}

func (vs *viewState) executePrepareCommit() (success bool) {
	log.Error("Preparing to commit viewstate %d for token %d\n", vs.id, vs.token)
	// TODO: if one vs has a lock we need, we stop

	// TODO: foreach object
	// TODO:     chec if it's still old versino

	// TODO: if not ok
	// TODO:     vs.commit_error = ..

	return true
}

func (vs *viewState) executeCommit() {
	log.Error("Commiting viewstate %d for token %d\n", vs.id, vs.token)
}

func (vs *viewState) executeRollback() {
	log.Error("Rollbacking viewstate %d for token %d\n", vs.id, vs.token)
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
		// only create on replay since it's already created otherwize
		if replay {
			err = mut.executeCreateVs(db)
		}

	case mut_obj_op:
		err = mut.executeObjectOperation(db, replay)

	case mut_prepare_vs:
		err = mut.executePrepareVs(db)

	case mut_commit_vs:
		err = mut.executeCommitVs(db)

	}

	return
}

func (mut *mutation) executeCreateVs(db *Db) (err os.Error) {

	// TODO: Return error if the viewstate already exists
	db.viewstateManager.createViewState(mut.segEntry.token, false, mut.vs)
	return nil
}

func (mut *mutation) executeObjectOperation(db *Db, replay bool) (err os.Error) {
	vs, found := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if !found {
		return os.NewError("Unknown ViewState to execute object operation on")
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

func (mut *mutation) executePrepareVs(db *Db) (err os.Error) {
	vs, found := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if !found {
		return os.NewError("Unknown ViewState to prepare for commit")
	}

	db.viewstateManager.prepare <- vs

	if vs.error != nil {
		return vs.error
	}

	return
}

func (mut *mutation) executeCommitVs(db *Db) (err os.Error) {
	vs, found := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if !found {
		return os.NewError("Unknown ViewState to commit")
	}

	db.viewstateManager.commit <- vs

	if vs.error != nil {
		return vs.error
	}

	return
}

func (mut *mutation) executeRollbackVs(db *Db) (err os.Error) {
	vs, found := db.viewstateManager.getViewState(mut.segEntry.token, mut.vs)
	if !found {
		return os.NewError("Unknown ViewState to rollback")
	}

	db.viewstateManager.rollback <- vs

	if vs.error != nil {
		return vs.error
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

	typ, err := reader.ReadUint16()						// type
	if err != nil {
		return mut, err
	}


	switch typ {

	case mut_create_vs:
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
	}

	return mut, err
}


