package db

import (
	"os"
	"gostore/tools/typedio"
	proto "goprotobuf.googlecode.com/hg/proto"
)

//
// Transaction view state manager
//
type viewStateManager struct {
	vs	map[uint32]*viewState
	db	*Db
	pos	uint32
}

func newViewStateManager(db *Db) *viewStateManager {
	m := new(viewStateManager)
	m.vs = make(map[uint32]*viewState)
	m.db = db
	return m
}

func (m *viewStateManager) newViewState() *viewState {
	vs := new(viewState)
	vs.db = m.db
	vs.objects = make(map[string]object)

	found := false
	for !found {
		if _, ok := m.vs[m.pos]; !ok {
			found = true
		} else {
			m.pos++
		}
	}
	
	// TODO: Create a "NewviewState" mutation

	m.vs[m.pos] = vs
	vs.id = m.pos
	return vs
}


//
// Transaction view state
//
type viewState struct {
	id		uint32
	db		*Db

	objects		map[string]object
}

func (vs *viewState) mapKey(containerName, key string) string {
	return containerName + "_" + key
}

func (vs *viewState) getObject(containerName, key string) (obj object, err os.Error) { 
	mapkey := vs.mapKey(containerName, key)

	if obj, found := vs.objects[mapkey]; found {
		return obj, nil
	}

	container, found := vs.db.containers[containerName]
	if !found {
		return object{}, os.NewError("Container not found")
	}

	obj, _ = container.objects[key]

	// TODO: if obj is not loaded, load it

	return
}

func (vs *viewState) mutateObject(containerName, key string, op mutationOperation) (err os.Error) {
	// TODO: use the real token!
	mut := &mutation{
		typ: mut_obj_op,
		op: op,
	}
	seg, segEntry := vs.db.segmentManager.writeMutation(0, mut)

	curObj, err := vs.getObject(containerName, key)
	if err != nil {
		return err
	}

	newObj := &object{
		segment: seg.id,
		position: segEntry.relativePosition(),
		data: curObj.data, // TODO: CLONE!!!
	}

	err = mut.executeOnObject(newObj)
	if err != nil {
		return
	}

	vs.objects[vs.mapKey(containerName, key)] = *newObj

	return
}




//
// Database mutation transaction operation
//
type mutationOperation interface {
	mutateObject(vs *viewState, obj *object) (err os.Error)
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
	typ	uint16

	vs	uint32

	// mut_obj_op
	op	mutationOperation
}

func (mut *mutation) executeOnObject(obj *object) (err os.Error) {
	return
}

func (mut *mutation) serialize(writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint16(mut.typ)				// type
	if err != nil {
		return
	}

	switch mut.typ {

	case mut_create_vs:
		err = writer.WriteUint32(mut.vs)			// viewstate
		if err != nil {
			return
		}


	case mut_obj_op:
		err = writer.WriteUint16(operation2id(mut.op.(operation)))		// operation id
		if err != nil {
			return
		}

		var bytes []uint8
		bytes, err = proto.Marshal(mut.op)
		if err != nil {
			return
		}

		err = writer.WriteUint32(uint32(len(bytes)))		// operation size
		if err != nil {
			return
		}

		writer.Write(bytes)					// operation
		if err != nil {
			return
		}

	}



	return
}

func mutationUnserialize(reader typedio.Reader) (mutation mutation, err os.Error) {
	/*
	var bytes []byte
	var bytecount uint32

	bytecount, err = reader.ReadUint32()
	if err != nil {
		return
	}

	bytes = make([]byte, int(bytecount))
	reader.Read(bytes)

	mutation = &TransactionOperation_Set{}
	err = proto.Unmarshal(bytes, mutation)*/

	return
}


