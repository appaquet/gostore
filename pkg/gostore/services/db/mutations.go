package db

import (
	"gostore/tools/typedio"
	"reflect"
)

var mutations = newMutationManager()

//
// Database mutations manager
//
type MutationManager struct {
	id2mutations	[]Mutation
	type2id		map[reflect.Type]uint16
	count		uint16
}

func newMutationManager() *MutationManager {
	mm := new(MutationManager)
	mm.id2mutations = make([]Mutation, 65536)
	mm.type2id = make(map[reflect.Type]uint16)
	return mm
}

func (mm *MutationManager) GetMutation(id uint16) Mutation {
	return mm.id2mutations[id]
}

func (mm *MutationManager) GetId(mut Mutation) uint16 {
	return mm.type2id[reflect.Typeof(mut)]
}

func (mm *MutationManager) AddMutation(mut Mutation) {
	mm.type2id[reflect.Typeof(mut)] = mm.count
	mm.id2mutations[mm.count] = mut
	mm.count++
}



//
// Database mutation
//
type Mutation interface {
	//Unserialize(reader typedio.Reader)
	Serialize(writer typedio.Writer)
}




