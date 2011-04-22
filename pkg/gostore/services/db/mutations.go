package db

import (
	"gostore/tools/typedio"
	"reflect"
)

type Mutation interface {
	//Unserialize(reader typedio.Reader)
	Serialize(writer typedio.Writer)
}

type DbMutation struct {
	id	uint16
	mut	Mutation
}

type MutationManager struct {
	id2mutations	[]*DbMutation
	type2mutations	map[reflect.Type]*DbMutation
	count		uint16
}

func newMutationManager() *MutationManager {
	mm := new(MutationManager)
	mm.id2mutations = make([]*DbMutation, 65536)
	mm.type2mutations = make(map[reflect.Type]*DbMutation)
	return mm
}

func (mm *MutationManager) GetMutationById(id uint16) *DbMutation {
	return mm.id2mutations[id]
}

func (mm *MutationManager) GetMutation(mut Mutation) *DbMutation {
	return mm.type2mutations[reflect.Typeof(mut)]
}

func (mm *MutationManager) AddMutation(mut Mutation) *DbMutation {
	dbm := &DbMutation{mm.count, mut}
	mm.type2mutations[reflect.Typeof(mut)] = dbm
	mm.id2mutations[mm.count] = dbm
	mm.count++
	return dbm
}









