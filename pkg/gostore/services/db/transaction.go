package db

import (
	"gostore/tools/typedio"
	"os"
)


type Transaction struct {
	id      	uint64
	mBlock		*TransBlock
	bCount		uint16
}

func NewEmptyTransaction() (t *Transaction) {
	t = new(Transaction)
	t.mBlock = t.newBlock(true)
	return t
}

func NewTransaction(cb func (b *TransBlock)) (t *Transaction) {
	t = NewEmptyTransaction()
	t.mBlock = t.newBlock(true)
	cb(t.mBlock)
	return t
}

func (t *Transaction) newBlock(assignId bool) *TransBlock {
	b := new(TransBlock)
	b.vars = make([]*TransVar,0)
	b.ops = make([]Operation,0)

	if assignId {
		b.id = t.bCount
		t.bCount++
	}

	return b
}

func (t *Transaction) Serialize(writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint64(t.id)				// transaction id
	if err != nil {
		return
	}

	err = writer.WriteUint16(t.bCount)			// block count
	if err != nil {
		return
	}

	err = t.mBlock.serialize(writer)			// master block
	if err != nil {
		return
	}

	return
}


func (t *Transaction) Unserialize(reader typedio.Reader) (err os.Error) {
	t.id, err = reader.ReadUint64()				// transaction id
	if err != nil {
		return
	}

	t.bCount, err = reader.ReadUint16()			// block count
	if err != nil {
		return
	}

	err = t.mBlock.unserialize(reader)			// master block
	if err != nil {
		return
	}

	return
}






type TransVar struct {
	block	uint16
	id	uint16
	value	interface{}
}


type TransBlock struct {
	transaction	*Transaction
	parent		*TransBlock

	id		uint16
	vars		[]*TransVar
	ops		[]Operation
}

func (b *TransBlock) Set(data... interface{}) {
	op := &OperationSet{data[0:len(data)-2], data[len(data)-1]}
	b.ops = append(b.ops, op)
}

func (b *TransBlock) Get(data... interface{}) *TransVar {
	return nil
}

func (b *TransBlock) NewVar() *TransVar {
	vr := &TransVar{ id: uint16(len(b.vars)), block: b.id, value: nil }
	b.vars = append(b.vars, vr)
	return vr
}

func (b *TransBlock) serializeVariable(v *TransVar, writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint16(v.block)		// block
	if err != nil {
		return
	}

	err = writer.WriteUint16(v.id)			// id

	return
}

func (b *TransBlock) unserializeVariable(reader typedio.Reader) (v *TransVar, err os.Error) {
	v = &TransVar{0,0,nil}

	v.block, err = reader.ReadUint16()		// block
	if err != nil {
		return
	}

	v.id, err = reader.ReadUint16()			// id
	b.vars[v.id] = v

	return
}


func (b *TransBlock) serialize(writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint16(b.id)				// block id
	if err != nil {
		return
	}

	err = writer.WriteUint16(uint16(len(b.vars)))		// variable count
	if err != nil {
		return
	}

	err = writer.WriteUint16(uint16(len(b.ops)))		// operation count
	if err != nil {
		return
	}

	for _, op := range b.ops {
		// TODO: Write op id

		err = op.serialize(writer)			// operation
		if err != nil {
			return
		}
	}

	return
}


func (b *TransBlock) unserialize(reader typedio.Reader) (err os.Error) {
	b.id, err = reader.ReadUint16()				// block id
	if err != nil {
		return
	}

	var nbVars uint16
	nbVars, err = reader.ReadUint16()			// variable count
	if err != nil {
		return
	}
	b.vars = make([]*TransVar, nbVars)

	var nbOps uint16
	nbOps, err = reader.ReadUint16()			// operation count
	if err != nil {
		return
	}
	//TODO: b.ops = make([]Operation, nbOps)

	for i:=uint16(0); i<nbOps; i++ {
		
	}

	return
}

