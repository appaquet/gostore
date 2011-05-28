package db

import (
	"os"
	"gostore/log"
	proto "goprotobuf.googlecode.com/hg/proto"
)

var _ = log.Fatal

const (
	op_return = 2
	op_set    = 3
	op_get    = 4
)

type operation interface {
	 executeTransaction(t *Transaction, b *TransactionBlock, vs *viewState) (ret *TransactionReturn)
}


func id2operation(id uint16) operation {
	switch (id) {
	case op_return:
		return &TransactionOperation_Return{}
	case op_set:
		return &TransactionOperation_Set{}
	}
	return nil
}

func operation2id(op operation) uint16 {
	switch op.(type) {
	case *TransactionOperation_Return:
		return op_return
	case *TransactionOperation_Set:
		return op_set
	}
	return 0
}





//
// Return
//
func (op *TransactionOperation_Return) executeTransaction(t *Transaction, b *TransactionBlock, vs *viewState) (ret *TransactionReturn) {
	ret = new(TransactionReturn)
	ret.Returns = make([]*TransactionValue, len(op.Returns))

	for i, obj := range op.Returns {
		if obj.Value != nil {
			ret.Returns[i] = obj.Value
		} else {
			v := b.getRealVar(obj.Variable)
			if v.Value != nil {
				ret.Returns[i] = v.Value
			} else {
				ret.Returns[i] = interface2value(nil)
			}
		}
	}

	return

}

func (op *TransactionOperation_Return) init(t *Transaction, ti *transactionInfo) (err os.Error) {
	return
}

// 
// Set
//
func (op *TransactionOperation_Set) executeTransaction(t *Transaction, b *TransactionBlock, vs *viewState) (ret *TransactionReturn) {
	// TODO: Walk!

	// set into a variable
	if op.Destination.Variable != nil {
		v := b.getRealVar(op.Destination.Variable.Variable)
		if op.Value.Value != nil {
			v.Value = op.Value.Value
		} else {
			valvar := b.getRealVar(op.Value.Variable)
			v.Value = valvar.Value
		}

	// set into a database object
	} else if op.Destination.Object != nil {
		// make sure we don't use variables anymore
		for _, ac := range op.Accessors {
			ac.MakeAbsoluteValue(b)
		}
		op.Value.MakeAbsoluteValue(b)

		partial := false // TODO: if walk, partial = true
		osErr := vs.mutateObject(op, partial)
		if osErr != nil {
			return &TransactionReturn{
				Error: &TransactionError{
					Id: proto.Uint32(0), // TODO: ERRNO
					Message: proto.String(osErr.String()),
				},
			}
		}
	}

	return
}

func (op *TransactionOperation_Set) init(t *Transaction, ti *transactionInfo) (err os.Error) {
	// TODO: token!
	if op.Destination.Object != nil {
		ti.readOnly = false
	}
	return
}

func (op *TransactionOperation_Set) mutateObject(obj *object) (err os.Error) {
	obj.data = op.Value.Value.Value()
	return
}

func (op *TransactionOperation_Set) getContainer() string {
	return op.Destination.Object.Container.Value().(string)
}

func (op *TransactionOperation_Set) getKey() string {
	return op.Destination.Object.Key.Value().(string)
}


// 
// Get
//
func (op *TransactionOperation_Get) executeTransaction(t *Transaction, b *TransactionBlock, vs *viewState) (ret *TransactionReturn) {
	// TODO: Walk!

	var value *TransactionValue

	if op.Source.Variable != nil {
		sourceVar := b.getRealVar(op.Source.Variable.Variable)
		value = sourceVar.Value


	} else if op.Source.Object != nil {
		// make sure we don't use variables anymore
		for _, ac := range op.Accessors {
			ac.MakeAbsoluteValue(b)
		}

		obj, osErr := vs.getObject(op.Source.Object.Container.Value().(string), op.Source.Object.Key.Value().(string), true)
		if osErr != nil {
			return &TransactionReturn{
				Error: &TransactionError{
					Id: proto.Uint32(0), // TODO: ERRNO
					Message: proto.String(osErr.String()),
				},
			}
		}
		value = interface2value(obj.data)
	}

	destVar := b.getRealVar(op.Destination)
	destVar.Value = value

	return
}

func (op *TransactionOperation_Get) init(t *Transaction, ti *transactionInfo) (err os.Error) {
	// TODO: token!
	return
}

