package db

const (
	op_return = 2
	op_setvar = 3
)

func (op *TransactionOperation_SetVariable) execute(t *Transaction, b *TransactionBlock, vs *ViewState) (err *TransactionError) {
	// TODO: Walk!
	v := b.getRealVar(op.Variable)
	if op.Value.Value != nil{
		v.Value = op.Value.Value	
	} else {
		valvar := b.getRealVar(op.Value.Variable)
		v.Value = valvar.Value
	}

	return
}

func (op *TransactionOperation_Return) execute(t *Transaction, b *TransactionBlock, vs *ViewState) (ret *TransactionReturn) {
	ret = new(TransactionReturn)
	ret.Returns = make([]*TransactionValue, len(op.Returns))

	for i, obj := range op.Returns {
		if obj.Value != nil {
			ret.Returns[i] = obj.Value
		} else {
			v := b.getRealVar(obj.Variable)
			ret.Returns[i] = v.Value
		}
	}

	return

}
