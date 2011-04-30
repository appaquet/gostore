package db

import (
	proto "goprotobuf.googlecode.com/hg/proto"
	"fmt"
)

const (
	val_int = 5
	val_bool = 6
	val_float = 7
	val_double = 8
	val_string = 9
	val_bytes = 10
	val_json = 11
	val_null = 12
)


//type Transaction struct {
//	Id	*uint64	"PB(varint,1,opt,name=id)"
//	Origin	*uint32	"PB(varint,2,opt,name=origin)"
//	Blocks	[]*Block	"PB(bytes,10,rep,name=blocks)"
//	MasterBlock	*uint32	"PB(varint,11,req,name=master_block)"
//	XXX_unrecognized	[]byte
//}

func NewEmptyTransaction() (t *Transaction) {
	t = new(Transaction)
	return t
}

func NewTransaction(cb func (b *TransactionBlock)) (t *Transaction) {
	t = NewEmptyTransaction()
	b := t.newBlock()
	t.MasterBlock = b.Id
	cb (b)
	return t
}

func (t *Transaction) newBlock() (b *TransactionBlock) {
	b = new(TransactionBlock)
	b.Id = proto.Uint32(uint32(len(t.Blocks)))
	t.Blocks = append(t.Blocks, b)
	return
}

func (t *Transaction) execute(vs *ViewState) (ret *TransactionReturn)  {
	b := t.Blocks[int(*t.MasterBlock)]
	return b.execute(t, vs)
}



//type TransactionValue struct {
//	Type	*uint32	"PB(varint,1,req,name=type)"
//	IntValue	*int64	"PB(varint,5,opt,name=int_value)"
//	BoolValue	*bool	"PB(varint,6,opt,name=bool_value)"
//	FloatValue	*float32	"PB(fixed32,7,opt,name=float_value)"
//	DoubleValue	*float64	"PB(fixed64,8,opt,name=double_value)"
//	StringValue	*string	"PB(bytes,9,opt,name=string_value)"
//	BytesValue	[]byte	"PB(bytes,10,opt,name=bytes_value)"
//	JsonValue	*string	"PB(bytes,11,opt,name=json_value)"
//	XXX_unrecognized	[]byte
//}
func (v *TransactionValue) String() string {
	return fmt.Sprintf("%v", v.Value())
}

func (v *TransactionValue) Value() interface{} {
	if v == nil {
		return nil
	}

	switch *v.Type {
	
	case val_string:
		return *v.StringValue

	}

	return nil
}

func interface2value(data interface{}) *TransactionValue {
	switch f := data.(type) {

	case string:
		return &TransactionValue{
			Type: proto.Uint32(val_string),
			StringValue: proto.String(f),
		}
	}

	return &TransactionValue {
		Type: proto.Uint32(val_null),
	}
}

func interface2object(data interface{}) *TransactionObject {
	if v, ok := data.(*TransactionVariable); ok {
		return &TransactionObject{
			Variable: v,
		}
	} 

	return &TransactionObject{
		Value: interface2value(data),
	}
}






func (b *TransactionBlock) SetVar(variable *TransactionVariable, data... interface{}) {
	if len(data) == 0 {
		panic("You must specify at least a value")	
	}
	if variable == nil {
		panic("Variable must not be null")	
	}


	acs := make([]*TransactionObject, len(data)-1)
	for i:=0; i<len(data)-1; i++ {
		acs[i] = interface2object(data[i])
	}

	val := interface2object(data[len(data)-1])

	op := &TransactionOperation{
		Type: proto.Uint32(op_setvar),
		Setvariable: &TransactionOperation_SetVariable {
			Variable: variable,
			Accessors: acs,
			Value: val,
		},
	}
	b.addOperation(op)
}

func (b *TransactionBlock) Set(data ...interface{}) {
}

func (b *TransactionBlock) Return(data... interface{}) {
	op := &TransactionOperation{
		Type: proto.Uint32(op_return),
		Return: &TransactionOperation_Return{},
	}
	op.Return.Returns = make([]*TransactionObject, len(data))

	for i, obj := range data {
		if vr, ok := obj.(*TransactionVariable); ok {
			op.Return.Returns[i] = &TransactionObject{
				Variable: vr,
			}
		} else {
			val := interface2value(obj)
			if val == nil {
				panic("Unknown return object")
			}

			op.Return.Returns[i] = &TransactionObject{
				Value: val,
			}
		}
	}

	b.addOperation(op)
}

func (b *TransactionBlock) Get(data... interface{}) *TransactionVariable {
	return nil
}

func (b *TransactionBlock) NewVar() *TransactionVariable {
	v := new(TransactionVariable)

	// TODO: we should only add variables that have values
	v.Block = b.Id
	v.Id = proto.Uint32(uint32(len(b.Variables)))
	b.Variables = append(b.Variables, v)
	return v
}

func (b *TransactionBlock) getRealVar(v *TransactionVariable) *TransactionVariable {
	// TODO: check if it's the right block. Else, pass to the parent
	return b.Variables[*v.Id]
}


func (b *TransactionBlock) addOperation(op *TransactionOperation) {
	b.Operations = append(b.Operations, op)
}

func (b *TransactionBlock) execute(t *Transaction, vs *ViewState) (ret *TransactionReturn) {
	ret = new(TransactionReturn)

	for _, op := range b.Operations {
		switch *op.Type {

		case op_setvar:
			ret.Error = op.Setvariable.execute(t, b, vs)
			if ret.Error != nil {
				return
			}

		case op_return:
			return op.Return.execute(t, b, vs)

		}

	}

	return
}
