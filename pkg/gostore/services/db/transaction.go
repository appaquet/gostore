package db

import (
	proto "goprotobuf.googlecode.com/hg/proto"
	"fmt"
	"os"
)

const (
	val_int    = 5
	val_bool   = 6
	val_float  = 7
	val_double = 8
	val_string = 9
	val_bytes  = 10
	val_null   = 11

	err_incomplete = 1
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

func NewTransaction(cb func(b *TransactionBlock)) (t *Transaction) {
	t = NewEmptyTransaction()
	b := t.newBlock()
	t.MasterBlock = b.Id
	cb(b)
	return t
}

func (t *Transaction) newBlock() (b *TransactionBlock) {
	b = new(TransactionBlock)
	b.Id = proto.Uint32(uint32(len(t.Blocks)))
	t.Blocks = append(t.Blocks, b)
	return
}

func (t *Transaction) init() (err os.Error) {
	for _, b := range t.Blocks {
		err = b.init()
		if err != nil {
			return
		}
	}

	return
}

func (t *Transaction) execute(vs *viewState) (ret *TransactionReturn) {
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
			Type:        proto.Uint32(val_string),
			StringValue: proto.String(f),
		}
	}

	return &TransactionValue{
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


//type TransactionObject struct {
//	Value	*TransactionValue	"PB(bytes,1,opt,name=value)"
//	Variable	*TransactionVariable	"PB(bytes,2,opt,name=variable)"
//	XXX_unrecognized	[]byte
//}
func (object *TransactionObject) MakeAbsoluteValue(b *TransactionBlock) {
	if object.Value != nil {
		return
	}

	v := b.getRealVar(object.Variable)
	object.Variable = nil
	object.Value = v.Value
}


func data2destination(data []interface{}) (dest *TransactionOperationDestination, rest []interface{}) {
	if v, ok := data[0].(*TransactionVariable); ok {
		dest = &TransactionOperationDestination{
			Variable: &TransactionOperationDestination_Variable{
				Variable: v,
			},
		}
		rest = data[1:]
	} else {
		if len(data) >= 2 {
			container, ok1 := data[0].(string)
			key, ok2 := data[1].(string)
			if !ok1 || !ok2 {
				panic("Invalid destination")
			} else {
				dest = &TransactionOperationDestination{
					Object: &TransactionOperationDestination_Object{
						Container: interface2value(container),
						Key:       interface2value(key),
					},
				}
				rest = data[2:]
			}

		} else {
			panic("Invalid destination")
		}
	}
	return

}


//type TransactionBlock struct {
//	Id			*uint32			"PB(varint,1,req,name=id)"
//	Parent			*TransactionBlock	"PB(bytes,2,opt,name=parent)"
//	Operations		[]*TransactionOperation	"PB(bytes,5,rep,name=operations)"
//	VariableCount		*uint32			"PB(varint,6,opt,name=variable_count)"
//	Variables		[]*TransactionVariable	"PB(bytes,7,rep,name=variables)"
//	XXX_unrecognized	[]byte
//}
func (b *TransactionBlock) init() (err os.Error) {
	if b.VariableCount != nil && len(b.Variables) != int(*b.VariableCount) {
		newVars := make([]*TransactionVariable, int(*b.VariableCount))
		for _, vr := range b.Variables {
			newVars[*vr.Id] = vr
		}

		for i:=uint32(0); i < *b.VariableCount; i++ {
			if newVars[i] == nil {
				newVars[i] = &TransactionVariable{
					Block: b.Id,
					Id: proto.Uint32(i),
				}
			}
		}
	}

	return
}


func (b *TransactionBlock) Set(data ...interface{}) {
	if len(data) < 2 {
		panic("You must at least specify destination and value")
	}

	dest, rest := data2destination(data)

	acs := make([]*TransactionObject, len(rest)-1)
	for i := 0; i < len(rest)-1; i++ {
		acs[i] = interface2object(rest[i])
	}

	val := interface2object(rest[len(rest)-1])

	op := &TransactionOperation{
		Type: proto.Uint32(op_set),
		Set: &TransactionOperation_Set{
			Destination: dest,
			Accessors:   acs,
			Value:       val,
		},
	}
	b.addOperation(op)
}

func (b *TransactionBlock) Return(data ...interface{}) {
	op := &TransactionOperation{
		Type:   proto.Uint32(op_return),
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

func (b *TransactionBlock) Get(data ...interface{}) *TransactionVariable {
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

func (b *TransactionBlock) execute(t *Transaction, vs *viewState) (ret *TransactionReturn) {
	ret = new(TransactionReturn)

	for _, op := range b.Operations {
		switch *op.Type {

		case op_set:
			opRet := op.Set.executeTransaction(t, b, vs)
			if opRet != nil && opRet.Error != nil {
				ret.Error = opRet.Error
				return
			}
			
		case op_return:
			return op.Return.executeTransaction(t, b, vs)

		}

	}

	return
}
