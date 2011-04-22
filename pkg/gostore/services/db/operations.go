package db

import (
	"os"
	"gostore/tools/typedio"
	"reflect"
)


type OperationManager struct {
	id2operation	[]Operation
	type2operation	map[reflect.Type]Operation
	count		uint16
}

func newOperationManager() *OperationManager {
	om := new(OperationManager)
	om.id2operation = make([]Operation, 65536)
	om.type2operation = make(map[reflect.Type]Operation)
	return om
}


func (om *OperationManager) AddOperation(op Operation) {
	om.type2operation[reflect.Typeof(op)] = op
	om.id2operation[om.count] = op
	om.count++
}



type Operation interface {
	serialize(writer typedio.Writer) (err os.Error)
	unserialize(block *TransBlock, reader typedio.Reader) (op Operation, err os.Error)
}


const (
	ttype_string 	= 's'
	ttype_int32	= 'i'
	ttype_int64	= 'I'
	ttype_var	= 'v'
)

func serializeData(writer typedio.Writer, value interface{}) (err os.Error) {
	err = nil
	switch f := value.(type) {
	case string:
		err = writer.WriteUint8(ttype_string)
		if err != nil {
			return
		}
		err = writer.WriteString(value.(string))
		if err != nil {
			return
		}
	case *TransVar:
		err = writer.WriteUint8(ttype_var)
		if err != nil {
			return
		}
	}
	return
}

func unserializeData(block *TransBlock, reader typedio.Reader) (val interface{}, err os.Error) {
	typ, err := reader.ReadUint8()
	if err != nil {
		return
	}

	switch typ {
	case ttype_string:
		val, err = reader.ReadString()
	default:
		err = os.NewError("Invalid data type")
	}

	return
}


type OperationSet struct {
	data	[]interface{}
	value	interface{}
}

func (o *OperationSet) serialize(writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint8(uint8(len(o.data)))			// data count
	if err != nil {
		return
	}

	for _, val := range o.data {
		err = serializeData(writer, val)			// data
		if err != nil {
			return
		}
	}
	
	err = serializeData(writer, o.value)				// value

	return
}

func (o *OperationSet) unserialize(block *TransBlock, reader typedio.Reader) (op Operation, err os.Error) {
	var dataCount uint8
	dataCount, err = reader.ReadUint8()				// data count

	_ = dataCount

	return
}
