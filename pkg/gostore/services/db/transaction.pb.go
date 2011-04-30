// Code generated by protoc-gen-go from "transaction.proto"
// DO NOT EDIT!

package db

import proto "goprotobuf.googlecode.com/hg/proto"
import "math"
import "os"

// Reference proto, math & os imports to suppress error if they are not otherwise used.
var _ = proto.GetString
var _ = math.Inf
var _ os.Error


type Transaction struct {
	Id	*uint64	"PB(varint,1,opt,name=id)"
	Origin	*uint32	"PB(varint,2,opt,name=origin)"
	Blocks	[]*TransactionBlock	"PB(bytes,10,rep,name=blocks)"
	MasterBlock	*uint32	"PB(varint,11,req,name=master_block)"
	XXX_unrecognized	[]byte
}
func (this *Transaction) Reset() {
	*this = Transaction{}
}

type TransactionReturn struct {
	Error	*TransactionError	"PB(bytes,1,opt,name=error)"
	Returns	[]*TransactionValue	"PB(bytes,2,rep,name=returns)"
	XXX_unrecognized	[]byte
}
func (this *TransactionReturn) Reset() {
	*this = TransactionReturn{}
}

type TransactionError struct {
	Id	*uint32	"PB(varint,1,req,name=id)"
	Message	*string	"PB(bytes,2,req,name=message)"
	XXX_unrecognized	[]byte
}
func (this *TransactionError) Reset() {
	*this = TransactionError{}
}

type TransactionBlock struct {
	Id	*uint32	"PB(varint,1,req,name=id)"
	Parent	*TransactionBlock	"PB(bytes,2,opt,name=parent)"
	Operations	[]*TransactionOperation	"PB(bytes,5,rep,name=operations)"
	Variables	[]*TransactionVariable	"PB(bytes,6,rep,name=variables)"
	XXX_unrecognized	[]byte
}
func (this *TransactionBlock) Reset() {
	*this = TransactionBlock{}
}

type TransactionVariable struct {
	Block	*uint32	"PB(varint,1,req,name=block)"
	Id	*uint32	"PB(varint,2,req,name=id)"
	Value	*TransactionValue	"PB(bytes,3,opt,name=value)"
	XXX_unrecognized	[]byte
}
func (this *TransactionVariable) Reset() {
	*this = TransactionVariable{}
}

type TransactionValue struct {
	Type	*uint32	"PB(varint,1,req,name=type)"
	IntValue	*int64	"PB(varint,5,opt,name=int_value)"
	BoolValue	*bool	"PB(varint,6,opt,name=bool_value)"
	FloatValue	*float32	"PB(fixed32,7,opt,name=float_value)"
	DoubleValue	*float64	"PB(fixed64,8,opt,name=double_value)"
	StringValue	*string	"PB(bytes,9,opt,name=string_value)"
	BytesValue	[]byte	"PB(bytes,10,opt,name=bytes_value)"
	JsonValue	*string	"PB(bytes,11,opt,name=json_value)"
	XXX_unrecognized	[]byte
}
func (this *TransactionValue) Reset() {
	*this = TransactionValue{}
}

type TransactionObject struct {
	Value	*TransactionValue	"PB(bytes,1,opt,name=value)"
	Variable	*TransactionVariable	"PB(bytes,2,opt,name=variable)"
	XXX_unrecognized	[]byte
}
func (this *TransactionObject) Reset() {
	*this = TransactionObject{}
}

type TransactionOperation struct {
	Type	*uint32	"PB(varint,1,req,name=type)"
	Return	*TransactionOperation_Return	"PB(group,2,opt,name=return)"
	Setvariable	*TransactionOperation_SetVariable	"PB(group,3,opt,name=setvariable)"
	XXX_unrecognized	[]byte
}
func (this *TransactionOperation) Reset() {
	*this = TransactionOperation{}
}

type TransactionOperation_Return struct {
	Returns	[]*TransactionObject	"PB(bytes,1,rep,name=returns)"
	XXX_unrecognized	[]byte
}
func (this *TransactionOperation_Return) Reset() {
	*this = TransactionOperation_Return{}
}

type TransactionOperation_SetVariable struct {
	Variable	*TransactionVariable	"PB(bytes,1,req,name=variable)"
	Accessors	[]*TransactionObject	"PB(bytes,2,rep,name=accessors)"
	Value	*TransactionObject	"PB(bytes,3,req,name=value)"
	XXX_unrecognized	[]byte
}
func (this *TransactionOperation_SetVariable) Reset() {
	*this = TransactionOperation_SetVariable{}
}

func init() {
}
