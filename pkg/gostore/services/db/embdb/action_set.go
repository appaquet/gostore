package embdb

import (
	"gostore/tools/typedio"
	"os"
	"json"
)

type ActionSet struct {
	transaction *Transaction

	namespace string
	objmap    string
	objkey    string
	key       []string

	data interface{}

	retVal *RetVal
}

func NewActionSetEmpty(transaction *Transaction) *ActionSet {
	a := new(ActionSet)
	a.transaction = transaction
	return a
}

func NewActionSet(transaction *Transaction, retVal *RetVal, data interface{}, namespace, objmap, objkey string, key ...string) Action {
	return Action(&ActionSet{
		transaction,
		namespace,
		objmap,
		objkey,
		key,
		data,
		retVal})
}

func (a *ActionSet) Id() byte {
	return action_set
}

func (a *ActionSet) ReadOnly() bool {
	return false
}

func (a *ActionSet) Serialize(writer typedio.Writer) os.Error {
	writer.WriteString(a.namespace) // namespace
	writer.WriteString(a.objmap)    // objmap
	writer.WriteString(a.objkey)    // objkey

	writer.WriteUint8(uint8(len(a.key))) // key count
	for _, key := range a.key {          // keys
		writer.WriteString(key)
	}

	// marshal data
	bytes, err := json.Marshal(a.data)
	if err != nil {
		return err
	}

	writer.WriteUint32(uint32(len(bytes))) // data length
	writer.Write(bytes)                    // data

	writer.WriteUint8(a.retVal.Id) // return value id

	return nil
}

func (a *ActionSet) Unserialize(reader typedio.Reader) os.Error {
	var err os.Error
	a.namespace, err = reader.ReadString() // namespace
	if err != nil {
		return err
	}

	a.objmap, err = reader.ReadString() // objmap
	if err != nil {
		return err
	}

	a.objkey, err = reader.ReadString() // objkey
	if err != nil {
		return err
	}

	count, err := reader.ReadUint8() // key count
	if err != nil {
		return err
	}

	a.key = make([]string, count) // keys
	for i := 0; i < int(count); i++ {
		a.key[i], err = reader.ReadString()
		if err != nil {
			return err
		}
	}

	dataLength, err := reader.ReadUint32() // data length
	if err != nil {
		return err
	}

	bytes := make([]byte, dataLength)
	reader.Read(bytes)

	err = json.Unmarshal(bytes, &a.data) // data
	if err != nil {
		return err
	}

	retVal, err := reader.ReadUint8() // return value id
	a.retVal = a.transaction.getRetVal(retVal)

	return nil
}

func (a *ActionSet) PreRun(db *Db) (Containers, os.Error) {
	object, parent, _ := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)
	if object != nil {
		return []Container{object}, nil
	}

	return []Container{parent}, nil
}

func (a *ActionSet) Run(db *Db) {
	object, _, _ := db.GetObject(a.namespace, a.objmap, a.objkey, true, true)

	// make sure object is not newer than transaction (log replayed)
	if object.version < a.transaction.id {
		Walk(true, &a.data, &object.data, a.key)
	}
}

func (a *ActionSet) PostRun(db *Db) {
	object, _, _ := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)

	if object != nil && object.version < a.transaction.id {
		object.incrementVersion(a.transaction.id) // increment version
	}
}

func (a *ActionSet) Commit(db *Db) {
	object, _, _ := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)

	if object != nil {
		object.save(a.namespace, a.objmap, a.objkey, db)
	}
}
