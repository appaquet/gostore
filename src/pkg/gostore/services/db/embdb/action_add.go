package embdb

import (
	"gostore/tools/typedio"
	"os"
)

type ActionAdd struct {
	transaction *Transaction

	namespace string
	objmap    string
	objkey    string
	key       []string

	increment float64

	retVal *RetVal
}

func NewActionAddEmpty(transaction *Transaction) *ActionAdd {
	a := new(ActionAdd)
	a.transaction = transaction
	return a
}

func NewActionAdd(transaction *Transaction, retVal *RetVal, increment float64, namespace, objmap, objkey string, key ...string) Action {
	return Action(&ActionAdd{
		transaction,
		namespace,
		objmap,
		objkey,
		key,
		increment,
		retVal})
}

func (a *ActionAdd) Id() byte {
	return action_add
}

func (a *ActionAdd) ReadOnly() bool {
	return false
}

func (a *ActionAdd) Serialize(writer typedio.Writer) os.Error {
	writer.WriteString(a.namespace) // namespace
	writer.WriteString(a.objmap)    // objmap
	writer.WriteString(a.objkey)    // objkey

	writer.WriteUint8(uint8(len(a.key))) // key count
	for _, key := range a.key {          // keys
		writer.WriteString(key)
	}

	writer.WriteFloat64(a.increment) // increment

	writer.WriteUint8(a.retVal.Id) // return value id

	return nil
}

func (a *ActionAdd) Unserialize(reader typedio.Reader) os.Error {
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

	a.increment, err = reader.ReadFloat64() // increment
	if err != nil {
		return err
	}

	retVal, err := reader.ReadUint8() // return value id
	a.retVal = a.transaction.getRetVal(retVal)

	return nil
}

func (a *ActionAdd) PreRun(db *Db) (Containers, os.Error) {
	object, parent, _ := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)

	// destination should be null or float64
	if object != nil {
		var dest interface{}
		Walk(false, &dest, &object.data, a.key)
		_, ok := (dest).(float64)
		if !ok && dest != nil {
			return nil, os.NewError("Destination is not numeric")
		}

		return []Container{object}, nil
	}

	// if object is null, object will be created. Locking parent
	return []Container{parent}, nil
}

func (a *ActionAdd) Run(db *Db) {
	object, _, _ := db.GetObject(a.namespace, a.objmap, a.objkey, true, true)

	// make sure object is not newer than transaction (log replayed)
	if object.version < a.transaction.id {
		var dest interface{}
		Walk(false, &dest, &object.data, a.key)

		if dest != nil {
			dest = dest.(float64) + a.increment
		} else {
			dest = a.increment
		}

		// set it back
		Walk(true, &dest, &object.data, a.key)
	}
}

func (a *ActionAdd) PostRun(db *Db) {
	object, _, _ := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)

	if object != nil && object.version < a.transaction.id {
		object.incrementVersion(a.transaction.id) // increment version
	}
}

func (a *ActionAdd) Commit(db *Db) {
	object, _, _ := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)

	if object != nil {
		object.save(a.namespace, a.objmap, a.objkey, db)
	}
}
