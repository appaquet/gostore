package embdb

import (
	"gostore/tools/typedio"
	"os"
)

type ActionDelete struct {
	transaction *Transaction

	namespace string
	objmap    string
	objkey    string
	key       []string

	retVal *RetVal
}

func NewActionDeleteEmpty(transaction *Transaction) *ActionDelete {
	a := new(ActionDelete)
	a.transaction = transaction
	return a
}

func NewActionDelete(transaction *Transaction, retVal *RetVal, namespace, objmap, objkey string, key ...string) Action {
	return Action(&ActionDelete{
		transaction,
		namespace,
		objmap,
		objkey,
		key,
		retVal})
}

func (a *ActionDelete) Id() byte {
	return action_delete
}

func (a *ActionDelete) ReadOnly() bool {
	return false
}

func (a *ActionDelete) Serialize(writer typedio.Writer) os.Error {
	writer.WriteString(a.namespace) // namespace
	writer.WriteString(a.objmap)    // objmap
	writer.WriteString(a.objkey)    // objkey

	writer.WriteUint8(uint8(len(a.key))) // key count
	for _, key := range a.key {          // keys
		writer.WriteString(key)
	}

	writer.WriteUint8(a.retVal.Id) // return value id

	return nil
}

func (a *ActionDelete) Unserialize(reader typedio.Reader) os.Error {
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

	retVal, err := reader.ReadUint8() // return value id
	a.retVal = a.transaction.getRetVal(retVal)

	return nil
}

func (a *ActionDelete) PreRun(db *Db) (Containers, os.Error) {
	// make sure namespace and object map exist
	objmap, _, _ := db.GetObjectMap(a.namespace, a.objmap, false)

	// we want to delete the object
	if len(a.key) == 0 {
		if objmap != nil {
			return []Container{objmap}, nil
		}

		// else, delete something in the object
	} else if object, _ := objmap.objects[a.objkey]; object != nil {
		return []Container{object}, nil
	}

	return []Container{}, nil
}

func (a *ActionDelete) Run(db *Db) {
	// make sure namespace and object map exist
	objmap, _, _ := db.GetObjectMap(a.namespace, a.objmap, false)

	if objmap != nil {
		// we want to delete the object
		if len(a.key) == 0 {
			objmap.objects[a.objkey] = nil, false

			// else, delete something the object
		} else if object, _ := objmap.objects[a.objkey]; object != nil {

		}
	}
}

func (a *ActionDelete) PostRun(db *Db) {
	// make sure namespace and object map exist
	objmap, _, _ := db.GetObjectMap(a.namespace, a.objmap, false)

	if objmap != nil {
		// if we deleted something from the object
		if object, _ := objmap.objects[a.objkey]; object != nil {
			if object.version < a.transaction.id {
				object.incrementVersion(a.transaction.id)
			}
		}
	}
}

func (a *ActionDelete) Commit(db *Db) {
	object, _, _ := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)

	// only delete the object if its null
	if object == nil {
		// TODO: Delete it from disk!
		//a.object.save(a.namespace, a.objmap, a.objkey, db)
	}
}
