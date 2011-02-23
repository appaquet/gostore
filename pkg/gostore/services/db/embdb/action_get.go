package embdb


import (
	"gostore/tools/typedio"
	"os"
)

type ActionGet struct {
	transaction *Transaction

	namespace string
	objmap    string
	objkey    string
	key       []string

	retVal *RetVal
}

func NewActionGetEmpty(transaction *Transaction) *ActionGet {
	a := new(ActionGet)
	a.transaction = transaction
	return a
}

func NewActionGet(transaction *Transaction, retVal *RetVal, namespace, objmap, objkey string, key ...string) Action {
	return Action(&ActionGet{
		transaction,
		namespace,
		objmap,
		objkey,
		key,
		retVal})
}

func (a *ActionGet) Id() byte {
	return action_get
}

func (a *ActionGet) ReadOnly() bool {
	return true
}

func (a *ActionGet) Serialize(writer typedio.Writer) os.Error {
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

func (a *ActionGet) Unserialize(reader typedio.Reader) os.Error {
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

func (a *ActionGet) PreRun(db *Db) (Containers, os.Error) {
	object, _, err := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)
	if err != nil {
		a.retVal.Error = err
		return nil, err
	}
	if object != nil {
		return []Container{object}, nil
	}

	return []Container{}, nil
}

func (a *ActionGet) Run(db *Db) {
	object, _, _ := db.GetObject(a.namespace, a.objmap, a.objkey, false, false)

	var data interface{}
	if object != nil {
		Walk(false, &data, &object.data, a.key)
		a.retVal.Value = data
	} else {
		a.retVal.Value = nil
	}
}

func (a *ActionGet) PostRun(db *Db) {
}

func (a *ActionGet) Commit(db *Db) {
	// Nothing to commit
}
