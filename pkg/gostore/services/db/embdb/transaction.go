package embdb

import (
	"os"
	"gostore/tools/typedio"
	"json"
	"gostore/log"
)


/*
	CreateList(namespace, name, opts)
	GetList(namespace, name)

	CreateMap(namespace, name, opts)
	GetMap(namespace, name)


	List
		Get(0)
		Get(-1)
		Prepend()
		Append()
		Delete(0)
		Iterate

	Map
		Get()
		Set
		Delete
		Iterate


	Object
		GetVersion
		Lock
		Unlock
		Get
		Set
		Delete
		Append
		Prepend

	Block
		x := b.DefineObject()
		x := b.DefineMap()
		x := b.DefineList()

		x.Set(obj.Get())


		Cond(var, op, var) = C()
		If(C(), func(b) {})
		Foreach(iterable, func(b, obj) {})

		Math(var, op, var) = M()

	Transaction
		Lock(obj, obj, obj, ...) -> One lock at the time!
		Unlock(obj, obj, obj, ...) -> Automatically done at the end

		Release -> If the transaction has a "Release" instruction, the db will wait for this release before launching a trx thread on another thread
		Return -> Abort and return


	t := db.NewTransaction(func(trx) {
		ns := trx.GetNamespace("ns")
		mymap := ns.GetMap("test")
		o := mymap.Get("test")

		list := db.DefineList()
		db.If(db.C(o.Get("key"), "=", "val"), func() {
			db.Foreach(o.Get("somelist"), func(i, val) {
				list.Append(val)
			})
		})

		db.Return(list)
	})

*/



//type Actions interface {
//Cond(val interface{}, operator, key ... string)
//Assert(val interface{}, operator, key ... string)
//Increment(incr int64, key ... string)
//Set(val interface{}, key ... string)
//Get(key ... string) *RetVal
//Append(val interface{}, key ... string)
//Delete(key ... string)

//AssertVersion(operator, val float64)
//GetVersion(operator, val float64)
//}

//
// A transaction contains action executed on the database.
//
type Transaction struct {
	id      int64
	retVals []*RetVal
	actions []Action

	readOnly  bool
	preRunned bool

	containers Containers
}

func NewTransaction() (t *Transaction) {
	t = new(Transaction)
	t.readOnly = true
	return t
}

func (t *Transaction) newRetVal() *RetVal {
	rv := new(RetVal)
	rv.Id = byte(len(t.retVals))
	t.retVals = append(t.retVals, rv)
	return rv
}

func (t *Transaction) getRetVal(id byte) *RetVal {
	return t.retVals[id]
}

func (t *Transaction) Set(namespace, objmap, objkey string, val interface{}, key ...string) *RetVal {
	retVal := t.newRetVal()
	a := NewActionSet(t, retVal, val, namespace, objmap, objkey, key...)
	t.readOnly = t.readOnly && a.ReadOnly()
	t.actions = append(t.actions, a)
	return retVal
}

func (t *Transaction) Get(namespace, objmap, objkey string, key ...string) *RetVal {
	retVal := t.newRetVal()
	a := NewActionGet(t, retVal, namespace, objmap, objkey, key...)
	t.readOnly = t.readOnly && a.ReadOnly()
	t.actions = append(t.actions, a)
	return retVal
}

func (t *Transaction) Add(namespace, objmap, objkey string, increment float64, key ...string) *RetVal {
	retVal := t.newRetVal()
	a := NewActionAdd(t, retVal, increment, namespace, objmap, objkey, key...)
	t.readOnly = t.readOnly && a.ReadOnly()
	t.actions = append(t.actions, a)
	return retVal
}

func (t *Transaction) Delete(namespace, objmap, objkey string, key ...string) *RetVal {
	retVal := t.newRetVal()
	a := NewActionDelete(t, retVal, namespace, objmap, objkey, key...)
	t.readOnly = t.readOnly && a.ReadOnly()
	t.actions = append(t.actions, a)
	return retVal
}

func (t *Transaction) Serialize(writer typedio.Writer) {
	writer.WriteInt64(t.id) // id

	writer.WriteUint8(byte(len(t.retVals))) // nb retvals
	for _, retVal := range t.retVals {
		retVal.Serialize(writer) // retval
	}

	writer.WriteUint8(byte(len(t.actions))) // nb actions
	for _, action := range t.actions {
		writer.WriteUint8(action.Id()) // action id
		action.Serialize(writer)       // action
	}
}

func (t *Transaction) Unserialize(reader typedio.Reader) {
	t.id, _ = reader.ReadInt64() // id

	var i uint8
	nbRetvals, _ := reader.ReadUint8() // nb retvals
	for i = 0; i < nbRetvals; i++ {
		retVal := t.newRetVal()
		retVal.Unserialize(reader) // retval
	}

	nbActions, _ := reader.ReadUint8() // nb actions
	t.actions = make([]Action, nbActions)
	for i = 0; i < nbActions; i++ {
		id, _ := reader.ReadUint8() // action id

		var action Action
		switch id {
		case action_get:
			action = NewActionGetEmpty(t)
			break
		case action_set:
			action = NewActionSetEmpty(t)
			break
		case action_add:
			action = NewActionAddEmpty(t)
			break
		case action_delete:
			action = NewActionDeleteEmpty(t)
			break
		}

		if action != nil {
			action.Unserialize(reader)
			t.actions[i] = action
			t.readOnly = t.readOnly && action.ReadOnly()
		} else {
			log.Error("Unserialized an unknown action: id=%d", id)
		}
	}
}


func (t *Transaction) preRun(db *Db) (err os.Error) {
	t.containers = make([]Container, 0)

	if !t.preRunned {
		for _, action := range t.actions {
			log.Debug("Prerunning action %s from transaction %d", action, t)
			actContainers, err := action.PreRun(db)
			if err != nil {
				return err
			}

			if actContainers != nil {
				for _, actCont := range actContainers {
					t.containers = append(t.containers, actCont)
				}
			}
		}

		// sort containers so that we always lock in the same order
		t.containers.Sort()

		t.preRunned = true
	}

	return nil
}

func (t *Transaction) run(db *Db) (err os.Error) {
	err = t.preRun(db)
	if err != nil {
		return err
	}

	t.lock()

	for _, action := range t.actions {
		log.Debug("Running action %s from transaction %d", action, t)
		action.Run(db)
	}

	for _, action := range t.actions {
		log.Debug("Postrunning action %s from transaction %d", action, t)
		action.PostRun(db)
	}

	t.unlock()

	return nil
}

func (t *Transaction) lock() {
	var last Container
	log.Debug("Lock: ", t.containers)
	for _, container := range t.containers {
		if container != last {
			container.Lock()
		}
		last = container
	}
}

func (t *Transaction) unlock() {
	var last Container
	log.Debug("Unlock: ", t.containers)
	for _, container := range t.containers {
		if container != last {
			container.Unlock()
		}
		last = container
	}
}

func (t *Transaction) commit(db *Db) {
	// TODO: make sure all actions are commited to disk
	err := t.preRun(db)
	if err != nil {
		log.Error("Couldn't commit transaction %s to disk because of an error", t, err)
	}

	for _, action := range t.actions {
		action.Commit(db)
	}
}


//
// Return value of an action of the transaction
//
type RetVal struct {
	Id    byte
	Value interface{}
	Error os.Error
}

func (rv *RetVal) Serialize(writer typedio.Writer) {
	bytes, _ := json.Marshal(rv.Value)
	writer.WriteUint32(uint32(len(bytes))) // value length
	writer.Write(bytes)                    // value

	var err string
	if rv.Error != nil {
		err = rv.Error.String()
	}
	writer.WriteString(err) // error
}

func (rv *RetVal) Unserialize(reader typedio.Reader) {
	valueLength, _ := reader.ReadUint32() // value length
	bytes := make([]byte, valueLength)
	reader.Read(bytes) // value
	json.Unmarshal(bytes, rv.Value)

	err, _ := reader.ReadString() // error
	rv.Error = os.NewError(err)
}


//
// Encapsulate transaction into a mutation object
// so that it can be handled by the commitlog
//
type databaseMutation struct {
	db    *Db
	trans *Transaction
}

func (dm *databaseMutation) Unserialize(reader typedio.Reader) {
	dm.trans = NewTransaction()
	dm.trans.Unserialize(reader)
}

func (dm *databaseMutation) Serialize(writer typedio.Writer) {
	dm.trans.Serialize(writer)
}

func (dm *databaseMutation) Commit() {
	dm.trans.commit(dm.db)
}

func (dm *databaseMutation) Execute() {
	dm.trans.run(dm.db)
}
