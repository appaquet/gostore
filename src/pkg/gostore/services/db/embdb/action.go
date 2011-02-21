package embdb

import (
	"gostore/tools/typedio"
	"os"
)

const (
	Operator_Equal = iota

	action_get = iota
	action_set
	action_add
	action_delete
)

type Action interface {
	Id() byte
	ReadOnly() bool // whether if the action modify the db or not


	Serialize(writer typedio.Writer) os.Error
	Unserialize(reader typedio.Reader) os.Error

	PreRun(db *Db) (Containers, os.Error) // tests and return modified containers
	Run(db *Db)
	PostRun(db *Db) // make sure everything is ok, increment version
	Commit(db *Db)  // commit should lock what its committing to disk
}
