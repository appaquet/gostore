package commitlog_test


import (
	"os"
	"gostore/tools/typedio"
	"gostore/tools/commitlog"
	"testing"
)

var (
	int64Val      int64
	int64Commited int64

	cl *commitlog.CommitLog
)

func Reset() {
	int64Val = 0

}


func Init() {
	if cl == nil {
		os.RemoveAll("_test/")
		cl = commitlog.New("_test/")

		cl.RegisterMutation(NewInt64Mutation(-1))
	}
}


type Int64Mutation struct {
	val int64
}

func NewInt64Mutation(val int64) commitlog.Mutation {
	mut := new(Int64Mutation)
	mut.val = val
	return commitlog.Mutation(mut)
}

func (m *Int64Mutation) Unserialize(reader typedio.Reader) {
	m.val, _ = reader.ReadInt64()
}

func (m *Int64Mutation) Serialize(writer typedio.Writer) {
	writer.WriteInt64(m.val)
}

func (m *Int64Mutation) Execute() {
	int64Val = m.val
}

func (m *Int64Mutation) Commit() {
	int64Commited = m.val
}


func TestCommitLog(t *testing.T) {
	Init()

	// execute mutation
	cl.Execute(NewInt64Mutation(100))
	if int64Val != 100 {
		t.Errorf("1) Mutation didn't change value: %d != 100\n", int64Val)
	}
	cl.Execute(NewInt64Mutation(200))
	if int64Val != 200 {
		t.Errorf("2) Mutation didn't change value: %d != 200\n", int64Val)
	}

	// replay it
	int64Val = 0
	cl.Replay()
	if int64Val != 200 {
		t.Errorf("3) Replay didn't replay mutation: %d != 200\n", int64Val)
	}

	// commit one
	cl.Commit(1)
	if int64Commited != 100 {
		t.Errorf("4) First mutation should have been commited: %d != 100\n", int64Commited)
	}

	// commit rest
	cl.Commit(-1)
	if int64Commited != 200 {
		t.Errorf("5) First mutation should have been commited: %d != 200\n", int64Commited)
	}

	// replay
	int64Val = 0
	cl.Replay()
	if int64Val != 0 {
		t.Errorf("6) Replay should have read nothing since everything was commited: %d != 0\n", int64Val)
	}

	cl.Execute(NewInt64Mutation(300))
	if int64Val != 300 {
		t.Errorf("7) Mutation didn't change value: %d != 300\n", int64Val)
	}
}
