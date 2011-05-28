package db

import (
	"testing"
	"fmt"
	"gostore/log"
)


func TestSet(t *testing.T) {
	db := newTestDb(true, 0)
	trx := NewTransaction(func(b *TransactionBlock) {
		myVar1 := b.NewVar()
		b.Set(myVar1, "salut 1.0")

		myVar2 := b.NewVar()
		b.Set(myVar2, myVar1)

		b.Set(myVar1, "salut 1.1")

		b.Set("test_container", "key", "val")

		b.Return(myVar1, myVar2)
	})

	ret := db.Execute(trx)
	if ret.Error != nil {
		t.Errorf("Got an error executing transaction: %s", *ret.Error.Message)
	}

	if len(ret.Returns) < 2 {
		t.Errorf("Should have 2 value returned")
	} else {
		fmt.Printf("%s %s\n", ret.Returns[0], ret.Returns[1])
	}
}

func TestGet(t *testing.T) {
	db := newTestDb(true, 0)
	ret := db.Execute(NewTransaction(func (b *TransactionBlock) {
		vr1 := b.NewVar()
		b.Set(vr1, "allo")
		ret1 := b.Get(vr1)

		b.Set("test_container", "key", "val")
		ret2 := b.Get("test_container", "key")

		b.Return(ret1, ret2)
	}))

	if len(ret.Returns) != 2 || ret.Returns[0].Value() != "allo" || ret.Returns[1].Value() != "val" {
		t.Errorf("Should have returned 2 values ['allo', 'val'], got: %v", ret.Returns)
	}


	// single get in a transaction
	ret = db.Execute(NewTransaction(func (b *TransactionBlock) {
		ret1 := b.Get("test_container", "key")
		b.Return(ret1)
	}))

	if len(ret.Returns) != 1 || ret.Returns[0].Value() != "val" {
		t.Errorf("Should have returned 0 values ['val'], got: %v", ret.Returns)
	}


	// override
	ret = db.Execute(NewTransaction(func (b *TransactionBlock) {
		b.Set("test_container", "key", "val2")
	}))


	ret = db.Execute(NewTransaction(func (b *TransactionBlock) {
		ret1 := b.Get("test_container", "key")
		b.Return(ret1)
	}))
	if len(ret.Returns) != 1 || ret.Returns[0].Value() != "val2" {
		t.Errorf("Should have returned 0 values ['val'], got: %v", ret.Returns)
	}


	// innexistant
	log.MaxLevel = 10
	ret = db.Execute(NewTransaction(func (b *TransactionBlock) {
		ret1 := b.Get("test_container", "key2")
		b.Return(ret1)
	}))
	if len(ret.Returns) != 1 || ret.Returns[0].Value() != nil {
		t.Errorf("Should have returned 0 values [nil], got: %v", ret.Returns)
	}


	ret = db.Execute(NewTransaction(func (b *TransactionBlock) {
		ret1 := b.Get("test_container", "key2")
		b.Return(ret1)
	}))
	if len(ret.Returns) != 1 || ret.Returns[0].Value() != nil {
		t.Errorf("Should have returned 0 values [nil], got: %v", ret.Returns)
	}
}


func BenchmarkSet(b *testing.B) {
	b.StopTimer()
	if benchDb == nil {
		benchDb = newTestDb(true, 0)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		trx := NewTransaction(func(b *TransactionBlock) {
			b.Set("test_container", "benchset", "salut 1.0")
		})

		benchDb.Execute(trx)
	}
}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()
	if benchDb == nil {
		benchDb = newTestDb(true, 0)
		benchDb.Execute(NewTransaction(func(b *TransactionBlock) {
			b.Set("test_container", "benchget", "somevalue")
		}))
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		ret := benchDb.Execute(NewTransaction(func(b *TransactionBlock) {
			b.Return(b.Get("test_container", "benchget"))
		}))

		if ret.Error != nil {
			fmt.Errorf("Got an error during benchmark: %v", *ret.Error.Message)
		}
	}
}
