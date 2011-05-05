package db

import (
	"testing"
	"fmt"
)


func TestSet(t *testing.T) {
	db := newDb(true)
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


func BenchmarkSet(b *testing.B) {
	b.StopTimer()
	if benchDb == nil {
		benchDb = newDb(true)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		trx := NewTransaction(func(b *TransactionBlock) {
			myVar1 := b.NewVar()
			b.Set(myVar1, "salut 1.0")
		})

		benchDb.Execute(trx)
	}
}
