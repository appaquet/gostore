package db

import (
	"testing"
	"os"
	"gostore/log"
	"gostore/tools/buffer"
	"fmt"
)

var (
	benchDb *Db
)

func setupTests() {
	dir, _ := os.Stat("_test")
	if dir == nil || !dir.IsDirectory() {
		os.Mkdir("_test", 0777)
	}
}


func newDb(wipe bool) *Db {
	log.MaxLevel = 2

	setupTests()

	if wipe {
		os.RemoveAll("_test/data")
	}

	os.Mkdir("_test/data", 0777)

	return NewDb(Config{DataPath:"_test/data/"})
}

func TestWalk(t *testing.T) {

	var container interface{}
	var data interface{}
	data = 1

	// create first level
	Walk(true, &data, &container, []string{})
	if container != 1 {
		t.Errorf("1) Container should be equal: 1!=%d", container)
	}

	// get first level
	data = nil
	Walk(false, &data, &container, []string{})
	if data != 1 {
		t.Errorf("2) Container should be equal: 1!=%d", container)
	}

	// create second level with array
	container = nil
	Walk(true, &data, &container, []string{"0"})
	if arr, ok := (container).([]interface{}); ok {
		if arr[0] != 1 {
			t.Errorf("3) arr[0] != 1, = %d", arr[0])
		}
	} else {
		t.Errorf("4) Container should be an array", container)
	}

	data = "OMG"
	Walk(false, &data, &container, []string{"0"})

	// get second level with array
	data = nil
	Walk(false, &data, &container, []string{"0"})
	if data != "OMG" {
		t.Errorf("5) arr[0] != OMG, = %s", data, container)
	}

	// add to a too small array
	data = nil
	Walk(true, &data, &container, []string{"2"})
	arr, _ := (container).([]interface{})
	if len(arr) != 3 {
		t.Errorf("6) len(arr[2]) != 3, = %d", len(arr))
	} else {
		if arr[2] != nil {
			t.Errorf("6) arr[2] != nil, = %s", arr[2])
		}
	}

	// create multi level map
	container = nil
	data = true
	Walk(true, &data, &container, []string{"hello", "how"})
	if mmap1, ok := (container).(map[string]interface{}); ok {
		if mmap2, ok := (mmap1["hello"]).(map[string]interface{}); ok {
			if mmap2["how"] != true {
				t.Errorf("6) mmap[hello][how] != true, = %s", mmap2["how"])
			}
		} else {
			t.Errorf("7) mmap[hello] != map, = %s", mmap2)
		}
	} else {
		t.Errorf("8) mmap != map, = %s", container)
	}

	// get multi level map
	Walk(true, &data, &container, []string{"hello", "how"})
	if data != true {
		t.Errorf("9) mmap[hello][how] != true, = %s", data)
	}
}

func TestSerialize(t *testing.T) {
	buf1 := buffer.New()
	trx1 := NewTransaction(func (b *TransBlock) {
		b.Set("test", "toto", 1)
	})
	err := trx1.Serialize(buf1)
	if err != nil {
		t.Errorf("Got an error serializing: %s", err)
	}

	buf1.Seek(0, 0)
	trx2 := NewEmptyTransaction()
	trx2.Unserialize(buf1)


	buf2 := buffer.New()
	err = trx2.Serialize(buf2)
	if err != nil {
		t.Errorf("Got an error serializing: %s", err)
	}


	fmt.Printf("%v\n", buf1.Bytes())
	fmt.Printf("%v\n", buf2.Bytes())
}




/*
func TestSet(t *testing.T) {
	dbinst := newDb(true)

	trx := NewTransaction()
	ret := trx.Set("namespace", "objmap", "objkey", map[string]interface{}{"test": 1})
	dbinst.Execute(trx)
	if ret.Error != nil {
		t.Errorf("1) Got an error for set: %s", ret.Error)
	}

	trx = NewTransaction()
	ret = trx.Get("namespace", "objmap", "objkey", "test")
	dbinst.Execute(trx)
	if ret.Error != nil {
		t.Errorf("2) Got an error for get: %s", ret.Error)
	}
	if ret.Value != 1 {
		t.Errorf("3) Written value should be 1, got %d", ret.Value)
	}

	trx = NewTransaction()
	ret = trx.Set("namespace", "objmap", "objkey", map[string]interface{}{"tata": true})
	dbinst.Execute(trx)
	if ret.Error != nil {
		t.Errorf("4) Got an error for set: %s", ret.Error)
	}

	trx = NewTransaction()
	ret = trx.Get("namespace", "objmap", "objkey", "tata")
	dbinst.Execute(trx)
	if ret.Error != nil {
		t.Errorf("5) Got an error for get: %s", ret.Error)
	}
	if ret.Value != true {
		t.Errorf("6) Written value should be 1, got %d", ret.Value)
	}

	trx = NewTransaction()
	ret = trx.Set("namespace", "objmap", "objkey", map[string]interface{}{"tata": []interface{}{1, 2, 3}})
	dbinst.Execute(trx)
	if ret.Error != nil {
		t.Errorf("7) Got an error for set: %s", ret.Error)
	}

	trx = NewTransaction()
	ret = trx.Get("namespace", "objmap", "objkey", "tata")
	dbinst.Execute(trx)
	if ret.Error != nil {
		t.Errorf("8) Got an error for get: %s", ret.Error)
	}
	if arr, ok := (ret.Value).([]interface{}); ok {
		if arr[2] != 3 {
			t.Errorf("9) Written value tata[2] should be 3, got %d", arr[2])
		}
	} else {
		t.Errorf("10) Written value should be an array, got %s", ret.Value)
	}

	trx = NewTransaction()
	ret = trx.Get("namespace", "objmap", "objkeydsd")
	dbinst.Execute(trx)
	if ret.Error != nil {
		t.Errorf("11) Shouldn't have got an error because object didn't exists: %s", ret.Error)
	}

	trx = NewTransaction()
	ret = trx.Get("namespace", "objmasp", "objkeydsd")
	dbinst.Execute(trx)
	if ret.Error == nil {
		t.Errorf("12) Should have got an error because object map didn't exists: %s", ret.Error)
	}

	trx = NewTransaction()
	ret = trx.Get("namsfespace", "objmasp", "objkeydsd")
	dbinst.Execute(trx)
	if ret.Error == nil {
		t.Errorf("13) Should have got an error because namespace didn't exists: %s", ret.Error)
	}
}


var (
	tests map[int]TestStep
	steps []TestStep

	initiated bool
)

type TestStep struct {
	objId int
	exec  func(dbinst *Db)
	test  func(dbinst *Db) (error string)
}

func (ts *TestStep) Execute(dbinst *Db) {
	ts.exec(dbinst)
	tests[ts.objId] = *ts
}

func initSteps(resetTests bool) {
	if !initiated {
		steps = []TestStep{
			// set test/test0/dsfsd to true
			TestStep{
				0,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "test0", "dsfsd", true)
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "test0", "dsfsd")
					dbinst.Execute(trx)

					if ret.Value != true {
						return fmt.Sprintf("Expected true, got: %s", ret.Value)
					}

					return ""
				}},

			// reset test/test0/dsfsd twice in a transaction
			TestStep{
				0,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "test0", "dsfsd", -1)
					trx.Set("test", "test0", "dsfsd", false)
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "test0", "dsfsd")
					dbinst.Execute(trx)

					if ret.Value != false {
						return fmt.Sprintf("Expected false, got: %s", ret.Value)
					}

					return ""
				}},

			// set test/test1/obj to [1,2,3]
			TestStep{
				1,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "test1", "obj", []interface{}{1, 2, 3})
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "test1", "obj")
					dbinst.Execute(trx)

					if arr, ok := ret.Value.([]interface{}); !ok || (arr[2] != 3 && arr[2] != float64(3)) {
						return fmt.Sprintf("Expected []interface{1,2,3}, got: %s", ret.Value)
					}

					return ""
				}},

			// override test/test1/obj so that it changes to [1,2,4]
			TestStep{
				1,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "test1", "obj", 4, "2")
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "test1", "obj")
					dbinst.Execute(trx)

					if arr, ok := ret.Value.([]interface{}); !ok || (arr[2] != 4 && arr[2] != float64(4)) {
						return fmt.Sprintf("Expected []interface{1,2,4}, got: %s", ret.Value)
					}

					return ""
				}},

			// increment an non existing object
			TestStep{
				2,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Add("test", "float", "myobj", float64(1))
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "float", "myobj")
					dbinst.Execute(trx)

					if ret.Value != float64(1) {
						return fmt.Sprintf("Expected 1, got: %f", ret.Value)
					}

					return ""
				}},

			// increment an existing object
			TestStep{
				2,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Add("test", "float", "myobj", float64(1))
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "float", "myobj")
					dbinst.Execute(trx)

					if ret.Value != float64(2) {
						return fmt.Sprintf("Expected 2, got: %f", ret.Value)
					}

					return ""
				}},

			// deincrement an existing object
			TestStep{
				2,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Add("test", "float", "myobj", float64(-4))
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "float", "myobj")
					dbinst.Execute(trx)

					if ret.Value != float64(-2) {
						return fmt.Sprintf("Expected -2, got: %f", ret.Value)
					}

					return ""
				}},

			// multiple increment, set in one transaction
			TestStep{
				3,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "dectr", "oobjj", float64(3))
					trx.Add("test", "dectr", "oobjj", float64(10))
					trx.Add("test", "dectr", "oobjj", float64(-2))
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "dectr", "oobjj")
					dbinst.Execute(trx)

					if ret.Value != float64(11) {
						return fmt.Sprintf("Expected 11, got: %f", ret.Value)
					}

					return ""
				}},

			// create and delete object in 2 transactions
			TestStep{
				4,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "del", "obj", float64(3))
					dbinst.Execute(trx)

					trx = NewTransaction()
					trx.Delete("test", "del", "obj")
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "del", "obj")
					dbinst.Execute(trx)

					if ret.Value != nil {
						return fmt.Sprintf("Expected nil, got: %s", ret.Value)
					}

					return ""
				}},

			// create and delete object in 1 transaction
			TestStep{
				5,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "del2", "obj", float64(3))
					trx.Delete("test", "del2", "obj")
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "del2", "obj")
					dbinst.Execute(trx)

					if ret.Value != nil {
						return fmt.Sprintf("Expected nil, got: %s", ret.Value)
					}

					return ""
				}},

			// recreate a deleted object from another transaction
			TestStep{
				5,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "del2", "obj", float64(10))
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "del2", "obj")
					dbinst.Execute(trx)

					if ret.Value != float64(10) {
						return fmt.Sprintf("Expected 10, got: %s", ret.Value)
					}

					return ""
				}},

			// create, delete and increment object in 1 transaction
			TestStep{
				6,
				func(dbinst *Db) {
					trx := NewTransaction()
					trx.Set("test", "del3", "obj", float64(3))
					trx.Delete("test", "del3", "obj")
					trx.Add("test", "del3", "obj", float64(2))
					dbinst.Execute(trx)
				},
				func(dbinst *Db) (error string) {
					trx := NewTransaction()
					ret := trx.Get("test", "del3", "obj")
					dbinst.Execute(trx)

					if ret.Value != float64(2) {
						return fmt.Sprintf("Expected nil, got: %s", ret.Value)
					}

					return ""
				}},
		}

		initiated = true
	}

	if resetTests {
		tests = make(map[int]TestStep)
	}
}

func TestStepsSimple(t *testing.T) {
	db := newDb(true)

	initSteps(true)

	for _, step := range steps {
		step.Execute(db)

		for testId, test := range tests {
			ret := test.test(db)
			if ret != "" {
				t.Errorf("Failed at test %d: %s", testId, ret)
			}
		}
	}
}


func TestStepsLog(t *testing.T) {
	// make the db crash at different state
	for i, _ := range steps {
		db := newDb(true)
		initSteps(true)

		// execute the state
		for j, step := range steps {
			step.Execute(db)

			// reset the db, it will replay its log
			if i == j {
				db = newDb(false)
			}

			for testId, test := range tests {
				ret := test.test(db)
				if ret != "" {
					t.Errorf("(%d,%d) Failed at test %d: %s", i, j, testId, ret)
				}
			}
		}
	}
}


func TestStepsCommit(t *testing.T) {
	// make the db crash at different state
	for i, _ := range steps {
		for j, _ := range steps {
			db := newDb(true)
			initSteps(true)

			// execute the state
			for k, step := range steps {
				step.Execute(db)

				// commit the db to disk
				if j == k {
					Commit(-1)
				}

				// reset the db, it will replay its log
				if i == k {
					db = newDb(false)
				}

				for testId, test := range tests {
					ret := test.test(db)
					if ret != "" {
						t.Errorf("(%d,%d,%d) Failed at test %d: %s", i, j, k, testId, ret)
					}
				}
			}
		}
	}
}


/*func TestReplay(t *testing.T) {
	db := newDb(true)

	trx := NewTransaction()
	trx.Set("namespace", "replaytest", "test", true)
	dbinst.Execute(trx)

	Commit(-1)
	db = newDb(false)

	trx = NewTransaction()
	ret := trx.Get("namespace", "replaytest", "test")
	dbinst.Execute(trx)

	if ret.Value != true {
		t.Errorf("1) Object should be true")
	}


	Empty()
	Replay()

	trx = NewTransaction()
	ret = trx.Get("namespace", "replaytest", "test")
	dbinst.Execute(trx)
	if ret.Error != nil {
		t.Errorf("1) Got an error for get: %s", ret.Error)
	}
	if ret.Value != true {
		t.Errorf("2) Object should equal true, got %s", ret.Value)
	}	


}


func BenchmarkWrite(b *testing.B) {
	if benchDb == nil {
		b.StopTimer()
		benchDb = newDb(true)
		b.StartTimer()
	}

	for i := 0; i < b.N; i++ {
		trx := NewTransaction()
		trx.Set("namespace", "bench", "bench", true)
		benchExecute(trx)
	}
}

func BenchmarkRead(b *testing.B) {
	if benchDb == nil {
		b.StopTimer()
		benchDb = newDb(false)
		b.StartTimer()
	}

	for i := 0; i < b.N; i++ {
		trx := NewTransaction()
		trx.Get("namespace", "bench", "bench")
		benchExecute(trx)
	}
}
*/


