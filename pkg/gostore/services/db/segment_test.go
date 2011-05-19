package db

import (
	"testing"
	//"fmt"
	//"rand"
	"gostore/cluster"
	"gostore/log"
	//"gostore/tools/typedio"
)


func TestSegmentCollection(t *testing.T) {
	log.MaxLevel = 10


	col := newSegmentCollection()
	i := 0

	seg1 := createSegment("_test/", 0, cluster.MAX_NODE_ID, 5)
	col.addSegment(seg1)

	seg2 := createSegment("_test/", 0, cluster.MAX_NODE_ID, 10)
	col.addSegment(seg2)



	if len(col.end) != 1 {
		t.Errorf("%d) Collection should have only 1 at end, got %d", i, len(col.end))
	}
	i++

	if len(col.beginning) != 1 {
		t.Errorf("%d) Collection should have only 1 at beginning, got %d", i, len(col.beginning))
	}
	i++

	if res := col.getEndSegment(0); res != seg2 {
		t.Errorf("%d) Collection should have seg2 for token 0: got %s", i, res)
	}
	i++
	if res := col.getBeginningSegment(0); res != seg1 {
		t.Errorf("%d) Collection should have seg1 for token 0: got %s", i, res)
	}
	i++


	seg3 := createSegment("_test/", 1000, cluster.MAX_NODE_ID, 11)
	col.addSegment(seg3)

	if len(col.end) != 2 {
		t.Errorf("%d) Collection should have 2 segment at end, got %d", i, len(col.end))
	}
	i++
	if len(col.beginning) != 1 {
		t.Errorf("%d) Collection should have 1 segment at beginning, got %d", i, len(col.beginning))
	}
	i++

	if res := col.getEndSegment(0); res != seg2 {
		t.Errorf("%d) Collection should have seg2 for token 0: got %s", i, res)
	}
	i++

	if res := col.getEndSegment(1000); res != seg3 {
		t.Errorf("%d) Collection should have seg3 for token 0: got %s", i, res)
	}
	i++

	seg4 := createSegment("_test/", 500, 2000, 15)
	col.addSegment(seg4)

	if len(col.end) != 3 {
		t.Errorf("%d) Collection should have 3 segment, got %d", i, len(col.end))
	}
	i++

	if res := col.getEndSegment(0); res != seg2 {
		t.Errorf("%d) Collection should have seg2 for token 0: got %s", i, res)
	}
	i++

	if res := col.getEndSegment(500); res != seg4 {
		t.Errorf("%d) Collection should have seg4 for token 500: got %s", i, res)
	}
	i++

	if res := col.getEndSegment(2001); res != seg3 {
		t.Errorf("%d) Collection should have seg3 for token 2001: got %s", i, res)
	}
	i++


	seg5 := createSegment("_test/", 1000, cluster.MAX_NODE_ID, 3)
	seg5.writable = false
	col.addSegment(seg5)
	if len(col.beginning) != 2 {
		t.Errorf("%d) Collection should have 2 segment, got %d", i, len(col.beginning))
	}
	i++
	if len(col.end) != 3 {
		t.Errorf("%d) Collection should have 3 segment, got %d", i, len(col.end))
	}
	i++
}


/*
type testMut struct {
}
func (tm *testMut) serialize(writer typedio.Writer) {
	writer.WriteString("HELLO")
}



func TestSegmentMutation(t *testing.T) {
	db := newDb(true)
	_ = fmt.Print
	mut := &testMut{}
	mutations.AddMutation(mut)


	for i:=0; i<1000; i++ {
		token := Token(rand.Intn(65536))
		db.segmentManager.WriteMutation(token, mut)
	}

	log.Debug("DONE")
}*/
