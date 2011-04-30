package db

import (
	"testing"
	"fmt"
	"rand"
	"gostore/cluster"
	"gostore/log"
	"gostore/tools/typedio"
)




func TestSegmentCollection(t *testing.T) {
	col := newSegmentCollection()
	i := 0

	seg1 := createSegment("_test/", 0, cluster.MAX_NODE_ID, 0)
	col.AddSegment(seg1)


	seg2 := createSegment("_test/", 0, cluster.MAX_NODE_ID, 10)
	col.AddSegment(seg2)

	if len(col.segments) != 1 {
		t.Errorf("%d) Collection should have only 1, got %d", i, len(col.segments))
	}
	i++

	if res := col.GetSegment(0); res != seg2 {
		t.Errorf("%d) Collection should have seg2 for token 0: got %s", i, res)
	}
	i++

	seg3 := createSegment("_test/", 1000, cluster.MAX_NODE_ID, 11)
	col.AddSegment(seg3)

	if len(col.segments) != 2 {
		t.Errorf("%d) Collection should have 2 segment, got %d", i, len(col.segments))
	}
	i++

	if res := col.GetSegment(0); res != seg2 {
		t.Errorf("%d) Collection should have seg2 for token 0: got %s", i, res)
	}
	i++

	if res := col.GetSegment(1000); res != seg3 {
		t.Errorf("%d) Collection should have seg3 for token 0: got %s", i, res)
	}
	i++


	seg4 := createSegment("_test/", 500, 2000, 15)
	col.AddSegment(seg4)

	if len(col.segments) != 3 {
		t.Errorf("%d) Collection should have 3 segment, got %d", i, len(col.segments))
	}
	i++

	if res := col.GetSegment(0); res != seg2 {
		t.Errorf("%d) Collection should have seg2 for token 0: got %s", i, res)
	}
	i++

	if res := col.GetSegment(500); res != seg4 {
		t.Errorf("%d) Collection should have seg4 for token 500: got %s", i, res)
	}
	i++

	if res := col.GetSegment(2001); res != seg3 {
		t.Errorf("%d) Collection should have seg3 for token 2001: got %s", i, res)
	}
	i++
}




type testMut struct {
}
func (tm *testMut) Serialize(writer typedio.Writer) {
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
}
