package main_test

import (
	"testing"
	"bytes"
	"io"
	"gostore/services/fs"
	"gostore/log"
	"gostore/tools/buffer"
	"time"
)

func TestWriteHeaderReplication(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestWriteHeaderReplication...")

	resp, other := GetProcessForPath("/tests/replication/write/header")
	resolv := tc.nodes[0].Cluster.Rings.GetGlobalRing().Resolve("/tests/replication/write/header")

	buf := buffer.NewFromString("write1")
	err := other.Fss.Write(fs.NewPath("/tests/replication/write/header"), buf.Size, "application/mytest", buf, nil)

	if err != nil {
		t.Error("1) Got an error while write: %s", err)
	}

	// check header on master
	header, err := resp.Fss.Header(fs.NewPath("/tests/replication/write/header"), nil)
	version := header.Version

	// check on first secondary
	secondaryid := resolv.GetOnline(1).Id
	header, err = tc.nodes[secondaryid].Fss.Header(fs.NewPath("/tests/replication/write/header"), nil)
	if header.Version != version {
		t.Errorf("2) Version on secondary node wasn't the same as on master: %d != %d", version, header.Version)
	}
	if header.MimeType != "application/mytest" {
		t.Errorf("3) Mimetype on secondary node wasn't the same as on master: %s != %s", "application/mytest", header.MimeType)
	}
	if header.Size != buf.Size {
		t.Errorf("4) Size on secondary node wasn't the same as on master: %d != %d", 6, header.Size)
	}

	// check on second secondary
	secondaryid = resolv.GetOnline(2).Id
	header, err = tc.nodes[secondaryid].Fss.Header(fs.NewPath("/tests/replication/write/header"), nil)
	if header.Version != version {
		t.Errorf("5) Version on secondary node wasn't the same as on master: %d != %d", version, header.Version)
	}
	if header.MimeType != "application/mytest" {
		t.Errorf("6) Mimetype on secondary node wasn't the same as on master: %s != %s", "application/mytest", header.MimeType)
	}
	if header.Size != buf.Size {
		t.Errorf("7) Size on secondary node wasn't the same as on master: %d != %d", 6, header.Size)
	}
}

func TestWriteExistsReplication(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestWriteExistsReplication...")

	_, other := GetProcessForPath("/tests/replication/write/exists")
	resolv := tc.nodes[0].Cluster.Rings.GetGlobalRing().Resolve("/tests/replication/write/exists")

	buf := buffer.NewFromString("write1")
	err := other.Fss.Write(fs.NewPath("/tests/replication/write/exists"), buf.Size, "application/mytest", buf, nil)
	if err != nil {
		t.Errorf("1) Got an error while write: %s", err)
	}

	// test force local on exists
	context := tc.nodes[0].Fss.NewContext()
	context.ForceLocal = true
	exists, err := other.Fss.Exists(fs.NewPath("/tests/replication/write/exists"), context)
	if err != nil {
		t.Errorf("2) Got an error while exists: %s", err)
	}
	if exists {
		t.Errorf("3) Shouldn't exists on another node when forced local")
	}

	// check on first secondary
	secondaryid := resolv.GetOnline(1).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	exists, err = tc.nodes[secondaryid].Fss.Exists(fs.NewPath("/tests/replication/write/exists"), context)
	if err != nil {
		t.Errorf("4) Got an error while exists: %s", err)
	}
	if !exists {
		t.Errorf("5) Received exists = false, should be true")
	}

	// check on second secondary
	secondaryid = resolv.GetOnline(2).Id
	exists, err = tc.nodes[secondaryid].Fss.Exists(fs.NewPath("/tests/replication/write/exists"), context)
	if err != nil {
		t.Errorf("6) Got an error while exists: %s", err)
	}
	if !exists {
		t.Errorf("7) Received exists = false, should be true")
	}
}

func TestChildAddReplication(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestChildAddReplication...")

	_, other := GetProcessForPath("/tests/replication/write/childadd")
	resolv := tc.nodes[0].Cluster.Rings.GetGlobalRing().Resolve("/tests/replication/write")
	_, otherparent := GetProcessForPath("/tests/replication/write")

	buf := buffer.NewFromString("write1")
	err := other.Fss.Write(fs.NewPath("/tests/replication/write/childadd"), buf.Size, "application/mytest", buf, nil)
	if err != nil {
		t.Errorf("1) Got an error while write: %s", err)
	}

	// check if its not on another node
	context := tc.nodes[0].Fss.NewContext()
	context.ForceLocal = true
	children, err := otherparent.Fss.Children(fs.NewPath("/tests/replication/write"), context)
	if err == nil && len(children) != 0 {
		t.Errorf("2) Children should not exists on another node")
	}

	// check on first secondary
	secondaryid := resolv.GetOnline(1).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	children, err = tc.nodes[secondaryid].Fss.Children(fs.NewPath("/tests/replication/write"), context)
	if err != nil {
		t.Errorf("3) Got an error while exists: %s", err)
	}
	found := false
	for _, child := range children {
		if child.Name == "childadd" {
			found = true
		}
	}
	if !found {
		t.Errorf("4) Couldn't find child 'childadd'")
	}

	// check on second secondary
	secondaryid = resolv.GetOnline(1).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	children, err = tc.nodes[secondaryid].Fss.Children(fs.NewPath("/tests/replication/write"), context)
	if err != nil {
		t.Errorf("5) Got an error while exists: %s", err)
	}
	found = false
	for _, child := range children {
		if child.Name == "childadd" {
			found = true
		}
	}
	if !found {
		t.Errorf("6) Couldn't find child 'childadd'")
	}
}

func TestDeleteReplication(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestDeleteReplication...")

	resp, other := GetProcessForPath("/tests/replication/write/childdelete")
	resolv := tc.nodes[0].Cluster.Rings.GetGlobalRing().Resolve("/tests/replication/write/childdelete")
	respparent, _ := GetProcessForPath("/tests/replication/write")
	resolvparent := tc.nodes[0].Cluster.Rings.GetGlobalRing().Resolve("/tests/replication/write")

	buf := buffer.NewFromString("write1")
	err := other.Fss.Write(fs.NewPath("/tests/replication/write/childdelete"), buf.Size, "application/mytest", buf, nil)
	if err != nil {
		t.Errorf("1) Got an error while write: %s", err)
	}

	// check if its not on another node
	err = other.Fss.Delete(fs.NewPath("/tests/replication/write/childdelete"), true, nil)
	if err != nil {
		t.Errorf("2) Got an error while deleting: %s", err)
	}

	// check on master
	context := tc.nodes[0].Fss.NewContext()
	context.ForceLocal = true
	exists, err := resp.Fss.Exists(fs.NewPath("/tests/replication/write/childdelete"), context)
	if err != nil {
		t.Errorf("3) Got an error while exists: %s", err)
	}
	if exists {
		t.Errorf("4) Shouldn't exists on master")
	}

	// check on first secondary
	secondaryid := resolv.GetOnline(1).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	exists, err = tc.nodes[secondaryid].Fss.Exists(fs.NewPath("/tests/replication/write/childdelete"), context)
	if err != nil {
		t.Errorf("5) Got an error while exists: %s", err)
	}
	if exists {
		t.Errorf("6) Shouldn't exists on master")
	}

	// check on second secondary
	secondaryid = resolv.GetOnline(2).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	exists, err = tc.nodes[secondaryid].Fss.Exists(fs.NewPath("/tests/replication/write/childdelete"), context)
	if err != nil {
		t.Errorf("7) Got an error while exists: %s", err)
	}
	if exists {
		t.Errorf("8) Shouldn't exists on master")
	}

	time.Sleep(500000000)

	// check master parent
	secondaryid = resolvparent.GetOnline(2).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	children, err := respparent.Fss.Children(fs.NewPath("/tests/replication/write"), context)
	if err != nil {
		t.Errorf("9) Got an error while exists: %s", err)
	}
	found := false
	for _, child := range children {
		if child.Name == "childdelete" {
			found = true
		}
	}
	if found {
		t.Errorf("10) Child 'childdelete' should exists on master anymore")
	}

	// check first secondary
	secondaryid = resolvparent.GetOnline(1).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	children, err = tc.nodes[secondaryid].Fss.Children(fs.NewPath("/tests/replication/write"), context)
	if err != nil {
		t.Errorf("11) Got an error while exists: %s", err)
	}
	found = false
	for _, child := range children {
		if child.Name == "childdelete" {
			found = true
		}
	}
	if found {
		t.Errorf("12) Child 'childdelete' should exists on secondary anymore")
	}

	// check second secondary
	secondaryid = resolvparent.GetOnline(2).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	children, err = tc.nodes[secondaryid].Fss.Children(fs.NewPath("/tests/replication/write"), context)
	if err != nil {
		t.Errorf("13) Got an error while exists: %s", err)
	}
	found = false
	for _, child := range children {
		if child.Name == "childdelete" {
			found = true
		}
	}
	if found {
		t.Errorf("14) Child 'childdelete' should exists on secondary anymore")
	}
}

func TestWriteReadReplication(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestWriteReadReplication...")

	_, other := GetProcessForPath("/tests/replication/write/read")
	resolv := tc.nodes[0].Cluster.Rings.GetGlobalRing().Resolve("/tests/replication/write/read")

	buf := buffer.NewFromString("write1")
	err := other.Fss.Write(fs.NewPath("/tests/replication/write/read"), buf.Size, "application/mytest", buf, nil)
	if err != nil {
		t.Errorf("1) Got an error while write: %s", err)
	}

	// test force local on exists
	context := tc.nodes[0].Fss.NewContext()
	context.ForceLocal = true
	bufwriter := bytes.NewBuffer(make([]byte, 0))
	buffer := io.Writer(bufwriter)
	n, err := other.Fss.Read(fs.NewPath("/tests/replication/write/read"), 0, -1, 0, buffer, context)
	if err != fs.ErrorFileNotFound {
		t.Errorf("2) File shouldn't exists on another node: %s", err)
	}

	// force local replication on each secondary
	tc.nodes[resolv.GetOnline(1).Id].Fss.Flush()
	tc.nodes[resolv.GetOnline(2).Id].Fss.Flush()

	// check on first secondary
	secondaryid := resolv.GetOnline(1).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	bufwriter = bytes.NewBuffer(make([]byte, 0))
	buffer = io.Writer(bufwriter)
	n, err = tc.nodes[secondaryid].Fss.Read(fs.NewPath("/tests/replication/write/read"), 0, -1, 0, buffer, context)
	if err != nil {
		t.Errorf("3) Got an error from read: %s", err)
	}
	if n != buf.Size || bytes.Compare(bufwriter.Bytes(), buf.Bytes()) != 0 {
		t.Errorf("4) Didn't read what was written: %s!=%s", buf.Bytes(), bufwriter)
	}

	// check on second secondary
	secondaryid = resolv.GetOnline(2).Id
	context = tc.nodes[secondaryid].Fss.NewContext()
	context.ForceLocal = true
	bufwriter = bytes.NewBuffer(make([]byte, 0))
	buffer = io.Writer(bufwriter)
	n, err = tc.nodes[secondaryid].Fss.Read(fs.NewPath("/tests/replication/write/read"), 0, -1, 0, buffer, context)
	if err != nil {
		t.Errorf("3) Got an error from read: %s", err)
	}
	if n != buf.Size || bytes.Compare(bufwriter.Bytes(), buf.Bytes()) != 0 {
		t.Errorf("4) Didn't read what was written: %s!=%s", buf.Bytes(), bufwriter)
	}
}
