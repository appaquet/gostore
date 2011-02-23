package main_test

import (
	"testing"
	"gostore/services/fs"
	"gostore/log"
	"bytes"
)


func TestHeaderExists(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestHeaderExists...")

	//t.Errorf("%s", tc.nodes[1].Cluster.Resolve("/header/should/not/exists").Get(0))
	header, err := tc.nodes[4].Fss.Header(fs.NewPath("/header/should/not/exists"), nil)
	if err != nil {
		t.Errorf("1) Header returned an an error for: %s\n", err)
	}
	if header.Exists {
		t.Errorf("2) Header Exists shouldn't be true: %s\n", header.Exists)
	}

	header, err = tc.nodes[2].Fss.Header(fs.NewPath("/header/should/not/exists"), nil)
	if err != nil {
		t.Errorf("3) Header returned an an error for: %s\n", err)
	}
	if header.Exists {
		t.Errorf("4) Header Exists shouldn't be true: %s", header.Exists)
	}

	byts := []byte("salut!")
	buf := bytes.NewBuffer(byts)
	tc.nodes[3].Fss.Write(fs.NewPath("/header/tests/io/exists"), int64(len(byts)), "", buf, nil)
	header, err = tc.nodes[1].Fss.Header(fs.NewPath("/header/tests/io/exists"), nil)
	if err != nil {
		t.Errorf("5) Header returned an an error for: %s\n", err)
	}
	if !header.Exists {
		t.Errorf("6) Header Exists shouldn't be false: %s", header.Exists)
	}
}


func TestHeaderSize(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestHeaderSize...")

	header, err := tc.nodes[2].Fss.Header(fs.NewPath("/header/should/not/exists"), nil)
	if err != nil {
		t.Errorf("1) Header returned an an error for: %s\n", err)
	}
	if header.Size > 0 {
		t.Errorf("2) Header Size shouldn't higher than 0: %d", header.Size)
	}

	byts := []byte("salut!")
	buf := bytes.NewBuffer(byts)
	tc.nodes[3].Fss.Write(fs.NewPath("/header/tests/io/size"), int64(len(byts)), "", buf, nil)
	header, err = tc.nodes[1].Fss.Header(fs.NewPath("/header/tests/io/size"), nil)
	if err != nil {
		t.Errorf("3) Header returned an an error for: %s\n", err)
	}
	if header.Size != 6 {
		t.Errorf("4) Header Exists should be equal to 6: %s", header.Size)
	}

	byts = []byte("ca")
	buf = bytes.NewBuffer(byts)
	tc.nodes[3].Fss.Write(fs.NewPath("/header/tests/io/size"), int64(len(byts)), "", buf, nil)
	header, err = tc.nodes[1].Fss.Header(fs.NewPath("/header/tests/io/size"), nil)
	if err != nil {
		t.Errorf("5) Header returned an an error for: %s\n", err)
	}
	if header.Size != 2 {
		t.Errorf("6) Header Exists should be equal to 2: %s", header.Size)
	}
}


func TestHeaderMimeType(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestHeaderMimeType...")

	byts := []byte("salut!")
	buf := bytes.NewBuffer(byts)
	tc.nodes[3].Fss.Write(fs.NewPath("/header/tests/mimetype"), int64(len(byts)), "text/html", buf, nil)
	header, err := tc.nodes[1].Fss.Header(fs.NewPath("/header/tests/mimetype"), nil)
	if err != nil {
		t.Errorf("1) Header returned an an error for: %s\n", err)
	}
	if header.MimeType != "text/html" {
		t.Errorf("2) Header type should be equal to text/html: %s", header.MimeType)
	}

	byts = []byte("salut!")
	buf = bytes.NewBuffer(byts)
	tc.nodes[3].Fss.Write(fs.NewPath("/header/tests/mimetype"), int64(len(byts)), "text/css", buf, nil)
	header, err = tc.nodes[1].Fss.Header(fs.NewPath("/header/tests/mimetype"), nil)
	if err != nil {
		t.Errorf("3) Header returned an an error for: %s\n", err)
	}
	if header.MimeType != "text/css" {
		t.Errorf("4) Header type should be equal to text/css: %s", header.MimeType)
	}
}
