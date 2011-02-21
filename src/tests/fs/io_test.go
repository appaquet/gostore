package main_test

import (
	"testing"
	"gostore/services/fs"
	"gostore/log"
	"gostore/tools/buffer"
	"bytes"
	"io"
)

/*
 * TO TEST:
 *   -Truncate
 *   -Unicode path
 */

func TestExists(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestExists...")

	exists, err := tc.nodes[1].Fss.Exists(fs.NewPath("/should/not/exists"), nil)
	if err != nil {
		t.Errorf("1) Exists returned an an error for: %s\n", err)
	}
	if exists == true {
		t.Errorf("2) Exists returned bad value: %s", exists)
	}

	exists, err = tc.nodes[2].Fss.Exists(fs.NewPath("/should/not/exists"), nil)
	if err != nil {
		t.Errorf("3) Exists returned an an error for: %s\n", err)
	}
	if exists == true {
		t.Errorf("4) Exists returned bad value: %s", exists)
	}

	byts := []byte("salut!")
	buf := bytes.NewBuffer(byts)
	tc.nodes[0].Fss.Write(fs.NewPath("/tests/io/exists"), int64(len(byts)), "", buf, nil)
	exists, err = tc.nodes[2].Fss.Exists(fs.NewPath("/tests/io/exists"), nil)
	if err != nil {
		t.Errorf("5) Exists returned an an error for: %s\n", err)
	}
	if exists == false {
		t.Errorf("6) Exists returned bad value: %s", exists)
	}
}


func TestReadWrite(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestReadWrite...")

	// Read write test
	path := fs.NewPath("/tests/io/write1")
	byts := []byte("write1")
	buf := bytes.NewBuffer(byts)
	err := tc.nodes[0].Fss.Write(path, int64(len(byts)), "", buf, nil)
	if err != nil {
		t.Errorf("1) Write returned an an error for: %s\n", err)
	}

	// Check if read data is the same as written
	bufwriter := bytes.NewBuffer(make([]byte, 0))
	buffer := io.Writer(bufwriter)
	n, err := tc.nodes[2].Fss.Read(path, 0, -1, 0, buffer, nil)
	if n != int64(len(byts)) || bytes.Compare(bufwriter.Bytes(), byts) != 0 {
		t.Errorf("2) Didn't read written data correctly: %s!=%s\n", byts, bufwriter)
	}

	// Get from another node
	bufwriter = bytes.NewBuffer(make([]byte, 0))
	buffer = io.Writer(bufwriter)
	n, err = tc.nodes[6].Fss.Read(path, 0, -1, 0, buffer, nil)
	if n != int64(len(byts)) || bytes.Compare(bufwriter.Bytes(), byts) != 0 {
		t.Errorf("3) Didn't read written data correctly: %s!=%s\n", byts, bufwriter)
	}

	// Rewrite on a file with new data
	path = fs.NewPath("/tests/io/write1")
	byts = []byte("this is new data blabla")
	buf = bytes.NewBuffer(byts)
	err = tc.nodes[4].Fss.Write(path, int64(len(byts)), "", buf, nil)
	if err != nil {
		t.Errorf("4) Write returned an an error for: %s\n", err)
	}

	// Check written data
	bufwriter = bytes.NewBuffer(make([]byte, 0))
	buffer = io.Writer(bufwriter)
	n, err = tc.nodes[9].Fss.Read(path, 0, -1, 0, buffer, nil)
	if n != int64(len(byts)) || bytes.Compare(bufwriter.Bytes(), byts) != 0 {
		t.Errorf("5) Didn't read written data correctly: %s!=%s\n", byts, bufwriter)
	}
}

func TestUnicode(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestUnicode...")

	// Read write test
	path := fs.NewPath("/tests/io/writeunicode")
	byts := []byte("Some é unicode à data 世界")
	buf := bytes.NewBuffer(byts)
	err := tc.nodes[0].Fss.Write(path, int64(len(byts)), "", buf, nil)
	if err != nil {
		t.Errorf("1) Write returned an an error for: %s\n", err)
	}

	// Check if read data is the same as written
	bufwriter := bytes.NewBuffer(make([]byte, 0))
	n, err := tc.nodes[2].Fss.Read(path, 0, -1, 0, io.Writer(bufwriter), nil)
	if n != int64(len(byts)) || bytes.Compare(bufwriter.Bytes(), byts) != 0 {
		t.Errorf("2) Didn't read written data correctly: %s!=%s\n", byts, bufwriter)
	}

	// Test unicode path
	path = fs.NewPath("/école èàaû/世界.txt")
	wrtnData := buffer.NewFromString("some data")
	err = tc.nodes[0].Fss.Write(path, wrtnData.Size, "", wrtnData, nil)
	if err != nil {
		t.Errorf("3) Error while writing: %s\n", err)
	}

	// Reading written data
	rdData := buffer.NewWithSize(0, false)
	n, err = tc.nodes[1].Fss.Read(path, 0, -1, 0, rdData, nil)
	if n != wrtnData.Size || bytes.Compare(rdData.Bytes(), wrtnData.Bytes()) != 0 {
		t.Errorf("4) Didn't read what had ben written: %s!=%s", rdData.Bytes(), wrtnData.Bytes())
	}

}


func TestReadNotExists(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestReadNotExists...")

	path := fs.NewPath("/should/not/exists")
	bufwriter := bytes.NewBuffer(make([]byte, 0))
	buffer := io.Writer(bufwriter)

	// Local
	_, err := tc.nodes[1].Fss.Read(path, 0, -1, 0, buffer, nil)
	if err != fs.ErrorFileNotFound {
		t.Errorf("1) Read didn't return that the file don't exists: %s\n", err)
	}

	// Remote
	_, err = tc.nodes[5].Fss.Read(path, 0, -1, 0, buffer, nil)
	if err != fs.ErrorFileNotFound {
		t.Errorf("2) Read didn't return that the file don't exists: %s\n", err)
	}
}


/*
 * Benchmarks
 */
func BenchmarkWrite(b *testing.B) {
	b.StopTimer()
	SetupCluster()
	path := fs.NewPath("/tests/io/benchwrite")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer([]byte("azsdrfjqlmndufir"))
		tc.nodes[6].Fss.Write(path, 16, "", buf, nil)
	}
}

func BenchmarkRead(b *testing.B) {
	b.StopTimer()
	SetupCluster()
	path := fs.NewPath("/tests/io/benchwrite")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		bufwriter := bytes.NewBuffer(make([]byte, 0))
		buffer := io.Writer(bufwriter)
		tc.nodes[7].Fss.Read(path, 0, -1, 0, buffer, nil)
	}
}

func BenchmarkExists(b *testing.B) {
	b.StopTimer()
	SetupCluster()
	path := fs.NewPath("/tests/io/benchwrite")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		tc.nodes[7].Fss.Exists(path, nil)
	}
}
