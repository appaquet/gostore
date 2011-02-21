package buffer_test

import (
	"gostore/tools/buffer"
	"testing"
)

func TestWriteRead(t *testing.T) {
	buff := buffer.New()

	buff.Write([]byte{1, 3, 1})

	if buff.Size != 3 {
		t.Errorf("Buffer size should be 3, got %d", buff.Size)
	}

	if buff.Pointer != 3 {
		t.Errorf("Pointer should be at 3, got %d", buff.Pointer)
	}

	buff.Seek(0, 0)

	if buff.Pointer != 0 {
		t.Errorf("Pointer should bt at 0 after seek, got %d", buff.Pointer)
	}

	rb := make([]byte, 3)
	n, err := buff.Read(rb)
	if err != nil {
		t.Errorf("Got an error reading: %s", err)
	}
	if n != 3 {
		t.Errorf("Should have read 3 bytes, got %d", n)
	}
}

func TestWriteOnce(t *testing.T) {
	buff := buffer.New()

	buff.WriteUint8(100)
	buff.Seek(0, 0)
	val, err := buff.ReadUint8()

	if val != 100 {
		t.Errorf("Read not equal 100: %d", val)
	}

	if err != nil {
		t.Errorf("Got an error: %s", err)
	}
}

func TestString(t *testing.T) {
	buff := buffer.NewFromString("test")

	str, err := buff.ReadString()
	if str != "test" {
		t.Errorf("1) Read string wasn't 'test', got %s", str)
	}
	if err != nil {
		t.Errorf("1) Got an error: %s", err)
	}

	buff.WriteString("toto")

	buff.Seek(0, 0)
	str, err = buff.ReadString()
	if str != "test" {
		t.Errorf("2) Read string wasn't 'test', got %s", str)
	}
	if err != nil {
		t.Errorf("2) Got an error: %s", err)
	}

	str, err = buff.ReadString()
	if str != "toto" {
		t.Errorf("3) Read string wasn't 'toto', got %s", str)
	}
	if err != nil {
		t.Errorf("3) Got an error: %s", err)
	}
}


func TestResize(t *testing.T) {
	buffer := buffer.New()
	buffer.Write([]byte{1, 2, 3})
	buffer.Seek(1, 0)

	buffer.Write([]byte{7, 7, 7})

	if buffer.Size != 4 {
		t.Errorf("Should have resized to 4, but size is now %d", buffer.Size)
	}

	buf := make([]byte, 4)
	n, _ := buffer.Read(buf)
	if n != 0 {
		t.Errorf("Pointer was at the end, shouldn't have read anything but had %d bytes returned", n)
	}

	buffer.Seek(1, 0)
	n, _ = buffer.Read(buf)
	if n != 3 {
		t.Errorf("Should have read 4, but returned %d", n)
	}

	if string(buf) != string([]byte{7, 7, 7, 0}) {
		t.Errorf("Should have read 7 7 7 0, but got %s", buf)
	}
}


func TestNoResize(t *testing.T) {
	buffer := buffer.NewWithSize(4, true)

	n, _ := buffer.Write([]byte{1, 2, 3, 4, 5, 6})
	if n != 4 {
		t.Errorf("Should have wrote 4, but wrote %d", n)
	}
	if buffer.Pointer != 4 {
		t.Errorf("Pointer should be at 4, but got %d", buffer.Pointer)
	}

	if buffer.Size != 4 {
		t.Errorf("Shouldn't have resized, but resized to %d", buffer.Size)
	}

	buffer.Seek(0, 0)

	buf := make([]byte, 10)
	n, _ = buffer.Read(buf)
	if n != 4 {
		t.Errorf("Should have read 4, but got %d", n)
	}

	if string(buf) != string([]byte{1, 2, 3, 4, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("Didn't read what was written...")
	}
}


func TestSpecific(t *testing.T) {
	buffer := buffer.New()

	buffer.WriteUint8(233)
	buffer.Seek(0, 0)
	val, err := buffer.ReadUint8()

	if err != nil {
		t.Errorf("Got an error reading: %s", err)
	}
	if val != 233 {
		t.Errorf("Should have read 233, but got %d", val)
	}

}
