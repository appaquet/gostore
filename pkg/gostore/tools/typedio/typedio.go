package typedio

import (
	"io"
	"os"
	"encoding/binary"
)

type Reader interface {
	io.Reader
	ReadInt64() (val int64, err os.Error)
	ReadInt32() (val int32, err os.Error)
	ReadInt16() (val int16, err os.Error)
	ReadUint64() (val uint64, err os.Error)
	ReadUint32() (val uint32, err os.Error)
	ReadUint16() (val uint16, err os.Error)
	ReadUint8() (val uint8, err os.Error)
	ReadFloat32() (val float32, err os.Error)
	ReadFloat64() (val float64, err os.Error)
	ReadBool() (val bool, err os.Error)
	ReadString() (string, os.Error)
}

type Writer interface {
	io.Writer
	WriteInt64(val int64) (err os.Error)
	WriteInt32(val int32) (err os.Error)
	WriteInt16(val int16) (err os.Error)
	WriteUint64(val uint64) (err os.Error)
	WriteUint32(val uint32) (err os.Error)
	WriteUint16(val uint16) (err os.Error)
	WriteUint8(val uint8) (err os.Error)
	WriteFloat32(val float32) (err os.Error)
	WriteFloat64(val float64) (err os.Error)
	WriteBool(val bool) (err os.Error)
	WriteString(data string) os.Error
}

type ReadWriter interface {
	Writer
	Reader
}


type readerImpl struct {
	io.Reader
}

func NewReader(reader io.Reader) Reader {
	return readerImpl{reader}
}

type writerImpl struct {
	io.Writer
}

func NewWriter(writer io.Writer) Writer {
	return writerImpl{writer}
}

type readwriterImpl struct {
	Reader
	Writer
}

func NewReadWriter(rw io.ReadWriter) ReadWriter {
	return readwriterImpl{NewReader(rw), NewWriter(rw)}
}


func (r readerImpl) ReadInt64() (val int64, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteInt64(val int64) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadInt32() (val int32, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteInt32(val int32) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadInt16() (val int16, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteInt16(val int16) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadUint64() (val uint64, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteUint64(val uint64) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadUint32() (val uint32, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteUint32(val uint32) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadUint16() (val uint16, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteUint16(val uint16) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadUint8() (val uint8, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteUint8(val uint8) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadFloat32() (val float32, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteFloat32(val float32) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadFloat64() (val float64, err os.Error) {
	err = binary.Read(r, binary.LittleEndian, &val)
	return
}

func (r writerImpl) WriteFloat64(val float64) (err os.Error) {
	return binary.Write(r, binary.LittleEndian, val)
}

func (r readerImpl) ReadBool() (val bool, err os.Error) {
	dec, err := r.ReadUint8()

	if dec == 1 {
		return true, err
	}

	return false, err
}

func (r writerImpl) WriteBool(val bool) (err os.Error) {
	if val {
		return r.WriteUint8(1)
	}

	return r.WriteUint8(0)
}


func (r readerImpl) ReadString() (string, os.Error) {
	size, err := r.ReadInt64()
	if err != nil {
		return "", err
	}

	if size > 0 {
		data := make([]byte, size)
		r.Read(data)
		return string(data), nil
	}

	return "", nil
}

func (r writerImpl) WriteString(data string) os.Error {
	b := []byte(data)

	var size int64 = int64(len(b))
	if err := r.WriteInt64(size); err != nil {
		return err
	}

	if _, err := r.Write(b); err != nil {
		return err
	}

	return nil
}
