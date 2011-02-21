package buffer

import (
	"os"
	"gostore/tools/typedio"
)

type Buffer struct {
	typedio.ReadWriter

	Size      int64
	data      []byte
	Pointer   int64
	FixedSize bool
}

func New() *Buffer {
	r := new(Buffer)
	r.ReadWriter = typedio.NewReadWriter(r)
	return r
}

func NewWithSize(size int64, fixedSize bool) *Buffer {
	r := New()
	r.data = make([]byte, size)
	r.Size = size
	r.FixedSize = fixedSize
	return r
}

func NewFromString(init string) *Buffer {
	r := New()

	if init != "" {
		r.WriteString(init)
		r.Seek(0, 0)
	}

	return r
}

func (r *Buffer) Bytes() []byte {
	return r.data
}

func (r *Buffer) Read(b []byte) (n int, err os.Error) {
	if r.Pointer >= r.Size {
		return 0, os.EOF
	}

	n = copy(b, r.data[r.Pointer:])
	r.Pointer += int64(n)

	return n, nil
}

func (r *Buffer) ReadAt(b []byte, off int64) (n int, err os.Error) {
	r.Pointer = int64(off)
	return r.Read(b)
}

func (r *Buffer) Write(b []byte) (n int, err os.Error) {
	bufLen := int64(len(b))

	if !r.FixedSize {
		endPtr := r.Pointer + bufLen
		if endPtr >= r.Size {
			r.data = append(r.data, b[bufLen-(endPtr-r.Size):]...)
			r.Size = int64(len(r.data))
		}
	}

	for r.Pointer < r.Size && int64(n) < bufLen {
		r.data[r.Pointer] = b[n]
		r.Pointer++
		n++
	}

	return n, nil
}


func (r *Buffer) WriteAt(b []byte, off int64) (n int, err os.Error) {
	r.Seek(off, 0)
	return r.Write(b)
}

func (r *Buffer) Seek(offset int64, whence int) (ret int64, err os.Error) {
	switch whence {
	default:
		return 0, os.EINVAL
	case 0:
		offset += 0
	case 1:
		offset += int64(r.Pointer)
	case 2:
		offset += 0
	}
	if offset < 0 || offset > int64(r.Size) {
		return 0, os.EINVAL
	}
	r.Pointer = offset
	return offset, nil
}
