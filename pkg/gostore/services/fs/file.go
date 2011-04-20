package fs

import (
	"os"
	"gostore/log"
	"fmt"
)

type File struct {
	fss         *FsService
	path        *Path
	localheader *LocalFileHeader
	version     uint32

	datapath string

	fd *os.File
}

func OpenFile(fss *FsService, localheader *LocalFileHeader, version int64) *File {
	file := new(File)

	file.fss = fss
	file.localheader = localheader
	file.fd = nil
	file.path = NewPath(localheader.header.Path)

	if version == 0 {
		version = localheader.header.Version
	}

	file.datapath = fmt.Sprintf("%s/%d.%d.data", fss.dataDir, file.path.Hash(), version)

	return file
}

func (f *File) Delete() {
	f.Close()
	os.Remove(f.datapath)
}

func (f *File) Exists() bool {
	dir, err := os.Stat(f.datapath)

	if dir != nil && err == nil && dir.IsRegular() {
		return true
	}

	return false
}

func (f *File) Open(truncate bool) os.Error {
	if f.fd == nil {
		flag := os.O_RDWR | os.O_CREATE
		if truncate {
			flag = flag | os.O_TRUNC
		}

		var err os.Error
		f.fd, err = os.OpenFile(f.datapath, flag, 0777)

		return err
	}

	return nil
}

func (f *File) Close() os.Error {
	if f.fd != nil {
		var err os.Error
		err = f.fd.Close()

		f.fd = nil
		return err
	}

	return nil
}

func (f *File) Size() int64 {
	dir, err := os.Stat(f.datapath)
	if err != nil {
		log.Error("Cannot stat data file: %s\n", f.path)
	} else {
		return dir.Size
	}

	return 0
}

func (f *File) Read(b []byte) (n int, err os.Error) {
	err = f.Open(false)
	if err != nil {
		return 0, err
	}

	return f.fd.Read(b)
}

func (f *File) ReadAt(b []byte, off int64) (n int, err os.Error) {
	err = f.Open(false)
	if err != nil {
		return 0, err
	}

	return f.fd.ReadAt(b, off)
}

func (f *File) Write(b []byte) (n int, err os.Error) {
	err = f.Open(true)
	if err != nil {
		return 0, err
	}

	return f.fd.Write(b)
}

func (f *File) WriteAt(b []byte, off int64) (n int, err os.Error) {
	err = f.Open(false)
	if err != nil {
		return 0, err
	}

	return f.fd.WriteAt(b, off)
}
