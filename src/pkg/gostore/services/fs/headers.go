package fs

import (
	"sync"
	"fmt"
)

type FileHeaders struct {
	fss *FsService

	headers      map[string]*LocalFileHeader
	headersmutex *sync.Mutex
}

func newFileHeaders(fss *FsService) *FileHeaders {
	fh := new(FileHeaders)
	fh.fss = fss

	fh.headers = make(map[string]*LocalFileHeader)
	fh.headersmutex = new(sync.Mutex)

	// TODO: Start disk committer

	return fh
}

func (fh *FileHeaders) GetFileHeader(path *Path) *LocalFileHeader {
	fh.headersmutex.Lock()

	header, found := fh.headers[path.String()]
	if !found {
		headerpath := fmt.Sprintf("%s/%d.head", fh.fss.dataDir, path.Hash())
		header = NewLocalFileHeader(headerpath)
		fh.headers[path.String()] = header
	}

	fh.headersmutex.Unlock()

	return header
}
