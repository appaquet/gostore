package fs

import (
	"io"
	"bytes"
	"os"
	"json"
	"gostore/log"
)

type LocalFileHeader struct {
	headerpath string

	header *FileHeader
}

func NewLocalFileHeader(headerpath string) *LocalFileHeader {
	lfh := new(LocalFileHeader)
	lfh.headerpath = headerpath

	file, err := os.Open(lfh.headerpath)

	// no error
	if err == nil {
		lfh.header = LoadFileHeader(file)
	} else {
		if ptherror, ok := err.(*os.PathError); ok && ptherror.Error == os.ENOENT {
			// if file doesn't exists, we don't show an error
			lfh.header = NewFileHeader()
		} else {
			lfh.header = NewFileHeader()
			log.Error("Couldn't load header file for %s: %d", lfh.headerpath, err)
		}

	}

	file.Close()

	return lfh
}

func (lfh *LocalFileHeader) Save() {
	// TODO: Fix this double marshalling!

	bytes, err := json.Marshal(lfh.header)

	if err != nil {
		log.Error("Couldn't marshal header: %s", err)
	}

	file, err := os.Create(lfh.headerpath)
	if err != nil {
		log.Error("Couldn't save header file for %s: %s", lfh.headerpath, err)

	} else {
		n, err := file.Write(lfh.header.ToJSON())

		if n != len(bytes) {
			log.Error("Didn't write all header data: written %n bytes", n)
		}

		if err != nil {
			log.Error("Couldn't save file header: %s", err)
		}
	}

	file.Close()
}


type FileHeader struct {
	Path        string
	Name        string
	Size        int64
	Version     int64  // current version of the data
	NextVersion int64  // next version to assign.
	Exists      bool   // the file exists
	MimeType    string // mime type
	Children    []FileChild
}

func NewFileHeader() *FileHeader {
	fh := new(FileHeader)
	fh.Version = 1
	fh.NextVersion = 2
	return fh
}

func LoadFileHeader(reader io.Reader) *FileHeader {
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	bytes := buf.Bytes()

	return LoadFileHeaderFromJSON(bytes)
}

func LoadFileHeaderFromJSON(bytes []byte) *FileHeader {
	fh := new(FileHeader)
	errtok := json.Unmarshal(bytes, fh)

	if errtok != nil {
		fh = new(FileHeader)
		log.Error("Couldn't load file header: %s, %s", errtok, bytes)
	}

	return fh
}

func (f *FileHeader) ToJSON() []byte {
	bytes, err := json.Marshal(f)
	if err != nil {
		log.Error("Couldn't marshal header: %s", err)
	}

	return bytes
}

func (f *FileHeader) GetChild(name string) *FileChild {
	for i := 0; i < len(f.Children); i++ {
		if f.Children[i].Name == name {
			return &f.Children[i]
		}
	}

	return nil
}

func (f *FileHeader) HasChild(name string) bool {
	if f.GetChild(name) != nil {
		return true
	}
	return false
}

func (f *FileHeader) AddChild(name string, mimetype string, size int64) {
	child := f.GetChild(name)

	if child == nil {
		oldchildren := f.Children
		f.Children = make([]FileChild, len(f.Children)+1)

		for i, child := range oldchildren {
			f.Children[i] = child
		}

		f.Children[len(f.Children)-1] = NewFileChild(name, mimetype, size)
	} else {
		child.MimeType = mimetype
		child.Size = size
	}
}

func (f *FileHeader) RemoveChild(name string) {
	if f.HasChild(name) {
		newchilds := make([]FileChild, len(f.Children)-1)
		ptr := 0
		for i := 0; i < len(f.Children); i++ {
			if f.Children[i].Name != name {
				newchilds[ptr] = f.Children[i]
				ptr++
			}
		}

		f.Children = newchilds
	}
}

func (f *FileHeader) ClearChildren() {
	f.Children = make([]FileChild, 0)
}


type FileChild struct {
	Name     string
	MimeType string
	Size     int64
}

func NewFileChild(name string, mimetype string, size int64) FileChild {
	ch := new(FileChild)
	ch.Name = name
	ch.MimeType = mimetype
	ch.Size = size
	return *ch
}
