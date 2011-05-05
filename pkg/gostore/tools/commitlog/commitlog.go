package commitlog

import (
	"os"
	"gostore/log"
	"gostore/tools/typedio"
	"io"
	"gostore/tools/buffer"
	"sync"
	"reflect"
)

// TODO: Implement multiple file + rotation

var (
	HEADER_WRITE_POS  int64 = 0
	HEADER_COMMIT_POS int64 = 8
	HEADER_END        int64 = 16
)

type mutationInfo struct {
	id       byte
	mutation Mutation
}

type CommitLog struct {
	fdMutex *sync.Mutex
	fd      *os.File
	typedFd typedio.ReadWriter

	writePtr  int64 // pointer in commit log where the next mutation will be written 
	commitPtr int64 // pointer in commit log of the next mutation that hasn't been commited yet


	count         byte
	mutations     map[byte]mutationInfo
	mutationsType map[reflect.Type]mutationInfo
}

func New(directory string) *CommitLog {
	cl := new(CommitLog)

	cl.mutations = make(map[byte]mutationInfo)
	cl.mutationsType = make(map[reflect.Type]mutationInfo)

	cl.fdMutex = new(sync.Mutex)

	// check if directory exists, create it otherwize
	dir, err := os.Stat(directory)
	if dir == nil || !dir.IsDirectory() {
		os.Mkdir(directory, 0777)
	}

	// check if log file exists and its at least the header size
	path := directory + "000.log"
	dir, err = os.Stat(path)
	existed := false
	if dir != nil && err == nil && dir.IsRegular() && dir.Size > HEADER_END {
		existed = true
	}

	// open the log
	flag := os.O_RDWR | os.O_CREATE
	cl.fd, err = os.OpenFile(path, flag, 0777)
	if err != nil {
		log.Fatal("CommitLog: Cannot open commit log file %s: %s", path, err)
	}
	cl.typedFd = typedio.NewReadWriter(cl.fd)

	// if commit log existed, read header
	if existed {
		cl.writePtr, _ = cl.typedFd.ReadInt64()
		cl.commitPtr, _ = cl.typedFd.ReadInt64()

	} else {
		// else, write header
		cl.writePtr = HEADER_END
		cl.commitPtr = HEADER_END

		cl.typedFd.WriteInt64(cl.writePtr)
		cl.typedFd.WriteInt64(cl.commitPtr)
	}

	return cl
}


// synchronised! should be not run in parallel
func (cl *CommitLog) Replay() int {
	cl.fd.Seek(cl.commitPtr, 0)
	replayed := 0

	// read all mutation until we reach the write pointer (the end)
	var curPtr int64 = cl.commitPtr
	for curPtr < cl.writePtr {
		mutInfo, size := cl.readOne(cl.typedFd)
		curPtr += size

		// execute the mutation
		mutInfo.mutation.Execute()
		replayed++
	}

	return replayed
}

func (cl *CommitLog) readOne(reader typedio.Reader) (mutInfo mutationInfo, size int64) {
	// read mutation id
	id, err := reader.ReadUint8()
	if err != nil {
		log.Fatal("CommitLog: Couldn't read mutation from commit log: %s", err)
	}

	// get the right mutation for the read id
	mutInfo, found := cl.mutations[id]
	if !found {
		log.Fatal("CommitLog: Cannot find mutation id %d!", id)
	}

	// read the mutation
	mutInfo.mutation.Unserialize(reader)

	// read size of the mutation record
	size, err = reader.ReadInt64()
	if err != nil && err != os.EOF {
		log.Fatal("CommitLog: Couldn't read size of mutation from commit log: %s", err)
	}

	return mutInfo, size
}


func (cl *CommitLog) RegisterMutation(mutation Mutation) {
	mutInfo := mutationInfo{cl.count, mutation}
	cl.count++

	cl.mutations[mutInfo.id] = mutInfo
	cl.mutationsType[reflect.TypeOf(mutation)] = mutInfo
}

func (cl *CommitLog) Execute(mutation Mutation) {
	typ := reflect.TypeOf(mutation)
	mutInfo, found := cl.mutationsType[typ]

	if !found {
		log.Fatal("CommitLog: Tried to execute an unknown mutation: %s of type %s", mutation, typ)
	}

	// write the modification into a buffer
	buf := buffer.New()
	buf.WriteUint8(mutInfo.id)   // mutation id
	mutation.Serialize(buf)      // mutation record
	buf.WriteInt64(buf.Size + 8) // record length

	// write record to disk
	buf.Seek(0, 0)
	cl.fdMutex.Lock()
	cl.fd.Seek(cl.writePtr, 0)
	io.Copyn(cl.fd, buf, buf.Size)

	// update commit log write pointer
	cl.fd.Seek(HEADER_WRITE_POS, 0)
	cl.writePtr += buf.Size
	cl.typedFd.WriteInt64(cl.writePtr)
	cl.fdMutex.Unlock()

	// execute the mutation
	mutation.Execute()
}


// commit mutations to disk
// if count is -1, commit all mutations
// synchronised! should be not run in parallel
func (cl *CommitLog) Commit(count int) {
	// read all mutation until we reach the write pointer (the end)
	var curPtr int64 = cl.commitPtr
	i := 0
	for (curPtr < cl.writePtr) && (count == -1 || i < count) {
		// make sure nothing else is written to file
		cl.fdMutex.Lock()

		// seek to the next commit position
		cl.fd.Seek(cl.commitPtr, 0)

		// read the mutation, commit it
		mutInfo, size := cl.readOne(cl.typedFd)
		mutInfo.mutation.Commit()
		curPtr += size
		i++

		// write the new commit position
		cl.fd.Seek(HEADER_COMMIT_POS, 0)
		cl.typedFd.WriteInt64(curPtr)
		cl.commitPtr = curPtr

		cl.fdMutex.Unlock()
	}
}
