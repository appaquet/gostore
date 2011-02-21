package fs

import (
	"gostore/log"
	"gostore/comm"
	"os"
	"io"
)

/*
 * Read
 */
func (fss *FsService) Read(path *Path, offset int64, size int64, version int64, writer io.Writer, context *Context) (returnReadN int64, returnError os.Error) {
	if context == nil {
		context = fss.NewContext()
	}

	message := fss.comm.NewMsgMessage(fss.serviceId)
	message.Function = "RemoteRead"
	context.ApplyContext(message)

	// write payload
	message.Message.WriteString(path.String())    // path
	message.Message.WriteInt64(offset)            // offset
	message.Message.WriteInt64(size)              // size
	message.Message.WriteInt64(version)           // version
	message.Message.WriteBool(context.ForceLocal) // force local

	message.OnResponse = func(response *comm.Message) {
		returnReadN, returnError = io.Copyn(writer, response.Data, response.DataSize)

		message.Wait <- true
	}

	message.OnError = func(response *comm.Message, error os.Error) {
		returnError = error
		message.Wait <- false
	}

	if context.ForceLocal {
		// handle locally
		fss.comm.SendNode(fss.cluster.MyNode, message)
	} else {
		resolveResult := fss.ring.Resolve(path.String())
		fss.comm.SendOne(resolveResult, message)
	}

	<-message.Wait
	return
}

func (fss *FsService) RemoteRead(message *comm.Message) {
	// Read payload
	str, _ := message.Message.ReadString() // path
	path := NewPath(str)
	offset, _ := message.Message.ReadInt64()    // offset
	size, _ := message.Message.ReadInt64()      // size
	version, _ := message.Message.ReadInt64()   // version
	forceLocal, _ := message.Message.ReadBool() // force local


	log.Debug("FSS: Received new need read message for path %s, version %d, at offset %d, size of %d\n", path, version, offset, size)

	result := fss.ring.Resolve(path.String())

	// TODO: When versioning will be added, should not be that way since we may have it, but not of the right version
	if result.InOnlineNodes(fss.cluster.MyNode) || forceLocal {
		localheader := fss.headers.GetFileHeader(path)
		file := OpenFile(fss, localheader, version) // get the file

		// if the file exists and we have it locally
		if localheader.header.Exists && file.Exists() {
			// Send it back
			response := fss.comm.NewDataMessage(fss.serviceId)

			// TODO: Handle offset
			response.Message.WriteInt64(offset)                     // offset
			response.Message.WriteInt64(localheader.header.Version) // version

			// TODO: Handle the asked read size
			response.DataSize = localheader.header.Size
			response.Data = io.Reader(file)
			response.DataAutoClose = true

			fss.comm.RespondSource(message, response)
		} else {
			// Check if I'm supposed to have it
			if result.IsFirst(fss.cluster.MyNode) || forceLocal {
				fss.comm.RespondError(message, ErrorFileNotFound)
			} else {
				fss.comm.RedirectFirst(result, message)
			}
		}
	} else {
		fss.comm.RedirectOne(result, message)
	}
}
