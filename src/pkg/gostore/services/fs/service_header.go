package fs

import (
	"gostore/log"
	"gostore/comm"
	"os"
	"bytes"
)

/*
 * Header
 */
func (fss *FsService) Header(path *Path, context *Context) (header *FileHeader, returnError os.Error) {
	bytes, returnError := fss.HeaderJSON(path, context)
	if returnError != nil {
		return nil, returnError
	}
	return LoadFileHeaderFromJSON(bytes), nil
}


func (fss *FsService) HeaderJSON(path *Path, context *Context) (returnValue []byte, returnError os.Error) {
	if context == nil {
		context = fss.NewContext()
	}

	message := fss.comm.NewMsgMessage(fss.serviceId)
	message.Function = "RemoteHeader"
	context.ApplyContext(message)

	// write payload
	message.Message.WriteString(path.String()) // path

	message.OnResponse = func(response *comm.Message) {
		returnValue = make([]byte, response.DataSize)
		response.Data.Read(returnValue)

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

func (fss *FsService) RemoteHeader(message *comm.Message) {

	// read payload
	str, _ := message.Message.ReadString()
	path := NewPath(str)

	log.Debug("FSS: Received new need header message for path %s\n", path)

	result := fss.ring.Resolve(path.String())

	// TODO: When versioning will be added, should not be that way since we may have it, but not of the right version
	if result.InOnlineNodes(fss.cluster.MyNode) {

		// If file exists localy or I'm the master
		localheader := fss.headers.GetFileHeader(path)
		if localheader.header.Exists || result.IsFirst(fss.cluster.MyNode) {

			// respond data
			response := fss.comm.NewDataMessage(fss.serviceId)

			localheader := fss.headers.GetFileHeader(path)
			header := localheader.header.ToJSON()
			response.DataSize = int64(len(header))
			response.Data = bytes.NewBuffer(header)

			fss.comm.RespondSource(message, response)
		} else {
			fss.comm.RedirectFirst(result, message)
		}

	} else {
		fss.comm.RedirectOne(result, message)
	}
}

func (fss *FsService) RemoteReplicaVersion(message *comm.Message) {
	// Read payload
	str, _ := message.Message.ReadString() // path
	path := NewPath(str)
	version, _ := message.Message.ReadInt64()     // current version
	nextversion, _ := message.Message.ReadInt64() // next version
	size, _ := message.Message.ReadInt64()        // size
	mimetype, _ := message.Message.ReadString()   // mimetype

	log.Debug("%d FSS: Received sync version replica for path '%s'\n", fss.cluster.MyNode.Id, path)

	// Get the header
	localheader := fss.headers.GetFileHeader(path)

	// Update the header
	localheader.header.Path = path.String()
	localheader.header.Name = path.BaseName()
	localheader.header.Exists = true
	localheader.header.Version = version
	localheader.header.NextVersion = nextversion
	localheader.header.MimeType = mimetype
	localheader.header.Size = size
	localheader.Save()

	// enqueue replication for background download
	fss.replicationEnqueue(path)

	// Send an acknowledgement
	req := fss.comm.NewMsgMessage(fss.serviceId)
	fss.comm.RespondSource(message, req)
}
