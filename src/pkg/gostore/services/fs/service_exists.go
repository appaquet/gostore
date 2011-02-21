package fs

import (
	"gostore/log"
	"gostore/comm"
	"os"
)


/*
 * Exists
 */
func (fss *FsService) Exists(path *Path, context *Context) (value bool, returnError os.Error) {
	if context == nil {
		context = fss.NewContext()
	}

	message := fss.comm.NewMsgMessage(fss.serviceId)
	message.Function = "RemoteExists"
	context.ApplyContext(message)

	// write payload
	message.Message.WriteString(path.String())    // path
	message.Message.WriteBool(context.ForceLocal) // force local

	message.OnResponse = func(response *comm.Message) {
		value, returnError = response.Message.ReadBool()
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

func (fss *FsService) RemoteExists(message *comm.Message) {

	// Read payload
	pathString, _ := message.Message.ReadString()
	path := NewPath(pathString)                 // path
	forceLocal, _ := message.Message.ReadBool() // force local

	log.Debug("%d: FSS: Received new exists message for path %s\n", fss.cluster.MyNode.Id, path)

	result := fss.ring.Resolve(path.String())

	// get header, check if it exists or handed off
	localheader := fss.headers.GetFileHeader(path)

	// if file exists locally or its been handed off
	if localheader.header.Exists {
		response := fss.comm.NewMsgMessage(fss.serviceId)
		response.Message.WriteBool(true)
		fss.comm.RespondSource(message, response)

	} else {
		// if I'm the master or we force local
		if result.IsFirst(fss.cluster.MyNode) || forceLocal {
			response := fss.comm.NewMsgMessage(fss.serviceId)
			response.Message.WriteBool(false)
			fss.comm.RespondSource(message, response)
		} else {
			fss.comm.RedirectFirst(result, message)
		}
	}
}
