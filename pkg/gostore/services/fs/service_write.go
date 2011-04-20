package fs

import (
	"gostore/comm"
	"gostore/log"
	"gostore/cluster"
	"os"
	"time"
	"io"
	"fmt"
)


/*
 * Write
 */
func (fss *FsService) Write(path *Path, size int64, mimetype string, data io.Reader, context *Context) (returnError os.Error) {
	if context == nil {
		context = fss.NewContext()
	}

	message := fss.comm.NewDataMessage(fss.serviceId)
	message.Function = "RemoteWrite"
	context.ApplyContext(message)

	// write payload
	message.Message.WriteString(path.String()) // path
	message.Message.WriteString(mimetype)      // mimetype
	message.Data = data
	message.DataSize = size

	message.OnResponse = func(response *comm.Message) {
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


func (fss *FsService) RemoteWrite(message *comm.Message) {
	// read payload
	str, _ := message.Message.ReadString() // path
	path := NewPath(str)
	mimetype, _ := message.Message.ReadString() // mimetype


	log.Debug("%d FSS: Received new write message for path %s and size of %d and type %s\n", fss.cluster.MyNode.Id, path, message.DataSize, mimetype)

	resolveResult := fss.ring.Resolve(path.String())

	if !resolveResult.IsFirst(fss.cluster.MyNode) {
		log.Error("FSS: Received write for which I'm not master: %s\n", message)
		fss.comm.RespondError(message, os.NewError(fmt.Sprintf("Cannot accept write, I'm not the master for %s", path)))
		return
	}

	// Write the data to a temporary file
	tempfile := fmt.Sprintf("%s/%d.%d.data", os.TempDir(), path.Hash(), time.Nanoseconds()) // TODO: Use config to get temp path

	fd, err := os.OpenFile(tempfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		os.Remove(tempfile)
		log.Error("%d: FSS: Got an error while creating a temporary file (%s) for write of %s: %s", tempfile, path, err)
		fss.comm.RespondError(message, os.NewError(fmt.Sprintf("Got an error while creating a temporary file: %s", err)))
		return
	}

	_, err = io.Copyn(fd, message.Data, message.DataSize)
	if err != nil && err != os.EOF {
		log.Error("%d: FSS: Got an error while creating a temporary file (%s) for write of %s: %s", tempfile, path, err)
		fss.comm.RespondError(message, os.NewError(fmt.Sprintf("Got an error while creating a temporary file: %s", err)))

		fd.Close()
		os.Remove(tempfile)
		return
	}

	fd.Close()

	fss.Lock(path.String())
	localheader := fss.headers.GetFileHeader(path)
	version := localheader.header.NextVersion
	localheader.header.NextVersion++
	localheader.header.Path = path.String()
	localheader.header.Name = path.BaseName()
	localheader.header.MimeType = mimetype
	localheader.header.Size = message.DataSize
	localheader.header.Version = version
	localheader.header.Exists = true

	file := OpenFile(fss, localheader, version)
	os.Rename(tempfile, file.datapath)

	localheader.Save()

	fss.Unlock(path.String())

	// send to parent
	syncParent := make(chan os.Error, 1)
	go func() {
		parent := path.ParentPath()
		if !path.Equals(parent) {
			req := fss.comm.NewMsgMessage(fss.serviceId)
			req.Function = "RemoteChildAdd"

			req.Timeout = 5000 // TODO: Config
			req.Retries = 10
			req.RetryDelay = 100
			req.OnTimeout = func(last bool) (retry bool, handled bool) {
				if last {
					log.Error("%d FSS: Couln't add %s to parent after 10 tries\n", fss.cluster.MyNode.Id, path)
				}
				return true, false
			}
			req.LastTimeoutAsError = true
			req.OnError = func(response *comm.Message, error os.Error) {
				syncParent <- error
			}
			req.OnResponse = func(response *comm.Message) {
				syncParent <- nil
			}

			req.Message.WriteString(parent.String())               // path
			req.Message.WriteString(path.Parts[len(path.Parts)-1]) // name
			req.Message.WriteString(mimetype)                      // type
			req.Message.WriteInt64(message.DataSize)               // size

			parentResolve := fss.ring.Resolve(parent.String())
			fss.comm.SendFirst(parentResolve, req)
		} else {
			syncParent <- nil
		}
	}()

	// send new header to all replicas
	syncReplica := fss.sendToReplicaNode(resolveResult, func(node *cluster.Node) *comm.Message {
		req := fss.comm.NewMsgMessage(fss.serviceId)
		req.Function = "RemoteReplicaVersion"

		req.Message.WriteString(path.String())                 // path
		req.Message.WriteInt64(localheader.header.Version)     // current version
		req.Message.WriteInt64(localheader.header.NextVersion) // next version
		req.Message.WriteInt64(localheader.header.Size)        // size
		req.Message.WriteString(localheader.header.MimeType)   // mimetype

		return req
	})

	replicaError := <-syncReplica
	if replicaError != nil {
		log.Error("FSS: Couldn't replicate header to nodes: %s\n", replicaError)
		fss.comm.RespondError(message, replicaError)
		return

		// TODO: ROLLBACK!!
	}

	parentError := <-syncParent
	if parentError != nil {
		log.Error("FSS: Couldn't add myself to parent: %s\n", parentError)
		fss.comm.RespondError(message, parentError)
		return

		// TODO: ROLLBACK!!
	}

	// confirm
	log.Debug("%d FSS: Sending write confirmation for path %s message %s\n", fss.cluster.MyNode.Id, path, message)
	response := fss.comm.NewMsgMessage(fss.serviceId)
	fss.comm.RespondSource(message, response)
}
