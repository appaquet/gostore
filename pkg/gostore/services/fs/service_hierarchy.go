package fs

import (
	"gostore/comm"
	"gostore/log"
	"gostore/cluster"
	"os"
	"fmt"
)

/*
 * Children
 */
func (fss *FsService) RemoteChildAdd(message *comm.Message) {
	// read payload
	str, _ := message.Message.ReadString()
	path := NewPath(str)                        // path
	child, _ := message.Message.ReadString()    // name
	mimetype, _ := message.Message.ReadString() // type
	size, _ := message.Message.ReadInt64()      // size

	log.Debug("%d FSS: Received message to add new child '%s' to '%s' (size=%d, type=%s)\n", fss.cluster.MyNode.Id, child, path, size, mimetype)

	// resolve path
	mynode := fss.cluster.MyNode
	resolv := fss.ring.Resolve(path.String())

	// only the master has the lock
	if resolv.IsFirst(mynode) {
		fss.Lock(path.String())
	}

	// add the child to the header
	localheader := fss.headers.GetFileHeader(path)
	existed := localheader.header.Exists

	localheader.header.Exists = true
	localheader.header.AddChild(child, mimetype, size)
	localheader.Save()

	// if i'm master, replicate to nodes and add ourself to master
	if resolv.IsFirst(mynode) {
		// unlock the path
		fss.Unlock(path.String())

		// cascade: add ourself to our parent. (check write, delete if logic changes here)
		syncParent := make(chan os.Error, 1)
		go func() {
			parent := path.ParentPath()
			if !path.Equals(parent) {
				msg := fss.comm.NewMsgMessage(fss.serviceId)
				msg.Function = "RemoteChildAdd"

				msg.Timeout = 5000 // TODO: Config
				msg.Retries = 10
				msg.RetryDelay = 100
				msg.OnTimeout = func(last bool) (retry bool, handled bool) {
					if last {
						log.Error("%d FSS: Couln't add %s to parent after 10 tries\n", fss.cluster.MyNode.Id, path)
					}
					return true, false
				}
				msg.LastTimeoutAsError = true
				msg.OnError = func(response *comm.Message, error os.Error) {
					syncParent <- error
				}
				msg.OnResponse = func(response *comm.Message) {
					syncParent <- nil
				}

				msg.Message.WriteString(parent.String())               // path
				msg.Message.WriteString(path.Parts[len(path.Parts)-1]) // name
				msg.Message.WriteString(mimetype)                      // type
				msg.Message.WriteInt64(size)                           // size

				parentResolve := fss.ring.Resolve(parent.String())
				fss.comm.SendFirst(parentResolve, msg)
			} else {
				syncParent <- nil
			}
		}()

		// replicate to nodes
		syncChan := fss.sendToReplicaNode(resolv, func(node *cluster.Node) *comm.Message {
			msg := fss.comm.NewMsgMessage(fss.serviceId)
			msg.Function = "RemoteChildAdd"

			msg.Message.WriteString(path.String()) // path
			msg.Message.WriteString(child)         // name
			msg.Message.WriteString(mimetype)      // type
			msg.Message.WriteInt64(size)           // size

			return msg
		})

		// wait for replicas sync check for sync error
		syncError := <-syncChan
		if syncError != nil {
			log.Error("FSS: Couldn't replicate add child to nodes: %s\n", syncError)
			fss.comm.RespondError(message, os.NewError(fmt.Sprintf("Couldn't replicate add child to nodes: %s\n", syncError)))
			// TODO: ROLLBACK!!
		}

		if !existed {
			parentError := <-syncParent
			if parentError != nil {
				log.Error("FSS: Couldn't add myself to parent: %s\n", parentError)
				fss.comm.RespondError(message, parentError)
				// TODO: ROLLBACK!!
			}
		}
	}

	// Send an acknowledgement
	msg := fss.comm.NewMsgMessage(fss.serviceId)
	fss.comm.RespondSource(message, msg)
}

func (fss *FsService) RemoteChildRemove(message *comm.Message) {
	str, _ := message.Message.ReadString() // path
	path := NewPath(str)
	child, _ := message.Message.ReadString() // child

	log.Debug("FSS: Received message to remove the child %s from %s\n", child, path)

	// resolve path
	mynode := fss.cluster.MyNode
	resolv := fss.ring.Resolve(path.String())

	// only the master has the lock
	if resolv.IsFirst(mynode) {
		fss.Lock(path.String())
	}

	localheader := fss.headers.GetFileHeader(path)
	localheader.header.RemoveChild(child)

	if resolv.IsFirst(mynode) {
		// replicate to nodes
		syncChan := fss.sendToReplicaNode(resolv, func(node *cluster.Node) *comm.Message {
			msg := fss.comm.NewMsgMessage(fss.serviceId)
			msg.Function = "RemoteChildRemove"

			msg.Message.WriteString(path.String()) // path
			msg.Message.WriteString(child)         // child name

			return msg
		})

		// wait for replicas sync
		syncError := <-syncChan

		// check for sync error
		if syncError != nil {
			log.Error("FSS: Couldn't replicate remove child to nodes: %s\n", syncError)
			fss.comm.RespondError(message, os.NewError("Couldn't replicate remove child to all nodes"))
		}

		// unlock
		fss.Unlock(path.String())
	}

	// Send an acknowledgement
	msg := fss.comm.NewMsgMessage(fss.serviceId)
	fss.comm.RespondSource(message, msg)
}

func (fss *FsService) Children(path *Path, context *Context) (returnValue []FileChild, returnError os.Error) {
	if context == nil {
		context = fss.NewContext()
	}

	message := fss.comm.NewMsgMessage(fss.serviceId)
	message.Function = "RemoteChildrenList"
	context.ApplyContext(message)

	// write payload
	message.Message.WriteString(path.String())    // path
	message.Message.WriteBool(context.ForceLocal) // force local

	message.OnResponse = func(response *comm.Message) {
		count, _ := response.Message.ReadUint16() // children count
		returnValue = make([]FileChild, count)

		var i uint16
		for i = 0; i < count; i++ {
			name, _ := response.Message.ReadString()     // child name
			mimetype, _ := response.Message.ReadString() // child mime type
			size, _ := response.Message.ReadInt64()      // child size

			returnValue[i] = NewFileChild(name, mimetype, size)
		}

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

func (fss *FsService) RemoteChildrenList(message *comm.Message) {
	str, _ := message.Message.ReadString()
	forceLocal, _ := message.Message.ReadBool()
	path := NewPath(str)

	log.Debug("FSS: Received message to list child for %s\n", path)

	result := fss.ring.Resolve(path.String())

	// I'm one of the nodes
	if result.InOnlineNodes(fss.cluster.MyNode) {
		localheader := fss.headers.GetFileHeader(path)

		// we have the header locally
		if localheader.header.Exists {
			children := localheader.header.Children

			// Create the message to send back
			var count uint16 = uint16(len(children))

			response := fss.comm.NewMsgMessage(fss.serviceId)

			// Write child count
			response.Message.WriteUint16(count) // children count
			for _, child := range children {
				response.Message.WriteString(child.Name)     // name
				response.Message.WriteString(child.MimeType) // type
				response.Message.WriteInt64(child.Size)      // size
			}

			fss.comm.RespondSource(message, response)

		} else { // we don't have the header, we redirect to the appropriate node

			// if i'm the master or forced local
			if result.IsFirst(fss.cluster.MyNode) || forceLocal {
				fss.comm.RespondError(message, ErrorFileNotFound)
			} else {
				fss.comm.RedirectFirst(result, message)
			}
		}

	} else { // I'm not one of the nodes

		// if its was forced local
		if forceLocal {
			fss.comm.RespondError(message, ErrorFileNotFound)
		} else {
			fss.comm.RedirectOne(result, message)
		}
	}
}
