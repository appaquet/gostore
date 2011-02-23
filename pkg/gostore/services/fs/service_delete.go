package fs

import (
	"gostore/comm"
	"gostore/log"
	"gostore/cluster"
	"os"
	"fmt"
)


/*
 * Delete
 */
func (fss *FsService) Delete(path *Path, recursive bool, context *Context) (returnError os.Error) {
	if context == nil {
		context = fss.NewContext()
	}

	message := fss.comm.NewMsgMessage(fss.serviceId)
	message.Function = "RemoteDelete"
	context.ApplyContext(message)

	// write payload
	message.Message.WriteString(path.String()) // path
	message.Message.WriteBool(recursive)       // recursive
	message.Message.WriteBool(true)            // first flag

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

func (fss *FsService) RemoteDelete(message *comm.Message) {
	strPath, _ := message.Message.ReadString() // path
	path := NewPath(strPath)
	recursive, _ := message.Message.ReadBool() // recursive flag
	first, _ := message.Message.ReadBool()     // first level flag

	log.Debug("%d FSS: Received a new delete message for path=%s recursive=%d\n", fss.cluster.MyNode.Id, path, recursive)

	resolveResult := fss.ring.Resolve(path.String())
	if resolveResult.IsFirst(fss.cluster.MyNode) {
		localheader := fss.headers.GetFileHeader(path)

		if !localheader.header.Exists {
			fss.comm.RespondError(message, ErrorFileNotFound)
			return

		} else {
			children := localheader.header.Children

			// if there are no children
			if len(children) == 0 {
				fss.Lock(path.String())
				localheader.header.Exists = false
				localheader.header.ClearChildren()
				localheader.header.Size = 0
				localheader.Save()

				// sync replicas
				syncChan := fss.sendToReplicaNode(resolveResult, func(node *cluster.Node) *comm.Message {
					msg := fss.comm.NewMsgMessage(fss.serviceId)
					msg.Function = "RemoteDeleteReplica"
					msg.Message.WriteString(path.String())             // path
					msg.Message.WriteInt64(localheader.header.Version) // version
					return msg
				})

				// wait for sync
				syncErr := <-syncChan
				if syncErr != nil {
					log.Error("FSS: Couldn't delete replica from nodes: %s\n", syncErr)
				}

				fss.Unlock(path.String())

			} else { // if there are children

				if recursive {
					// Lock the file
					fss.Lock(path.String())

					// Send delete to children
					c := make(chan int, 1)
					for _, child := range children {
						try := 0
						var deletechild func()
						deletechild = func() {
							childpath := path.ChildPath(child.Name)

							msg := fss.comm.NewMsgMessage(fss.serviceId)
							msg.Function = "RemoteDelete"

							// write payload
							msg.Message.WriteString(childpath.String()) // path
							msg.Message.WriteBool(recursive)            // recursive = 1 here
							msg.Message.WriteBool(false)                // not first here


							childres := fss.ring.Resolve(childpath.String())

							msg.Timeout = 1000 // TODO: Config
							msg.OnTimeout = func(last bool) (retry bool, handled bool) {
								if try < 10 {
									try++
									deletechild()
								} else {
									log.Error("FSS: Couldn't delete child=%s of path=%s after 10 tries\n", child, path)
									c <- 1
								}

								return true, false
							}
							msg.OnResponse = func(message *comm.Message) {
								c <- 1
							}

							fss.comm.SendNode(childres.GetFirst(), msg)
						}
						deletechild()

						<-c
					}

					// delete the file locally
					file := OpenFile(fss, localheader, 0)
					file.Delete()

					// delete in the header
					localheader.header.ClearChildren()
					localheader.header.Exists = false
					localheader.header.Size = 0
					localheader.Save()

					// sync replicas
					syncChan := fss.sendToReplicaNode(resolveResult, func(node *cluster.Node) *comm.Message {
						msg := fss.comm.NewMsgMessage(fss.serviceId)
						msg.Function = "RemoteDeleteReplica"
						msg.Message.WriteString(path.String())                  // path
						localheader.header.Version, _ = msg.Message.ReadInt64() // version
						return msg
					})

					// wait for sync
					syncErr := <-syncChan
					if syncErr != nil {
						log.Error("FSS: Couldn't delete replica from nodes: %s\n", syncErr)
						fss.comm.RespondError(message, os.NewError(fmt.Sprintf("Couldn't replicate %s to nodes: %s", path.String(), syncErr)))
						return
					}

					fss.Unlock(path.String())

				} else { // non recursive flag, I have children so respond error
					fss.comm.RespondError(message, ErrorNotEmpty)
					return
				}
			}

			// Remove the child from the parent (check ChildAdd, Write if logic changes here)
			if first {
				try := 0
				var deleteparent func()
				deleteparent = func() {
					parent := path.ParentPath()
					if !path.Equals(parent) {

						msg := fss.comm.NewMsgMessage(fss.serviceId)
						msg.Function = "RemoteChildRemove"
						msg.LastTimeoutAsError = false

						msg.Message.WriteString(parent.String())               // parent path
						msg.Message.WriteString(path.Parts[len(path.Parts)-1]) // name

						msg.Timeout = 1000 // TODO: Config
						msg.OnTimeout = func(last bool) (retry bool, handled bool) {
							if try < 10 {
								try++
								deleteparent()
							} else {
								log.Error("FSS: Couldn't delete path=%s from parent after 10 tries\n", path)
								fss.comm.RespondError(message, os.NewError(fmt.Sprintf("Couldn't delete %s from parent after 10 tries.", path.String())))
							}

							return true, false
						}

						parentResolve := fss.ring.Resolve(parent.String())
						fss.comm.SendNode(parentResolve.GetFirst(), msg)
					}
				}
				go deleteparent()

				// Send a confirmation
				result := fss.comm.NewMsgMessage(fss.serviceId)
				fss.comm.RespondSource(message, result)

			} else { // i'm an underneat child and asked to remove by parent, simply acknowledge it

				// Send an acknowledgement
				msg := fss.comm.NewMsgMessage(fss.serviceId)
				fss.comm.RespondSource(message, msg)
			}
		}

	} else { // not master, send it to master
		fss.comm.RedirectFirst(resolveResult, message)
	}
}

func (fss *FsService) RemoteDeleteReplica(message *comm.Message) {
	// read payload
	str, _ := message.Message.ReadString()
	path := NewPath(str)                      // path
	version, _ := message.Message.ReadInt64() // version

	log.Debug("%d FSS: Received sync delete replica for path '%s' version '%d'\n", fss.cluster.MyNode.Id, path, version)

	// Get the header
	localheader := fss.headers.GetFileHeader(path)

	// Delete the file localy
	file := OpenFile(fss, localheader, version)
	file.Delete()

	// Delete in the header
	localheader.header.Exists = false
	localheader.header.ClearChildren()
	localheader.header.Size = 0
	localheader.Save()

	// todo: add to garbage collector

	// Send an acknowledgement
	msg := fss.comm.NewMsgMessage(fss.serviceId)
	fss.comm.RespondSource(message, msg)
}
