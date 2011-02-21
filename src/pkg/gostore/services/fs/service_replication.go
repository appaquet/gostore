package fs

import (
	"gostore/comm"
	"gostore/log"
	"gostore/cluster"
	"time"
	"fmt"
	"os"
)

/*
 * Replication watcher
 */
type ReplicationFile struct {
	path *Path
}

func (fss *FsService) replicationWatcher() {
	for fss.running {
		if fss.replQueue.Len() > 0 {
			fss.replQueueMutex.Lock()
			if fss.replQueue.Len() > 0 {
				next := fss.replQueue.Front()
				fss.replQueue.Remove(next)
				fss.replQueueMutex.Unlock()

				path := next.Value.(*Path)
				localheader := fss.headers.GetFileHeader(path)

				log.Info("%d: FSS: Starting replica download for path %s version %d...", fss.cluster.MyNode.Id, path, localheader.header.Version)

				// TODO: Make sure we don't download the same replica twice...

				// check if the file doesn't already exist locally
				file := OpenFile(fss, localheader, 0)
				if !file.Exists() {
					// TODO: Use config to get temp path
					tempfile := fmt.Sprintf("%s/%d.%d.%d.data", os.TempDir(), path.Hash(), time.Nanoseconds(), localheader.header.Version)

					fd, err := os.Open(tempfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
					if err == nil {
						_, err = fss.Read(path, 0, -1, 0, fd, nil)
						fd.Close()
						if err == nil {
							os.Rename(tempfile, file.datapath)
							log.Info("%d: FSS: Successfully replicated %s version %d locally", fss.cluster.MyNode.Id, path, localheader.header.Version)

						} else {
							log.Error("%d: FSS: Couldn't replicate file %s locally because couldn't read: %s", fss.cluster.MyNode.Id, path, err)
						}

					} else {
						log.Error("%d: FSS: Couldn't open temporary file %s to download replica localy for path %s", fss.cluster.MyNode.Id, tempfile, path)
						os.Remove(tempfile)
					}

				} else {
					log.Info("%d: FSS: Local replica for %s version %d already exist", fss.cluster.MyNode.Id, path, localheader.header.Version)
				}

			} else {
				fss.replQueueMutex.Unlock()
			}

		} else {
			// if no more replica in the queue, stop replica force
			if fss.replQueue.Len() == 0 {
				fss.replForce = false
			}
		}

		// TODO: Put that in configuration
		if !fss.replForce {
			time.Sleep(100 * 1000 * 1000)
		}
	}
}

func (fss *FsService) Flush() os.Error {
	fss.replForce = true

	for fss.replQueue.Len() > 0 || fss.replForce {
		// sleep 1ms
		time.Sleep(1000000)
	}

	return nil
}

func (fss *FsService) replicationEnqueue(path *Path) {
	fss.replQueueMutex.Lock()
	fss.replQueue.PushBack(path)
	fss.replQueueMutex.Unlock()
}


func (fss *FsService) sendToReplicaNode(resolv *cluster.ResolveResult, req_cb func(node *cluster.Node) *comm.Message) chan os.Error {
	toSyncCount := resolv.Count() - 1 // minus one for the master
	var syncError os.Error = nil
	myNodeId := fss.cluster.MyNode.Id
	errChan := make(chan os.Error, 1) // channel used to return data to the messageor
	c := make(chan bool, toSyncCount) // channel used to wait for all replicas

	if toSyncCount > 0 {
		go func() {
			for i := 0; i < resolv.Count(); i++ {
				node := resolv.Get(i)

				if node.Status == cluster.Status_Online && node.Id != myNodeId {
					// get the new message
					req := req_cb(node)

					req.Timeout = 1000 // TODO: Config
					req.OnResponse = func(message *comm.Message) {
						log.Debug("%d: FSS: Received acknowledge message for message %s\n", fss.cluster.MyNode.Id, req)
						c <- true
					}
					req.OnTimeout = func(last bool) (retry bool, handled bool) {
						// TODO: Retry it!
						syncError = comm.ErrorTimeout
						log.Error("%d: FSS: Couldn't send message to replicate node %s because of a timeout for message %s\n", fss.cluster.MyNode.Id, node, req)
						c <- true

						return true, false
					}
					req.OnError = func(message *comm.Message, syncError os.Error) {
						log.Error("%d: FSS: Received an error while sending to replica %s for message %s: %d %s\n", fss.cluster.MyNode.Id, req, node, syncError)
						c <- true
					}

					fss.comm.SendNode(node, req)
				}
			}

			// wait for nodes to sync the handoff
			for i := 0; i < toSyncCount; i++ {
				<-c
			}

			errChan <- syncError
		}()
	} else {
		errChan <- nil
	}

	return errChan
}
