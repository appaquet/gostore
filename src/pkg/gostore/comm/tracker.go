package comm

import (
	"time"
	"gostore/log"
	"gostore/cluster"
)

type MessageTracker struct {
	message  *Message
	sentTime int64
	lastTime int64
	retries  int

	destination *cluster.Node
}

func (comm *Comm) startMessageTracker() {
	// track messages
	for {
		if comm.running {
			for hash, msgTrack := range comm.messageTrackers {
				diff := int((time.Nanoseconds() - msgTrack.lastTime) / 1000000)

				// TODO: Use the right retry delay 

				// check the acknowledgement
				if msgTrack.message.Timeout != 0 && diff >= msgTrack.message.Timeout {
					comm.trackersMutex.Lock()
					msgTrack, found := comm.messageTrackers[hash]

					// make sure the message tracker is still there
					// we may have received an ack while locking
					if found {
						msgTrack.retries++
						msgTrack.lastTime = time.Nanoseconds() + int64(msgTrack.message.RetryDelay*1000*1000)

						if msgTrack.retries > msgTrack.message.Retries {
							log.Warning("%d: Comm: Timeout for message %s after %d ms. Not retrying!\n", comm.Cluster.MyNode.Id, msgTrack.message, diff)

							// remove from acknowledgable messages list
							comm.messageTrackers[hash] = nil, false

							go func() {
								if msgTrack.message.OnTimeout != nil {
									msgTrack.message.OnTimeout(true)
								}

								if msgTrack.message.LastTimeoutAsError {
									msgTrack.message.OnError(msgTrack.message, ErrorTimeout)
								}

								msgTrack.message.Release()
							}()

						} else {
							go func() {
								retry := true
								handled := false
								if msgTrack.message.OnTimeout != nil {
									retry, handled = msgTrack.message.OnTimeout(false)
								}

								if retry && !handled {
									seekable, _ := msgTrack.message.DataIsSeekable()
									if msgTrack.message.Data != nil && !seekable {
										log.Error("%d: Comm: Cannot automatically retry message %s after %d ms because data is not seekable!", comm.Cluster.MyNode.Id, msgTrack.message, diff)
									} else {
										log.Warning("%d: Comm: Timeout for message %s after %d ms. Retrying %d of %d\n", comm.Cluster.MyNode.Id, msgTrack.message, diff, msgTrack.retries, msgTrack.message.Retries)

										comm.SendNode(msgTrack.destination, msgTrack.message)
									}
								}
							}()
						}
					}

					comm.trackersMutex.Unlock()

					// cleanup for OnError and OnResponse tracked messages
				} else if diff >= TRACKER_CLEAN_TIME {
					comm.trackersMutex.Lock()
					_, found := comm.messageTrackers[hash]

					// make sure the message is still there
					// we may have received an error or response 
					// while waiting for the locking
					if found {
						comm.messageTrackers[hash] = nil, false
					}

					comm.trackersMutex.Unlock()
				}
			}
		}

		// sleep
		time.Sleep(TRACKER_LOOP_SLEEP * 1000 * 1000)
	}
}

func (comm *Comm) watchMessage(message *Message, destination *cluster.Node) {
	comm.trackersMutex.Lock()

	hash := message.Hash()
	msgTrack, found := comm.messageTrackers[hash]
	if !found {
		msgTrack := new(MessageTracker)
		msgTrack.message = message
		msgTrack.sentTime = time.Nanoseconds()
		msgTrack.lastTime = msgTrack.sentTime
		msgTrack.destination = destination
		comm.messageTrackers[message.Hash()] = msgTrack
	} else {
		msgTrack.lastTime = time.Nanoseconds()
		msgTrack.destination = destination
	}

	comm.trackersMutex.Unlock()
}

// Handles an incoming message. If the message is handled by one
// of message callbacks (OnError, OnResponse), returns true so
// that it wont be sent to services
func (comm *Comm) handleTracker(message *Message) bool {
	hash := message.InitHash()
	_, found := comm.messageTrackers[hash]
	if found {
		comm.trackersMutex.Lock()

		// check if the message is still there
		// we may have been marked as timeout while locking
		ackmsg, found := comm.messageTrackers[hash]
		if found {
			comm.messageTrackers[hash] = nil, false
			comm.trackersMutex.Unlock()

			if message.FunctionId == FUNC_ERROR && ackmsg.message.OnError != nil {
				err := ReadErrorPayload(message)
				message.SeekZero()
				ackmsg.message.OnError(message, err)
				return true

			} else if ackmsg.message.OnResponse != nil {
				ackmsg.message.OnResponse(message)
				return true
			}
		} else {
			comm.trackersMutex.Unlock()
		}
	}

	return false
}
