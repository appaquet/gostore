package fs

import (
	"gostore/comm"
)

type Context struct {
	ForceLocal bool

	MessageTimeout    int
	MessageRetry      int
	MessageRetryDelay int
}


func (fss *FsService) NewContext() *Context {
	context := new(Context)
	context.ForceLocal = false

	// TODO: read from config
	context.MessageTimeout = 1000
	context.MessageRetry = 3
	context.MessageRetryDelay = 500

	return context
}

func (context *Context) ApplyContext(message *comm.Message) {
	message.Timeout = context.MessageTimeout
	message.Retries = context.MessageRetry
	message.RetryDelay = context.MessageRetryDelay

	if message.Timeout > 0 {
		message.LastTimeoutAsError = true
	}
}
