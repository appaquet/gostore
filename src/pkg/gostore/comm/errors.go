package comm

import (
	"os"
)

var (
	ErrorTimeout = os.NewError("Message timeout")
	ErrorUnknown = os.NewError("Unknown error")
)

func WriteErrorPayload(message *Message, error os.Error) {
	message.Message.WriteString(error.String())
}

func ReadErrorPayload(message *Message) os.Error {
	str, _ := message.Message.ReadString()
	return os.NewError(str)
}
