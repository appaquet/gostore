package db

import (
	"fmt"
)


type Root struct {
	containers map[string]Container
}

func newRoot() *Root {
	r := new(Root)
	r.containers = make(map[string]Container)
	return r
}

func (r *Root) String() string {
	return fmt.Sprintf("Root[%s]", r.containers)
}


type Container struct {
	objects map[string]ContainerObject
}

func (c *Container) String() string {
	return fmt.Sprintf("Container[%s]", c.objects)
}





type ContainerObject struct {
	flags		byte
	segment		uint16
	position	uint32

	data		interface{}
}

const (
	obj_flag_deleted byte = 0x01
)

func (o *ContainerObject) String() string {
	return fmt.Sprintf("ContainerObject[seg=%d, pos=%d, data=%v]", o.segment, o.position, o.data)
}

func (o *ContainerObject) isFlag(flag byte) bool {
	if o.flags&flag == flag {
		return true
	}

	return false
}

func (o *ContainerObject) setFlag(flag byte, value bool) {
	if value {
		o.flags = o.flags | flag
	} else {
		o.flags = o.flags | ^flag
	}
}

