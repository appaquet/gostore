package db

import (
	"fmt"
)


type container struct {
	objects map[string]object
}

func newContainer() container {
	c := container{
		objects: make(map[string]object),
	}
	return c
}

func (c *container) String() string {
	return fmt.Sprintf("container[%s]", c.objects)
}


type object struct {
	flags    byte
	segment  uint16
	position uint32

	data interface{}
}

const (
	obj_flag_deleted	byte = 0x01 // has been deleted
	obj_flag_new		byte = 0x02 // mark as "new" in a viewstate (modified)
)

func (o *object) String() string {
	return fmt.Sprintf("object[seg=%d, pos=%d, data=%v]", o.segment, o.position, o.data)
}

func (o *object) isFlag(flag byte) bool {
	if o.flags&flag == flag {
		return true
	}

	return false
}

func (o *object) setFlag(flag byte, value bool) {
	if value {
		o.flags = o.flags | flag
	} else {
		o.flags = o.flags | ^flag
	}
}
