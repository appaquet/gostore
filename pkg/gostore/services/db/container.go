package db

import (
	"fmt"
	"sync"
)


type container struct {
	objects map[string]object
	mutex	*sync.Mutex
}

func newContainer() container {
	c := container{
		objects: make(map[string]object),
		mutex: new(sync.Mutex),
	}
	return c
}

func (c *container) getObject(key string) (object, bool) {
	c.mutex.Lock()
	o, f := c.objects[key]
	c.mutex.Unlock()

	return o,f
}

func (c *container) setObject(key string, obj object) {
	c.mutex.Lock()
	c.objects[key] = obj
	c.mutex.Unlock()
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
	obj_flag_exists		byte = 1  // exists on disk, has not been deleted
	obj_flag_new		byte = 2  // mark as "new" in a viewstate (modified)

	obj_flag_partial1	byte = 4  // partial count bit #1
	obj_flag_partial2	byte = 8  // partial count bit #1
	obj_flag_partial3	byte = 16 // partial count bit #1
	obj_flag_partial4	byte = 32 // partial count bit #1
)

func (o *object) String() string {
	return fmt.Sprintf("object[seg=%d, pos=%d, data=%v]", o.segment, o.position, o.data)
}

func (o *object) getFlag(flag byte) bool {
	if o.flags&flag == flag {
		return true
	}
	return false
}

func (o *object) setFlag(flag byte, value bool) {
	if value {
		o.flags = o.flags | flag
	} else {
		o.flags = o.flags & ^flag
	}
}

func (o *object) setPartialCount(count byte) {
	if count > 15 {
		panic("Cannot set partial modification count over 15")
	}
	o.flags = (o.flags & ^byte(obj_flag_partial1+obj_flag_partial2+obj_flag_partial3+obj_flag_partial4)) | count << 2
}

func (o *object) getPartialCount() byte {
	var count byte
	count = o.flags << 2
	count = count >> 4
	return count
}
