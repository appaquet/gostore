package comm

import (
	"gostore"
	"os"
	"reflect"
	"strings"
	"gostore/log"
)

// interface that must be implemented by services
type Service interface {
	HandleUnmanagedMessage(msg *Message)
	HandleUnmanagedError(errorMessage *Message, error os.Error)
	Boot()
}


// services collection
type Services struct {
	wrappers        []*serviceWrapper
	service2wrapper map[Service]*serviceWrapper
}

func newServices() *Services {
	services := new(Services)
	services.wrappers = make([]*serviceWrapper, 255)
	services.service2wrapper = make(map[Service]*serviceWrapper)
	return services
}

func (services *Services) AddService(service Service, sconfig gostore.ConfigService) {
	wrapper := new(serviceWrapper)
	wrapper.service = service
	wrapper.name2id = make(map[string]byte)
	wrapper.id2method = make([]*reflect.Method, 255)

	if sconfig.Id == 0 {
		log.Fatal("Service id cannot be 0")
	}

	wrapper.id = sconfig.Id
	services.wrappers[sconfig.Id] = wrapper
	services.service2wrapper[service] = wrapper

	wrapper.registerFunctions()
}

func (services *Services) GetService(id byte) Service {
	return services.wrappers[id].service
}

func (services *Services) GetWrapper(id byte) *serviceWrapper {
	return services.wrappers[id]
}

func (services *Services) ServicesIter() chan Service {
	c := make(chan Service)
	go func() {
		for _, wrapper := range services.wrappers {
			if wrapper != nil {
				c <- wrapper.service
			}
		}
		close(c)
	}()
	return c
}

func (services *Services) BootServices() {
	for _, wrapper := range services.wrappers {
		if wrapper != nil {
			wrapper.service.Boot()
		}
	}
}


// service wrapper with remotely callable methods of the service
type serviceWrapper struct {
	service     Service
	id          byte
	name2id     map[string]byte
	id2method   []*reflect.Method
	methodCount byte
}

func (wrapper *serviceWrapper) registerFunctions() {
	typ := reflect.TypeOf(wrapper.service)

	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mname := method.Name

		if strings.HasPrefix(mname, "Remote") {
			wrapper.name2id[mname] = wrapper.methodCount
			wrapper.id2method[wrapper.methodCount] = &method
			wrapper.methodCount++
		}
	}
}

func (wrapper *serviceWrapper) functionName2Id(name string) byte {
	id, found := wrapper.name2id[name]

	if !found {
		log.Fatal("Method %s not found in service #%d!!", name, wrapper.id)
		return 0
	}

	return id + RESERVED_FUNCTIONS
}

func (wrapper *serviceWrapper) callFunction(id byte, message *Message) (handled bool) {
	method := wrapper.id2method[id-RESERVED_FUNCTIONS]

	if method != nil {
		rService := reflect.ValueOf(wrapper.service)
		rMessage := reflect.ValueOf(message)
		method.Func.Call([]reflect.Value{rService, rMessage})
		return true
	}

	return false
}
