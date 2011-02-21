// Author: Andre-Philippe Paquet
// Date: November 2010

//	REST API helper. Create an http server and delegate calls for this
//	API to the interface implementation.
package rest

import (
	"http"
	"net"
	"gostore/log"
	"io/ioutil"
)

// API Handler interface implemented by services 
// serving this API
type Handler interface {
	Handle(r *ResponseWriter, req *Request)
}

// API server
type Server struct {
	handler Handler
	servmux *http.ServeMux
}


// Returns an API Server
func NewServer(handler Handler, adr string) *Server {
	server := new(Server)
	server.handler = handler
	server.servmux = http.NewServeMux()

	con, err := net.Listen("tcp", adr)
	if err != nil {
		log.Error("API: Couldn't create listener socket: %s\n", err)
	}

	// Add handling function at root, delegating everything to the handler
	server.servmux.HandleFunc("/", func(httpresp http.ResponseWriter, httpreq *http.Request) {
		log.Debug("API: Connection on url %s\n", httpreq.URL)

		resp := &ResponseWriter{httpresp}

		var req *Request
		if req = NewRequest(resp, httpreq); req == nil {
			log.Error("API: Couldn't create request object")
			return
		}

		handler.Handle(resp, req)

		// TODO: Remove that! Shouldn't be here!!
		// Read the rest, so we don't cause Broken Pipe on the other end if we don't read to the end
		ioutil.ReadAll(req.Body)
	})

	// Start serving the API on another thread
	go func() {
		log.Debug("API: Starting API server on adr %s\n", adr)
		err = http.Serve(con, server.servmux)
		con.Close()

		if err != nil {
			log.Fatal("API: Serve error: ", err.String())
		}
	}()

	return server
}
