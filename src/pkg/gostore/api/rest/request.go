// Author: Andre-Philippe Paquet
// Date: November 2010

package rest

import (
	"http"
	"gostore/log"
)


// API Request structure. Composed of the normal http.Request structure
// plus parsed parameters
type Request struct {
	*http.Request

	Params map[string][]string
}

// Returns a new request composed of the original http.Request structure
func NewRequest(r *ResponseWriter, httpreq *http.Request) *Request {
	req := new(Request)
	req.Request = httpreq

	// try to parse the URL
	params, err := http.ParseQuery(httpreq.URL.RawQuery)
	if err != nil {
		log.Error("API: Couldn't parse GET parameters: %s\n", err)
		r.ReturnError("Parameters error")
		return nil
	}
	req.Params = params

	return req
}
