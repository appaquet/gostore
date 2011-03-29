// Author: Andre-Philippe Paquet
// Date: November 2010

package rest

import (
	"json"
	"http"
	"gostore/log"
)

// Response writer structure composed of the http response writer. 
// Add JSON returns and error handling functionality
type ResponseWriter struct {
	http.ResponseWriter
}

// Returns an error in a JSON format
func (r *ResponseWriter) ReturnError(msg string) {
	resp := make(map[string]interface{})
	resp["message"] = msg
	r.ReturnJSON(resp)
}

// Returns an interface marshaled in JSON
func (r *ResponseWriter) ReturnJSON(v interface{}) {

	bytes, err := json.Marshal(v)
	if err != nil {
		log.Error("API: Couldn't marshall JSON response: %s\n", err)
	}

	r.Header().Set("Content-Type", "application/json")
	r.Write(bytes)
}
