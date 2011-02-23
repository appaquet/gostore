package fs

import (
	"os"
	"gostore/api/rest"
	"gostore/log"
)

type api struct {
	fss    *FsService
	server *rest.Server
}

func createApi(fss *FsService) *api {
	fsa := new(api)
	fsa.fss = fss

	fsa.server = rest.NewServer(fsa, fss.apiAddress)
	return fsa
}

func parsePath(req *rest.Request) (*Path, bool) {
	url := req.URL.Path

	path := NewPath(url)
	return path, path.Valid()
}

func (fsa *api) Handle(resp *rest.ResponseWriter, req *rest.Request) {
	/*
		GET	/path						Get whole data content
		HEAD /path						Get header
		POST /path						Write whole file
		DELETE /path					Delete file
		PUT /path?part=head				Update header
		PUT /path?part=data&off=..		Update data at offset X
	*/

	path, ok := parsePath(req)

	if ok {
		switch req.Method {
		case "POST":
			fsa.post(resp, req, path)
			break
		case "GET":
			part, ok := req.Params["part"]

			if !ok || part[0] == "data" {
				fsa.get(resp, req, path)
			} else {
				fsa.head(resp, req, path)
			}

			break
		case "HEAD":
			fsa.head(resp, req, path)
			break
		case "DELETE":
			fsa.delete(resp, req, path)
			break
		}
	} else {
		resp.ReturnError("Invalid path")
	}
}


func (api *api) post(resp *rest.ResponseWriter, req *rest.Request, path *Path) {
	log.Debug("FSS API: Received a write request for %d bytes\n", req.ContentLength)

	mimetype := "application/octet-stream"
	mtar, ok := req.Params["type"]
	if ok {
		mimetype = mtar[0]
	}

	err := api.fss.Write(path, req.ContentLength, mimetype, req.Body, nil)
	if err != nil {
		log.Error("API: Fs Write returned an error: %s\n", err)
		resp.ReturnError(err.String())
	}

	log.Debug("API: Fs Write returned\n")
}

func (api *api) get(resp *rest.ResponseWriter, req *rest.Request, path *Path) {
	log.Debug("FSS API: Received a read request for path %s\n", path)

	// TODO: Handle offset
	// TODO: Handle version
	// TODO: Handle size
	_, err := api.fss.Read(path, 0, -1, 0, resp, nil)
	log.Debug("API: Fs Read data returned\n")
	if err != nil && err != os.EOF {
		log.Error("API: Fs Read returned an error for %s: %s\n", path, err)
		resp.ReturnError(err.String())
	}
}

func (api *api) head(resp *rest.ResponseWriter, req *rest.Request, path *Path) {
	log.Debug("FSS API: Received a head request for path %s\n", path)

	header, err := api.fss.HeaderJSON(path, nil)

	resp.Write(header)

	log.Debug("API: Fs Header data returned\n")
	if err != nil {
		log.Error("API: Fs header returned an error: %s\n", err)
		resp.ReturnError(err.String())
	}
}


func (api *api) delete(resp *rest.ResponseWriter, req *rest.Request, path *Path) {
	log.Debug("FSS API: Received a delete request for %s\n", path)

	recursive := false
	mrec, ok := req.Params["recursive"]
	if ok {
		recursive = (mrec[0] == "1") || (mrec[0] == "true")
	}

	err := api.fss.Delete(path, recursive, nil)
	if err != nil {
		log.Error("API: Fs Write returned an error: %s\n", err)
		resp.ReturnError(err.String())
	}
}
