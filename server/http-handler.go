package main


import (
	"net/http"
	"strings"
	"io"
	"alaredis/lib"
	"errors"
	"strconv"
	"log"
)


type HttpHandler struct {
	storage *Storage
	bodyParser lib.BodyParser
	opBodyParsers []func(r io.Reader, val *interface{}) error
}


var OPERATIONS = map[string]int {
	`delete`: OP_DELETE,
	`get`: OP_GET,
	`set`: OP_SET,
	`lset`: OP_LSET,
	`lseti`: OP_LSETI,
	`lget`: OP_LGET,
	`lgeti`: OP_LGETI,
	`dset`: OP_DSET,
	`dseti`: OP_DSETI,
	`dget`: OP_DGET,
	`dgeti`: OP_DGETI,
	`dkeys`: OP_DKEYS,
}



func NewHttpHandler(storage *Storage, bodyParser lib.BodyParser) *HttpHandler {
	h := new(HttpHandler)
	h.storage = storage
	h.bodyParser = bodyParser

	h.opBodyParsers = make([]func(r io.Reader, val *interface{}) error, len(OPERATIONS))
	h.opBodyParsers[OP_SET] = func(r io.Reader, val *interface{}) error {
		v, err := h.bodyParser.GetStringValue(r)
		*val = v
		return err
	}
	h.opBodyParsers[OP_LSETI] = h.opBodyParsers[OP_SET]
	h.opBodyParsers[OP_DSETI] = h.opBodyParsers[OP_SET]
	h.opBodyParsers[OP_LSET] = func(r io.Reader, val *interface{}) error {
		v, err := h.bodyParser.GetListValue(r)
		*val = v
		return err
	}
	h.opBodyParsers[OP_DSET] = func(r io.Reader, val *interface{}) error {
		v, err := h.bodyParser.GetDictValue(r)
		*val = v
		return err
	}

	return h
}

func (h *HttpHandler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	req, err := h.createInnerRequest(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	//log.Printf("Received request %s with body '%v'", r.URL, (*req).val)
	h.storage.processInnerRequest(req)
	select {
	case val:=<-req.outCh:
		if val == nil {
			w.WriteHeader(http.StatusNoContent)
		} else {
			buf, err := h.bodyParser.ComposeBody(val)
			if err == nil {
				w.Write(buf.Bytes())
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	case err:=<-req.errChan:
		if err != nil {
			switch err.(type) {
			case *BadRequest:
				http.Error(w, err.Error(), http.StatusBadRequest)
			case *ObjectNotFound:
				http.Error(w, err.Error(), http.StatusNotFound)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else {
			log.Printf("Got nil error from request %+v", req)
		}
	}
}


func (h *HttpHandler) createInnerRequest(w http.ResponseWriter, r *http.Request) (*innerRequest, error) {
	// url - /<operation>/<key>/<idx>
	pathParams := strings.Split(r.URL.Path, `/`);
	op, ok := OPERATIONS[pathParams[1]]
	if !ok {
		return nil, errors.New("Operation is not supported or defined")
	}

	if !isMethodSupported(r.Method, op) {
		return nil, errors.New("Method "+r.Method+" is not allowed for requested operation")
	}

	if len(pathParams) < 3 || len(pathParams[2])== 0 {
		return nil, errors.New("Key is not set or is empty")
	}
	key := pathParams[2]
	var idx string
	if op == OP_LGETI || op == OP_LSETI || op == OP_DSETI || op == OP_DGETI {
		if len(pathParams) < 3 || len(pathParams[3]) == 0 {
			return nil, errors.New("Index param is not set")
		}
		idx = pathParams[3]
	}
	f := h.opBodyParsers[op]
	var val interface{}
	if f != nil {
		f(r.Body, &val)
	}
	ttlStr := r.URL.Query().Get("ttl")
	var ttl int64
	if len(ttlStr) > 0 {
		var err error
		ttl, err = strconv.ParseInt(ttlStr, 10, 64)
		if err != nil {
			return nil, errors.New("Non integer ttl: "+err.Error())
		}
	}
	return (*h.storage).newInnerRequest(op, key, idx, val, ttl), nil
}

func isMethodSupported(method string, operation int) bool {
	switch operation {
	case OP_GET, OP_LGETI, OP_LGET, OP_DGETI, OP_DGET:
		return strings.ToUpper(method) == http.MethodGet
	default:
		return strings.ToUpper(method) == http.MethodPost
	}
}