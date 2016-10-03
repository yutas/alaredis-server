package main


import (
	"net/http"
	"strings"
	"io"
	"alaredis/lib"
	"errors"
	"time"
	"strconv"
	"log"
)


type HttpHandler struct {
	storage *Storage
	bodyParser lib.BodyParser
	opBodyParsers []func(r io.Reader, val *interface{}) error
	timeout time.Duration
}


var OPERATIONS = map[string]int {
	`delete`: OP_DELETE,
	`get`: OP_GET,
	`set`: OP_SET,
	`lset`: OP_LSET,
	`lseti`: OP_LSETI,
	`lget`: OP_LGET,
	`lgeti`: OP_LGETI,
	`linsert`: OP_LINSERT,
	`dset`: OP_DSET,
	`dseti`: OP_DSETI,
	`dget`: OP_DGET,
	`dgeti`: OP_DGETI,
	`dkeys`: OP_DKEYS,
}



func NewHttpHandler(storage *Storage, bodyParser lib.BodyParser, timeoutMs int) *HttpHandler {
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

	h.timeout = time.Duration(timeoutMs*1000)
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
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write((*buf).Bytes())
		}
	case err:=<-req.errChan:
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			log.Printf("Got nil error from request %+v", req)
		}
	//case <-time.After(h.timeout):
	//	http.Error(w, ``, http.StatusGatewayTimeout)
	}
}

// inner methods
func (h *HttpHandler) createInnerRequest(w http.ResponseWriter, r *http.Request) (*innerRequest, error) {
	// url - /<operation>/<key>/<idx>
	pathParams := strings.Split(r.URL.Path, `/`);
	op, ok := OPERATIONS[pathParams[1]]
	if !ok {
		return nil, errors.New("Operation is not supported or defined")
	}
	if len(pathParams) < 3 || len(pathParams[2])== 0 {
		return nil, errors.New("Key is not set or is empty")
	}
	key := pathParams[2]
	var idx string
	if op == OP_LGETI || op == OP_LSETI || op == OP_LINSERT || op == OP_DSETI || op == OP_DGETI {
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
	var ttl int
	if len(ttlStr) > 0 {
		var err error
		ttl, err = strconv.Atoi(ttlStr)
		if err != nil {
			return nil, errors.New("Non integer ttl: "+err.Error())
		}
	}
	return (*h.storage).newInnerRequest(op, key, idx, val, ttl), nil
}