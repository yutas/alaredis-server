package main

import (
	"testing"
	"time"
)

const (
	STORAGE_RESPONSE_TIMEOUT = time.Duration(1e9)
)

var OPERATION_NAMES = map[int]string {
	OP_DELETE: `delete`,
	OP_GET: `get`,
	OP_SET: `set`,
	OP_LSET: `lset`,
	OP_LSETI: `lseti`,
	OP_LGET: `lget`,
	OP_LGETI: `lgeti`,
	OP_LINSERT: `linsert`,
	OP_DSET: `dset`,
	OP_DGET: `dget`,
	OP_DKEYS: `dkeys`,
}




func TestMemStorage_Strings(t *testing.T) {
	s := *NewStorage(1)
	s.run()
	k := `test key`
	v := `test value`
	s.testOperation(t, OP_SET, k, ``, v, 0, nil, nil)
	s.testOperation(t, OP_GET, k, ``, ``, 0, v, nil)
	s.stop()
}


func (r *innerRequest) String() string {
	opDescr := OPERATION_NAMES[r.op]+"/"+r.key
	if len(r.idx) > 0 {
		opDescr = opDescr+"/"+r.idx
	}
	return opDescr
}


func (s *Storage) testOperation(t *testing.T, op int, key string, idx string, val interface{}, ttl uint32, expectedValue interface{}, expectedErr error) {
	req := s.newInnerRequest(op, key, idx, val, ttl)
	s.processInnerRequest(req)
	select {
	case responseValue := <- req.outCh:
		if expectedErr != nil {
			t.Errorf("[%s] Wrong response: expected error '%v', got good response with value '%v'", req, expectedErr, responseValue)
			t.Fail()
		} else if responseValue != expectedValue {
			t.Errorf("[%s] Wrong response value: expected '%v', got '%v'", req, expectedValue, responseValue)
			t.Fail()
		}
	case responseErr := <- req.errChan:
		if expectedErr == nil {
			t.Errorf("[%s] Wrong response: expected good response with value '%v', got error '%v'", req, expectedValue, responseErr)
			t.Fail()
		} else if expectedErr != responseErr {
			t.Errorf("[%s] Wrong response: expected error '%v', got error '%v'", req, expectedErr, responseErr)
			t.Fail()
		}
	case <-time.After(STORAGE_RESPONSE_TIMEOUT):
		t.Errorf("[%s] Got storage response timeout", req)
		t.Fail()
	}
}
