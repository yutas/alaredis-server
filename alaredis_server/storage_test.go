package main

import (
	"testing"
	"time"
	"sort"
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
	OP_DSET: `dset`,
	OP_DSETI: `dseti`,
	OP_DGET: `dget`,
	OP_DKEYS: `dkeys`,
}

type operation struct {
	op  int
	key string
	idx string
	val interface{}
	ttl int64
	expectedValue interface{}
	expectedErr string
}


func TestStorage_Strings(t *testing.T) {
	s := *NewStorage(1)
	s.run()
	k := `test key`
	v := `test value`
	s.testOperation(t, operation{op:OP_SET, key:k, val:v})
	s.testOperation(t, operation{op:OP_GET, key:k, expectedValue:v})
	s.testOperation(t, operation{op:OP_DELETE, key:k})
	s.testOperation(t, operation{op:OP_GET, key:k, expectedErr:`Object not found for key 'test key'`})
	s.stop()
}


func TestStorage_TTL(t *testing.T) {
	s := *NewStorage(1)
	s.run()
	k := `test key`
	v := `test value`
	s.testOperation(t, operation{op:OP_SET, key:k, val:v, ttl:2})
	s.testOperation(t, operation{op:OP_GET, key:k, expectedValue:v})
	time.Sleep(time.Duration(1*1e9))
	s.testOperation(t, operation{op:OP_GET, key:k, expectedValue:v})
	time.Sleep(time.Duration(1*1e9))
	s.testOperation(t, operation{op:OP_GET, key:k, expectedErr:`Object not found for key 'test key'`})
	s.stop()
}

func TestStorage_Lists(t *testing.T) {
	s := *NewStorage(1)
	s.run()
	k := `test key`
	v := []string{`value1`, `value2`, `value3`}
	s.testOperation(t, operation{op:OP_LSET, key:k, val:v})
	s.testOperation(t, operation{op:OP_LGET, key:k, expectedValue:v})
	v[1] = `another value`
	s.testOperation(t, operation{op:OP_LSETI, key:k, idx:`1`, val:v[1]})
	s.testOperation(t, operation{op:OP_LGET, key:k, expectedValue:v})
	s.testOperation(t, operation{op:OP_LSETI, key:k, idx:`5`, val:`illegal value`, expectedErr:`BadRequest: List index out of range`})
	s.testOperation(t, operation{op:OP_LGETI, key:k, idx:`0`, expectedValue:v[0]})
	s.testOperation(t, operation{op:OP_LGET, key:k, expectedValue:v})
	s.testOperation(t, operation{op:OP_DELETE, key:k})
	s.testOperation(t, operation{op:OP_LGET, key:k, expectedErr:`Object not found for key 'test key'`})
	s.stop()
}

func TestStorage_Dicts(t *testing.T) {
	s := *NewStorage(1)
	s.run()
	k := `test key`
	v := map[string]string{`k1`:`value1`, `k2`:`value2`, `k3`:`value3`}
	s.testOperation(t, operation{op:OP_DSET, key:k, val:v})
	s.testOperation(t, operation{op:OP_DGET, key:k, expectedValue:v})
	v[`k2`] = `something else`
	s.testOperation(t, operation{op:OP_DSETI, key:k, idx:`k2`, val:v[`k2`]})
	s.testOperation(t, operation{op:OP_DGET, key:k, expectedValue:v})
	s.testOperation(t, operation{op:OP_DSETI, key:k, idx:`k5`, val:`new value`})
	v[`k5`] = `new value`
	s.testOperation(t, operation{op:OP_DGET, key:k, expectedValue:v})
	keys := make([]string, len(v))
	// we'll try compare keys in reverse order
	i:=len(v)
	for k := range v {
		keys[i-1] = k
		i--
	}
	s.testOperation(t, operation{op:OP_DKEYS, key:k, expectedValue:keys})
	s.testOperation(t, operation{op:OP_DELETE, key:k})
	s.testOperation(t, operation{op:OP_DGET, key:k, expectedErr:`Object not found for key 'test key'`})
	s.stop()
}

func (r *innerRequest) String() string {
	opDescr := OPERATION_NAMES[r.op]+"/"+r.key
	if len(r.idx) > 0 {
		opDescr = opDescr+"/"+r.idx
	}
	return opDescr
}


func (s *Storage) testOperation(t *testing.T, op operation) {
	// create copy of value
	var valCopy interface{}
	switch op.val.(type) {
	case []string:
		cpy := make([]string, len(op.val.([]string)))
		copy(cpy, op.val.([]string))
		valCopy = cpy
	case map[string]string:
		cpy := make(map[string]string)
		for k,v := range op.val.(map[string]string) {
			cpy[k] = v
		}
		valCopy = cpy
	default:
		valCopy = op.val
	}

	// create and perform request
	req := s.newInnerRequest(op.op, op.key, op.idx, valCopy, op.ttl)
	s.processInnerRequest(req)
	select {
	case responseValue := <- req.outCh:
		if op.expectedErr != `` {
			t.Errorf("[%s] Wrong response: expected error '%v', got good response with value '%v'", req, op.expectedErr, responseValue)
			t.Fail()
		} else {
			equal := false
			switch op.op {
			case OP_GET, OP_DGETI, OP_LGETI:
				equal = op.expectedValue == responseValue
			case OP_LGET:
				equal = testListEq(op.expectedValue.([]string), responseValue.([]string))
			case OP_DGET:
				equal = testDictEq(op.expectedValue.(map[string]string), responseValue.(map[string]string))
			case OP_DKEYS:
				sort.Strings(op.expectedValue.([]string))
				sort.Strings(responseValue.([]string))
				equal = testListEq(op.expectedValue.([]string), responseValue.([]string))
			default:
				equal = op.expectedValue == responseValue
			}

			if !equal {
				t.Errorf("[%s] Wrong response value: expected '%v', got '%v'", req, op.expectedValue, responseValue)
				t.Fail()
			}
		}
	case responseErr := <- req.errChan:
		if op.expectedErr == `` {
			t.Errorf("[%s] Wrong response: expected good response with value '%v', got error '%v'", req, op.expectedValue, responseErr)
			t.Fail()
		} else if op.expectedErr != responseErr.Error() {
			t.Errorf("[%s] Wrong response: expected error '%v', got error '%v'", req, op.expectedErr, responseErr)
			t.Fail()
		}
	case <-time.After(STORAGE_RESPONSE_TIMEOUT):
		t.Errorf("[%s] Got storage response timeout", req)
		t.Fail()
	}
}


func testListEq(a, b []string) bool {

	if a == nil && b == nil {
		return true;
	}

	if a == nil || b == nil {
		return false;
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}


func testDictEq(a, b map[string]string) bool {
	if a == nil && b == nil {
		return true;
	}

	if a == nil || b == nil {
		return false;
	}

	if len(a) != len(b) {
		return false
	}

	for k, v1 := range a {
		v2, ok := b[k]
		if !ok || v1 != v2 {
			return false
		}
	}
	return true
}