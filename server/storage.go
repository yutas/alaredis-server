package main

import (
	"hash/crc32"
	"sync"
	"errors"
	"log"
	"strconv"
)

const (
	TYPE_NULL = iota
	TYPE_STRING
	TYPE_LIST
	TYPE_DICT
)

const (
	OP_DELETE = iota
	OP_SET
	OP_GET
	OP_LSET
	OP_LSETI
	OP_LGET
	OP_LGETI
	OP_LINSERT
	OP_DSET
	OP_DSETI
	OP_DGET
	OP_DGETI
	OP_DKEYS
)


type Storage struct {
	bucketsNum       int
	buckets          []*StorageBucket
	meta             map[string]*keyMeta
	metaLock         sync.RWMutex
	requestChan      chan *innerRequest
	bucketChans      []chan *innerRequest
	stopChan         chan struct{}
	keyExpirationMon *keyExpirationMonitor
}

type innerRequest struct {
	op      int
	key     string
	m       *keyMeta
	b       uint8
	idx     string
	ttl     int
	val     interface{}
	outCh   chan interface{}
	errChan chan error
}

func NewStorage(bucketsNum int) *Storage {
	s := new(Storage)
	s.bucketsNum = bucketsNum
	s.buckets = make([]*StorageBucket, s.bucketsNum, s.bucketsNum)
	s.bucketChans = make([]chan *innerRequest, s.bucketsNum, s.bucketsNum)
	for i:=0; i<s.bucketsNum;i++ {
		s.buckets[i] = newStorageBucket()
		s.bucketChans[i] = make(chan *innerRequest, 100)
	}
	s.meta = make(map[string]*keyMeta)
	s.requestChan = make(chan *innerRequest)
	s.metaLock = sync.RWMutex{}
	s.stopChan = make(chan struct{}, 1)
	s.keyExpirationMon = newTTLMonitor(s.bucketsNum*2, s.onKeyExpire)
	return s
}

func (s *Storage) newInnerRequest(op int, key string, idx string, val interface{}, ttl int) *innerRequest {
	req := new(innerRequest)
	req.op = op
	req.key = key
	m, ok := s.getKeyMeta(key)
	if !ok {
		m = newKeyMeta(key)
	}
	req.m = m
	req.idx = idx
	req.val = val
	req.ttl = ttl
	req.b = uint8(m.hash%uint32(s.bucketsNum))
	req.outCh = make(chan interface{}, 1)
	req.errChan = make(chan error, 1)
	return req
}

func (s *Storage) run() {

	opHandlers := make([]func(req *innerRequest) (interface{}, error), len(OPERATIONS))
	opHandlers[OP_DELETE] = s.delete
	opHandlers[OP_SET] = s.set
	opHandlers[OP_GET] = s.get
	opHandlers[OP_LSET] = s.lset
	opHandlers[OP_LGET] = s.lget
	opHandlers[OP_LSETI] = s.lseti
	opHandlers[OP_LGETI] = s.lgeti
	opHandlers[OP_DSET] = s.dset
	opHandlers[OP_DSETI] = s.dseti
	opHandlers[OP_DGET] = s.dget
	opHandlers[OP_DGETI] = s.dgeti
	opHandlers[OP_DKEYS] = s.dkeys

	// starting workers, processing requests, one per bucket
	for i:=0;i<s.bucketsNum;i++ {
		b := i
		log.Printf("Started worker for bucket #%d", b)
		go func() {
			for {
				select {
				case req := <-s.bucketChans[b]:
					val, err := opHandlers[req.op](req)
					if err == nil {
						req.outCh <- val

					} else {
						req.errChan <- err
					}
				case <-s.stopChan:
					log.Printf("Stopped worker for bucket #%d", b)
					return
				}
			}
		} ()
	}

	s.keyExpirationMon.run()
}

func (s *Storage) stop() {
	s.stopChan <- struct {}{}
	close(s.stopChan)
}

func (s *Storage) processInnerRequest(req *innerRequest) {
	s.bucketChans[req.b] <- req
}

func (s *Storage) getKeyMeta(k string) (*keyMeta, bool) {
	s.metaLock.RLock()
	m, ok := s.meta[k]
	s.metaLock.RUnlock()
	return m, ok
}

func (s *Storage) setKeyMeta(k string, m *keyMeta) {
	s.metaLock.Lock()
	s.meta[k] = m
	s.metaLock.Unlock()
}

func (s *Storage) onKeyExpire(m *keyMeta) {
	s.processInnerRequest(s.newInnerRequest(OP_DELETE, m.key, ``, nil, 0))
}

func (s *Storage) delete(req *innerRequest) (interface{}, error) {
	k := req.key

	s.metaLock.Lock()
	delete(s.meta, k)
	(*s.buckets[req.b]).Delete(k)
	s.metaLock.Unlock()
	return nil, nil
}

func (s *Storage) set(req *innerRequest) (interface{}, error) {
	k := req.key
	ttl := req.ttl
	v, ok := req.val.(string)
	if !ok {
		return nil, errors.New("Incoming object is not string")
	}
	s.keyExpirationMon.monitor(req.m, ttl)
	req.m.t = TYPE_STRING
	s.setKeyMeta(k, req.m)
	s.buckets[req.b].Set(k, v)
	return nil, nil
}
func (s *Storage) get(req *innerRequest) (interface{}, error) {
	k := req.key
	m := req.m

	if m.t != TYPE_STRING {
		return nil, errors.New("Stored object is not string")
	}
	v,_ := s.buckets[req.b].Get(k)
	return v, nil
}

func (s *Storage) lset(req *innerRequest) (interface{}, error) {
	k := req.key
	ttl := req.ttl
	v, ok := req.val.([]string)
	if !ok {
		return nil, errors.New("Incoming object is not list")
	}

	s.keyExpirationMon.monitor(req.m, ttl)
	req.m.t = TYPE_LIST
	s.setKeyMeta(k, req.m)
	(*s.buckets[req.b]).Set(k, v)
	return nil, nil
}
func (s *Storage) lseti(req *innerRequest) (interface{}, error) {
	if req.m.t != TYPE_LIST {
		return nil, errors.New("Stored object is not list")
	}
	k := req.key
	idx, err := strconv.Atoi(req.idx)
	if err != nil {
		return nil, errors.New("Non integer index: "+err.Error())
	}
	v, ok := req.val.(string)
	if !ok {
		return nil, errors.New("Incoming object is not string")
	}
	_ = v
	_ = idx
	listPtr, _ := (*s.buckets[req.b]).Get(k)
	list, _ := (*listPtr).([]string)
	if idx >= len(list) {
		return nil, errors.New("List index out of range")
	}
	list[idx] = v
	return nil, nil
}
func (s *Storage) lget(req *innerRequest) (interface{}, error) {
	k := req.key
	m := req.m

	if m.t != TYPE_LIST {
		return nil, errors.New("Stored object is not list")
	}
	v, _ := (*s.buckets[req.b]).Get(k)
	return v, nil
}
func (s *Storage) lgeti(req *innerRequest) (interface{}, error) {
	k := req.key
	m := req.m

	if m.t != TYPE_LIST {
		return nil, errors.New("Stored object is not list")
	}

	idx, err := strconv.Atoi(req.idx)
	if err != nil {
		return nil, errors.New("Non integer index: "+err.Error())
	}

	listPtr, _ := (*s.buckets[req.b]).Get(k)
	list, _ := (*listPtr).([]string)
	if idx >= len(list) {
		return nil, errors.New("List index out of range")
	}
	return list[idx], nil
}
func (s *Storage) linsert(req *innerRequest) (interface{}, error) { return nil, nil }

func (s *Storage) dset(req *innerRequest) (interface{}, error) {
	k := req.key
	ttl := req.ttl
	v, ok := req.val.(map[string]string)
	if !ok {
		return nil, errors.New("Incoming object is not dict")
	}

	s.keyExpirationMon.monitor(req.m, ttl)
	req.m.t = TYPE_DICT
	s.setKeyMeta(k, req.m)
	(*s.buckets[req.b]).Set(k, v)
	return nil, nil
}
func (s *Storage) dseti(req *innerRequest) (interface{}, error) {
	k := req.key
	v, ok := req.val.(string)
	if !ok {
		return nil, errors.New("Incoming object is not string")
	}
	idx := req.idx
	dictPtr, ok := (*s.buckets[req.b]).Get(k)
	if !ok {
		req.m.t = TYPE_DICT
		s.setKeyMeta(k, req.m)
		(*s.buckets[req.b]).Set(k, map[string]string{idx:v})
	} else {
		dict := (*dictPtr).(map[string]string)
		dict[idx] = v
	}
	return nil, nil
}
func (s *Storage) dget(req *innerRequest) (interface{}, error) {
	k := req.key
	m := req.m

	if m.t != TYPE_DICT {
		return nil, errors.New("Stored object is not dict")
	}
	v, _ := (*s.buckets[req.b]).Get(k)
	return v, nil
}
func (s *Storage) dgeti(req *innerRequest) (interface{}, error) {
	k := req.key
	m := req.m

	if m.t != TYPE_DICT {
		return nil, errors.New("Stored object is not dict")
	}

	idx := req.idx
	dictPtr, _ := (*s.buckets[req.b]).Get(k)
	dict := (*dictPtr).(map[string]string)
	val, ok := dict[idx]
	if !ok {
		return nil, errors.New("Dict does not contain index '"+idx+"'")
	}
	return val, nil
}
func (s *Storage) dkeys(req *innerRequest) (interface{}, error) {
	k := req.key
	m := req.m

	if m.t != TYPE_DICT {
		return nil, errors.New("Stored object is not dict")
	}
	dictPtr, _ := (*s.buckets[req.b]).Get(k)
	dict := (*dictPtr).(map[string]string)
	keys := make([]string, len(dict))
	i := 0
	for key := range dict {
		keys[i] = key
		i++
	}
	return keys, nil
}




/*
 *  keyMeta
 */

type keyMeta struct {
	key string
	hash     uint32
	t        uint8
}

func newKeyMeta(k string) *keyMeta {
	m := new(keyMeta)
	m.key = k
	m.hash = crc32.ChecksumIEEE([]byte(k))
	m.t = TYPE_NULL
	return m
}