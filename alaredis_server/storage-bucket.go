package main

import "sync"

type StorageBucket struct {
	data   map[string]interface{}
	requestChan chan *innerRequest
}

func newStorageBucket() *StorageBucket {
	b := new(StorageBucket)
	b.data = make(map[string]interface{})
	b.requestChan = make(chan *innerRequest, 100)
	return b
}

func (b *StorageBucket) Delete(k string) {
	delete(b.data, k)
}

func (b *StorageBucket) Set(k string, v interface{}) {
	b.data[k] = v

}

func (b *StorageBucket) Get(k string) (*interface{}, bool) {
	v, ok := b.data[k]
	return &v, ok
}