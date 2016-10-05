package main

import "sync"

type StorageBucket struct {
	data   map[string]interface{}
	rwLock sync.RWMutex
	requestChan chan *innerRequest
}


func newStorageBucket() *StorageBucket {
	b := new(StorageBucket)
	b.data = make(map[string]interface{})
	b.rwLock = sync.RWMutex{}
	b.requestChan = make(chan *innerRequest, 100)
	return b
}


func (b *StorageBucket) Delete(k string) {
	b.rwLock.Lock()
	defer b.rwLock.Unlock()

	delete(b.data, k)
}

func (b *StorageBucket) Set(k string, v interface{}) {
	b.rwLock.Lock()
	defer b.rwLock.Unlock()

	b.data[k] = v

}
func (b *StorageBucket) Get(k string) (*interface{}, bool) {
	b.rwLock.RLock()
	defer b.rwLock.RUnlock()
	
	v, ok := b.data[k]
	return &v, ok
}