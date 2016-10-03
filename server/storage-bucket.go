package main

import "sync"

type StorageBucket struct {
	data   map[string]interface{}
	rwLock sync.RWMutex
}


func newStorageBucket() *StorageBucket {
	b := new(StorageBucket)
	b.data = make(map[string]interface{})
	b.rwLock = sync.RWMutex{}
	return b
}


func (b *StorageBucket) Delete(k string) {
	b.rwLock.Lock()
	delete(b.data, k)
	b.rwLock.Unlock()
}

func (b *StorageBucket) Set(k string, v interface{}) {
	b.rwLock.Lock()
	b.data[k] = v
	b.rwLock.Unlock()

}
func (b *StorageBucket) Get(k string) (*interface{}, bool) {
	b.rwLock.RLock()
	defer b.rwLock.RUnlock()
	v, ok := b.data[k]
	return &v, ok
}