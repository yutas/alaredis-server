package main

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

func (b *StorageBucket) delete(k string) {
	delete(b.data, k)
}

func (b *StorageBucket) set(k string, v interface{}) {
	b.data[k] = v

}

func (b *StorageBucket) get(k string) (*interface{}, bool) {
	v, ok := b.data[k]
	return &v, ok
}