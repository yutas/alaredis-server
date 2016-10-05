package main

import (
	"time"
	"sort"
)

type ttlMonitor struct {
	expireAtList    sortedTimeList
	expireAtKeysMap map[int64][]*keyMeta
	keyExpireAtMap  map[*keyMeta]int64
	applicationChan chan *application
	updateChan      chan struct{}
	onKeyExpire	func(m *keyMeta)
}


type application struct {
	m *keyMeta
	expireAt int64
}


func newTTLMonitor(inputQueueSize int, onKeyExpire func(m *keyMeta)) *ttlMonitor {
	mon := new(ttlMonitor)
	mon.expireAtList = make(sortedTimeList, 0, 256)
	mon.expireAtKeysMap = make(map[int64][]*keyMeta)
	mon.keyExpireAtMap = make(map[*keyMeta]int64)
	mon.applicationChan = make(chan *application, inputQueueSize)
	mon.updateChan = make(chan struct{}, 1)
	mon.onKeyExpire = onKeyExpire
	return mon
}


func (mon *ttlMonitor) run() {

	// applications watcher
	go func() {
		for {
			select {
			case appl := <-mon.applicationChan:
				curExpireAt := mon.keyExpireAtMap[appl.m]
				if appl.expireAt > 0 && appl.expireAt != curExpireAt {
					// set new expire at
					mon.add(appl.m, appl.expireAt)
					mon.updateChan <- struct{}{}
				} else if curExpireAt > 0 && appl.expireAt == 0 {
					// delete old expire at, as new one is zero
					mon.remove(appl.m, curExpireAt)
					mon.onKeyExpire(appl.m)
					mon.updateChan <- struct{}{}
				}
			}
		}
	}()

	// expiration watcher
	go func() {
		for {
			if mon.expireAtList.len() == 0 {
				select {
				case <- mon.updateChan:
					// do nothing, just continue loop
					//log.Print("Updating expiration order")
				}
			} else {
				var nearestExpireAt = mon.expireAtList.getFirst()
				select {
				case <-time.After(time.Duration(nearestExpireAt-time.Now().Unix())*1e9):
					//log.Print("Removing keys")
					mon.removeAll(nearestExpireAt)
				case <-mon.updateChan:
					// do nothing, just continue loop
					//log.Print("Updating expiration order")
				}
			}
		}
	}()
}


func (mon *ttlMonitor) monitor(m *keyMeta, ttl int64) {
	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Unix() + ttl
	} else {
		expireAt = 0
	}
	mon.applicationChan <- &application{m, expireAt}
}

func (mon *ttlMonitor) unmonitor(m *keyMeta) {
	mon.applicationChan <- &application{m, 0}
}

func (mon *ttlMonitor) remove(meta *keyMeta, expireAt int64) {
	if expireAt == 0 { return }
	delete(mon.keyExpireAtMap, meta)
	keys, ok := mon.expireAtKeysMap[expireAt]
	if ok {
		i := -1
		var m *keyMeta
		for i, m = range keys {
			if m == meta { break }
		}
		if i > -1 {
			keys[i] = keys[len(keys)-1]
			keys = keys[:len(keys)-1]
		}
		if len(keys) == 0 {
			delete(mon.expireAtKeysMap, expireAt)
		}
	}
	mon.expireAtList.remove(expireAt)
}

func (mon *ttlMonitor) removeAll(expireAt int64) {
	keys, ok := mon.expireAtKeysMap[expireAt]
	if ok {
		for _, m := range keys {
			delete(mon.keyExpireAtMap, m)
			mon.onKeyExpire(m)
		}
		delete(mon.expireAtKeysMap, expireAt)
	}
	mon.expireAtList.remove(expireAt)
}

func (mon *ttlMonitor) add(meta *keyMeta, expireAt int64) {
	mon.keyExpireAtMap[meta] = expireAt
	keys, ok := mon.expireAtKeysMap[expireAt]
	if !ok {
		keys = make([]*keyMeta, 0, 8)
		mon.expireAtList.add(expireAt)
	}
	mon.expireAtKeysMap[expireAt] = append(keys, meta)
}



type sortedTimeList []int64

func (l *sortedTimeList) add(t int64) {
	*l = append(*l, t)
	sort.Sort(l)
}

func (l *sortedTimeList) remove(t int64) {
	i := -1
	var t1 int64
	for i, t1 = range *l {
		if t1 == t { break }
	}
	if i > -1 {
		*l = append((*l)[:i], (*l)[i+1:]...)
	}
}

func (l *sortedTimeList) len() int {
	return len(*l)
}

func (l *sortedTimeList) getFirst() int64 {
	return (*l)[0]
}

// Len is the number of elements in the collection.
func (l sortedTimeList) Len() int {
	return len(l)
}
// Less reports whether the element with
// index i should sort before the element with index j.
func (l sortedTimeList) Less(i, j int) bool {
	return l[i] < l[j]
}
// Swap swaps the elements with indexes i and j.
func (l sortedTimeList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

