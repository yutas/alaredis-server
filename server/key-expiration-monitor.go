package main

import (
	"time"
	"sort"
	"log"
)

type keyExpirationMonitor struct {
	expireAtList    sortedTimeList
	expireAtKeysMap map[int][]*keyMeta
	keyExpireAtMap  map[*keyMeta]int
	applicationChan chan *application
	updateChan      chan struct{}
	onKeyExpire	func(m *keyMeta)
}


type application struct {
	m *keyMeta
	expireAt int
}


func newTTLMonitor(bufSize int, onKeyExpire func(m *keyMeta)) *keyExpirationMonitor {
	mon := new(keyExpirationMonitor)
	mon.expireAtList = sortedTimeList{make([]int, 0, 256)}
	mon.expireAtKeysMap = make(map[int][]*keyMeta)
	mon.keyExpireAtMap = make(map[*keyMeta]int)
	mon.applicationChan = make(chan *application, bufSize)
	mon.updateChan = make(chan struct{}, 1)
	mon.onKeyExpire = onKeyExpire
	return mon
}


func (mon *keyExpirationMonitor) run() {

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
				case <-time.After(time.Duration(nearestExpireAt-int(time.Now().Unix()))*1e9):
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


func (mon *keyExpirationMonitor) monitor(m *keyMeta, ttl int) {
	var expireAt int
	if ttl > 0 {
		expireAt = int(time.Now().Unix()) + ttl
	} else {
		expireAt = 0
	}
	mon.applicationChan <- &application{m, expireAt}
}

func (mon *keyExpirationMonitor) unmonitor(m *keyMeta) {
	mon.applicationChan <- &application{m, 0}
}

func (mon *keyExpirationMonitor) remove(meta *keyMeta, expireAt int) {
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
	}
	mon.expireAtList.remove(expireAt)
	mon.onKeyExpire(meta)
}

func (mon *keyExpirationMonitor) removeAll(expireAt int) {
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

func (mon *keyExpirationMonitor) add(meta *keyMeta, expireAt int) {
	mon.keyExpireAtMap[meta] = expireAt
	keys, ok := mon.expireAtKeysMap[expireAt]
	if !ok {
		keys = make([]*keyMeta, 0, 8)
		mon.expireAtList.add(expireAt)
	}
	mon.expireAtKeysMap[expireAt] = append(keys, meta)
}



type sortedTimeList struct {
	slice []int
}

func (l *sortedTimeList) add(t int) {
	l.slice = append(l.slice, t)
	sort.Ints(l.slice)
}

func (l *sortedTimeList) remove(t int) {
	i := -1
	var t1 int
	for i, t1 = range l.slice {
		if t1 == t { break }
	}
	if i > -1 {
		l.slice = append(l.slice[:i], l.slice[i+1:]...)
	}
}

func (l *sortedTimeList) len() int {
	return len(l.slice)
}

func (l *sortedTimeList) getFirst() int {
	return l.slice[0]
}