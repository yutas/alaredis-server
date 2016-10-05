package main

import (
	"testing"
	"log"
	"time"
)

func TestTTLMonitor_Simple(t *testing.T) {
	mon := newTTLMonitor(10, func(m *keyMeta) {
		log.Printf("Key '%s' expired!\n", m.key)
	})

	k1 := newKeyMeta(`test-key1`)
	k2 := newKeyMeta(`test-key2`)
	bigExpireAt := time.Now().Unix()+20
	smallExpireAt := time.Now().Unix()+10

	// add one key
	mon.add(k1, bigExpireAt)
	if len(mon.expireAtKeysMap) != 1 {
		t.Errorf("expireAtKeysMap was not updated: %v", mon.expireAtKeysMap)
		t.Fail()
	}
	if len(mon.expireAtList) != 1 {
		t.Errorf("expireAtKeysMap was not updated: %v", mon.expireAtList)
		t.Fail()
	}
	if len(mon.keyExpireAtMap) != 1 {
		t.Errorf("keyExpireAtMap was not updated: %v", mon.keyExpireAtMap)
		t.Fail()
	}
	if mon.expireAtList[0] != bigExpireAt {
		t.Errorf("expireAtList is not updated: %v", mon.expireAtList)
		t.Fail()
	}

	// add another key
	mon.add(k2, smallExpireAt)
	if len(mon.expireAtKeysMap) != 2 {
		t.Errorf("expireAtKeysMap was not updated: %v", mon.expireAtKeysMap)
		t.Fail()
	}
	if len(mon.expireAtList) != 2 {
		t.Errorf("expireAtKeysMap was not updated: %v", mon.expireAtList)
		t.Fail()
	}
	if len(mon.keyExpireAtMap) != 2 {
		t.Errorf("keyExpireAtMap was not updated: %v", mon.keyExpireAtMap)
		t.Fail()
	}
	if mon.expireAtList[0] != smallExpireAt && mon.expireAtList[1] != bigExpireAt {
		t.Errorf("expireAtList is not sorted: %v", mon.expireAtList)
		t.Fail()
	}

	// remove second key
	mon.remove(k2, smallExpireAt)
	if len(mon.expireAtKeysMap) != 1 {
		t.Errorf("expireAtKeysMap was not updated: %v", mon.expireAtKeysMap)
		t.Fail()
	}
	if len(mon.expireAtList) != 1 {
		t.Errorf("expireAtKeysMap was not updated: %v", mon.expireAtList)
		t.Fail()
	}
	if len(mon.keyExpireAtMap) != 1 {
		t.Errorf("keyExpireAtMap was not updated: %v", mon.keyExpireAtMap)
		t.Fail()
	}
	if mon.expireAtList[0] != bigExpireAt {
		t.Errorf("expireAtList is not sorted: %v", mon.expireAtList)
		t.Fail()
	}
}
func TestTTLMonitor_Expiration(t *testing.T) {
	mon := newTTLMonitor(10, func(m *keyMeta) {
		log.Printf("Key '%s' expired!\n", m.key)
	})
	mon.run()

	k1 := newKeyMeta(`test-key1`)
	k2 := newKeyMeta(`test-key2`)
	mon.monitor(k1, 4)
	mon.monitor(k2, 2)
	log.Print("Waiting for keys expiration")
	time.Sleep(time.Duration(5*1e9))

	if len(mon.expireAtKeysMap) != 0 {
		t.Error("expireAtKeysMap was not cleared")
		t.Fail()
	}
	if len(mon.expireAtList) != 0 {
		t.Error("expireAtKeysMap was not cleared")
		t.Fail()
	}
	if len(mon.keyExpireAtMap) != 0 {
		t.Error("keyExpireAtMap was not cleared")
		t.Fail()
	}
}
