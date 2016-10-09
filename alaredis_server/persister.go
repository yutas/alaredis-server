package main

import (
	"syscall"
	"log"
	"os"
	"encoding/gob"
	"bytes"
	"fmt"
	"time"
	"io"
)

type Persister struct {
	memStorage *Storage
	dir string
	process *os.Process
}

type storedItem struct {
	K string
	V interface{}
	E int64
}

func (p *Persister) restore(filePath string) error {

	f, err := os.Open(filePath)
	if err != nil { return err }

	buf := new(bytes.Buffer)
	dec := gob.NewDecoder(buf)
	s := p.memStorage
	cnt := 0

	// first read ttl data
	var item storedItem
	for {
		buf.Reset()
		_, err := readSizedData(f, buf)
		if err == io.EOF { break }
		if err != nil {
			return err
		}
		dec.Decode(&item)
		ttl := item.E-time.Now().Unix()
		if item.E == 0 || ttl > 0 {
			cnt++
			s.processInnerRequest(s.newInnerRequest(OP_SET, item.K, ``, item.V, ttl))
		}
	}
	log.Printf("Restored %d items from file %s", cnt, filePath)
	return nil
}

func (p *Persister) persist() error {
	filePath := p.dir+"/"+fmt.Sprintf("cached-data-%s.gob", time.Now().Format("20060102150405"))
	log.Printf("Persisting data to file '%s'", filePath)
	f, err := os.Create(filePath)
	if err != nil { return err }

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	s := p.memStorage
	cnt := 0

	for _, m := range s.keyMetaMap {
		bnum := m.hash%uint32(s.bucketsNum)
		item := storedItem{
			K: m.key,
			V: s.buckets[bnum].data[m.key],
			E: s.ttlMonitor.keyExpireAtMap[m],
		}
		buf.Reset()
		enc.Encode(item)
		n, err := writeSizedData(f, buf.Bytes())
		if err != nil { return err }
		cnt += n
	}
	f.Close()
	log.Printf("Written %d bytes", cnt)
	return nil
}

func (p *Persister) forkPersist() {
	if p.process != nil {
		log.Printf("ERROR: Can not start persistency child process - previous one (pid %d) did not finish yet.", p.process.Pid)
		return
	}
	ret, _, err := syscall.Syscall(syscall.SYS_FORK, 0, 0, 0)
	if err != 0 {
		log.Printf("ERROR: Failed to fork process - %v", err)
		return
	}
	if ret == 0 {
		// child process
		p.persist()
		os.Exit(0)
	} else {
		log.Printf("Forked process with pid %d to persis data to disk", ret)
		p.process = &os.Process{Pid:int(ret)}
		p.process.Wait()
		log.Printf("Child process with pid %d terminated", ret)
		p.process = nil
	}
}

func (p *Persister) wait() {
	if p.process != nil {
		p.process.Wait()
	}
}

