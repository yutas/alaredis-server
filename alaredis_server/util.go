package main

import (
	"io"
	"encoding/binary"
	"bytes"
)

func writeSizedData(w io.Writer, buf []byte) (int, error) {
	cnt := 0
	binary.Write(w, binary.LittleEndian, int32(len(buf)))
	cnt += 4
	n, err := w.Write(buf)
	if err != nil { return cnt, err }
	cnt += n
	return cnt, nil
}

func readSizedData(r io.Reader, buf *bytes.Buffer) (int, error) {
	cnt := 0
	sizeBuf := make([]byte, 4)
	n, err := io.ReadAtLeast(r, sizeBuf, 4)
	cnt += n
	if err != nil { return cnt, err }
	var size int32
	binary.Read(bytes.NewReader(sizeBuf), binary.LittleEndian, &size)
	b := make([]byte, size)
	n, err = io.ReadAtLeast(r, b, int(size))
	buf.Write(b)
	if err != nil { return cnt, err }
	cnt += n
	return cnt, nil
}