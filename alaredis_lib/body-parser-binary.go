package alaredis_lib

import (
	"io"
	"errors"
	"encoding/binary"
	"bytes"
)

type BodyParserBinary struct {

}

func (p BodyParserBinary) ComposeBody(val interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	switch val.(type) {
	case string:
		s := val.(string)
		binary.Write(buf, binary.LittleEndian, int32(len(s)))
		buf.Write([]byte(s))
	case []string:
		s := val.([]string)
		for i := range s {
			binary.Write(buf, binary.LittleEndian, int32(len(s[i])))
			buf.Write([]byte(s[i]))
		}
	case map[string]string:
		s := val.(map[string]string)
		for i, v := range s {
			binary.Write(buf, binary.LittleEndian, int32(len(i)))
			buf.Write([]byte(i))
			binary.Write(buf, binary.LittleEndian, int32(len(v)))
			buf.Write([]byte(v))
		}
	}
	return buf, nil
}

func (h BodyParserBinary) parseBody(body io.Reader, data *[]string, limit int) (error) {
	var size int32
	valsCnt := 0
	for {
		sizeBuf := make([]byte, 4)
		n, err := io.ReadAtLeast(body, sizeBuf, 4)
		if err == io.EOF { break }
		binary.Read(bytes.NewReader(sizeBuf), binary.LittleEndian, &size)

		valBuf := make([]byte, size)
		n, err = io.ReadAtLeast(body, valBuf, int(size))
		if n > 0 {
			*data = append(*data, string(valBuf))
		}
		if err == io.EOF { break }

		valsCnt++
		if limit > 0 && valsCnt >= limit { break }
	}
	return nil
}

func (p BodyParserBinary) GetStringValue(body io.Reader) (string, error) {
	var data []string
	err := p.parseBody(body, &data, 1)
	return data[0], err
}

func (p BodyParserBinary) GetListValue(body io.Reader) ([]string, error) {
	var data []string
	err := p.parseBody(body, &data, 0)
	return data, err
}

func (p BodyParserBinary) GetDictValue(body io.Reader) (map[string]string, error) {
	var data []string
	err := p.parseBody(body, &data, 0)
	if err != nil { return nil, err }
	if len(data) % 2 == 1 {
		return nil, errors.New("Key count is not equal to values count")
	}
	m := make(map[string]string, len(data)/2)
	k := ""
	for i := 0; i < len(data); i++ {
		if i%2==0 {
			k = data[i]
		} else {
			m[k] = data[i]
		}
	}
	return m, nil
}

func (p BodyParserBinary) GetContentType() string {
	return `application/octet-stream`
}