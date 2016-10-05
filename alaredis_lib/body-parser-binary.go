package alaredis_lib

import (
	"io"
	"errors"
)

type BodyParserBinary struct {

}

func (p BodyParserBinary) ComposeBody(w io.Writer, val interface{}) error {
	return nil
}

func (h BodyParserBinary) ParseBody(body io.Reader, data *[]string) (error) {
	var size int
	sbuf := make([]byte, 4)
	var vbuf []byte
	for {
		n,err := io.ReadAtLeast(body, sbuf, len(sbuf))
		size = int(sbuf[0]) << 24 | int(sbuf[1]) << 16 | int(sbuf[2]) << 8 | int(sbuf[3])

		n, err = io.ReadAtLeast(body, vbuf, size)
		if n > 0 {
			*data = append(*data, string(vbuf))
		}
		if err == io.EOF {
			break
		}
	}
	return nil
}

func (p BodyParserBinary) GetStringValue(body io.Reader) (string, error) {
	var data []string
	err := p.ParseBody(body, &data)
	return data[0], err
}

func (p BodyParserBinary) GetListValue(body io.Reader) ([]string, error) {
	var data []string
	err := p.ParseBody(body, &data)
	return data, err
}

func (p BodyParserBinary) GetDictValue(body io.Reader) (map[string]string, error) {
	var data []string
	err := p.ParseBody(body, &data)
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