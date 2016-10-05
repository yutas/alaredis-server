package alaredis_lib

import (
	"io"
	"encoding/json"
	"bytes"
)

type BodyParserJson struct {

}

func (p BodyParserJson) ParseBody(r io.Reader, val interface{}) error {
	err := json.NewDecoder(r).Decode(val)
	return err
}

func (p BodyParserJson) ComposeBody(val interface{}) (*bytes.Buffer, error) {
	b,err := json.Marshal(val);
	return bytes.NewBuffer(b), err
}

func (p BodyParserJson) GetStringValue(r io.Reader) (string, error) {
	var v string
	err := p.ParseBody(r, &v)
	return v, err
}

func (p BodyParserJson) GetListValue(r io.Reader) ([]string, error) {
	var v []string
	err := p.ParseBody(r, &v)
	return v, err
}

func (p BodyParserJson) GetDictValue(r io.Reader) (map[string]string, error) {
	var v map[string]string
	err := p.ParseBody(r, &v)
	return v, err
}

func (p BodyParserJson) GetContentType() string {
	return `application/json`
}