package alaredis_lib

import (
	"io"
	"bytes"
)

type BodyParser interface {
	ComposeBody(val interface{}) (*bytes.Buffer, error)
	GetStringValue(body io.Reader) (string, error)
	GetListValue(body io.Reader) ([]string, error)
	GetDictValue(body io.Reader) (map[string]string, error)
	GetContentType() string
}
