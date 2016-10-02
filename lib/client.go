package lib

import (
	"net/http"
	"strings"
	"fmt"
	"io"
	"bytes"
	"io/ioutil"
)

type CacheClient struct {
	baseUrl string
	client *http.Client
	bodyParser BodyParser
}

func NewClient(scheme string, host string, port uint, bodyParser BodyParser) *CacheClient {
	c := new(CacheClient)
	c.baseUrl = fmt.Sprintf("%s://%s:%d", scheme, host, port)
	c.client = &http.Client{}
	c.bodyParser = bodyParser

	return c
}

func (c *CacheClient) url(action string, key string) string {
	return strings.Join([]string{c.baseUrl, action, key}, `/`)
}

func (c *CacheClient) doRequest(method string, url string, body interface{}) (io.ReadCloser, error) {
	var reader bytes.Buffer
	if body != nil {
		r, err := c.bodyParser.ComposeBody(body)
		if err != nil { return nil, err }
		reader = *r
	}
	req, err := http.NewRequest(method, url, &reader)
	if err != nil {
		return nil, err
	}
	//start := time.Now()
	resp, err := c.client.Do(req)
	//elapsed := time.Since(start)
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
		}
		return nil, err
	}
	if resp.StatusCode/100 > 2 {
		if resp != nil {
			defer resp.Body.Close()
		}
		return nil, NotFoundError{"Not found"}
	}
	return resp.Body, err
}

func (c *CacheClient) Set(k string, v string) error {
	bodyReader, err := c.doRequest("POST", c.url(`set`, k), v)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	return err
}

func (c *CacheClient) Get(k string) (string, error) {
	bodyReader, err := c.doRequest("GET", c.url(`get`, k), nil)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	if err != nil {
		return ``, err
	}
	return c.bodyParser.GetStringValue(bodyReader)
}

func (c *CacheClient) Delete(k string) error {
	bodyReader, err := c.doRequest("POST", c.url(`delete`, k), nil)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	return err
}

type NotFoundError struct {
	msg string
}

func (e NotFoundError) Error() string {
	return e.msg
}