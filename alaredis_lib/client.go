package alaredis_lib

import (
	"net/http"
	"strings"
	"fmt"
	"io"
	"bytes"
	"io/ioutil"
	"strconv"
	"errors"
)

type CacheClient struct {
	baseUrl string
	client *http.Client
	bodyParser BodyParser
}

func NewClient(host string, port int, bodyParser BodyParser) *CacheClient {
	c := new(CacheClient)
	c.baseUrl = fmt.Sprintf("http://%s:%d", host, port)
	c.client = &http.Client{}
	c.bodyParser = bodyParser
	return c
}

func (c *CacheClient) GetBaseUrl() string {
	return c.baseUrl
}

func (c *CacheClient) Url(action string, key string, idx string, ttl int) string {
	url := strings.Join([]string{c.baseUrl, action, key}, `/`)
	if len(idx) > 0 {
		url = url+`/`+idx
	}
	if ttl > 0 {
		url = url+`?ttl=`+fmt.Sprint(ttl)
	}
	return url
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
	resp, err := c.client.Do(req)
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
		}
		return nil, err
	}
	if resp.StatusCode/100 >= 4 {
		if resp != nil {
			defer resp.Body.Close()
			defer io.Copy(ioutil.Discard, resp.Body)
			// read error string from body
			buf := new(bytes.Buffer)
			buf.ReadFrom(resp.Body)
			return nil, errors.New(buf.String())
	 	} else {
			return nil, errors.New("Undefined error")
		}
	}
	return resp.Body, err
}

// OP_SET
func (c *CacheClient) Set(k string, v string, ttl int) error {
	bodyReader, err := c.doRequest("POST", c.Url(`set`, k, ``, ttl), v)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	return err
}

// OP_GET
func (c *CacheClient) Get(k string) (string, error) {
	bodyReader, err := c.doRequest("GET", c.Url(`get`, k, ``, 0), nil)
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

// OP_DELETE
func (c *CacheClient) Delete(k string) error {
	bodyReader, err := c.doRequest("DELETE", c.Url(`delete`, k, ``, 0), nil)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	return err
}

// OP_LSET
func (c *CacheClient) LSet(k string, v []string, ttl int) error {
	bodyReader, err := c.doRequest("POST", c.Url(`lset`, k, ``, ttl), v)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	return err
}

// OP_LSETI
func (c *CacheClient) LSetI(k string, v string, idx int) error {
	bodyReader, err := c.doRequest("POST", c.Url(`lseti`, k, strconv.Itoa(idx), 0), v)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	return err
}

// OP_LGET
func (c *CacheClient) LGet(k string) ([]string, error) {
	bodyReader, err := c.doRequest("GET", c.Url(`lget`, k, ``, 0), nil)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	if err != nil {
		return nil, err
	}
	return c.bodyParser.GetListValue(bodyReader)
}

// OP_LGETI
func (c *CacheClient) LGetI(k string, idx int) (string, error) {
	bodyReader, err := c.doRequest("GET", c.Url(`lgeti`, k, strconv.Itoa(idx), 0), nil)
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
// OP_DSET
func (c *CacheClient) DSet(k string, v map[string]string, ttl int) error {
	bodyReader, err := c.doRequest("POST", c.Url(`dset`, k, ``, ttl), v)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	return err
}

// OP_DSETI
func (c *CacheClient) DSetI(k string, v string, idx string) error {
	bodyReader, err := c.doRequest("POST", c.Url(`dseti`, k, idx, 0), v)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	return err
}

// OP_DGET
func (c *CacheClient) DGet(k string) (map[string]string, error) {
	bodyReader, err := c.doRequest("GET", c.Url(`dget`, k, ``, 0), nil)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	if err != nil {
		return nil, err
	}
	return c.bodyParser.GetDictValue(bodyReader)
}

// OP_DGETI
func (c *CacheClient) DGetI(k string, idx string) (string, error) {
	bodyReader, err := c.doRequest("GET", c.Url(`dgeti`, k, idx, 0), nil)
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

// OP_DKEYS
func (c *CacheClient) DKeys(k string) ([]string, error) {
	bodyReader, err := c.doRequest("GET", c.Url(`dkeys`, k, ``, 0), nil)
	if bodyReader != nil {
		defer bodyReader.Close()
		// just to read data to end
		defer io.Copy(ioutil.Discard, bodyReader)
	}
	if err != nil {
		return nil, err
	}
	return c.bodyParser.GetListValue(bodyReader)
}