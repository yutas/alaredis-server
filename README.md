# alaredis-server
Simple in-memory cache server according to https://github.com/gojuno/test_tasks

* **alaredis_lib** provides package with  cache client and body readers implementation
* **alaredis_server** provides executable for cache server
* **alaredis_performance_test** - executable for simple performance test, running several query types

## Installation
```bash
go get github.com/yutas/alaredis-server/alaredis_server
go get github.com/yutas/alaredis-server/alaredis_performance_test
```

## Usage
### Server
Next command will run http server for cache, listening port 8080. Cache will use 2 buckets to store values, and go runtime will use 2 system threads:
```bash
alaredis_server -b 2 -thr 2 -p 8080
```

Simple test:
```bash
curl http://localhost:8080/set/foo -XPOST -d '"bar"'
curl http://localhost:8080/get/foo # will return "bar" to command line
```

### Client
Installation:
```bash
go get github.com/yutas/alaredis-server/alaredis_lib
```
```go
package main

import "github.com/yutas/alaredis-server/alaredis_lib"

func main() {
	c := alaredis_lib.NewClient(`localhost`, 8080)
	c.Set(`foo`, `bar`, 0)
	val, err := c.Get(`foo`)
	if err != nil {
		print(`Got error: `, err.Error())
	} else {
		print(`Got cached value: `, val)
	}
}
```

## Performance test
Performance test executable (alaredis_performance_test) works in 2 modes:

1. Generation of query list (-gen)

2. Performing queries from list (-run)

This options can be passed altogether and test will first generate list of queries and then run them to cache.

Example:
```bash
performance-test -file performance-cache-queries.tsv -gen -num 1000 -run -conc 2 -thr 2
```
Will receive output:
```bash
Processed 1000 queries, got 0 wrong responses, 0 unexpected errors, 0 errors where missed
Performance statistics:
Made 247 requests 'set': 7430.009387 rps
Made 553 requests 'get': 8812.496496 rps
Made 200 requests 'delete': 9558.872378 rps
```


## API specification
### Request format
Cache server processes http requests of next format:
```bash
<method> /<operation>/<key>[/<index>][?ttl=<ttl>]

<body>
```
Where
* **method** - GET for all get-requests, POST for others
* **operation** - operation for cache to perform
* **key** - string key on which operation will be performed
* **index** - int index in list or string key in dicts, on which operation will be performed (only for lists and dicts values)
* **ttl** - time in seconds, during wich key will be alive. Does not work for indexed values of lists and dicts
* **body** - object in json format

### Operations

| Operation        | Action           | Comments  |
| ------------- |:-------------:| -----:|
| delete    | remove cached object |  |
| set | set string object | if there is some object of any time, it will be rewritten |
| get | get string object | if object is not string, error will be returned | 
| lset | set list object | overwrites existing object, if any |
| lget | get list object | if object is not list, error will be returned |
| lseti | set string value in list by index | index param is required, and must not be out of list bounds |
| lgeti | get string value from list by index |index param is required, and must not be out of list bounds |
| dset | set dict object | overwrites existing object, if any |
| dget | get dict object | if object is not dict, error will be returned |
| dseti| set string value to dict by string index | if there is no cached object, it will be created. If object is not dict, error will be returned. Index param is required |
| dgeti | get string value from dict by string index | index param is required. If there is no such index, error will be returned |
| dkeys | get list of keys for dict object | if cached object is not dict, error will be returned|
