package main

import (
	"time"
	"math/rand"
	"os"
	"strings"
	"fmt"
	"bufio"
	"alaredis/lib"
	"log"
	"flag"
	"net/http"
	"io/ioutil"
	"strconv"
	"errors"
	"io"
	"runtime"
)

const (
	LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	TYPE_STRING = 0
	TYPE_LIST = 1
	TYPE_DICT = 2
)

const (
	OP_DELETE = iota
	OP_SET
	OP_GET
	OP_LSET
	OP_LSETI
	OP_LGET
	OP_LGETI
	OP_DSET
	OP_DSETI
	OP_DGET
	OP_DGETI
	OP_DKEYS
)

var OPERATION_NAMES = map[int]string {
	OP_DELETE: `delete`,
	OP_GET: `get`,
	OP_SET: `set`,
	OP_LSET: `lset`,
	OP_LSETI: `lseti`,
	OP_LGET: `lget`,
	OP_LGETI: `lgeti`,
	OP_DSET: `dset`,
	OP_DSETI: `dseti`,
	OP_DGETI: `dgeti`,
	OP_DGET: `dget`,
	OP_DKEYS: `dkeys`,
}

type TestCase struct {
	op	      int
	method        string
	uri           string
	requestValue  string
	responseCode  int
	responseValue string
	error         bool
}

type Record struct {
	Type int
	Body interface{}
	ttl uint
	createdAt time.Time
	updatedAt time.Time
	deleted bool
}

type caseProducer func(k string, recordExists bool, record *Record) (*TestCase, bool)


var bodyParser = lib.BodyParserJson{}

func main() {
	var genCases bool
	var runCases bool
	var casesFile string
	var num int
	var logFile string
	var concurrence int
	var threads int
	var port int
	var host string
	flag.BoolVar(&genCases, "gen", false, `New queries list will be generated`)
	flag.BoolVar(&runCases, "run", false, `Run cases from file`)
	flag.StringVar(&casesFile, "file", ``, `Path to file with queries list`)
	flag.IntVar(&num, "num", 1000, `Amount of queries to run`)
	flag.StringVar(&logFile, "log", ``, `Path to log file`)
	flag.IntVar(&concurrence, "conc", 1, `Number of concurrent http requests made`)
	flag.IntVar(&threads, "thr", 0, `sets GOMAXPROCS value`)
	flag.StringVar(&host, "h", `localhost`, `cache host`)
	flag.IntVar(&port, "p", 8080, `cache port`)
	flag.Parse()

	if threads > 0 {
		runtime.GOMAXPROCS(threads)
	}

	if logFile != `` {
		f, err := os.OpenFile(logFile, os.O_RDWR | os.O_CREATE | os.O_TRUNC , 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	rand.Seed(time.Now().UnixNano())

	client := lib.NewClient(host, port, bodyParser)

	if genCases {
		letters := "abcd"
		generateCases(client, num, &letters, 4, 0, casesFile)
	}
	if runCases {
		log.Printf("Going to run queries from file %s to alaredis server %s", casesFile, client.GetBaseUrl())
		testCases(client, casesFile, num, concurrence)
	}
}

func bodyToString(body interface{}) string {
	buf, _ := bodyParser.ComposeBody(body)
	return string(buf.Bytes())
}


func generateCases(client *lib.CacheClient,limit int, letters *string, keyLen int, errRate float32, filePath string) {
	f,_ := os.Create(filePath)
	defer f.Close()
	records := make(map[string]Record, 100)
	operations := []caseProducer{
		//delete
		func(k string, recordExists bool, record *Record) (*TestCase, bool) {
			testCase := TestCase{
				op: OP_DELETE,
				method: `POST`,
				uri: client.Url(`delete`, k, ``, 0),
				responseCode: 204,
			}
			record.deleted = true
			return &testCase, false
		},
		//get
		func(k string, recordExists bool, record *Record) (*TestCase, bool) {
			testCase := TestCase{
				op: OP_GET,
				method: `GET`,
				uri: client.Url(`get`, k, ``, 0),
				responseCode: 200,
			}
			if recordExists && record.Type == TYPE_STRING {
				testCase.responseValue = bodyToString(record.Body)
			} else {
				testCase.responseCode = 404
				testCase.error = true
			}
			return &testCase, false
		},
		//set
		func(k string, recordExists bool, record *Record) (*TestCase, bool) {
			body := k + "-value-" + randStringAllLetters(rand.Intn(100)+1)
			testCase := TestCase{
				op: OP_SET,
				method: `POST`,
				uri: client.Url(`set`, k, ``, 0),
				requestValue: bodyToString(body),
				responseCode:204,
			}
			record.Body = body
			record.Type = TYPE_STRING
			return &testCase, true
		},
		//lset
		//func(k string, recordExists bool, record *Record) (*TestCase, bool) {
		//	length := rand.Intn(10)+1
		//	list := make([]string, 0, length)
		//	for i:=0;i<length;i++ {
		//		list[i] = randStringAllLetters(rand.Intn(100)+1)
		//	}
		//	testCase := TestCase{OP_SET, k, ``, 0, list, false}
		//	record.Body = list
		//	record.Type = TYPE_LIST
		//	return &testCase, true
		//},
		//lseti
		//lget
		//lgeti
		//linsert
		//dset
		//dget
		//dkeys
	}
	probabilities := []float32{0.2, 0.536, 0.264,}
	maxOpIdx := len(operations)
	var i int
	for i=0; i<limit; i++ {
		k := randString(letters, keyLen)
		var opIdx int
		for {
			opIdx = rand.Intn(maxOpIdx)
			if probabilities[opIdx] > 0 && probabilities[opIdx] >= rand.Float32() { break }
		}
		record, ok := records[k]
		c, saveRecord := operations[opIdx](k, ok, &record)
		if saveRecord {
			records[k] = record
		}
		if record.deleted { delete(records, k) }

		var e string
		if c.error { e = `1` } else { e = `0` }
		f.WriteString(strings.Join([]string{
			fmt.Sprint(c.op), c.method, c.uri, c.requestValue,
			fmt.Sprint(c.responseCode), c.responseValue, e,
		}, "\t")+"\n")
	}
}

type queryDuration struct {
	query []string
	duration time.Duration
}


func testCases(c *lib.CacheClient, filePath string,limit int, concurrence int) {
	f,_ := os.Open(filePath)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	processedCnt := 0
	wrongResponseCnt := 0
	unexpectedErrorsCnt := 0
	missedErrorsCnt := 0

	endChans := make([]chan struct{}, concurrence+1)
	durationChan := make(chan queryDuration, 100)
	queryChan := make(chan []string, 100)

	// requesting goroutines
	for i:=0; i<concurrence; i++ {
		fmt.Printf("Starting requesting gorutine #%d\n", i)
		endChans[i] = make(chan struct{}, 1)
		endChan := endChans[i]
		go func() {
			httpClient := http.Client{}
			for query := range queryChan {
				//log.Print("Running query", query)
				req, _ := http.NewRequest(query[1], query[2], strings.NewReader(query[3]))

				start := time.Now()
				resp, err := httpClient.Do(req)
				durationChan <- queryDuration{query:query, duration: time.Since(start)}

				if err != nil {
					log.Printf("[request error] Failed to perform request: %v", err)
					continue
				}
				var responseBody string
				var responseCode int
				if resp != nil {
					responseCode = resp.StatusCode
					buf, _ := ioutil.ReadAll(resp.Body)
					responseBody = string(buf)
					io.Copy(ioutil.Discard, resp.Body)
					resp.Body.Close()
				}

				if resp.StatusCode/100 >= 4 {
					if responseBody != `` {
						err = errors.New(responseBody)
					} else {
						err = errors.New("Undefined error")
					}
				}

				errorIsExpected := query[6] == `1`
				if err == nil && errorIsExpected {
					missedErrorsCnt++
					log.Printf("Missed error for query '%v'", query)
				} else if err != nil && !errorIsExpected {
					unexpectedErrorsCnt++
					log.Printf("Got unexpected error '%v' for query '%v'", err, query)
				} else if code, _ := strconv.Atoi(query[4]); code != responseCode {
					unexpectedErrorsCnt++
					log.Printf("Got unexpected response code '%v' instead of %d for query '%v'", responseCode, code, query)
				} else if err == nil && responseBody != query[5] {
					unexpectedErrorsCnt++
					log.Printf("Got unexpected response '%v' for query '%v'", responseBody, query)
				}
			}
			endChan <- struct {}{}
		} ()
	}

	//measuring goroutine
	durations := make(map[int]time.Duration)
	counts := make(map[int]int)
	go func(){
		for qd := range durationChan {
			op, _ := strconv.Atoi(qd.query[0])
			durations[op] = durations[op]+qd.duration
			counts[op]++
		}
	}()

	for scanner.Scan() {
		queryChan <- strings.Split(scanner.Text(), "\t")
		processedCnt++
		if processedCnt % 10000 == 0 {
			fmt.Printf(
				"Processed %d queries, got %d wrong responses, %d unexpected errors, %d errors where missed\n",
				processedCnt, wrongResponseCnt, unexpectedErrorsCnt, missedErrorsCnt,
			)
		}
		if processedCnt >= limit {
			break
		}
	}
	close(queryChan)
	// wait for all goroutines to finish their work
	for i:=0; i<len(endChans)-1; i++ {
		<-endChans[i]
	}
	close(durationChan)
	time.Sleep(1e9)
	fmt.Print("Done!\n")
	fmt.Printf(
		"Processed %d queries, got %d wrong responses, %d unexpected errors, %d errors where missed\n",
		processedCnt, wrongResponseCnt, unexpectedErrorsCnt, missedErrorsCnt,
	)
	fmt.Print("Performance statistics:\n")
	for op, cnt := range counts {
		fmt.Printf("Made %d requests '%s': %f rps\n", cnt, OPERATION_NAMES[op], float64(cnt)/durations[op].Seconds())
	}
}


func randString(letters *string, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = (*letters)[rand.Intn(len(*letters))]
	}
	return string(b)
}

func randStringAllLetters(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = LETTERS[rand.Intn(len(LETTERS))]
	}
	return string(b)
}