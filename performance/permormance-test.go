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
)

const (
	host = `localhost`
	port = 8080
	LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	TYPE_STRING = 0
	TYPE_LIST = 1
	TYPE_DICT = 2
	OP_DELETE = `0`
	OP_SET = `1`
	OP_GET = `2`
)


type TestCase struct {
	operation string
	key string
	value string
	error bool
}

type Record struct {
	Type uint8
	Len int
	Body string
	ttl uint
	createdAt time.Time
	updatedAt time.Time
	deleted bool
}

type caseProducer func(k string, recordExists bool, record *Record) (*TestCase, bool)




func main() {

	genCasesPtr := flag.Bool("gen", false, `New queries list will be generated`)
	filePtr := flag.String("file", ``, `Path to file with queries list`)
	numPtr := flag.Int("num", 1000, `Amount of queries to run`)
	logPtr := flag.String("log", ``, `Path to log file`)
	concurrencePtr := flag.Int("conc", 1, `Number of concurrent http requests made`)
	flag.Parse()

	if (*logPtr) != `` {
		f, err := os.OpenFile(*logPtr, os.O_RDWR | os.O_CREATE | os.O_TRUNC , 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	rand.Seed(time.Now().UnixNano())
	if (*genCasesPtr) {
		letters := "abcdef"
		generateCases(numPtr, &letters, 5, 0, filePtr)
	}
	c := lib.NewClient(`http`, host, port, lib.BodyParserJson{})
	testCases(c, filePtr, numPtr, concurrencePtr)

	//c.Set(`test`, `test value123`)
	//response, err := c.Get(`test1`)
	//if err != nil {
	//	log.Printf("Got error: %s", err.Error())
	//}
	//log.Printf("Got in response: '%s'", response)
}


func generateCases(limit *int, letters *string, keyLen int, errRate float32, filePath *string) {
	f,_ := os.Create(*filePath)
	defer f.Close()
	records := make(map[string]Record, 100)
	operations := []caseProducer{
		//delete
		func(k string, recordExists bool, record *Record) (*TestCase, bool) {
			testCase := TestCase{OP_DELETE, k, ``, false}
			record.deleted = true
			return &testCase, false
		},
		//get
		func(k string, recordExists bool, record *Record) (*TestCase, bool) {
			testCase := TestCase{OP_GET, k, "", false}
			if recordExists && record.Type == TYPE_STRING {
				testCase.value = record.Body
			} else {
				testCase.error = true
			}
			return &testCase, !testCase.error
		},
		//set
		func(k string, recordExists bool, record *Record) (*TestCase, bool) {
			body := k + "-value-" + randStringAllLetters(rand.Intn(100)+1)
			testCase := TestCase{OP_SET, k, body, false}
			record.Body = body
			record.Type = TYPE_STRING
			record.createdAt = time.Now()
			record.updatedAt = time.Now()
			record.Len = -1
			return &testCase, true
		},
		//lset
		//lseti
		//lget
		//lgeti
		//linsert
		//dset
		//dget
		//dkeys
	}
	probabilities := []float32{0.2, 0.264, 0.536}
	maxOpIdx := len(operations)
	var i int
	for i=0; i<*limit; i++ {
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
			fmt.Sprintf("%v", c.operation), c.key, c.value, e,
		}, "\t")+"\n")
	}
}


func testCases(c *lib.CacheClient, filePath *string, limit *int, concurrence *int) {
	f,_ := os.Open(*filePath)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	processedCnt := 0
	wrongResponseCnt := 0
	unexpectedErrorsCnt := 0
	missedErrorsCnt := 0
	queryChan := make(chan []string, 100)
	for i:=0; i<*concurrence; i++ {
		fmt.Printf("Starting requesting gorutine #%d\n", i)
		go func() {
			for query := range queryChan {
				errorIsExpected := query[3] == `1`
				//log.Print("Running query", query)
				switch query[0] {
				case OP_DELETE:
					err := c.Delete(query[1])
					if err == nil && errorIsExpected {
						missedErrorsCnt++
						log.Printf("Missed error for query %v", query)
					} else if err != nil && !errorIsExpected {
						unexpectedErrorsCnt++
						log.Printf("Got unexpected error '%s' for query %v", err.Error(), query)
					}
				case OP_SET:
					err := c.Set(query[1], query[2])
					if err == nil && errorIsExpected {
						missedErrorsCnt++
						log.Printf("Missed error for query %v", query)
					} else if err != nil && !errorIsExpected {
						unexpectedErrorsCnt++
						log.Printf("Got unexpected error '%s' for query %v", err.Error(), query)
					}
				case OP_GET:
					v, err := c.Get(query[1])
					if err == nil && errorIsExpected {
						missedErrorsCnt++
						log.Printf("Missed error for query %v", query)
					} else if err != nil && !errorIsExpected {
						unexpectedErrorsCnt++
						log.Printf("Got unexpected error '%s' for query %v", err.Error(), query)
					} else if (v != query[2]) {
						log.Printf("Got wrong response: '%s' for query %v", v, query)
						wrongResponseCnt++
					}
				}
				//time.Sleep(1000000000)
			}
		} ()
	}
	for scanner.Scan() {
		queryChan <- strings.Split(scanner.Text(), "\t")
		processedCnt++
		if processedCnt % 10000 == 0 {
			fmt.Printf(
				"Processed %d queries, got %d wrong responses, %d unexpected errors, %d errors where missed\n",
				processedCnt, wrongResponseCnt, unexpectedErrorsCnt, missedErrorsCnt,
			)
		}
		if processedCnt >= *limit {
			break
		}
	}
	close(queryChan)
	fmt.Print("Done!\n")
	fmt.Printf(
		"Processed %d queries, got %d wrong responses, %d unexpected errors, %d errors where missed\n",
		processedCnt, wrongResponseCnt, unexpectedErrorsCnt, missedErrorsCnt,
	)
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