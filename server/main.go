package main

import (
	"net/http"
	"log"
	"fmt"
	"alaredis/lib"
	"flag"
	"os"
	"runtime"
	"github.com/icub3d/graceful"
	"os/signal"
	"syscall"
	"runtime/pprof"
	 _ "net/http/pprof"

)

func main() {

	var logFile string
	var bucketsNum int
	var threadsNum int
	var listenPort int
	var timeoutMS int = 10e6
	var cpuprofile = ``

	flag.StringVar(&logFile, "log", ``, `path to log file`)
	flag.IntVar(&bucketsNum, "b", 8, `number of buckets used by storage`)
	flag.IntVar(&threadsNum, "thr", 0, `number of os threads to be used by goroutines`)
	flag.IntVar(&listenPort, "p", 8080, `port to be listen by http server`)
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")

	flag.Parse()

	if (logFile) != `` {
		f, err := os.OpenFile(logFile, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	if (threadsNum) > 0 {
		runtime.GOMAXPROCS(threadsNum)
	}

	if cpuprofile != `` {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	storage := NewStorage(bucketsNum)
	httpHandler := NewHttpHandler(storage, lib.BodyParserJson{}, timeoutMS)
	http.HandleFunc("/", (*httpHandler).HandleRequest)

	storage.run()
	defer storage.stop()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signals
		log.Printf("Got signal %v, shutting down...\n", sig)
		graceful.Close()
	}()

	err := graceful.ListenAndServe(fmt.Sprintf(":%d", listenPort), nil)
	log.Printf("Got http serve error %v", err)
}