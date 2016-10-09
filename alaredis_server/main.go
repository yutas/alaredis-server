package main

import (
	"net/http"
	"log"
	"fmt"
	"flag"
	"os"
	"github.com/icub3d/graceful"
	"os/signal"
	"syscall"
	"runtime/pprof"
	 _ "net/http/pprof"
	_ "github.com/mkevac/debugcharts"
	"runtime"
	"github.com/yutas/alaredis-server/alaredis_lib"
	"time"
)

func main() {

	/**
	 * Parameters
	 */

	var logFile string
	var bucketsNum int
	var listenPort int
	var cpuprofile = ``
	var threads = 0
	var persist = false
	var persistDir = ``
	var restoreFile = ``

	flag.StringVar(&logFile, "log", ``, `path to log file`)
	flag.IntVar(&bucketsNum, "b", 4, `number of buckets used by storage`)
	flag.IntVar(&listenPort, "p", 8080, `port to be listen by http server`)
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")
	flag.IntVar(&threads, "thr", 0, `sets GOMAXPROCS value`)
	flag.BoolVar(&persist, "persist", false, "whether to use data persistence to file")
	flag.StringVar(&persistDir, "pdir", "", "dir for persisted data")
	flag.StringVar(&restoreFile, "restore", "", "file with persisted data to be restored from")
	flag.Parse()

	/**
	 * Processing parameters
	 */

	if (logFile) != `` {
		f, err := os.OpenFile(logFile, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	if cpuprofile != `` {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if threads > 0 {
		runtime.GOMAXPROCS(threads)
	}


	/**
	 * Working with storage and http server
	 */

	storage := NewStorage(bucketsNum)
	var persister *Persister
	if persist {
		persister = &Persister{memStorage: storage, dir: persistDir}
	}
	storage.run()
	if len(restoreFile) > 0 {
		if err := persister.restore(restoreFile); err != nil {
			log.Printf("Failed to restore cache data from file '%s': %v", restoreFile, err)
		}
	}
	if persist {
		// persistence period is hardcoded
		ticker := time.NewTicker(time.Minute * 5)
		go func() {
			for range ticker.C {
				persister.forkPersist()
			}
		}()
	}
	httpHandler := NewHttpHandler(storage, alaredis_lib.BodyParserJson{})
	http.HandleFunc("/", (*httpHandler).HandleRequest)



	/**
	 * Shutdown
	 */
	gracefulShutdown := false
	gracefulShutdownFinished := make(chan struct{}, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signals
		gracefulShutdown = true
		log.Printf("Got signal %v, shutting down...\n", sig)
		graceful.Close()
		persister.wait()
		gracefulShutdownFinished <- struct {}{}
	}()

	log.Printf("Listening port %d", listenPort)
	err := graceful.ListenAndServe(fmt.Sprintf(":%d", listenPort), nil)
	log.Printf("Got http serve error '%v'", err)
	if gracefulShutdown {
		<-gracefulShutdownFinished
	}
}