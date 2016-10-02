package main

import "time"

func main() {
	for i:=0;i<8;i++ {
		b := i
		go func() {
			println(b)
		}()
	}
	time.Sleep(1e9)
}