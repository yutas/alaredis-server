package main

import (
	"alaredis/lib"
	"log"
)


func main() {
	c := lib.NewClient(`localhost`, 8080, lib.BodyParserJson{})
	//c.LSet(`test`, []string{`foo`, `bar`}, 0)
	err := c.Delete(`test`)
	if err != nil {
		log.Printf("Got error %v", err)
	}
}