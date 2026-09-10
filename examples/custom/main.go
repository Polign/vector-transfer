package main

import (
	"log"
	"os"

	"github.com/anuptalwalkar/vector-transfer/app"
	"github.com/anuptalwalkar/vector-transfer/connector"
	"github.com/anuptalwalkar/vector-transfer/examples/custom/memorydb"
)

func main() {
	if err := app.Run(os.Args[1:], connector.Factories{"memorydb": memorydb.Factory()}); err != nil {
		log.Fatal(err)
	}
}
