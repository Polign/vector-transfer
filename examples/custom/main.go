package main

import (
	"log"
	"os"

	"github.com/Polign/vector-transfer/app"
	"github.com/Polign/vector-transfer/connector"
	"github.com/Polign/vector-transfer/examples/custom/memorydb"
)

func main() {
	if err := app.Run(os.Args[1:], connector.Factories{"memorydb": memorydb.Factory()}); err != nil {
		log.Fatal(err)
	}
}
