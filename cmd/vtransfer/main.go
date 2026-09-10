package main

import (
	"log"
	"os"

	"github.com/anuptalwalkar/vector-transfer/app"
)

func main() {
	if err := app.Run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
