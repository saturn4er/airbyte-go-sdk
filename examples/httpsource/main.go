package main

import (
	"log"
	"os"

	"github.com/saturn4er/airbyte-go-sdk"
	"github.com/saturn4er/airbyte-go-sdk/examples/httpsource/apisource"
)

func main() {
	hsrc := apisource.NewAPISource("https://dummyjson.com")
	runner := airbyte.NewSourceRunner(hsrc, os.Stdout)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
