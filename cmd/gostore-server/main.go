package main

import (
	"gostore/log"
	"gostore"
	"gostore/process"
	"flag"
	_ "http/pprof"
	"http"
)

func main() {
	// Flags
	var configpath *string = flag.String("config", "gostore.conf", "configuration")
	var verbosity *int = flag.Int("verbosity", 3, "degree of verbosity")
	flag.Parse()

	// Set log verbosity
	log.MaxLevel = *verbosity

	log.Info("Reading config\n")
	config := gostore.LoadConfig(*configpath)

	log.Info("Starting server...\n")

	process.NewProcess(config)

	log.Info("Server started\n")

	http.ListenAndServe(":8080", nil)

	// Wait
	c := make(chan int)
	<-c
}
