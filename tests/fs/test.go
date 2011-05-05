package main

import (
	"http"
	"fmt"
	"io/ioutil"
	"time"
	"bytes"
	"rand"
	"flag"
	"sync"
	"strings"
)

var (
	servers []string
	data    []byte

	mutex       *sync.Mutex
	requestsSum int
	elapsedSum  float64

	end        bool
	trackerEnd chan bool = make(chan bool)

	startTime   int64
	elapsedTime float64
)

func main() {
	// Flags
	var concurrencyFlag *int = flag.Int("c", 1, "Number of multiple requests to make")
	var requestsFlag *int = flag.Int("n", 100, "Number of requests to perform")
	var serversFlag *string = flag.String("servers", "127.0.0.1:30002", "List of servers separated by semicolon (ip:port;ip:port;ip:port)")
	var operationFlag *string = flag.String("op", "read", "Operation to execute (write or read)")
	var sizeFlag *int = flag.Int("size", 1024, "Size of the data to send")

	flag.Parse()

	// generate data
	fmt.Printf("Generating data %d bytes of random data... ", *sizeFlag)

	data = make([]byte, *sizeFlag)
	for i := 0; i < *sizeFlag; i++ {
		data[i] = byte(rand.Intn(255))
	}
	fmt.Printf("Data generated!\n")

	// parsing servers
	servers = strings.Split(*serversFlag, ";", -1)

	// stats tracker
	mutex = new(sync.Mutex)
	go tracker()

	cs := make(chan bool, *concurrencyFlag)

	startTime = time.Nanoseconds()
	reqPerThread := *requestsFlag / *concurrencyFlag
	for i := 0; i < *concurrencyFlag; i++ {
		switch *operationFlag {
		case "write":
			go writeTest(i*1000, reqPerThread, cs)
		default:
			go readTest(i*1000, reqPerThread, cs)
		}
	}

	for i := 0; i < *concurrencyFlag; i++ {
		<-cs
	}

	elapsedTime = float64(time.Nanoseconds()-startTime) / 1000000

	end = true

	<-trackerEnd
}

func tracker() {
	var lastRequests int = 0
	var lastSum float64 = 0

	fmt.Printf("\n--------------- TESTING ---------------- \n")

	for !end {
		time.Sleep(1000000000)

		mutex.Lock()
		diffRequests := requestsSum - lastRequests
		diffSum := elapsedSum - lastSum
		lastRequests = requestsSum
		lastSum = elapsedSum
		mutex.Unlock()

		fmt.Printf("1 second: %d reqs, %f ms/s avg \n", diffRequests, diffSum/float64(diffRequests))
	}

	fmt.Printf("\n--------------- SUMMARY ---------------- \n")

	fmt.Printf("Execution time: %f ms\n", elapsedTime)
	fmt.Printf("Total requests count: %d\n", requestsSum)

	fmt.Printf("Requests per second: %f\n", float64(requestsSum)/elapsedTime*1000)
	fmt.Printf("Avg time per requests: %f\n", elapsedSum/float64(requestsSum))

	trackerEnd <- true
}

func writeTest(start int, requests int, end chan bool) {
	for i := 0; i < requests; i++ {

		server := randomServer()
		url := fmt.Sprintf("http://%s/home%d/nitro%d?size=%d", server, start+i, start+requests-i, len(data))
		buf := bytes.NewBuffer(data)

		startTime := time.Nanoseconds()
		r, err := http.Post(url, "application/json", buf)
		if err != nil {
			fmt.Printf("ERROR: Posting error: %s\n", err)
		} else {
			ioutil.ReadAll(r.Body)
		}
		elapsed := time.Nanoseconds() - startTime

		mutex.Lock()
		elapsedSum += float64(elapsed) / 1000000
		requestsSum++
		mutex.Unlock()
	}

	end <- true
}

func readTest(start int, requests int, end chan bool) {
	for i := 0; i < requests; i++ {
		startTime := time.Nanoseconds()

		server := randomServer()
		url := fmt.Sprintf("http://%s/home%d/nitro%d", server, start+i, start+requests-i)

		resp, _, _ := http.Get(url)
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		elapsed := time.Nanoseconds() - startTime

		mutex.Lock()
		elapsedSum += float64(elapsed) / 1000000
		requestsSum++
		mutex.Unlock()
	}

	end <- true
}


func randomServer() string {
	return servers[rand.Intn(len(servers))]
}
