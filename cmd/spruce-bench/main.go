package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/lajosnagyuk/spruce/pkg/spruce"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "Spruce URL")
	token := flag.String("token", os.Getenv("SPRUCE_TOKEN"), "bearer token")
	allowInsecure := flag.Bool("allow-insecure-credentials", false, "allow credentials over HTTP for isolated tests")
	topic := flag.String("topic", "bench", "topic")
	n := flag.Int("n", 10000, "messages")
	size := flag.Int("size", 256, "payload bytes")
	workers := flag.Int("workers", 8, "publishers")
	batch := flag.Int("batch", 1, "messages per HTTP request")
	flag.Parse()
	if *n <= 0 || *size < 0 || *size > 1<<20 || *workers <= 0 || *batch <= 0 || *batch > 4096 {
		log.Fatal("n, workers, and batch must be positive; size must be 0..1MiB; batch must not exceed 4096")
	}
	c := spruce.New(*server)
	c.Token = *token
	c.AllowInsecureCredentials = *allowInsecure
	payload := make([]byte, *size)
	lat := make([]int64, *n)
	var next atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				startIndex := int(next.Add(int64(*batch)) - int64(*batch))
				i := startIndex
				if i >= *n {
					return
				}
				count := *batch
				if i+count > *n {
					count = *n - i
				}
				s := time.Now()
				if count == 1 {
					if _, e := c.Publish(context.Background(), *topic, payload, spruce.PublishOptions{}); e != nil {
						log.Fatal(e)
					}
				} else {
					payloads := make([][]byte, count)
					for j := range payloads {
						payloads[j] = payload
					}
					if _, e := c.PublishBatch(context.Background(), *topic, payloads, spruce.PublishOptions{}); e != nil {
						log.Fatal(e)
					}
				}
				perMessage := time.Since(s).Microseconds() / int64(count)
				for j := 0; j < count; j++ {
					lat[i+j] = perMessage
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) int64 { return lat[int(float64(len(lat)-1)*p)] }
	fmt.Printf("messages=%d batch=%d bytes=%d elapsed=%s msg/s=%.0f MiB/s=%.2f amortized-p50=%dus amortized-p95=%dus amortized-p99=%dus\n", *n, *batch, *n**size, elapsed, float64(*n)/elapsed.Seconds(), float64(*n**size)/(1<<20)/elapsed.Seconds(), pct(.5), pct(.95), pct(.99))
}
