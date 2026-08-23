package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lajosnagyuk/spruce/pkg/spruce"
)

func fill(payload []byte, index uint64) {
	binary.BigEndian.PutUint64(payload[:8], index)
	state := index ^ 0x9e3779b97f4a7c15
	for i := 8; i < len(payload); i++ {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[i] = byte(state)
	}
}
func valid(payload []byte, index uint64) bool {
	if len(payload) < 8 || binary.BigEndian.Uint64(payload[:8]) != index {
		return false
	}
	state := index ^ 0x9e3779b97f4a7c15
	for i := 8; i < len(payload); i++ {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		if payload[i] != byte(state) {
			return false
		}
	}
	return true
}
func main() {
	server := flag.String("server", "http://127.0.0.1:18090", "server")
	token := flag.String("token", os.Getenv("SPRUCE_TOKEN"), "token")
	seconds := flag.Int("seconds", 300, "publish duration")
	rate := flag.Int("rate", 60, "messages per second")
	size := flag.Int("size", 900*1024, "payload bytes")
	workers := flag.Int("workers", 8, "publish workers")
	flag.Parse()
	if *seconds <= 0 || *rate <= 0 || *size < 8 || *size > 1<<20 || *workers <= 0 {
		fmt.Fprintln(os.Stderr, "seconds, rate, and workers must be positive; size must be between 8 and 1048576 bytes")
		os.Exit(2)
	}
	if int64(*seconds) > int64(^uint(0)>>1)/int64(*rate) {
		fmt.Fprintln(os.Stderr, "seconds multiplied by rate is too large")
		os.Exit(2)
	}
	total := *seconds * *rate
	if *rate > 10000 || total > 10_000_000 {
		fmt.Fprintln(os.Stderr, "rate must not exceed 10000 messages per second and total messages must not exceed 10000000")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*seconds+180)*time.Second)
	defer cancel()
	topic := fmt.Sprintf("binary-soak-%d", time.Now().UnixNano())
	seen := make([]atomic.Uint32, total)
	var enqueued, published, received, invalid, duplicates, overloadRejected, publishErrors, subscribeErrors atomic.Int64
	var firstPublishError, firstSubscribeError sync.Once
	ready := make(chan struct{})
	go func() {
		client := spruce.New(*server)
		client.Token = *token
		client.AllowInsecureCredentials = true
		close(ready)
		err := client.Subscribe(ctx, spruce.SubscribeOptions{Topic: topic, Group: "binary-verifier", Concurrency: 64, MaxPayloadBytes: 1 << 20}, func(_ context.Context, d spruce.Delivery) error {
			if len(d.Payload) != *size {
				invalid.Add(1)
				return nil
			}
			index := binary.BigEndian.Uint64(d.Payload[:8])
			if index >= uint64(total) || !valid(d.Payload, index) {
				invalid.Add(1)
				return nil
			}
			if !seen[index].CompareAndSwap(0, 1) {
				duplicates.Add(1)
			} else {
				received.Add(1)
			}
			return nil
		})
		if err != nil && ctx.Err() == nil {
			subscribeErrors.Add(1)
			firstSubscribeError.Do(func() { fmt.Fprintf(os.Stderr, "first_subscription_error=%v\n", err) })
		}
	}()
	<-ready
	time.Sleep(time.Second)
	jobs := make(chan int, *workers*2)
	var wg sync.WaitGroup
	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := spruce.New(*server)
			client.Token = *token
			client.AllowInsecureCredentials = true
			for index := range jobs {
				payload := make([]byte, *size)
				fill(payload, uint64(index))
				if _, err := client.Publish(ctx, topic, payload, spruce.PublishOptions{TTL: 2 * time.Minute, Key: fmt.Sprintf("%d", index%64)}); err != nil {
					if strings.Contains(err.Error(), "429") {
						overloadRejected.Add(1)
					} else {
						publishErrors.Add(1)
						firstPublishError.Do(func() { fmt.Fprintf(os.Stderr, "first_publish_error=%v\n", err) })
					}
				} else {
					published.Add(1)
				}
			}
		}()
	}
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	started := time.Now()
produce:
	for index := 0; index < total; index++ {
		select {
		case <-ctx.Done():
			break produce
		case <-ticker.C:
			select {
			case jobs <- index:
				enqueued.Add(1)
			case <-ctx.Done():
				break produce
			}
		}
	}
	productionElapsed := time.Since(started)
	close(jobs)
	wg.Wait()
	publishElapsed := time.Since(started)
	deadline := time.Now().Add(2 * time.Minute)
	for received.Load() < published.Load() && subscribeErrors.Load() == 0 && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	time.Sleep(time.Second)
	missing := published.Load() - received.Load()
	fmt.Printf("topic=%s payload_bytes=%d planned=%d enqueued=%d published=%d received=%d overload_rejected=%d missing=%d duplicates=%d invalid=%d publish_errors=%d subscription_errors=%d production_elapsed=%s publish_elapsed=%s elapsed=%s offered_rate=%.2f accepted_rate=%.2f payload_gib=%.2f\n", topic, *size, total, enqueued.Load(), published.Load(), received.Load(), overloadRejected.Load(), missing, duplicates.Load(), invalid.Load(), publishErrors.Load(), subscribeErrors.Load(), productionElapsed, publishElapsed, time.Since(started), float64(enqueued.Load())/productionElapsed.Seconds(), float64(published.Load())/publishElapsed.Seconds(), float64(published.Load()*int64(*size))/(1<<30))
	if missing != 0 || duplicates.Load() != 0 || invalid.Load() != 0 || publishErrors.Load() != 0 || subscribeErrors.Load() != 0 {
		os.Exit(1)
	}
}
