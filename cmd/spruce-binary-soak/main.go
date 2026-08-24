package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lajosnagyuk/spruce/pkg/spruce"
)

func fill(payload []byte, index uint64, compressible bool) {
	binary.BigEndian.PutUint64(payload[:8], index)
	binary.BigEndian.PutUint64(payload[8:16], uint64(time.Now().UnixNano()))
	if compressible {
		pattern := []byte(`,"workspace":"spruce","event":"deployment.updated","status":"ready"`)
		for i := 16; i < len(payload); i++ {
			payload[i] = pattern[(i-16)%len(pattern)]
		}
		return
	}
	state := index ^ 0x9e3779b97f4a7c15
	for i := 8; i < len(payload); i++ {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[i] = byte(state)
	}
}
func valid(payload []byte, index uint64, compressible bool) bool {
	if len(payload) < 16 || binary.BigEndian.Uint64(payload[:8]) != index {
		return false
	}
	state := index ^ 0x9e3779b97f4a7c15
	pattern := []byte(`,"workspace":"spruce","event":"deployment.updated","status":"ready"`)
	for i := 16; i < len(payload); i++ {
		if compressible {
			if payload[i] != pattern[(i-16)%len(pattern)] {
				return false
			}
			continue
		}
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
	ratesArg := flag.String("rates", "", "comma-separated messages-per-second phases spread across the duration")
	size := flag.Int("size", 900*1024, "payload bytes")
	sizesArg := flag.String("sizes", "", "comma-separated payload sizes cycled by message index")
	workers := flag.Int("workers", 8, "publish workers")
	topics := flag.Int("topics", 1, "number of topics distributed round-robin")
	handlerDelay := flag.Duration("handler-delay", 0, "artificial delay per consumed message")
	compression := flag.String("compression", "off", "payload compression: off, gzip, or zstd")
	data := flag.String("data", "random", "payload content: random or compressible")
	nackEvery := flag.Int("nack-every", 0, "NACK every Nth message once to measure redelivery; zero disables")
	flag.Parse()
	rates := []int{*rate}
	if *ratesArg != "" {
		rates = rates[:0]
		for _, value := range strings.Split(*ratesArg, ",") {
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || parsed <= 0 {
				fmt.Fprintln(os.Stderr, "rates must be comma-separated positive integers")
				os.Exit(2)
			}
			rates = append(rates, parsed)
		}
	}
	sizes := []int{*size}
	if *sizesArg != "" {
		sizes = sizes[:0]
		for _, value := range strings.Split(*sizesArg, ",") {
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				fmt.Fprintln(os.Stderr, "sizes must be comma-separated integers")
				os.Exit(2)
			}
			sizes = append(sizes, parsed)
		}
	}
	for _, payloadSize := range sizes {
		if payloadSize < 16 || payloadSize > 1<<20 {
			fmt.Fprintln(os.Stderr, "every payload size must be between 16 and 1048576 bytes")
			os.Exit(2)
		}
	}
	if *seconds <= 0 || len(rates) == 0 || len(sizes) == 0 || *workers <= 0 || *topics <= 0 || *topics > 32 || *handlerDelay < 0 || *handlerDelay > 10*time.Second {
		fmt.Fprintln(os.Stderr, "seconds, rate, and workers must be positive; size must be between 8 and 1048576 bytes")
		os.Exit(2)
	}
	if *compression != "off" && *compression != spruce.CompressionGzip && *compression != spruce.CompressionZstd {
		fmt.Fprintln(os.Stderr, "compression must be off, gzip, or zstd")
		os.Exit(2)
	}
	if *data != "random" && *data != "compressible" {
		fmt.Fprintln(os.Stderr, "data must be random or compressible")
		os.Exit(2)
	}
	if *nackEvery < 0 {
		fmt.Fprintln(os.Stderr, "nack-every must not be negative")
		os.Exit(2)
	}
	compressionMode := *compression
	if compressionMode == "off" {
		compressionMode = spruce.CompressionOff
	}
	compressible := *data == "compressible"
	phaseSeconds := make([]int, len(rates))
	total := 0
	for i, phaseRate := range rates {
		if phaseRate > 10000 {
			fmt.Fprintln(os.Stderr, "rates must not exceed 10000 messages per second")
			os.Exit(2)
		}
		phaseSeconds[i] = *seconds / len(rates)
		if i < *seconds%len(rates) {
			phaseSeconds[i]++
		}
		if phaseSeconds[i] > (10_000_000-total)/phaseRate {
			fmt.Fprintln(os.Stderr, "total messages must not exceed 10000000")
			os.Exit(2)
		}
		total += phaseSeconds[i] * phaseRate
	}
	if total == 0 {
		fmt.Fprintln(os.Stderr, "duration must provide time for at least one rate phase")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*seconds+180)*time.Second)
	defer cancel()
	topicPrefix := fmt.Sprintf("binary-soak-%d", time.Now().UnixNano())
	topicNames := make([]string, *topics)
	for i := range topicNames {
		topicNames[i] = fmt.Sprintf("%s-%d", topicPrefix, i)
	}
	seen := make([]atomic.Uint32, total)
	nackedAt := make([]atomic.Int64, total)
	var enqueued, published, publishedBytes, received, invalid, duplicates, overloadRejected, publishErrors, subscribeErrors atomic.Int64
	var latencyMu sync.Mutex
	latencies := make([]int64, 0, min(total, 100000))
	firstDeliveryLatencies := make([]int64, 0, min(total, 100000))
	redeliveryLatencies := make([]int64, 0, min(total, 100000))
	var firstPublishError, firstSubscribeError sync.Once
	ready := make(chan struct{}, len(topicNames))
	for topicIndex, topic := range topicNames {
		go func() {
			client := spruce.New(*server)
			client.Token = *token
			client.AllowInsecureCredentials = true
			ready <- struct{}{}
			err := client.Subscribe(ctx, spruce.SubscribeOptions{Topic: topic, Group: "binary-verifier", Concurrency: 64, MaxPayloadBytes: 1 << 20}, func(callbackCtx context.Context, d spruce.Delivery) error {
				if len(d.Payload) < 8 {
					invalid.Add(1)
					return nil
				}
				index := binary.BigEndian.Uint64(d.Payload[:8])
				if index >= uint64(total) || int(index%uint64(len(topicNames))) != topicIndex || len(d.Payload) != sizes[index%uint64(len(sizes))] || !valid(d.Payload, index, compressible) {
					invalid.Add(1)
					return nil
				}
				now := time.Now().UnixNano()
				publishedAt := int64(binary.BigEndian.Uint64(d.Payload[8:16]))
				latencyMu.Lock()
				if d.Attempt == 1 && len(firstDeliveryLatencies) < cap(firstDeliveryLatencies) {
					firstDeliveryLatencies = append(firstDeliveryLatencies, (now-publishedAt)/1000)
				} else if d.Attempt > 1 && len(redeliveryLatencies) < cap(redeliveryLatencies) {
					if nacked := nackedAt[index].Load(); nacked > 0 {
						redeliveryLatencies = append(redeliveryLatencies, (now-nacked)/1000)
					}
				}
				latencyMu.Unlock()
				if *nackEvery > 0 && d.Attempt == 1 && index%uint64(*nackEvery) == 0 {
					nackedAt[index].Store(now)
					return fmt.Errorf("planned redelivery")
				}
				if *handlerDelay > 0 {
					timer := time.NewTimer(*handlerDelay)
					defer timer.Stop()
					select {
					case <-callbackCtx.Done():
						return callbackCtx.Err()
					case <-timer.C:
					}
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
	}
	for range topicNames {
		<-ready
	}
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
				payload := make([]byte, sizes[index%len(sizes)])
				fill(payload, uint64(index), compressible)
				publishStarted := time.Now()
				if _, err := client.Publish(ctx, topicNames[index%len(topicNames)], payload, spruce.PublishOptions{TTL: 2 * time.Minute, Key: fmt.Sprintf("%d", index%64), Compression: compressionMode}); err != nil {
					if strings.Contains(err.Error(), "429") {
						overloadRejected.Add(1)
					} else {
						publishErrors.Add(1)
						firstPublishError.Do(func() { fmt.Fprintf(os.Stderr, "first_publish_error=%v\n", err) })
					}
				} else {
					latency := time.Since(publishStarted).Microseconds()
					latencyMu.Lock()
					if len(latencies) < cap(latencies) {
						latencies = append(latencies, latency)
					}
					latencyMu.Unlock()
					published.Add(1)
					publishedBytes.Add(int64(len(payload)))
				}
			}
		}()
	}
	started := time.Now()
	index := 0

produce:
	for phase, phaseRate := range rates {
		ticker := time.NewTicker(time.Second / time.Duration(phaseRate))
		phaseDeadline := time.Now().Add(time.Duration(phaseSeconds[phase]) * time.Second)
		for time.Now().Before(phaseDeadline) && index < total {
			select {
			case <-ctx.Done():
				ticker.Stop()
				break produce
			case <-ticker.C:
				select {
				case jobs <- index:
					enqueued.Add(1)
					index++
				case <-ctx.Done():
					ticker.Stop()
					break produce
				}
			}
		}
		ticker.Stop()
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
	sizeLabels := make([]string, len(sizes))
	for i, payloadSize := range sizes {
		sizeLabels[i] = strconv.Itoa(payloadSize)
	}
	rateLabels := make([]string, len(rates))
	for i, phaseRate := range rates {
		rateLabels[i] = strconv.Itoa(phaseRate)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	sort.Slice(firstDeliveryLatencies, func(i, j int) bool { return firstDeliveryLatencies[i] < firstDeliveryLatencies[j] })
	sort.Slice(redeliveryLatencies, func(i, j int) bool { return redeliveryLatencies[i] < redeliveryLatencies[j] })
	percentile := func(values []int64, p float64) int64 {
		if len(values) == 0 {
			return 0
		}
		return values[min(len(values)-1, int(float64(len(values))*p))]
	}
	fmt.Printf("topic_prefix=%s topics=%d handler_delay=%s compression=%s data=%s nack_every=%d payload_sizes=%s requested_rates=%s planned=%d enqueued=%d published=%d received=%d overload_rejected=%d missing=%d duplicates=%d invalid=%d publish_errors=%d subscription_errors=%d production_elapsed=%s publish_elapsed=%s elapsed=%s offered_rate=%.2f accepted_rate=%.2f payload_gib=%.2f publish_latency_p50_us=%d publish_latency_p95_us=%d publish_latency_p99_us=%d first_delivery_p50_us=%d first_delivery_p95_us=%d first_delivery_p99_us=%d redelivery_p50_us=%d redelivery_p95_us=%d redelivery_p99_us=%d\n", topicPrefix, len(topicNames), *handlerDelay, *compression, *data, *nackEvery, strings.Join(sizeLabels, "|"), strings.Join(rateLabels, "|"), total, enqueued.Load(), published.Load(), received.Load(), overloadRejected.Load(), missing, duplicates.Load(), invalid.Load(), publishErrors.Load(), subscribeErrors.Load(), productionElapsed, publishElapsed, time.Since(started), float64(enqueued.Load())/productionElapsed.Seconds(), float64(published.Load())/publishElapsed.Seconds(), float64(publishedBytes.Load())/(1<<30), percentile(latencies, .50), percentile(latencies, .95), percentile(latencies, .99), percentile(firstDeliveryLatencies, .50), percentile(firstDeliveryLatencies, .95), percentile(firstDeliveryLatencies, .99), percentile(redeliveryLatencies, .50), percentile(redeliveryLatencies, .95), percentile(redeliveryLatencies, .99))
	if missing != 0 || duplicates.Load() != 0 || invalid.Load() != 0 || publishErrors.Load() != 0 || subscribeErrors.Load() != 0 {
		os.Exit(1)
	}
}
