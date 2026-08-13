package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lajosnagyuk/spruce/pkg/spruce"
)

type deliveryCounts struct {
	mu        sync.Mutex
	broadcast [][][]int
	group     [][]int
	invalid   int
}

func main() {
	server := flag.String("server", "http://localhost:8080", "Spruce URL")
	token := flag.String("token", os.Getenv("SPRUCE_TOKEN"), "bearer token")
	allowInsecure := flag.Bool("allow-insecure-credentials", false, "allow credentials over HTTP for isolated tests")
	topics := flag.Int("topics", 3, "number of topics")
	messages := flag.Int("messages", 1000, "messages per topic")
	producers := flag.Int("producers", 4, "concurrent producers")
	broadcastConsumers := flag.Int("broadcast-consumers", 2, "broadcast consumers per topic")
	groupConsumers := flag.Int("group-consumers", 3, "members in one group per topic")
	timeout := flag.Duration("timeout", 30*time.Second, "scenario timeout")
	maxMissing := flag.Int("max-missing", 0, "maximum accepted missing deliveries")
	maxDuplicates := flag.Int("max-duplicates", 0, "maximum accepted duplicate deliveries")
	publishRate := flag.Int("publish-rate", 0, "maximum aggregate publishes per second; zero is unlimited")
	ttl := flag.Duration("ttl", time.Minute, "published message TTL")
	dedupe := flag.Bool("dedupe", false, "debounce repeated delivery IDs before invoking logical handlers")
	pauseAfter := flag.Int("pause-after", 0, "pause after this many queued publishes; zero disables")
	pauseFor := flag.Duration("pause-for", 0, "duration of the one-time publisher pause")
	flag.Parse()
	if *topics <= 0 || *messages <= 0 || *producers <= 0 || *broadcastConsumers < 0 || *groupConsumers < 0 || *publishRate < 0 {
		log.Fatal("counts must be positive; consumer counts may be zero")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	run := time.Now().UTC().Format("20060102T150405.000000000")
	counts := &deliveryCounts{broadcast: make([][][]int, *topics), group: make([][]int, *topics)}
	for topic := range *topics {
		counts.broadcast[topic] = make([][]int, *broadcastConsumers)
		for consumer := range *broadcastConsumers {
			counts.broadcast[topic][consumer] = make([]int, *messages)
		}
		counts.group[topic] = make([]int, *messages)
	}

	var subscribers sync.WaitGroup
	var subscriptionErrors atomic.Int64
	for topic := range *topics {
		topicName := fmt.Sprintf("integration-%s-%d", run, topic)
		for consumer := range *broadcastConsumers {
			topic, consumer := topic, consumer
			deduper := spruce.NewDeduper(*messages*2, *ttl)
			subscribers.Add(1)
			go func() {
				defer subscribers.Done()
				client := spruce.New(*server)
				client.Token = *token
				client.AllowInsecureCredentials = *allowInsecure
				err := client.Subscribe(ctx, spruce.SubscribeOptions{Topic: topicName, Concurrency: 32}, func(_ context.Context, d spruce.Delivery) error {
					if *dedupe && deduper.Seen(d.MessageID) {
						return nil
					}
					index, ok := decode(run, topic, d.Payload, *messages)
					counts.mu.Lock()
					if ok {
						counts.broadcast[topic][consumer][index]++
					} else {
						counts.invalid++
					}
					counts.mu.Unlock()
					return nil
				})
				if err != nil && ctx.Err() == nil {
					subscriptionErrors.Add(1)
				}
			}()
		}
		groupDeduper := spruce.NewDeduper(*messages*2, *ttl)
		for range *groupConsumers {
			topic := topic
			subscribers.Add(1)
			go func() {
				defer subscribers.Done()
				client := spruce.New(*server)
				client.Token = *token
				client.AllowInsecureCredentials = *allowInsecure
				err := client.Subscribe(ctx, spruce.SubscribeOptions{Topic: topicName, Group: "workers", Concurrency: 32}, func(_ context.Context, d spruce.Delivery) error {
					if *dedupe && groupDeduper.Seen(d.MessageID) {
						return nil
					}
					index, ok := decode(run, topic, d.Payload, *messages)
					counts.mu.Lock()
					if ok {
						counts.group[topic][index]++
					} else {
						counts.invalid++
					}
					counts.mu.Unlock()
					return nil
				})
				if err != nil && ctx.Err() == nil {
					subscriptionErrors.Add(1)
				}
			}()
		}
	}

	// Streaming registration has no protocol-level ready frame. Give the gateway and
	// brokers a bounded interval to establish every subscription before publishing.
	time.Sleep(time.Second)
	type job struct{ topic, message int }
	jobs := make(chan job)
	publishStarted := time.Now()
	var publishers sync.WaitGroup
	var publishErrors atomic.Int64
	for range *producers {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			client := spruce.New(*server)
			client.Token = *token
			client.AllowInsecureCredentials = *allowInsecure
			for item := range jobs {
				payload := []byte(fmt.Sprintf("%s/%d/%d", run, item.topic, item.message))
				topicName := fmt.Sprintf("integration-%s-%d", run, item.topic)
				if _, err := client.Publish(ctx, topicName, payload, spruce.PublishOptions{TTL: *ttl}); err != nil {
					publishErrors.Add(1)
				}
			}
		}()
	}
	var pace <-chan time.Time
	var ticker *time.Ticker
	if *publishRate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*publishRate))
		pace = ticker.C
		defer ticker.Stop()
	}
	queued := 0
produce:
	for topic := range *topics {
		for message := range *messages {
			if *pauseAfter > 0 && queued == *pauseAfter && *pauseFor > 0 {
				select {
				case <-time.After(*pauseFor):
				case <-ctx.Done():
					break produce
				}
			}
			if pace != nil {
				select {
				case <-pace:
				case <-ctx.Done():
					break produce
				}
			}
			select {
			case jobs <- job{topic: topic, message: message}:
				queued++
			case <-ctx.Done():
				break produce
			}
		}
	}
	close(jobs)
	publishers.Wait()
	publishElapsed := time.Since(publishStarted)

	complete := false
	for !complete && ctx.Err() == nil {
		counts.mu.Lock()
		complete = counts.complete(*broadcastConsumers, *groupConsumers)
		counts.mu.Unlock()
		if !complete {
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel()
	subscribers.Wait()

	missing, duplicates := 0, 0
	counts.mu.Lock()
	for topic := range *topics {
		for consumer := range *broadcastConsumers {
			for _, count := range counts.broadcast[topic][consumer] {
				if count == 0 {
					missing++
				}
				if count > 1 {
					duplicates += count - 1
				}
			}
		}
		if *groupConsumers > 0 {
			for _, count := range counts.group[topic] {
				if count == 0 {
					missing++
				}
				if count > 1 {
					duplicates += count - 1
				}
			}
		}
	}
	invalid := counts.invalid
	counts.mu.Unlock()
	total := *topics * *messages
	fmt.Printf("topics=%d messages=%d producers=%d broadcast_consumers=%d group_consumers=%d publish_elapsed=%s publish_msg_s=%.0f missing=%d duplicates=%d invalid=%d publish_errors=%d subscription_errors=%d\n",
		*topics, total, *producers, *broadcastConsumers, *groupConsumers, publishElapsed,
		float64(total)/publishElapsed.Seconds(), missing, duplicates, invalid, publishErrors.Load(), subscriptionErrors.Load())
	if missing > *maxMissing || duplicates > *maxDuplicates || invalid != 0 || publishErrors.Load() != 0 || subscriptionErrors.Load() != 0 {
		log.Fatal("integration scenario failed")
	}
}

func (c *deliveryCounts) complete(broadcastConsumers, groupConsumers int) bool {
	for topic := range c.broadcast {
		for consumer := range broadcastConsumers {
			for _, count := range c.broadcast[topic][consumer] {
				if count == 0 {
					return false
				}
			}
		}
		if groupConsumers > 0 {
			for _, count := range c.group[topic] {
				if count == 0 {
					return false
				}
			}
		}
	}
	return true
}

func decode(run string, topic int, payload []byte, messages int) (int, bool) {
	prefix := fmt.Sprintf("%s/%d/", run, topic)
	if !strings.HasPrefix(string(payload), prefix) {
		return 0, false
	}
	index, err := strconv.Atoi(string(payload[len(prefix):]))
	return index, err == nil && index >= 0 && index < messages
}
