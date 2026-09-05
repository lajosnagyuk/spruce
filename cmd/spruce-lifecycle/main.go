// spruce-lifecycle checks independently observed delivery to each consumer group.
// Its latency clock lives entirely in this process, including with remote brokers.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lajosnagyuk/spruce/pkg/spruce"
)

type event struct {
	Run      string `json:"run"`
	Producer int    `json:"producer"`
	Sequence int    `json:"sequence"`
	Key      int    `json:"key"`
	Topic    int    `json:"topic"`
	Sent     int64  `json:"sent_ns"`
	Data     string `json:"data"`
}

type report struct {
	Run                 string      `json:"run"`
	Producers           int         `json:"producers"`
	Topics              int         `json:"topics"`
	Groups              int         `json:"groups"`
	Members             int         `json:"members_per_group"`
	Keys                int         `json:"keys"`
	Ack                 string      `json:"ack"`
	Compression         string      `json:"compression"`
	PaddingBytes        int         `json:"payload_padding_bytes"`
	Accepted            int         `json:"accepted"`
	ConfirmedCopyCounts map[int]int `json:"confirmed_copy_counts,omitempty"`
	PublishErrors       int         `json:"publish_errors"`
	Missing             int         `json:"missing_group_deliveries"`
	Duplicates          int         `json:"duplicate_group_deliveries"`
	Unconfirmed         int         `json:"deliveries_without_publish_confirmation"`
	Invalid             int         `json:"invalid"`
	OrderRegressions    int         `json:"per_producer_key_order_regressions"`
	SubscriptionErrors  int64       `json:"terminal_subscription_errors"`
	PublishSeconds      float64     `json:"publish_seconds"`
	AcceptedPerSecond   float64     `json:"accepted_per_second"`
	PublishMS           [3]float64  `json:"publish_p50_p95_p99_ms"`
	DeliveryMS          [3]float64  `json:"first_delivery_p50_p95_p99_ms"`
}

func percentiles(values []time.Duration) [3]float64 {
	var result [3]float64
	if len(values) == 0 {
		return result
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	for i, q := range []float64{.5, .95, .99} {
		result[i] = float64(values[int(float64(len(values)-1)*q)]) / float64(time.Millisecond)
	}
	return result
}

func main() {
	server := flag.String("server", "http://localhost:8080", "public gateway URL")
	producers := flag.Int("producers", 4, "sequential publishers")
	topics := flag.Int("topics", 4, "topics")
	groups := flag.Int("groups", 2, "independent consumer groups per topic")
	members := flag.Int("members", 2, "members per group")
	keys := flag.Int("keys", 16, "keys per producer")
	count := flag.Int("messages", 1000, "messages per producer")
	size := flag.Int("size", 256, "padding bytes per event")
	ack := flag.String("ack", "one-peer", "publish acknowledgement mode")
	compression := flag.String("compression", "off", "off, gzip, or zstd; synthetic padding is deliberately compressible")
	rate := flag.Float64("rate", 0, "aggregate offered messages/s; zero is unpaced")
	timeout := flag.Duration("timeout", 60*time.Second, "whole scenario deadline")
	settle := flag.Duration("settle", 2*time.Second, "observe late duplicates after all accepted deliveries")
	insecure := flag.Bool("allow-insecure-credentials", false, "allow synthetic development credentials over HTTP")
	allowDuplicates := flag.Bool("allow-duplicates", false, "allow measured at-least-once redeliveries")
	allowReorder := flag.Bool("allow-reorder", false, "record ordering violations without passing them as ordered")
	flag.Parse()
	if *producers < 1 || *producers > 1024 || *topics < 1 || *topics > 256 || *groups < 1 || *groups > 256 || *members < 1 || *members > 256 || *keys < 1 || *keys > 1000000 || *count < 1 || *count > 1000000 || *size < 0 || *size > 900<<10 || *rate < 0 || *timeout <= 0 || *settle < 0 || int64(*producers)*int64(*count) > 1000000 || int64(*topics)*int64(*groups)*int64(*members) > 1024 || int64(*producers)*int64(*count)*int64(*groups) > 4000000 {
		fmt.Fprintln(os.Stderr, "invalid or excessive scenario bounds")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	run := fmt.Sprintf("lifecycle-%d", time.Now().UnixNano())
	total := *producers * *count
	accepted := make([]bool, total)
	sentAt := make([]int64, total)
	observed := make([][]int, *groups)
	for g := range observed {
		observed[g] = make([]int, total)
	}
	var mu sync.Mutex
	var deliveryLatencies, publishLatencies []time.Duration
	var invalid, duplicates, orderRegressions, publishErrors int
	copyCounts := make(map[int]int)
	last := make(map[[3]int]int)
	var terminal atomic.Int64
	ready := make(chan struct{}, *topics**groups**members)
	var subscribers sync.WaitGroup
	padding := strings.Repeat("x", *size)
	newClient := func() *spruce.Client {
		c := spruce.New(*server)
		c.Token = os.Getenv("SPRUCE_TOKEN")
		c.AllowInsecureCredentials = *insecure
		return c
	}
	for topic := range *topics {
		for group := range *groups {
			for range *members {
				subscribers.Add(1)
				go func(topic, group int) {
					defer subscribers.Done()
					c := newClient()
					defer c.CloseIdleConnections()
					var first sync.Once
					c.OnEvent = func(e spruce.ClientEvent) {
						if e.Operation == "subscription_connected" {
							first.Do(func() { ready <- struct{}{} })
						}
					}
					err := c.Subscribe(ctx, spruce.SubscribeOptions{Topic: fmt.Sprintf("%s-%d", run, topic), Group: fmt.Sprintf("group-%d", group), Concurrency: 1}, func(_ context.Context, d spruce.Delivery) error {
						var e event
						decodeErr := json.Unmarshal(d.Payload, &e)
						mu.Lock()
						defer mu.Unlock()
						if decodeErr != nil || e.Run != run || e.Topic != topic || e.Producer < 0 || e.Producer >= *producers || e.Sequence < 0 || e.Sequence >= *count || e.Key != e.Sequence%*keys || d.Key != fmt.Sprint(e.Key) || e.Data != padding || e.Sent <= 0 {
							invalid++
							return fmt.Errorf("invalid lifecycle payload")
						}
						index := e.Producer**count + e.Sequence
						if sentAt[index] != e.Sent {
							invalid++
							return fmt.Errorf("timestamp differs from published event")
						}
						observed[group][index]++
						if observed[group][index] > 1 {
							duplicates++
							return nil
						}
						deliveryLatencies = append(deliveryLatencies, time.Since(time.Unix(0, e.Sent)))
						lane := [3]int{group, e.Producer, e.Key}
						if previous, ok := last[lane]; ok && e.Sequence < previous {
							orderRegressions++
						}
						if previous, ok := last[lane]; !ok || e.Sequence > previous {
							last[lane] = e.Sequence
						}
						return nil
					})
					if err != nil && ctx.Err() == nil {
						terminal.Add(1)
						fmt.Fprintln(os.Stderr, err)
					}
				}(topic, group)
			}
		}
	}
	for range *topics * *groups * *members {
		select {
		case <-ready:
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "subscriptions did not become ready")
			os.Exit(1)
		}
	}
	fmt.Fprintln(os.Stderr, "subscriptions_ready=true")
	started := time.Now()
	var publishers sync.WaitGroup
	for producer := range *producers {
		publishers.Add(1)
		go func(producer int) {
			defer publishers.Done()
			c := newClient()
			defer c.CloseIdleConnections()
			for sequence := range *count {
				if *rate > 0 {
					due := started.Add(time.Duration(float64(sequence**producers+producer) / *rate * float64(time.Second)))
					timer := time.NewTimer(time.Until(due))
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						return
					}
				}
				if ctx.Err() != nil {
					return
				}
				key := sequence % *keys
				topic := (producer**keys + key) % *topics
				e := event{Run: run, Producer: producer, Sequence: sequence, Key: key, Topic: topic, Sent: time.Now().UnixNano(), Data: padding}
				index := producer**count + sequence
				mu.Lock()
				sentAt[index] = e.Sent
				mu.Unlock()
				body, _ := json.Marshal(e)
				result, err := c.PublishRetry(ctx, fmt.Sprintf("%s-%d", run, topic), body, spruce.PublishOptions{Key: fmt.Sprint(key), ProducerID: fmt.Sprintf("%s-%d", run, producer), IdempotencyKey: fmt.Sprint(sequence), Ack: *ack, Compression: *compression, TTL: 10 * time.Minute}, spruce.RetryOptions{})
				elapsed := time.Since(time.Unix(0, e.Sent))
				mu.Lock()
				if err != nil {
					publishErrors++
					fmt.Fprintln(os.Stderr, "publish:", err)
				} else {
					accepted[index] = true
					if result.ConfirmedCopies > 0 {
						copyCounts[result.ConfirmedCopies]++
					}
					publishLatencies = append(publishLatencies, elapsed)
				}
				mu.Unlock()
			}
		}(producer)
	}
	publishers.Wait()
	publishSeconds := time.Since(started).Seconds()
	complete := func() bool {
		mu.Lock()
		defer mu.Unlock()
		for i, ok := range accepted {
			if ok {
				for g := range observed {
					if observed[g][i] == 0 {
						return false
					}
				}
			}
		}
		return true
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	for !complete() && ctx.Err() == nil {
		select {
		case <-ticker.C:
		case <-ctx.Done():
		}
	}
	ticker.Stop()
	timer := time.NewTimer(*settle)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
	}
	cancel()
	subscribers.Wait()
	r := report{ConfirmedCopyCounts: copyCounts, Run: run, Producers: *producers, Topics: *topics, Groups: *groups, Members: *members, Keys: *keys, Ack: *ack, Compression: *compression, PaddingBytes: *size, PublishErrors: publishErrors, Invalid: invalid, Duplicates: duplicates, OrderRegressions: orderRegressions, SubscriptionErrors: terminal.Load(), PublishSeconds: publishSeconds, PublishMS: percentiles(publishLatencies), DeliveryMS: percentiles(deliveryLatencies)}
	for i, ok := range accepted {
		if ok {
			r.Accepted++
		}
		for g := range observed {
			if ok && observed[g][i] == 0 {
				r.Missing++
			}
			if !ok && observed[g][i] > 0 {
				r.Unconfirmed++
			}
		}
	}
	r.AcceptedPerSecond = float64(r.Accepted) / publishSeconds
	_ = json.NewEncoder(os.Stdout).Encode(r)
	if r.Accepted != total || r.PublishErrors+r.Missing+r.Invalid > 0 || r.SubscriptionErrors > 0 || (!*allowDuplicates && r.Duplicates > 0) || (!*allowReorder && r.OrderRegressions > 0) {
		os.Exit(1)
	}
}
