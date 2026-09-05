package broker

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkPublishWithUnrelatedGroups(b *testing.B) {
	for _, groups := range []int{0, 512} {
		b.Run(fmt.Sprint(groups), func(b *testing.B) {
			cfg := DefaultConfig()
			cfg.CacheBytes = 1 << 30
			cfg.StreamMemoryBytes = 512 << 20
			cfg.MaxStreams = 1024
			broker := New(cfg)
			defer broker.Close()
			broker.mu.Lock()
			for i := range groups {
				if !broker.registerGroupLocked(fmt.Sprint("unrelated-", i), "workers") {
					b.Fatal("registration failed")
				}
				broker.addSubscriberLocked(&subscriber{id: fmt.Sprint("member-", i), topic: fmt.Sprint("unrelated-", i), group: "workers", ch: make(chan Delivery, 1)})
			}
			broker.mu.Unlock()
			expiry := time.Now().Add(time.Minute).UnixMilli()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := broker.accept(&Message{ID: fmt.Sprint(i), Topic: "active", Payload: []byte("x"), ExpiresAt: expiry}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPublishWithUnrelatedTopics(b *testing.B) {
	cfg := DefaultConfig()
	cfg.CacheBytes = 1 << 30
	broker := New(cfg)
	defer broker.Close()
	expiry := time.Now().Add(time.Minute).UnixMilli()
	for i := range 512 {
		if _, err := broker.accept(&Message{ID: fmt.Sprint("seed", i), Topic: fmt.Sprint("topic", i), ExpiresAt: expiry}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := broker.accept(&Message{ID: fmt.Sprint(i), Topic: "active", ExpiresAt: expiry}); err != nil {
			b.Fatal(err)
		}
	}
}
