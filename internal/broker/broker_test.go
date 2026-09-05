package broker

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheEvictsOldestWithinBound(t *testing.T) {
	template := &Message{ID: "a", Topic: "t", Payload: make([]byte, 80)}
	c := newCache(messageSize(template) * 2)
	for i := 0; i < 10; i++ {
		m := &Message{ID: string(rune('a' + i)), Topic: "t", Payload: make([]byte, 80), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
		if _, e := c.put(m, time.Now().UnixMilli()); e != nil {
			t.Fatal(e)
		}
	}
	if c.bytes > c.maxBytes {
		t.Fatalf("cache exceeded bound: %d", c.bytes)
	}
	if c.evicted.Load() == 0 {
		t.Fatal("expected eviction")
	}
}

func TestCacheSnapshotOrdersReplayByAcceptanceTime(t *testing.T) {
	now := time.Now()
	c := newCache(1 << 20)
	messages := []*Message{
		{ID: "later", Topic: "ordered", Key: "key", Payload: []byte("2"), CreatedAt: now.Add(time.Second).UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()},
		{ID: "earlier", Topic: "ordered", Key: "key", Payload: []byte("1"), CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()},
	}
	if _, err := c.putBatch(messages, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	replay := c.snapshot("ordered", 1)
	if len(replay) != 2 || replay[0].ID != "earlier" || replay[1].ID != "later" {
		t.Fatalf("unexpected replay order: %v", []string{replay[0].ID, replay[1].ID})
	}
}
func TestPublishAndMetrics(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheBytes = 1 << 20
	b := New(cfg)
	defer b.Close()
	s := httptest.NewServer(b.Handler())
	defer s.Close()
	resp, e := http.Post(s.URL+"/v1/topics/test/messages", "application/octet-stream", bytes.NewReader([]byte("hello")))
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var out map[string]any
	if json.NewDecoder(resp.Body).Decode(&out) != nil || out["id"] == nil {
		t.Fatal("missing message id")
	}
	metrics, e := http.Get(s.URL + "/metrics")
	if e != nil {
		t.Fatal(e)
	}
	defer metrics.Body.Close()
	body, _ := io.ReadAll(metrics.Body)
	if !bytes.Contains(body, []byte("spruce_publish_total 1")) {
		t.Fatalf("unexpected metrics: %s", body)
	}
}
func TestReplicationDeduplicates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken = "test-peer-token"
	cfg.ClusterID = "test-cluster"
	b := New(cfg)
	defer b.Close()
	m := &Message{ID: "same", Topic: "test", Payload: []byte("hi"), CreatedAt: time.Now().UnixMilli(), ExpiresAt: time.Now().Add(time.Minute).UnixMilli(), Origin: "origin", Sequence: 1}
	var body bytes.Buffer
	if err := writePeerBatch(&body, []*Message{m}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		r := httptest.NewRequest("POST", "/internal/replicate", bytes.NewReader(body.Bytes()))
		r.Header.Set("Spruce-Peer-Token", cfg.PeerToken)
		r.Header.Set("Spruce-Cluster-ID", cfg.ClusterID)
		r.Header.Set("Spruce-Peer-Version", "2")
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatalf("status %d", w.Code)
		}
	}
	if got := len(b.cache.snapshot("test", 0)); got != 1 {
		t.Fatalf("got %d messages", got)
	}
}

func TestReplicationRejectsWrongCluster(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken = "peer-token"
	cfg.ClusterID = "cluster-a"
	b := New(cfg)
	defer b.Close()
	m := &Message{ID: "foreign", Topic: "test", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	var body bytes.Buffer
	if err := writePeerBatch(&body, []*Message{m}); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/internal/replicate", &body)
	r.Header.Set("Spruce-Peer-Token", cfg.PeerToken)
	r.Header.Set("Spruce-Cluster-ID", "cluster-b")
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized || b.cache.has(m.ID) {
		t.Fatalf("status=%d cached=%v", w.Code, b.cache.has(m.ID))
	}
}

func TestSnapshotPagesAreByteBoundedAndCursorStable(t *testing.T) {
	c := newCache(64 << 20)
	now := time.Now()
	for i := 0; i < 40; i++ {
		m := &Message{ID: string(rune('a' + i)), Topic: "snapshot", Payload: make([]byte, 1<<20), CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Hour).UnixMilli()}
		if _, err := c.put(m, now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	page, cursor, valid := c.page("", 24<<20)
	if !valid || len(page) == 0 || len(page) >= 40 || cursor != page[len(page)-1].ID {
		t.Fatalf("invalid first page: messages=%d cursor=%q valid=%v", len(page), cursor, valid)
	}
	next, _, valid := c.page(cursor, 24<<20)
	if !valid || len(next) == 0 || next[0].ID == cursor {
		t.Fatalf("invalid continuation: messages=%d valid=%v", len(next), valid)
	}
}

func TestSnapshotRejectsEvictedCursor(t *testing.T) {
	c := newCache(320)
	now := time.Now()
	first := &Message{ID: "first", Topic: "snapshot", Payload: make([]byte, 80), ExpiresAt: now.Add(time.Hour).UnixMilli()}
	_, _ = c.put(first, now.UnixMilli())
	for i := 0; i < 10; i++ {
		_, _ = c.put(&Message{ID: string(rune('k' + i)), Topic: "snapshot", Payload: make([]byte, 80), ExpiresAt: now.Add(time.Hour).UnixMilli()}, now.UnixMilli())
	}
	if _, _, valid := c.page(first.ID, 24<<20); valid {
		t.Fatal("evicted cursor was accepted")
	}
}

func TestBootstrapFailsClosedAfterPartialPeerResponse(t *testing.T) {
	calls := atomic.Int32{}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) > 1 {
			w.WriteHeader(http.StatusConflict)
			return
		}
		m := &Message{ID: "one", Topic: "snapshot", Payload: []byte("x"), CreatedAt: time.Now().UnixMilli(), ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}
		w.Header().Set("Spruce-Next-Cursor", m.ID)
		_ = writePeerBatch(w, []*Message{m})
	}))
	defer peer.Close()
	cfg := DefaultConfig()
	cfg.Peers, cfg.PeerToken, cfg.ClusterID = []string{peer.URL}, "peer", "cluster"
	b := New(cfg)
	defer b.Close()
	if err := b.SyncFromPeers(context.Background()); err == nil {
		t.Fatal("partial bootstrap succeeded")
	}
}

func TestPublishBatch(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	var body bytes.Buffer
	for _, payload := range [][]byte{[]byte("one"), []byte{0, 1, 2, 255}} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
		body.Write(size[:])
		body.Write(payload)
	}
	r := httptest.NewRequest("POST", "/v1/topics/batch/batches", &body)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 202 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if got := len(b.cache.snapshot("batch", 0)); got != 2 {
		t.Fatalf("got %d messages", got)
	}
}

func TestPublishBatchV2PreservesPerEntryKeys(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	var body bytes.Buffer
	for i, payload := range [][]byte{[]byte("one"), {0, 1, 2, 255}} {
		key := []byte([]string{"first", "second"}[i])
		var keySize [2]byte
		binary.BigEndian.PutUint16(keySize[:], uint16(len(key)))
		body.Write(keySize[:])
		body.Write(key)
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
		body.Write(size[:])
		body.Write(payload)
	}
	r := httptest.NewRequest("POST", "/v1/topics/batch-v2/batches", &body)
	r.Header.Set("Spruce-Batch-Version", "2")
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	messages := b.cache.snapshot("batch-v2", 0)
	if len(messages) != 2 || messages[0].Key != "first" || messages[1].Key != "second" {
		t.Fatalf("per-entry keys lost: %#v", messages)
	}
}

func TestPerformancePublishBatchAllocationCeiling(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts include race instrumentation")
	}
	body := benchmarkBatch(512, 256)
	// Expire history outside the timed section so calibration measures
	// successful admission rather than a full retention cache.
	var failedStatus int
	result := testing.Benchmark(func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.CacheBytes = 64 << 20
		broker := New(cfg)
		defer broker.Close()
		b.ResetTimer()
		for range b.N {
			b.StopTimer()
			broker.cache.expire(time.Now().Add(time.Hour).UnixMilli())
			b.StartTimer()
			r := httptest.NewRequest("POST", "/v1/topics/bench/batches", bytes.NewReader(body))
			w := httptest.NewRecorder()
			broker.Handler().ServeHTTP(w, r)
			if w.Code != 202 {
				failedStatus = w.Code
				return
			}
		}
	})
	if failedStatus != 0 {
		t.Fatalf("benchmark publish status %d", failedStatus)
	}
	if allocations := result.AllocsPerOp(); allocations > 2108 {
		t.Fatalf("batch allocation-count regression: got %d allocations/request, ceiling 2108", allocations)
	}
	if bytes := result.AllocedBytesPerOp(); bytes > 320<<10 {
		t.Fatalf("batch allocation-byte regression: got %d bytes/request, ceiling %d", bytes, 320<<10)
	}
}

func TestPerformanceExpiryIndexTracksCacheExactly(t *testing.T) {
	c := newCache(32 << 10)
	now := time.Now()
	for i := 0; i < 1000; i++ {
		m := &Message{ID: strconv.Itoa(i), Topic: "expiry", Payload: make([]byte, 64), ExpiresAt: now.Add(time.Duration(i+1) * time.Millisecond).UnixMilli()}
		if _, err := c.put(m, now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	assertCacheExpiryInvariant(t, c)
	c.expire(now.Add(2 * time.Second).UnixMilli())
	assertCacheExpiryInvariant(t, c)
	if c.expiry.Len() != 0 || len(c.items) != 0 || c.bytes != 0 {
		t.Fatalf("expiry/cache drift after expiration: heap=%d cache=%d bytes=%d", c.expiry.Len(), len(c.items), c.bytes)
	}
}

func TestPerformanceSustainedChurnKeepsIndexesBounded(t *testing.T) {
	c := newCache(1 << 20)
	now := time.Now()
	for batch := 0; batch < 200; batch++ {
		messages := make([]*Message, 256)
		for i := range messages {
			id := strconv.Itoa(batch*len(messages) + i)
			messages[i] = &Message{ID: id, Topic: "churn", Payload: make([]byte, 256), ExpiresAt: now.Add(time.Minute).UnixMilli()}
		}
		if _, err := c.putBatch(messages, now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	assertCacheExpiryInvariant(t, c)
	if c.bytes > c.maxBytes {
		t.Fatalf("cache bound drift: bytes=%d max=%d", c.bytes, c.maxBytes)
	}
	if len(c.order) > 2*len(c.items)+orderCompactTombstones {
		t.Fatalf("order index retained excessive tombstones: order=%d items=%d", len(c.order), len(c.items))
	}
	if len(c.topics["churn"]) > 2*len(c.items)+topicCompactTombstones {
		t.Fatalf("topic index retained excessive tombstones: topic=%d items=%d", len(c.topics["churn"]), len(c.items))
	}
}

func expiryMessageCount(c *cache) int {
	total := 0
	for _, item := range c.expiryItems {
		total += item.count
	}
	return total
}

func assertCacheExpiryInvariant(t *testing.T, c *cache) {
	t.Helper()
	if len(c.expiryItems) != c.expiry.Len() {
		t.Fatalf("expiry map/heap drift: map=%d heap=%d", len(c.expiryItems), c.expiry.Len())
	}
	seen := make(map[*Message]bool, len(c.items))
	for index, item := range c.expiry {
		if item.index != index || c.expiryItems[item.at] != item {
			t.Fatalf("expiry bucket index mismatch: index=%d stored=%d", index, item.index)
		}
		count := 0
		var previous *Message
		for message := item.head; message != nil; message = message.expiryNext {
			if seen[message] || message.expiry != item || message.expiryPrev != previous || message.ExpiresAt != item.at || c.items[message.ID] != message {
				t.Fatalf("invalid expiry linkage for message %q", message.ID)
			}
			seen[message] = true
			previous = message
			count++
		}
		if count != item.count {
			t.Fatalf("expiry bucket count mismatch: walked=%d stored=%d", count, item.count)
		}
	}
	if len(seen) != len(c.items) || expiryMessageCount(c) != len(c.items) {
		t.Fatalf("expiry/cache membership drift: seen=%d indexed=%d cache=%d", len(seen), expiryMessageCount(c), len(c.items))
	}
}

func benchmarkBatch(messages, payloadBytes int) []byte {
	var encoded bytes.Buffer
	payload := make([]byte, payloadBytes)
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
	for range messages {
		encoded.Write(size[:])
		encoded.Write(payload)
	}
	return encoded.Bytes()
}

func BenchmarkPublishBatch512(b *testing.B) {
	benchmarkPublishBatch(b, 512, 256)
}

func BenchmarkPublishBatch(b *testing.B) {
	for _, messages := range []int{1, 16, 64, 256, 512} {
		b.Run(strconv.Itoa(messages), func(b *testing.B) { benchmarkPublishBatch(b, messages, 256) })
	}
}

func BenchmarkConcurrentPublishBatch64(b *testing.B) {
	cfg := DefaultConfig()
	cfg.DefaultTTL = 100 * time.Millisecond
	broker := New(cfg)
	defer broker.Close()
	body := benchmarkBatch(64, 256)
	b.SetBytes(64 * 256)
	b.ReportMetric(64, "messages/op")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r := httptest.NewRequest("POST", "/v1/topics/bench/batches", bytes.NewReader(body))
			w := httptest.NewRecorder()
			broker.Handler().ServeHTTP(w, r)
			if w.Code != 202 {
				b.Errorf("status %d", w.Code)
				return
			}
		}
	})
}

func benchmarkPublishBatch(b *testing.B, messages, payloadBytes int) {
	cfg := DefaultConfig()
	cfg.DefaultTTL = 100 * time.Millisecond
	cfg.CacheBytes = 64 << 20
	broker := New(cfg)
	defer broker.Close()
	body := benchmarkBatch(messages, payloadBytes)
	b.SetBytes(int64(payloadBytes * messages))
	b.ReportMetric(float64(messages), "messages/op")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := httptest.NewRequest("POST", "/v1/topics/bench/batches", bytes.NewReader(body))
		w := httptest.NewRecorder()
		broker.Handler().ServeHTTP(w, r)
		if w.Code != 202 {
			b.Fatalf("status %d", w.Code)
		}
	}
}

func BenchmarkCachePutBatch512(b *testing.B) {
	c := newCache(64 << 20)
	now := time.Now()
	payload := make([]byte, 256)
	var sequence atomic.Uint64
	b.ReportMetric(512, "messages/op")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		messages := make([]*Message, 512)
		for i := range messages {
			id := strconv.FormatUint(sequence.Add(1), 10)
			messages[i] = &Message{ID: id, Topic: "bench", Payload: payload, CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()}
		}
		if _, err := c.putBatch(messages, now.UnixMilli()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteFrame(b *testing.B) {
	for _, payloadBytes := range []int{64, 256, 4096, 1 << 20} {
		b.Run(strconv.Itoa(payloadBytes), func(b *testing.B) {
			d := Delivery{DeliveryID: "delivery", MessageID: "message", Topic: "bench", CreatedAt: time.Now().UnixMilli(), Attempt: 1, Payload: make([]byte, payloadBytes)}
			b.SetBytes(int64(payloadBytes))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := writeFrame(io.Discard, d); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDeliverConsumerGroup(b *testing.B) {
	for _, members := range []int{1, 4, 16, 64} {
		b.Run(strconv.Itoa(members), func(b *testing.B) {
			broker := New(DefaultConfig())
			defer broker.Close()
			for i := range members {
				s := &subscriber{id: strconv.Itoa(i), topic: "bench", group: "workers", ch: make(chan Delivery, 1)}
				broker.addSubscriberLocked(s)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := &Message{ID: strconv.Itoa(i), Topic: "bench", Payload: []byte("payload"), CreatedAt: time.Now().UnixMilli(), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
				broker.deliver(m, "", 1)
				broker.mu.Lock()
				for id, pending := range broker.pending {
					delete(broker.pending, id)
					if pending.deadline.index >= 0 {
						heap.Remove(&broker.pendingDeadlines, pending.deadline.index)
					}
					if s := broker.subs[pending.subscriberID]; s != nil {
						<-s.ch
						s.inflightBytes -= pending.bytes
					}
					broker.unlinkTopicPendingLocked(pending)
					broker.completeGroupWorkLocked(pending.message.Topic, pending.group, pending.message.ID)
					broker.pendingBytes -= pending.bytes
					releasePending(pending)
				}
				broker.mu.Unlock()
			}
		})
	}
}

func BenchmarkPublishDeliverAck256(b *testing.B) {
	cfg := DefaultConfig()
	cfg.DefaultTTL = 100 * time.Millisecond
	broker := New(cfg)
	defer broker.Close()
	s := &subscriber{id: "consumer", topic: "bench", ch: make(chan Delivery, 256)}
	broker.mu.Lock()
	broker.addSubscriberLocked(s)
	broker.mu.Unlock()
	body := benchmarkBatch(256, 256)
	acks := make([]string, 256)
	b.SetBytes(256 * 256)
	b.ReportMetric(256, "messages/op")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := httptest.NewRequest("POST", "/v1/topics/bench/batches", bytes.NewReader(body))
		w := httptest.NewRecorder()
		broker.Handler().ServeHTTP(w, r)
		if w.Code != 202 {
			b.Fatalf("status %d", w.Code)
		}
		for i := range acks {
			acks[i] = (<-s.ch).DeliveryID
		}
		broker.removeAcks(acks)
	}
}

func BenchmarkWritePeerBatch512(b *testing.B) {
	now := time.Now()
	payload := make([]byte, 256)
	messages := make([]*Message, 512)
	for i := range messages {
		messages[i] = &Message{ID: strconv.Itoa(i), Topic: "bench", Payload: payload, CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()}
	}
	b.SetBytes(512 * 256)
	b.ReportMetric(512, "messages/op")
	b.ReportAllocs()
	var encoded bytes.Buffer
	encoded.Grow(512 * 320)
	b.ResetTimer()
	for range b.N {
		encoded.Reset()
		if err := writePeerBatch(&encoded, messages); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWritePeerBatch512Cold(b *testing.B) {
	now := time.Now()
	payload := make([]byte, 256)
	messages := make([]*Message, 512)
	for i := range messages {
		messages[i] = &Message{ID: strconv.Itoa(i), Topic: "bench", Payload: payload, CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()}
	}
	b.SetBytes(512 * 256)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var encoded bytes.Buffer
		if err := writePeerBatch(&encoded, messages); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNextID(b *testing.B) {
	broker := New(DefaultConfig())
	defer broker.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = broker.nextID()
	}
}

func TestMixedTTLExpiresOutOfInsertionOrder(t *testing.T) {
	c := newCache(1 << 20)
	now := time.Now()
	long := &Message{ID: "long", Topic: "t", Payload: []byte("long"), ExpiresAt: now.Add(time.Hour).UnixMilli()}
	short := &Message{ID: "short", Topic: "t", Payload: []byte("short"), ExpiresAt: now.Add(time.Millisecond).UnixMilli()}
	_, _ = c.put(long, now.UnixMilli())
	_, _ = c.put(short, now.UnixMilli())
	c.expire(now.Add(time.Second).UnixMilli())
	if c.has("short") {
		t.Fatal("short TTL message remained cached")
	}
	if !c.has("long") {
		t.Fatal("long TTL message was incorrectly removed")
	}
	if len(c.items) != 1 || c.order[short.orderIndex] != nil {
		t.Fatalf("expired message retained: items=%d order_slot=%v", len(c.items), c.order[short.orderIndex])
	}
}

func TestExpiredOneShotTopicsReleaseIndexes(t *testing.T) {
	c := newCache(1 << 20)
	now := time.Now()
	for i := 0; i < 1000; i++ {
		m := &Message{ID: string(rune(i + 1)), Topic: "topic-" + string(rune(i+1)), Payload: []byte("x"), ExpiresAt: now.Add(time.Millisecond).UnixMilli()}
		if _, err := c.put(m, now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	c.expire(now.Add(time.Second).UnixMilli())
	if len(c.topics) != 0 || len(c.topicTombstones) != 0 {
		t.Fatalf("dead topic indexes retained: topics=%d tombstones=%d", len(c.topics), len(c.topicTombstones))
	}
}

func TestTopicCompactionClearsDiscardedPointers(t *testing.T) {
	c := newCache(1 << 20)
	now := time.Now()
	for i := 0; i < 600; i++ {
		expires := now.Add(time.Hour)
		if i < 400 {
			expires = now.Add(time.Millisecond)
		}
		m := &Message{ID: string(rune(i + 1)), Topic: "mixed", Payload: []byte("x"), ExpiresAt: expires.UnixMilli()}
		if _, err := c.put(m, now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	original := c.topics["mixed"]
	c.expire(now.Add(time.Second).UnixMilli())
	for i := len(c.topics["mixed"]); i < len(original); i++ {
		if original[i] != nil {
			t.Fatalf("discarded topic pointer retained at %d", i)
		}
	}
	for i, m := range c.topics["mixed"] {
		if m != nil && m.ExpiresAt <= now.Add(time.Second).UnixMilli() {
			t.Fatalf("expired topic pointer retained at %d", i)
		}
	}
}

func TestDeliveryFrameManualJSONRoundTrip(t *testing.T) {
	want := Delivery{DeliveryID: "d\"\n", MessageID: "m", Topic: "topic", Key: "k\\", Headers: map[string]string{"quoted\"": "line\n"}, CreatedAt: 42, Attempt: 3, Payload: []byte{0, 1, 255}}
	var frame bytes.Buffer
	if err := writeFrame(&frame, want); err != nil {
		t.Fatal(err)
	}
	data := frame.Bytes()
	metadataLength := int(binary.BigEndian.Uint32(data[:4]))
	payloadLength := int(binary.BigEndian.Uint32(data[4:8]))
	var got Delivery
	if err := json.Unmarshal(data[8:8+metadataLength], &got); err != nil {
		t.Fatal(err)
	}
	if got.DeliveryID != want.DeliveryID || got.Key != want.Key || got.Headers["quoted\""] != "line\n" || payloadLength != len(want.Payload) || !bytes.Equal(data[8+metadataLength:], want.Payload) {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}

func TestMalformedBatchDoesNotPartiallyCommit(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	var body bytes.Buffer
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], 3)
	body.Write(size[:])
	body.WriteString("one")
	body.Write([]byte{0, 0})
	r := httptest.NewRequest("POST", "/v1/topics/atomic/batches", &body)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatalf("status %d", w.Code)
	}
	if got := len(b.cache.snapshot("atomic", 0)); got != 0 {
		t.Fatalf("partially committed %d messages", got)
	}
}

func TestInvalidAckModeDoesNotPublish(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	r := httptest.NewRequest("POST", "/v1/topics/mode/messages", bytes.NewReader([]byte("x")))
	r.Header.Set("Spruce-Ack", "typo")
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatalf("status %d", w.Code)
	}
	if got := len(b.cache.snapshot("mode", 0)); got != 0 {
		t.Fatalf("published %d messages", got)
	}
}

func TestIdempotencyKeyReturnsSameMessage(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	var ids []string
	for range 2 {
		r := httptest.NewRequest("POST", "/v1/topics/idempotent/messages", bytes.NewReader([]byte("x")))
		r.Header.Set("Spruce-Idempotency-Key", "producer-1/42")
		r.Header.Set("Spruce-Producer-ID", "producer-1")
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, r)
		if w.Code != 202 {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		var result struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, result.ID)
	}
	if ids[0] != ids[1] {
		t.Fatalf("IDs differ: %q != %q", ids[0], ids[1])
	}
	if got := len(b.cache.snapshot("idempotent", 0)); got != 1 {
		t.Fatalf("cached %d messages", got)
	}
}

func TestIdempotencyConflictRejectsDifferentPayload(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	for i, payload := range []string{"first", "different"} {
		r := httptest.NewRequest("POST", "/v1/topics/idempotent/messages", bytes.NewBufferString(payload))
		r.Header.Set("Spruce-Idempotency-Key", "same")
		r.Header.Set("Spruce-Producer-ID", "producer")
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, r)
		want := 202
		if i == 1 {
			want = 409
		}
		if w.Code != want {
			t.Fatalf("attempt %d status %d: %s", i, w.Code, w.Body.String())
		}
	}
}

func TestDeliveryIsPendingBeforeConsumerCanAck(t *testing.T) {
	cfg := DefaultConfig()
	b := New(cfg)
	defer b.Close()
	s := &subscriber{id: "s", topic: "t", ch: make(chan Delivery, 1)}
	m := &Message{ID: "m", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if !b.sendDelivery(s, m, 1) {
		t.Fatal("delivery rejected")
	}
	d := <-s.ch
	b.removeAcks([]string{d.DeliveryID})
	b.mu.RLock()
	_, exists := b.pending[d.DeliveryID]
	b.mu.RUnlock()
	if exists {
		t.Fatal("acknowledged delivery remained pending")
	}
	if len(b.pendingDeadlines) != 0 {
		t.Fatal("acknowledged delivery retained a stale deadline")
	}
}

func TestPendingPoolReturnsScrubbedState(t *testing.T) {
	p := new(pending)
	*p = pending{deliveryID: "delivery", message: &Message{ID: "message", Payload: []byte("secret")}, attempt: 3, group: "group", subscriberID: "subscriber", bytes: 123, next: time.Now(), deliveredAt: time.Now()}
	resetPending(p)
	if p.deliveryID != "" || p.message != nil || p.attempt != 0 || p.group != "" || p.subscriberID != "" || p.bytes != 0 || !p.next.IsZero() || !p.deliveredAt.IsZero() || p.deadline.index != 0 {
		t.Fatalf("reset pending retained state: %+v", p)
	}
}

func TestCacheDigestIsOrderIndependentAndContentSensitive(t *testing.T) {
	now := time.Now()
	left, right := newCache(1<<20), newCache(1<<20)
	a := &Message{ID: "a", Topic: "t", Headers: map[string]string{"b": "2", "a": "1"}, Payload: []byte("one"), CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()}
	b := &Message{ID: "b", Topic: "t", Payload: []byte("two"), CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()}
	_, _ = left.putBatch([]*Message{a, b}, now.UnixMilli())
	_, _ = right.putBatch([]*Message{
		{ID: b.ID, Topic: b.Topic, Payload: append([]byte(nil), b.Payload...), CreatedAt: b.CreatedAt, ExpiresAt: b.ExpiresAt},
		{ID: a.ID, Topic: a.Topic, Headers: map[string]string{"a": "1", "b": "2"}, Payload: append([]byte(nil), a.Payload...), CreatedAt: a.CreatedAt, ExpiresAt: a.ExpiresAt},
	}, now.UnixMilli())
	leftDigest := digestMessages(left.digestSnapshot())
	rightDigest := digestMessages(right.digestSnapshot())
	right.mu.Lock()
	right.items["b"].Payload[0] ^= 0xff
	right.mu.Unlock()
	changedDigest := digestMessages(right.digestSnapshot())
	if leftDigest != rightDigest || changedDigest == rightDigest {
		t.Fatalf("digest mismatch: left=%q right=%q changed=%q", leftDigest, rightDigest, changedDigest)
	}
}

func TestCacheDigestRequiresPeerAuthentication(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken = "peer-token"
	cfg.ClusterID = "cluster-a"
	b := New(cfg)
	defer b.Close()

	unauthorized := httptest.NewRecorder()
	b.Handler().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/internal/cache-digest", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}

	authorizedRequest := httptest.NewRequest(http.MethodGet, "/internal/cache-digest", nil)
	authorizedRequest.Header.Set("Spruce-Peer-Token", cfg.PeerToken)
	authorizedRequest.Header.Set("Spruce-Cluster-ID", cfg.ClusterID)
	authorized := httptest.NewRecorder()
	b.Handler().ServeHTTP(authorized, authorizedRequest)
	if authorized.Code != http.StatusOK || authorized.Body.Len() == 0 {
		t.Fatalf("authorized status=%d body=%q", authorized.Code, authorized.Body.String())
	}

	b.digestSlot <- struct{}{}
	busy := httptest.NewRecorder()
	b.Handler().ServeHTTP(busy, authorizedRequest.Clone(authorizedRequest.Context()))
	<-b.digestSlot
	if busy.Code != http.StatusTooManyRequests {
		t.Fatalf("busy status=%d", busy.Code)
	}
}

func TestPendingDeliverySurvivesRejectedCachePressure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheBytes = 512
	b := New(cfg)
	defer b.Close()
	now := time.Now()
	m := &Message{ID: "pending", Topic: "t", Payload: []byte("original-payload"), ExpiresAt: now.Add(time.Minute).UnixMilli()}
	if inserted, err := b.cache.put(m, now.UnixMilli()); err != nil || !inserted {
		t.Fatalf("cache put: inserted=%v err=%v", inserted, err)
	}
	s := &subscriber{id: "s", topic: "t", ch: make(chan Delivery, 1)}
	if !b.sendDelivery(s, m, 1) {
		t.Fatal("delivery rejected")
	}
	d := <-s.ch
	for i := 0; b.cache.has(m.ID) && i < 20; i++ {
		_, _ = b.cache.put(&Message{ID: "pressure-" + strconv.Itoa(i), Topic: "t", Payload: make([]byte, 128), ExpiresAt: now.Add(time.Minute).UnixMilli()}, now.UnixMilli())
	}
	if !b.cache.has(m.ID) || string(d.Payload) != "original-payload" {
		t.Fatalf("pending payload lost under pressure: cached=%v payload=%q", b.cache.has(m.ID), d.Payload)
	}
	b.removeAcks([]string{d.DeliveryID})
	if len(b.pending) != 0 || len(b.pendingDeadlines) != 0 || b.pendingBytes != 0 {
		t.Fatalf("pending state remained after ACK: pending=%d deadlines=%d bytes=%d", len(b.pending), len(b.pendingDeadlines), b.pendingBytes)
	}
}

func TestPendingTerminalRetryAndNackLifecycle(t *testing.T) {
	t.Run("max attempts", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.AckDeadline = time.Millisecond
		cfg.MaxAttempts = 1
		b := New(cfg)
		defer b.Close()
		s := &subscriber{id: "terminal", topic: "t", ch: make(chan Delivery, 1)}
		m := &Message{ID: "terminal", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
		if !b.sendDelivery(s, m, 1) {
			t.Fatal("delivery rejected")
		}
		<-s.ch
		waitForPendingCount(t, b, 0)
		if len(b.pendingDeadlines) != 0 || b.pendingBytes != 0 || b.metrics.Dropped.Load() == 0 {
			t.Fatalf("terminal retry did not converge: deadlines=%d bytes=%d dropped=%d", len(b.pendingDeadlines), b.pendingBytes, b.metrics.Dropped.Load())
		}
	})
	t.Run("nack then ack", func(t *testing.T) {
		b := New(DefaultConfig())
		defer b.Close()
		s := &subscriber{id: "retry", topic: "t", ch: make(chan Delivery, 2)}
		b.mu.Lock()
		b.addSubscriberLocked(s)
		b.mu.Unlock()
		m := &Message{ID: "retry", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
		if !b.sendDelivery(s, m, 1) {
			t.Fatal("delivery rejected")
		}
		first := <-s.ch
		b.applyNacks([]string{first.DeliveryID})
		select {
		case retried := <-s.ch:
			if retried.Attempt != 2 || retried.MessageID != m.ID {
				t.Fatalf("invalid retry: %+v", retried)
			}
			b.removeAcks([]string{retried.DeliveryID})
		case <-time.After(2 * time.Second):
			t.Fatal("NACK was not redelivered")
		}
		waitForPendingCount(t, b, 0)
		if len(b.pendingDeadlines) != 0 || b.pendingBytes != 0 {
			t.Fatalf("retry ACK did not converge: deadlines=%d bytes=%d", len(b.pendingDeadlines), b.pendingBytes)
		}
	})
}

func waitForPendingCount(t *testing.T, b *Broker, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		got := len(b.pending)
		b.mu.RUnlock()
		if got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pending count did not reach %d", want)
}

func TestExpiryBucketsMixedDeadlineUnlinking(t *testing.T) {
	c := newCache(1 << 20)
	now := time.Now()
	messages := []*Message{
		{ID: "a1", Topic: "t", Payload: []byte("a"), ExpiresAt: now.Add(time.Second).UnixMilli()},
		{ID: "b1", Topic: "t", Payload: []byte("b"), ExpiresAt: now.Add(2 * time.Second).UnixMilli()},
		{ID: "a2", Topic: "t", Payload: []byte("a"), ExpiresAt: now.Add(time.Second).UnixMilli()},
		{ID: "c1", Topic: "t", Payload: []byte("c"), ExpiresAt: now.Add(3 * time.Second).UnixMilli()},
		{ID: "a3", Topic: "t", Payload: []byte("a"), ExpiresAt: now.Add(time.Second).UnixMilli()},
	}
	if _, err := c.putBatch(messages, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	assertCacheExpiryInvariant(t, c)
	c.mu.Lock()
	c.removeLocked(messages[2], true)
	c.removeLocked(messages[4], true)
	c.mu.Unlock()
	assertCacheExpiryInvariant(t, c)
	c.expire(now.Add(1500 * time.Millisecond).UnixMilli())
	assertCacheExpiryInvariant(t, c)
	if c.has("a1") || !c.has("b1") || !c.has("c1") {
		t.Fatal("mixed deadline expiration removed the wrong messages")
	}
}

func TestFailedAcceptanceDoesNotPoisonIdempotency(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheBytes = 100
	b := New(cfg)
	defer b.Close()
	for range 2 {
		r := httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("payload")))
		r.Header.Set("Spruce-Idempotency-Key", "key")
		r.Header.Set("Spruce-Producer-ID", "producer")
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, r)
		if w.Code != 507 {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
	}
}

func TestIdempotentOnePeerRetryRetriesReplication(t *testing.T) {
	var attempts atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/capabilities" {
			w.Header().Set("Spruce-Peer-Version", "2")
			w.WriteHeader(204)
			return
		}
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer peer.Close()
	cfg := DefaultConfig()
	cfg.Peers = []string{peer.URL}
	cfg.PeerToken = "peer-token"
	cfg.ClusterID = "test-cluster"
	b := New(cfg)
	defer b.Close()
	for i, want := range []int{503, 202} {
		r := httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("payload")))
		r.Header.Set("Spruce-Idempotency-Key", "key")
		r.Header.Set("Spruce-Producer-ID", "producer")
		r.Header.Set("Spruce-Ack", "one-peer")
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("attempt %d status %d: %s", i, w.Code, w.Body.String())
		}
	}
	if attempts.Load() < 2 {
		t.Fatal("replication was not retried")
	}
	if len(b.cache.snapshot("t", 0)) != 1 {
		t.Fatal("retry duplicated local message")
	}
}

func TestRetryTargetsOnlyItsConsumerGroup(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	broadcast := &subscriber{id: "broadcast", topic: "t", ch: make(chan Delivery, 1)}
	g1 := &subscriber{id: "g1", topic: "t", group: "one", ch: make(chan Delivery, 1)}
	g2 := &subscriber{id: "g2", topic: "t", group: "two", ch: make(chan Delivery, 1)}
	b.addSubscriberLocked(broadcast)
	b.addSubscriberLocked(g1)
	b.addSubscriberLocked(g2)
	m := &Message{ID: "m", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	b.deliver(m, "one", 2)
	if len(g1.ch) != 1 || len(g2.ch) != 0 || len(broadcast.ch) != 0 {
		t.Fatalf("retry leaked: broadcast=%d g1=%d g2=%d", len(broadcast.ch), len(g1.ch), len(g2.ch))
	}
}

func TestGroupReplayArbitratesOneMember(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	a := &subscriber{id: "a", topic: "t", group: "workers", ch: make(chan Delivery, 1)}
	c := &subscriber{id: "b", topic: "t", group: "workers", ch: make(chan Delivery, 1)}
	b.mu.Lock()
	b.addSubscriberLocked(a)
	b.addSubscriberLocked(c)
	b.mu.Unlock()
	m := &Message{ID: "replay", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	b.deliver(m, "workers", 1)
	if len(a.ch)+len(c.ch) != 1 {
		t.Fatalf("group replay fan-out: a=%d b=%d", len(a.ch), len(c.ch))
	}
}

func TestGroupCheckpointSuppressesCompletedMessage(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	expiresAt := time.Now().Add(time.Minute).UnixMilli()
	m := &Message{ID: "completed", Topic: "t", Payload: []byte("x"), ExpiresAt: expiresAt}
	first := &subscriber{id: "first", topic: "t", group: "workers", ch: make(chan Delivery, 1)}
	b.mu.Lock()
	b.addSubscriberLocked(first)
	b.mu.Unlock()
	if !b.sendDelivery(first, m, 1) {
		t.Fatal("initial delivery rejected")
	}
	delivery := <-first.ch
	b.removeAcks([]string{delivery.DeliveryID})
	b.mu.Lock()
	b.removeSubscriberLocked(first)
	reconnected := &subscriber{id: "reconnected", topic: "t", group: "workers", ch: make(chan Delivery, 1)}
	otherGroup := &subscriber{id: "other", topic: "t", group: "other", ch: make(chan Delivery, 1)}
	b.addSubscriberLocked(reconnected)
	b.addSubscriberLocked(otherGroup)
	b.mu.Unlock()
	b.deliver(m, "", 1)
	if len(reconnected.ch) != 0 {
		t.Fatal("completed message was redelivered to the same group")
	}
	if len(otherGroup.ch) != 1 {
		t.Fatal("checkpoint leaked into an unrelated group")
	}
}

func TestGroupCheckpointBoundAndExpiry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CheckpointEntries = 2
	b := New(cfg)
	defer b.Close()
	now := time.Now()
	b.applyCheckpoints([]groupCheckpoint{
		{Topic: "t", Group: "g", MessageID: "one", ExpiresAt: now.Add(time.Minute).UnixMilli()},
		{Topic: "t", Group: "g", MessageID: "two", ExpiresAt: now.Add(time.Minute).UnixMilli()},
		{Topic: "t", Group: "g", MessageID: "three", ExpiresAt: now.Add(time.Minute).UnixMilli()},
		{Topic: "t", Group: "g", MessageID: "expired", ExpiresAt: now.Add(-time.Second).UnixMilli()},
	})
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.checkpointCount != 2 {
		t.Fatalf("checkpoint count %d exceeds configured bound", b.checkpointCount)
	}
	if b.checkpointActiveLocked("t", "g", "one", now.UnixMilli()) {
		t.Fatal("oldest checkpoint was not evicted")
	}
	if !b.checkpointActiveLocked("t", "g", "two", now.UnixMilli()) || !b.checkpointActiveLocked("t", "g", "three", now.UnixMilli()) {
		t.Fatal("live checkpoints were not retained")
	}
	if b.checkpointActiveLocked("t", "g", "expired", now.UnixMilli()) {
		t.Fatal("expired checkpoint remained active")
	}
}

func TestCheckpointBootstrapFromPeer(t *testing.T) {
	peerCfg := DefaultConfig()
	peerCfg.PeerToken, peerCfg.ClusterID = "peer", "cluster"
	peer := New(peerCfg)
	defer peer.Close()
	peer.applyCheckpoints([]groupCheckpoint{{Topic: "t", Group: "workers", MessageID: "completed", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}})
	server := httptest.NewServer(peer.Handler())
	defer server.Close()
	cfg := DefaultConfig()
	cfg.Peers, cfg.PeerToken, cfg.ClusterID = []string{server.URL}, "peer", "cluster"
	b := New(cfg)
	defer b.Close()
	if err := b.SyncFromPeers(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.mu.RLock()
	active := b.checkpointActiveLocked("t", "workers", "completed", time.Now().UnixMilli())
	b.mu.RUnlock()
	if !active {
		t.Fatal("joining replica did not bootstrap the group checkpoint")
	}
}

func TestInflightByteLimitRejectsDelivery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxInflightBytes = 1
	b := New(cfg)
	defer b.Close()
	s := &subscriber{id: "s", topic: "t", ch: make(chan Delivery, 1)}
	m := &Message{ID: "m", Topic: "t", Payload: []byte("large"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if b.sendDelivery(s, m, 1) {
		t.Fatal("delivery exceeded inflight byte limit")
	}
	if len(b.pending) != 0 {
		t.Fatal("rejected delivery entered pending state")
	}
}

func TestFailedChannelAdmissionRemovesDeadline(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := &subscriber{id: "slow", topic: "t", ch: make(chan Delivery, 1)}
	m := &Message{ID: "m", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if !b.sendDelivery(s, m, 1) {
		t.Fatal("first delivery rejected")
	}
	for range 1000 {
		if b.sendDelivery(s, m, 1) {
			t.Fatal("delivery entered a full subscriber channel")
		}
	}
	if len(b.pendingDeadlines) != len(b.pending) || len(b.pending) != 1 {
		t.Fatalf("deadline heap=%d pending=%d", len(b.pendingDeadlines), len(b.pending))
	}
}

func TestSubscriberInflightLimitIsIndependent(t *testing.T) {
	cfg := DefaultConfig()
	m := &Message{ID: "m", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	cfg.MaxSubscriberInflightBytes = messageSize(m)
	cfg.MaxInflightBytes = cfg.MaxSubscriberInflightBytes * 4
	b := New(cfg)
	defer b.Close()
	slow := &subscriber{id: "slow", topic: "t", ch: make(chan Delivery, 2)}
	healthy := &subscriber{id: "healthy", topic: "t", ch: make(chan Delivery, 2)}
	if !b.sendDelivery(slow, m, 1) || b.sendDelivery(slow, m, 1) {
		t.Fatal("subscriber byte limit was not enforced")
	}
	if !b.sendDelivery(healthy, m, 1) {
		t.Fatal("slow subscriber consumed healthy subscriber budget")
	}
}

func TestPeerActionQueueIsByteBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ActionQueueBytes = 40
	b := New(cfg)
	defer b.Close()
	p := &peer{acks: make(chan actionBatch, 2), nacks: make(chan actionBatch, 2)}
	b.peers = []*peer{p}
	request := ackRequest{DeliveryIDs: []string{"1234"}}
	if !b.broadcastAction(context.Background(), "ack", request) {
		t.Fatal("first action was rejected")
	}
	if b.broadcastAction(context.Background(), "ack", request) {
		t.Fatal("action exceeded queue byte limit")
	}
	if got := p.actionBytes.Load(); got != 40 {
		t.Fatalf("queued bytes=%d", got)
	}
}

func TestDeliveryIndexExcludesOtherTopics(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	wanted := &subscriber{id: "wanted", topic: "wanted", ch: make(chan Delivery, 1)}
	other := &subscriber{id: "other", topic: "other", ch: make(chan Delivery, 1)}
	b.addSubscriberLocked(wanted)
	b.addSubscriberLocked(other)
	b.deliver(&Message{ID: "m", Topic: "wanted", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}, "", 1)
	if len(wanted.ch) != 1 || len(other.ch) != 0 {
		t.Fatalf("wanted=%d other=%d", len(wanted.ch), len(other.ch))
	}
}

func TestGroupQueuesBehindSaturatedPreferredMember(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	a := &subscriber{id: "a", topic: "t", group: "\x00", ch: make(chan Delivery, 1)}
	c := &subscriber{id: "c", topic: "t", group: "\x00", ch: make(chan Delivery, 1)}
	b.addSubscriberLocked(a)
	b.addSubscriberLocked(c)
	m := &Message{ID: "message", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	preferred, other := a, c
	if hashValue(m.Topic, a.group, deliveryAffinity(m), c.id) > hashValue(m.Topic, a.group, deliveryAffinity(m), a.id) {
		preferred, other = c, a
	}
	preferred.ch <- Delivery{}
	b.deliver(m, "\x00", 1)
	if len(other.ch) != 0 {
		t.Fatal("queued key changed ownership under channel pressure")
	}
}

func TestConsumerGroupKeepsKeyAffinity(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	a := &subscriber{id: "consumer-a", topic: "events", group: "workers", ch: make(chan Delivery, 32)}
	c := &subscriber{id: "consumer-b", topic: "events", group: "workers", ch: make(chan Delivery, 32)}
	b.mu.Lock()
	b.addSubscriberLocked(a)
	b.addSubscriberLocked(c)
	b.mu.Unlock()

	owners := make(map[string]string)
	for i := range 24 {
		key := []string{"workspace-a", "workspace-b", "workspace-c"}[i%3]
		m := &Message{ID: fmt.Sprintf("message-%d", i), Topic: "events", Key: key, Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
		b.deliver(m, "workers", 1)
		select {
		case d := <-a.ch:
			if previous := owners[d.Key]; previous != "" && previous != a.id {
				t.Fatalf("key %q moved from %s to %s", d.Key, previous, a.id)
			}
			owners[d.Key] = a.id
			b.removeAcks([]string{d.DeliveryID})
		case d := <-c.ch:
			if previous := owners[d.Key]; previous != "" && previous != c.id {
				t.Fatalf("key %q moved from %s to %s", d.Key, previous, c.id)
			}
			owners[d.Key] = c.id
			b.removeAcks([]string{d.DeliveryID})
		default:
			t.Fatalf("message %d was not delivered", i)
		}
	}
}

func TestConsumerGroupKeyAffinitySurvivesRedelivery(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	a := &subscriber{id: "consumer-a", topic: "events", group: "workers", ch: make(chan Delivery, 2)}
	c := &subscriber{id: "consumer-b", topic: "events", group: "workers", ch: make(chan Delivery, 2)}
	b.mu.Lock()
	b.addSubscriberLocked(a)
	b.addSubscriberLocked(c)
	b.mu.Unlock()
	m := &Message{ID: "message", Topic: "events", Key: "workspace", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if _, err := b.accept(m); err != nil {
		t.Fatal(err)
	}
	owner := a
	if len(c.ch) == 1 {
		owner = c
	}
	first := <-owner.ch
	b.applyNacks([]string{first.DeliveryID})
	select {
	case retry := <-owner.ch:
		if retry.Attempt != 2 {
			t.Fatal("redelivery attempt was not incremented")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("redelivery moved away from the key owner")
	}
}

func TestConsumerGroupAffinitySurvivesStreamReconnect(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	a := &subscriber{id: "stream-a-1", affinityID: "member-a", topic: "events", group: "workers", ch: make(chan Delivery, 2), cancel: func() {}}
	c := &subscriber{id: "stream-b", affinityID: "member-b", topic: "events", group: "workers", ch: make(chan Delivery, 2), cancel: func() {}}
	b.mu.Lock()
	b.addSubscriberLocked(a)
	b.addSubscriberLocked(c)
	b.mu.Unlock()
	first := &Message{ID: "first", Topic: "events", Key: "workspace", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	b.deliver(first, "workers", 1)
	owner, other := a, c
	if len(c.ch) == 1 {
		owner, other = c, a
	}
	delivery := <-owner.ch
	b.removeAcks([]string{delivery.DeliveryID})
	reconnected := &subscriber{id: owner.id + "-replacement", affinityID: owner.affinityID, topic: owner.topic, group: owner.group, ch: make(chan Delivery, 2), cancel: func() {}}
	b.mu.Lock()
	b.fenceAffinityMemberLocked(reconnected)
	b.addSubscriberLocked(reconnected)
	b.mu.Unlock()
	if !owner.detached {
		t.Fatal("replacement did not fence the prior stream")
	}
	b.deliver(&Message{ID: "second", Topic: "events", Key: "workspace", Payload: []byte("x"), ExpiresAt: first.ExpiresAt}, "workers", 1)
	if len(reconnected.ch) != 1 || len(other.ch) != 0 {
		t.Fatalf("reconnected=%d other=%d", len(reconnected.ch), len(other.ch))
	}
}

func TestSubscriptionRejectsOversizedMemberIdentity(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	r := httptest.NewRequest(http.MethodGet, "/v1/subscriptions/stream?topic=t&member="+strings.Repeat("x", 256), nil)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "invalid_member") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestInvalidSinceIsRejected(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	r := httptest.NewRequest("GET", "/v1/subscriptions/stream?topic=t&since=garbage", nil)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatalf("status %d", w.Code)
	}
}

func TestLegacyTimestampSinceReplaysCleanTopic(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	now := time.Now()
	if _, err := b.accept(&Message{ID: "legacy", Topic: "t", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(b.Handler())
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.URL+"/v1/subscriptions/stream?topic=t&since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var sizes [8]byte
	_, err = io.ReadFull(resp.Body, sizes[:])
	var d Delivery
	if err == nil {
		meta := make([]byte, binary.BigEndian.Uint32(sizes[:4]))
		_, err = io.ReadFull(resp.Body, meta)
		if err == nil {
			err = json.Unmarshal(meta, &d)
		}
	}
	if err != nil || d.MessageID != "legacy" {
		t.Fatalf("delivery=%+v err=%v", d, err)
	}
}

func TestLegacyTimestampSinceFailsClosedAfterTopicLoss(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheBytes = messageSize(&Message{ID: "loss-0", Topic: "t", Payload: make([]byte, 100)}) * 2
	b := New(cfg)
	defer b.Close()
	now := time.Now()
	for i := range 4 {
		_, _ = b.accept(&Message{ID: fmt.Sprintf("loss-%d", i), Topic: "t", Payload: make([]byte, 100), CreatedAt: now.Add(time.Duration(i) * time.Millisecond).UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()})
	}
	b.cache.mu.Lock()
	b.cache.dropOldestLocked()
	b.cache.mu.Unlock() // Inject history loss independently of pressure admission.
	r := httptest.NewRequest(http.MethodGet, "/v1/subscriptions/stream?topic=t&since=1", nil)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "cursor_expired") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestOpaqueCursorPathRemainsAvailableOnCleanTopic(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest(http.MethodGet, "/v1/subscriptions/stream?topic=t&cursor="+url.QueryEscape(encodeReplayCursor(map[string]uint64{})), nil).WithContext(ctx)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestClientTokenProtectsPublicAPI(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ClientToken = "secret"
	b := New(cfg)
	defer b.Close()
	r := httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("x")))
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("status %d", w.Code)
	}
	r = httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("x")))
	r.Header.Set("Authorization", "secret")
	w = httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("raw token status %d", w.Code)
	}
	r = httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("x")))
	r.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 202 {
		t.Fatalf("authorized status %d", w.Code)
	}
}

func TestBasicAuthenticationProtectsPublicAPI(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BasicUsername, cfg.BasicPassword = "badboy", "secret"
	b := New(cfg)
	defer b.Close()
	r := httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("x")))
	r.SetBasicAuth("badboy", "secret")
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("x")))
	r.SetBasicAuth("badboy", "wrong")
	w = httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized || w.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("status=%d challenge=%q", w.Code, w.Header().Get("WWW-Authenticate"))
	}
}

func TestHealthBypassesPublicAdmissionLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConcurrentRequests = 1
	b := New(cfg)
	defer b.Close()
	b.requestSlots <- struct{}{}
	defer func() { <-b.requestSlots }()
	r := httptest.NewRequest("GET", "/health/live", nil)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatalf("health status %d", w.Code)
	}
}

func TestPublishAdmissionRejectsWithRetryHint(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PublishAdmissionWait = time.Millisecond
	b := New(cfg)
	defer b.Close()
	b.publishAdmissionBytes.Store(cfg.PublishAdmissionBytes)
	r := httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("x")))
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d retry-after=%q body=%s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
}

func TestPublishAdmissionReleasesBudget(t *testing.T) {
	cfg := DefaultConfig()
	b := New(cfg)
	defer b.Close()
	r := httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("payload")))
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := b.publishAdmissionBytes.Load(); got != 0 {
		t.Fatalf("admission bytes=%d", got)
	}
}

func TestReplicationPressureRejectsBeforeLocalAcceptance(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PublishAdmissionWait = time.Millisecond
	cfg.ReplicationQueueBytes = maxBatchBytes
	b := New(cfg)
	defer b.Close()
	blocked := &peer{ch: make(chan []*Message, 1)}
	blocked.queuedBytes.Store(b.replicationHighWaterBytes())
	b.peers = []*peer{blocked}
	r := httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("payload")))
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), "replication_overloaded") {
		t.Fatalf("status=%d retry-after=%q body=%s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
	if got := b.metrics.Published.Load(); got != 0 {
		t.Fatalf("published=%d", got)
	}
	if got := b.metrics.ReplicationPressureRejected.Load(); got != 1 {
		t.Fatalf("replication pressure rejected=%d", got)
	}
}

func TestReplicationPressureAllowsHealthyPeer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicationQueueBytes = maxBatchBytes
	b := New(cfg)
	defer b.Close()
	blocked := &peer{ch: make(chan []*Message, 1)}
	blocked.queuedBytes.Store(b.replicationHighWaterBytes())
	healthy := &peer{ch: make(chan []*Message, 1)}
	b.peers = []*peer{blocked, healthy}
	r := httptest.NewRequest("POST", "/v1/topics/t/messages", bytes.NewReader([]byte("payload")))
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	select {
	case messages := <-healthy.ch:
		if len(messages) != 1 || string(messages[0].Payload) != "payload" {
			t.Fatalf("replicated messages=%v", messages)
		}
	default:
		t.Fatal("healthy peer did not receive replication")
	}
}

func TestStreamRejectsCursorBehindEvictionWatermark(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheBytes = messageSize(&Message{ID: "second", Topic: "t", Payload: make([]byte, 100)})
	b := New(cfg)
	defer b.Close()
	now := time.Now()
	first := &Message{ID: "first", Topic: "t", Payload: make([]byte, 100), CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli(), Origin: "origin", Sequence: 1}
	second := &Message{ID: "second", Topic: "t", Payload: make([]byte, 100), CreatedAt: now.Add(time.Millisecond).UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli(), Origin: "origin", Sequence: 2}
	if _, err := b.cache.put(first, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	b.cache.mu.Lock()
	b.cache.dropOldestLocked()
	b.cache.mu.Unlock() // Inject a lost retained copy.
	if _, err := b.cache.put(second, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/v1/subscriptions/stream?topic=t&cursor="+url.QueryEscape(encodeReplayCursor(map[string]uint64{"origin": 0})), nil)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "cursor_expired") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := b.metrics.CursorExpired.Load(); got != 1 {
		t.Fatalf("cursor expired metric=%d", got)
	}
}

func TestCacheRejectsAggregateOversizedBatch(t *testing.T) {
	c := newCache(180)
	messages := []*Message{
		{ID: "one", Topic: "t", Payload: make([]byte, 80), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()},
		{ID: "two", Topic: "t", Payload: make([]byte, 80), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()},
	}
	if _, err := c.putBatch(messages, time.Now().UnixMilli()); err == nil {
		t.Fatal("aggregate oversized batch was accepted")
	}
	if c.has("one") || c.has("two") {
		t.Fatal("rejected batch partially mutated the cache")
	}
}

func TestOpaqueReplayCursorIgnoresWallClockOrdering(t *testing.T) {
	c := newCache(1 << 20)
	now := time.Now()
	messages := []*Message{
		{ID: "first", Topic: "t", Origin: "a", Sequence: 1, CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()},
		{ID: "equal-time", Topic: "t", Origin: "a", Sequence: 2, CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()},
		{ID: "late-skewed-origin", Topic: "t", Origin: "b", Sequence: 1, CreatedAt: now.Add(-time.Hour).UnixMilli(), ExpiresAt: now.Add(time.Minute).UnixMilli()},
	}
	if _, err := c.putBatch(messages, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	replay := c.replayLocked("t", map[string]uint64{"a": 1})
	c.mu.Unlock()
	if len(replay) != 2 || replay[0].ID != "equal-time" || replay[1].ID != "late-skewed-origin" {
		t.Fatalf("unexpected replay: %#v", replay)
	}
}

func TestOpaqueReplayCursorRoundTripAndPressureFrontier(t *testing.T) {
	cursor := map[string]uint64{"origin-a": 7, "origin-b": 3}
	decoded, err := decodeReplayCursor(encodeReplayCursor(cursor))
	if err != nil || decoded["origin-a"] != 7 || decoded["origin-b"] != 3 {
		t.Fatalf("decoded=%v err=%v", decoded, err)
	}
	frontier := &topicFrontier{Sequences: map[string]uint64{"origin-a": 8}}
	if !frontierBehind(decoded, frontier) {
		t.Fatal("cursor behind pressure frontier was accepted")
	}
	decoded["origin-a"] = 8
	if frontierBehind(decoded, frontier) {
		t.Fatal("cursor at pressure frontier was rejected")
	}
}

func TestConcurrentAcceptanceSequencesMatchDeliveryOrder(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := &subscriber{id: "s", topic: "t", ch: make(chan Delivery, 128)}
	b.mu.Lock()
	b.addSubscriberLocked(s)
	b.mu.Unlock()
	var started sync.WaitGroup
	started.Add(64)
	for i := range 64 {
		go func(i int) {
			defer started.Done()
			_, _ = b.accept(&Message{ID: fmt.Sprintf("m-%d", i), Topic: "t", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()})
		}(i)
	}
	started.Wait()
	var previous uint64
	for range 64 {
		d := <-s.ch
		if d.sequence != previous+1 {
			t.Fatalf("delivery sequence jumped from %d to %d", previous, d.sequence)
		}
		previous = d.sequence
	}
}

func TestReplayingConsumerGroupDefersToOneOwner(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	a := &subscriber{id: "a", topic: "t", group: "g", replaying: true, ch: make(chan Delivery, 1)}
	c := &subscriber{id: "c", topic: "t", group: "g", replaying: true, ch: make(chan Delivery, 1)}
	b.mu.Lock()
	b.addSubscriberLocked(a)
	b.addSubscriberLocked(c)
	b.mu.Unlock()
	b.deliver(&Message{ID: "m", Topic: "t", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}, "", 1)
	b.mu.RLock()
	count := len(b.groupWork[checkpointScope{topic: "t", group: "g"}].work)
	b.mu.RUnlock()
	if count != 1 {
		t.Fatalf("group replay fanout: a=%d c=%d", len(a.deferred), len(c.deferred))
	}
}

func TestCursorAndLegacyPeerBounds(t *testing.T) {
	tooMany := make(map[string]uint64, 257)
	for i := range 257 {
		tooMany[fmt.Sprint(i)] = 1
	}
	if encodeReplayCursor(tooMany) != "" {
		t.Fatal("oversized origin vector encoded")
	}
	m := &Message{ID: "m", Topic: "t", Payload: []byte("opaque"), CreatedAt: 1, ExpiresAt: 2, Origin: "ignored", Sequence: 9}
	var body bytes.Buffer
	if err := writePeerBatchV1(&body, []*Message{m}); err != nil {
		t.Fatal(err)
	}
	decoded, err := readPeerBatchV1(&body, 1<<20)
	if err != nil || len(decoded) != 1 || decoded[0].Origin != "" || string(decoded[0].Payload) != "opaque" {
		t.Fatalf("decoded=%v err=%v", decoded, err)
	}
}

func TestReplicationHoldsSequenceGapUntilContiguous(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := &subscriber{id: "s", topic: "t", ch: make(chan Delivery, 2)}
	b.mu.Lock()
	b.addSubscriberLocked(s)
	b.mu.Unlock()
	expires := time.Now().Add(time.Minute).UnixMilli()
	second := &Message{ID: "second", Topic: "t", Origin: "origin.t", Sequence: 2, ExpiresAt: expires}
	if err := b.acceptReplicatedBatch([]*Message{second}); err != nil {
		t.Fatal(err)
	}
	if len(s.ch) != 0 || b.cache.has(second.ID) {
		t.Fatal("out-of-order message was exposed")
	}
	b.cache.mu.Lock()
	if !b.cache.topicUnsafeLocked("t", time.Now().UnixMilli()) {
		t.Fatal("gap topic was not rejected")
	}
	if b.cache.topicUnsafeLocked("unrelated", time.Now().UnixMilli()) {
		t.Fatal("gap contaminated unrelated topic")
	}
	b.cache.mu.Unlock()
	first := &Message{ID: "first", Topic: "t", Origin: "origin.t", Sequence: 1, ExpiresAt: expires}
	if err := b.acceptReplicatedBatch([]*Message{first}); err != nil {
		t.Fatal(err)
	}
	for want := uint64(1); want <= 2; want++ {
		if got := (<-s.ch).sequence; got != want {
			t.Fatalf("sequence=%d want=%d", got, want)
		}
	}
	b.cache.mu.Lock()
	if b.cache.topicUnsafeLocked("t", time.Now().UnixMilli()) {
		t.Fatal("resolved gap remained unsafe")
	}
	b.cache.mu.Unlock()
}

func TestReplicationGapOverflowRemainsTopicScopedUnsafe(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	b.cache.reorderLimit = 0
	m := &Message{ID: "overflow", Topic: "a", Origin: "origin.a", Sequence: 2, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if err := b.acceptReplicatedBatch([]*Message{m}); !errors.Is(err, errRetentionCapacity) {
		t.Fatalf("overflow acknowledged: %v", err)
	}
	b.cache.mu.Lock()
	defer b.cache.mu.Unlock()
	if !b.cache.topicUnsafeLocked("a", time.Now().UnixMilli()) {
		t.Fatal("overflow topic was not rejected")
	}
	if b.cache.topicUnsafeLocked("b", time.Now().UnixMilli()) {
		t.Fatal("overflow contaminated unrelated topic")
	}
}

func TestReplayFrontierAndGapStateExpire(t *testing.T) {
	c := newCache(1 << 20)
	now := time.Now().UnixMilli()
	c.frontiers["old"] = &topicFrontier{Sequences: map[string]uint64{"o": 1}, ExpiresAt: now - 1}
	c.reorder["o"] = map[uint64]*Message{2: {ID: "gap", Topic: "old", Origin: "o", Sequence: 2, ExpiresAt: now - 1}}
	c.reorderBytes = messageSize(c.reorder["o"][2])
	c.expire(now)
	if len(c.frontiers) != 0 || len(c.reorder) != 0 || c.reorderBytes != 0 {
		t.Fatalf("frontiers=%d reorder=%d bytes=%d", len(c.frontiers), len(c.reorder), c.reorderBytes)
	}
}

func TestLegacyFrontierEndpointFailsClosedAndV2Reprobes(t *testing.T) {
	legacy := httptest.NewServer(http.NotFoundHandler())
	defer legacy.Close()
	cfg := DefaultConfig()
	cfg.PeerToken = "peer"
	cfg.ClusterID = "cluster"
	b := New(cfg)
	defer b.Close()
	p := &peer{url: legacy.URL}
	before := time.Now().Add(cfg.MaxTTL - time.Second).UnixMilli()
	if err := b.syncReplayFrontiersFromPeer(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if entries := b.cache.unsafe["*"]; entries == nil || entries["legacy-frontier:"+legacy.URL] < before {
		t.Fatal("legacy peer did not fail replay closed")
	}
	v2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Spruce-Peer-Version", "2")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer v2.Close()
	p.url = v2.URL
	p.lastV2Probe.Store(0)
	b.probePeerV2(context.Background(), p)
	if !p.v2.Load() {
		t.Fatal("v2 capability was not renegotiated")
	}
}

func TestRejectsMaxMessageAboveSnapshotLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMessage = 23<<20 + 1
	defer func() {
		if recover() == nil {
			t.Fatal("oversized snapshot configuration was accepted")
		}
	}()
	_ = New(cfg)
}

func TestDeliveryLagAdmissionIsTopicScopedAndClears(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeliveryLagLimit = 10 * time.Millisecond
	b := New(cfg)
	defer b.Close()

	p := acquirePending()
	*p = pending{deliveryID: "lagged", message: &Message{ID: "message", Topic: "lagged"}, deliveredAt: time.Now().Add(-time.Second)}
	b.mu.Lock()
	b.pending[p.deliveryID] = p
	b.linkTopicPendingLocked(p)
	b.mu.Unlock()

	publish := func(topic string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/topics/"+topic+"/messages", bytes.NewReader([]byte("payload")))
		req.SetPathValue("topic", topic)
		response := httptest.NewRecorder()
		b.publish(response, req)
		return response
	}
	if response := publish("lagged"); response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "1" {
		t.Fatalf("lagged topic status=%d retry-after=%q", response.Code, response.Header().Get("Retry-After"))
	}
	if response := publish("healthy"); response.Code != http.StatusAccepted {
		t.Fatalf("healthy topic status=%d body=%s", response.Code, response.Body.String())
	}
	b.mu.Lock()
	delete(b.pending, p.deliveryID)
	b.unlinkTopicPendingLocked(p)
	b.mu.Unlock()
	releasePending(p)
	if response := publish("lagged"); response.Code != http.StatusAccepted {
		t.Fatalf("recovered topic status=%d body=%s", response.Code, response.Body.String())
	}
	if got := b.metrics.DeliveryPressureRejected.Load(); got != 1 {
		t.Fatalf("delivery pressure rejected=%d", got)
	}
}

func FuzzReadPeerBatch(f *testing.F) {
	var seed bytes.Buffer
	_ = writePeerBatch(&seed, []*Message{{ID: "id", Topic: "t", Payload: []byte{0, 1, 255}, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}})
	f.Add(seed.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = readPeerBatch(bytes.NewReader(data), 1<<20) })
}
