package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheEvictsOldestWithinBound(t *testing.T) {
	c := newCache(300)
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
	m := &Message{ID: "same", Topic: "test", Payload: []byte("hi"), CreatedAt: time.Now().UnixMilli(), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	var body bytes.Buffer
	if err := writePeerBatch(&body, []*Message{m}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		r := httptest.NewRequest("POST", "/internal/replicate", bytes.NewReader(body.Bytes()))
		r.Header.Set("Spruce-Peer-Token", cfg.PeerToken)
		r.Header.Set("Spruce-Cluster-ID", cfg.ClusterID)
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

func BenchmarkPublishBatch512(b *testing.B) {
	cfg := DefaultConfig()
	cfg.CacheBytes = 64 << 20
	broker := New(cfg)
	defer broker.Close()
	var encoded bytes.Buffer
	payload := make([]byte, 256)
	for range 512 {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
		encoded.Write(size[:])
		encoded.Write(payload)
	}
	body := encoded.Bytes()
	b.SetBytes(int64(len(payload) * 512))
	b.ReportMetric(512, "messages/op")
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
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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

func TestGroupFallsBackFromSaturatedPreferredMember(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	a := &subscriber{id: "a", topic: "t", group: "\x00", ch: make(chan Delivery, 1)}
	c := &subscriber{id: "c", topic: "t", group: "\x00", ch: make(chan Delivery, 1)}
	b.addSubscriberLocked(a)
	b.addSubscriberLocked(c)
	m := &Message{ID: "message", Topic: "t", Payload: []byte("x"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	preferred, other := a, c
	if hashValue(m.ID, c.id) > hashValue(m.ID, a.id) {
		preferred, other = c, a
	}
	preferred.ch <- Delivery{}
	b.deliver(m, "\x00", 1)
	if len(other.ch) != 1 {
		t.Fatal("healthy group member did not receive fallback delivery")
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

func TestRejectsMaxMessageAboveSnapshotLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMessage = 23<<20 + 1
	defer func() {
		if recover() == nil { t.Fatal("oversized snapshot configuration was accepted") }
	}()
	_ = New(cfg)
}

func FuzzReadPeerBatch(f *testing.F) {
	var seed bytes.Buffer
	_ = writePeerBatch(&seed, []*Message{{ID: "id", Topic: "t", Payload: []byte{0, 1, 255}, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}})
	f.Add(seed.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = readPeerBatch(bytes.NewReader(data), 1<<20) })
}
