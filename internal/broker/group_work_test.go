package broker

import (
	"bytes"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func workSubscriber(b *Broker, id string) *subscriber {
	s := &subscriber{id: id, affinityID: id, topic: "t", group: "g", ch: make(chan Delivery, 32), cancel: func() {}}
	b.mu.Lock()
	b.addSubscriberLocked(s)
	b.mu.Unlock()
	return s
}
func acceptWork(t *testing.T, b *Broker, id, key string, expiry int64) {
	t.Helper()
	if _, err := b.accept(&Message{ID: id, Topic: "t", Key: key, Payload: []byte(id), ExpiresAt: expiry}); err != nil {
		t.Fatal(err)
	}
}
func readWork(t *testing.T, s *subscriber) Delivery {
	t.Helper()
	select {
	case d := <-s.ch:
		return d
	case <-time.After(3 * time.Second):
		t.Fatal("delivery deadline")
		return Delivery{}
	}
}
func TestGroupCompletionGatesOnlyItsKey(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := workSubscriber(b, "a")
	expiry := time.Now().Add(time.Minute).UnixMilli()
	acceptWork(t, b, "first", "key", expiry)
	first := readWork(t, s)
	acceptWork(t, b, "second", "key", expiry)
	acceptWork(t, b, "independent", "other", expiry)
	if d := readWork(t, s); d.MessageID != "independent" {
		t.Fatalf("unfinished key was overtaken: %s", d.MessageID)
	}
	select {
	case d := <-s.ch:
		t.Fatalf("second escaped before ACK: %+v", d)
	case <-time.After(40 * time.Millisecond):
	}
	b.removeAcks([]string{first.DeliveryID})
	if d := readWork(t, s); d.MessageID != "second" {
		t.Fatalf("wrong next event: %+v", d)
	}
}
func TestGroupHeadSurvivesRetryExhaustionAndNack(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxAttempts = 1
	cfg.AckDeadline = 10 * time.Millisecond
	b := New(cfg)
	defer b.Close()
	s := workSubscriber(b, "a")
	expiry := time.Now().Add(time.Minute).UnixMilli()
	acceptWork(t, b, "first", "key", expiry)
	first := readWork(t, s)
	acceptWork(t, b, "second", "key", expiry)
	b.applyNacks([]string{first.DeliveryID})
	retry := readWork(t, s)
	if retry.MessageID != "first" {
		t.Fatal("NACK released the key")
	}
	retry = readWork(t, s)
	if retry.MessageID != "first" {
		t.Fatal("attempt limit released the key")
	}
	b.removeAcks([]string{retry.DeliveryID})
	if d := readWork(t, s); d.MessageID != "second" {
		t.Fatal("completion did not advance the key")
	}
}
func TestDisconnectedGroupRetainsHeadAcrossMemberReplacement(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := workSubscriber(b, "a")
	expiry := time.Now().Add(time.Minute).UnixMilli()
	acceptWork(t, b, "first", "key", expiry)
	first := readWork(t, s)
	b.mu.Lock()
	b.removeSubscriberLocked(s)
	b.mu.Unlock()
	acceptWork(t, b, "second", "key", expiry)
	replacement := workSubscriber(b, "b")
	b.applyNacks([]string{first.DeliveryID})
	retry := readWork(t, replacement)
	if retry.MessageID != "first" {
		t.Fatal("replacement overtook unfinished work")
	}
	b.removeAcks([]string{retry.DeliveryID})
	if d := readWork(t, replacement); d.MessageID != "second" {
		t.Fatal(d.MessageID)
	}
}
func TestGroupExpiryReleasesKeyAndIndexBudget(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := workSubscriber(b, "a")
	acceptWork(t, b, "expired", "key", time.Now().Add(60*time.Millisecond).UnixMilli())
	readWork(t, s)
	acceptWork(t, b, "live", "key", time.Now().Add(time.Minute).UnixMilli())
	d := readWork(t, s)
	if d.MessageID != "live" {
		t.Fatal(d.MessageID)
	}
	b.removeAcks([]string{d.DeliveryID})
	b.mu.RLock()
	remaining := len(b.groupWork[checkpointScope{topic: "t", group: "g"}].work)
	b.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("index retained completed/expired entries: %d", remaining)
	}
}
func TestRetentionRejectsWholeBatchAndPreservesSequence(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheBytes = 900
	b := New(cfg)
	defer b.Close()
	expiry := time.Now().Add(time.Minute).UnixMilli()
	acceptWork(t, b, "original", "key", expiry)
	batch := []*Message{{ID: "next-a", Topic: "t", Payload: make([]byte, 100), ExpiresAt: expiry}, {ID: "next-b", Topic: "t", Payload: make([]byte, 100), ExpiresAt: expiry}}
	if err := b.acceptBatch(batch); !errors.Is(err, errRetentionCapacity) {
		t.Fatalf("batch admission: %v", err)
	}
	if b.cache.has("next-a") || b.cache.has("next-b") || !b.cache.has("original") {
		t.Fatal("capacity rejection changed retained content")
	}
	m := &Message{ID: "fits", Topic: "t", ExpiresAt: expiry}
	if _, err := b.accept(m); err != nil || m.Sequence != 2 {
		t.Fatalf("rejection consumed sequence: %d %v", m.Sequence, err)
	}
	if b.cache.evicted.Load() != 0 {
		t.Fatal("retention evicted accepted work")
	}
}
func TestRetentionHTTPPressureIsRetryable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheBytes = 600
	b := New(cfg)
	defer b.Close()
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/topics/t/messages?ack=available", bytes.NewReader(make([]byte, 200))))
		if (i == 0 && w.Code != 202) || (i == 1 && (w.Code != 429 || w.Header().Get("Retry-After") == "")) {
			t.Fatalf("attempt %d: %d %s", i, w.Code, w.Body.String())
		}
	}
}

func TestReplicationIndexPressurePreservesBufferedSuccessor(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := workSubscriber(b, "a")
	expiry := time.Now().Add(time.Minute).UnixMilli()
	first := &Message{ID: "one", Topic: "t", Key: "key", Origin: "source", Sequence: 1, ExpiresAt: expiry}
	next := &Message{ID: "two", Topic: "t", Key: "key", Origin: "source", Sequence: 2, ExpiresAt: expiry}
	if err := b.acceptReplicatedBatch([]*Message{next}); err != nil {
		t.Fatal(err)
	}
	filler := b.cfg.StreamMemoryBytes - b.streamMemoryBytes.Load() - workCharge(first)
	if !b.reserveStreamMemory(filler) {
		t.Fatal("reserve")
	}
	defer b.streamMemoryBytes.Add(-filler)
	if err := b.acceptReplicatedBatch([]*Message{first}); !errors.Is(err, errRetentionCapacity) {
		t.Fatalf("expected index pressure: %v", err)
	}
	d := readWork(t, s)
	if d.MessageID != "one" {
		t.Fatal(d.MessageID)
	}
	b.removeAcks([]string{d.DeliveryID})
	if err := b.acceptReplicatedBatch([]*Message{first}); err != nil {
		t.Fatal(err)
	}
	d = readWork(t, s)
	if d.MessageID != "two" {
		t.Fatalf("buffered successor lost: %s", d.MessageID)
	}
}

func TestGroupIndexPressureRejectsBeforeCacheAndSequence(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	workSubscriber(b, "a")
	other := &subscriber{id: "b", topic: "t", group: "other", ch: make(chan Delivery, 1)}
	b.mu.Lock()
	b.addSubscriberLocked(other)
	b.mu.Unlock()
	m := &Message{ID: "first", Topic: "t", Key: "key", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	baseline := b.streamMemoryBytes.Load()
	filler := b.cfg.StreamMemoryBytes - baseline - workCharge(m)
	if !b.reserveStreamMemory(filler) {
		t.Fatal("reserve")
	}
	if _, err := b.accept(m); !errors.Is(err, errRetentionCapacity) {
		t.Fatalf("index admission: %v", err)
	}
	if b.cache.has(m.ID) || m.Sequence != 0 || b.streamMemoryBytes.Load() != baseline+filler {
		t.Fatal("rejected work mutated cache, sequence or index budget")
	}
	b.streamMemoryBytes.Add(-filler)
	if _, err := b.accept(m); err != nil || m.Sequence != 1 {
		t.Fatalf("recovery: %v sequence %d", err, m.Sequence)
	}
}

func TestDisconnectedQueueLeavesReconnectHeadroom(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StreamMemoryBytes = streamMemoryReservation + 4096
	b := New(cfg)
	defer b.Close()
	s := workSubscriber(b, "a")
	acceptWork(t, b, "initial", "key", time.Now().Add(time.Minute).UnixMilli())
	b.mu.Lock()
	b.removeSubscriberLocked(s)
	b.mu.Unlock()
	rejected := false
	for i := 0; i < 32; i++ {
		_, err := b.accept(&Message{ID: fmt.Sprint(i), Topic: "t", Key: "key", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()})
		if errors.Is(err, errRetentionCapacity) {
			rejected = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !rejected {
		t.Fatal("queue did not reach its configured budget")
	}
	if !b.reserveStreamMemory(streamMemoryReservation) {
		t.Fatal("disconnected backlog consumed reconnect headroom")
	}
	b.streamMemoryBytes.Add(-streamMemoryReservation)
}

func TestSlowGroupedKeyDoesNotRejectUnrelatedPublish(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeliveryLagLimit = time.Millisecond
	b := New(cfg)
	defer b.Close()
	s := workSubscriber(b, "a")
	acceptWork(t, b, "slow", "a", time.Now().Add(time.Minute).UnixMilli())
	readWork(t, s)
	time.Sleep(5 * time.Millisecond)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/topics/t/messages?ack=available", bytes.NewBufferString("other"))
	r.Header.Set("Spruce-Key", "b")
	b.Handler().ServeHTTP(w, r)
	if w.Code != 202 {
		t.Fatalf("slow key blocked unrelated publication: %d %s", w.Code, w.Body.String())
	}
	if d := readWork(t, s); d.Key != "b" {
		t.Fatal(d.Key)
	}
}

func TestReplicaReorderOverflowRejectsConfirmation(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	b.cache.reorderLimit = 1
	expiry := time.Now().Add(time.Minute).UnixMilli()
	second := &Message{ID: "second", Topic: "t", Origin: "source", Sequence: 2, ExpiresAt: expiry}
	third := &Message{ID: "third", Topic: "t", Origin: "source", Sequence: 3, ExpiresAt: expiry}
	if err := b.acceptReplicatedBatch([]*Message{second}); err != nil {
		t.Fatal(err)
	}
	if err := b.acceptReplicatedBatch([]*Message{third}); !errors.Is(err, errRetentionCapacity) {
		t.Fatalf("unretained replica copy must not be confirmed: %v", err)
	}
	first := &Message{ID: "first", Topic: "t", Origin: "source", Sequence: 1, ExpiresAt: expiry}
	if err := b.acceptReplicatedBatch([]*Message{first, third}); err != nil {
		t.Fatal(err)
	}
	for _, m := range []*Message{first, second, third} {
		if !b.cache.has(m.ID) {
			t.Fatalf("retry did not recover %s", m.ID)
		}
	}
}

func TestReplicaBufferedRetryDoesNotReserveTwice(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	expiry := time.Now().Add(time.Minute).UnixMilli()
	gap := &Message{ID: "gap", Topic: "t", Origin: "source", Sequence: 2, ExpiresAt: expiry}
	if err := b.acceptReplicatedBatch([]*Message{gap}); err != nil {
		t.Fatal(err)
	}
	filler := &Message{ID: "filler", Topic: "other", ExpiresAt: expiry, Payload: make([]byte, 1024)}
	if _, err := b.accept(filler); err != nil {
		t.Fatal(err)
	}
	b.cache.mu.Lock()
	b.cache.maxBytes = b.cache.bytes + b.cache.reorderBytes
	b.cache.mu.Unlock()
	if err := b.acceptReplicatedBatch([]*Message{gap, gap}); err != nil {
		t.Fatalf("already retained retry charged new capacity: %v", err)
	}
	if _, err := b.accept(&Message{ID: "extra", Topic: "other", ExpiresAt: expiry}); !errors.Is(err, errRetentionCapacity) {
		t.Fatalf("local admission consumed reserved reorder capacity: %v", err)
	}
}

func TestGroupIndexShrinksWithoutLosingQueuedWork(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := workSubscriber(b, "a")
	expiry := time.Now().Add(time.Minute).UnixMilli()
	for i := 0; i < 128; i++ {
		acceptWork(t, b, fmt.Sprint(i), "key", expiry)
	}
	for i := 0; i < 127; i++ {
		d := readWork(t, s)
		if d.MessageID != fmt.Sprint(i) {
			t.Fatalf("queue changed during compaction: %+v", d)
		}
		b.removeAcks([]string{d.DeliveryID})
	}
	b.mu.RLock()
	g := b.groupWork[checkpointScope{topic: "t", group: "g"}]
	peak := g.indexPeak
	b.mu.RUnlock()
	if peak > 8 {
		t.Fatalf("emptying queue retained its peak map capacity: %d", peak)
	}
	if d := readWork(t, s); d.MessageID != "127" {
		t.Fatalf("last queued work lost: %+v", d)
	}
}

func TestRepeatedBatchIdentityDoesNotConsumeSequence(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	expiry := time.Now().Add(time.Minute).UnixMilli()
	first := &Message{ID: "first", Topic: "t", ExpiresAt: expiry}
	if err := b.acceptBatch([]*Message{first}); err != nil {
		t.Fatal(err)
	}
	retry := &Message{ID: "first", Topic: "t", ExpiresAt: expiry}
	next := &Message{ID: "next", Topic: "t", ExpiresAt: expiry}
	if err := b.acceptBatch([]*Message{retry, next}); err != nil {
		t.Fatal(err)
	}
	if retry.Sequence != first.Sequence || next.Sequence != first.Sequence+1 {
		t.Fatalf("duplicate created a sequence hole: first=%d retry=%d next=%d", first.Sequence, retry.Sequence, next.Sequence)
	}
}

func TestReplicaSequenceFrontierDoesNotConfirmMissingCopy(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	b.cache.mu.Lock()
	b.cache.receivedThrough["source"] = 2
	b.cache.mu.Unlock()
	m := &Message{ID: "missing", Topic: "t", Origin: "source", Sequence: 1, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if err := b.acceptReplicatedBatch([]*Message{m}); !errors.Is(err, errReplicaSequenceMissing) {
		t.Fatalf("sequence frontier falsely confirmed a retained copy: %v", err)
	}
}
