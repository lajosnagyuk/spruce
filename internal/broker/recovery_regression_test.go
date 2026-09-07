package broker

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRepairPromotesEntireBufferedChainOnce(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := workSubscriber(b, "repair-chain")
	expires := time.Now().Add(time.Minute).UnixMilli()
	for sequence := uint64(2); sequence <= 4; sequence++ {
		m := &Message{ID: fmt.Sprintf("chain-%d", sequence), Topic: "t", Key: "k", Origin: "origin", Sequence: sequence, Payload: []byte{byte(sequence), 0}, ExpiresAt: expires}
		if err := b.acceptReplicatedBatch([]*Message{m}); err != nil {
			t.Fatal(err)
		}
	}
	first := &Message{ID: "chain-1", Topic: "t", Key: "k", Origin: "origin", Sequence: 1, Payload: []byte{1, 0}, ExpiresAt: expires}
	b.cache.mu.Lock()
	err := b.acceptBatchLocked([]*Message{first}, true)
	through := b.cache.receivedThrough["origin"]
	reorderBytes := b.cache.reorderBytes
	gap := len(b.cache.reorder)
	b.cache.mu.Unlock()
	if err != nil || through != 4 || reorderBytes != 0 || gap != 0 {
		t.Fatalf("repair chain: err=%v through=%d reorder_bytes=%d gaps=%d", err, through, reorderBytes, gap)
	}
	for sequence := uint64(1); sequence <= 4; sequence++ {
		id := fmt.Sprintf("chain-%d", sequence)
		m := b.cache.get(id)
		if m == nil || string(m.Payload) != string([]byte{byte(sequence), 0}) {
			t.Fatalf("missing or modified cached message %s", id)
		}
	}
	for sequence := uint64(1); sequence <= 4; sequence++ {
		d := readWork(t, s)
		if d.MessageID != fmt.Sprintf("chain-%d", sequence) {
			t.Fatalf("delivery order: got %s for sequence %d", d.MessageID, sequence)
		}
		b.removeAcks([]string{d.DeliveryID})
	}
	select {
	case d := <-s.ch:
		t.Fatalf("duplicate repair delivery: %+v", d)
	default:
	}
}

func TestRepairRetainsSuccessorWhenGroupAdmissionIsFull(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := workSubscriber(b, "repair-capacity")
	expires := time.Now().Add(time.Minute).UnixMilli()
	second := &Message{ID: "capacity-2", Topic: "t", Origin: "capacity", Sequence: 2, Payload: []byte("second"), ExpiresAt: expires}
	first := &Message{ID: "capacity-1", Topic: "t", Origin: "capacity", Sequence: 1, Payload: []byte("first"), ExpiresAt: expires}
	if err := b.acceptReplicatedBatch([]*Message{second}); err != nil {
		t.Fatal(err)
	}
	remaining := b.cfg.StreamMemoryBytes - b.streamMemoryBytes.Load() - workCharge(first)
	if remaining <= 0 || !b.reserveStreamMemory(remaining) {
		t.Fatalf("could not set deterministic group capacity: remaining=%d used=%d", remaining, b.streamMemoryBytes.Load())
	}
	defer func() {
		if remaining > 0 {
			b.streamMemoryBytes.Add(-remaining)
		}
	}()
	b.cache.mu.Lock()
	err := b.acceptBatchLocked([]*Message{first}, true)
	b.cache.mu.Unlock()
	if err != nil {
		t.Fatalf("predecessor admission should succeed while successor is blocked: %v", err)
	}
	select {
	case d := <-s.ch:
		if d.MessageID != first.ID {
			t.Fatalf("unexpected first delivery: %s", d.MessageID)
		}
		b.removeAcks([]string{d.DeliveryID})
	default:
		t.Fatal("predecessor was not delivered")
	}
	b.cache.mu.Lock()
	retained := b.cache.reorder[second.Origin][second.Sequence]
	retainedBytes := b.cache.reorderBytes
	b.cache.mu.Unlock()
	if retained != second || retainedBytes != messageSize(second) {
		t.Fatalf("successor retention: retained=%v bytes=%d want=%d", retained != nil, retainedBytes, messageSize(second))
	}
	b.streamMemoryBytes.Add(-remaining)
	remaining = 0
	b.cache.mu.Lock()
	err = b.acceptBatchLocked([]*Message{first}, true)
	through := b.cache.receivedThrough[second.Origin]
	queued := b.cache.reorderBytes
	b.cache.mu.Unlock()
	if err != nil || through != 2 || queued != 0 || !b.cache.has(second.ID) {
		t.Fatalf("retry promotion: err=%v through=%d queued=%d cached=%t", err, through, queued, b.cache.has(second.ID))
	}
	if d := readWork(t, s); d.MessageID != second.ID {
		t.Fatalf("successor delivery: %s", d.MessageID)
	}
}

func TestExpiredBufferedSuccessorIsConsumedWithoutCharge(t *testing.T) {
	for _, repair := range []bool{false, true} {
		t.Run(fmt.Sprintf("repair=%t", repair), func(t *testing.T) {
			b := New(DefaultConfig())
			defer b.Close()
			now := time.Now().UnixMilli()
			origin := fmt.Sprintf("expiry-%t", repair)
			for _, m := range []*Message{
				{ID: "expiry-2", Topic: "t", Origin: origin, Sequence: 2, Payload: []byte("expired"), ExpiresAt: now - 1},
				{ID: "expiry-3", Topic: "t", Origin: origin, Sequence: 3, Payload: []byte("live"), ExpiresAt: now + time.Minute.Milliseconds()},
			} {
				if err := b.acceptReplicatedBatch([]*Message{m}); err != nil {
					t.Fatal(err)
				}
			}
			first := &Message{ID: "expiry-1", Topic: "t", Origin: origin, Sequence: 1, Payload: []byte("first"), ExpiresAt: now + time.Minute.Milliseconds()}
			b.cache.mu.Lock()
			b.cache.nextMetadataExpiry = now + time.Hour.Milliseconds()
			b.cache.mu.Unlock()
			var err error
			if repair {
				b.cache.mu.Lock()
				err = b.acceptBatchLocked([]*Message{first}, true)
				b.cache.mu.Unlock()
			} else {
				err = b.acceptReplicatedBatch([]*Message{first})
			}
			b.cache.mu.Lock()
			through := b.cache.receivedThrough[origin]
			queued := b.cache.reorderBytes
			_, expiredRetained := b.cache.items["expiry-2"]
			_, expiredGap := b.cache.reorder[origin][2]
			b.cache.mu.Unlock()
			if err != nil || through != 3 || queued != 0 || expiredRetained || expiredGap || !b.cache.has("expiry-3") {
				t.Fatalf("expiry transition: err=%v through=%d queued=%d cache_expired=%t gap_expired=%t live=%t", err, through, queued, expiredRetained, expiredGap, b.cache.has("expiry-3"))
			}
		})
	}
}

func TestRepairExpiredOnlyGapClearsMetadata(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	now := time.Now().UnixMilli()
	origin := "expired-only"
	gap := &Message{ID: "expired-only-2", Topic: "t", Origin: origin, Sequence: 2, Payload: []byte("expired"), ExpiresAt: now - 1}
	if err := b.acceptReplicatedBatch([]*Message{gap}); err != nil {
		t.Fatal(err)
	}
	b.cache.mu.Lock()
	b.cache.nextMetadataExpiry = now + time.Hour.Milliseconds()
	b.cache.mu.Unlock()
	first := &Message{ID: "expired-only-1", Topic: "t", Origin: origin, Sequence: 1, Payload: []byte("first"), ExpiresAt: now + time.Minute.Milliseconds()}
	b.cache.mu.Lock()
	err := b.acceptBatchLocked([]*Message{first}, true)
	_, hasGap := b.cache.reorder[origin]
	_, hasSince := b.cache.reorderSince[origin]
	unsafe := b.cache.topicUnsafeLocked("t", now)
	queued := b.cache.reorderBytes
	b.cache.mu.Unlock()
	if err != nil || hasGap || hasSince || unsafe || queued != 0 {
		t.Fatalf("expired-only repair metadata: err=%v gap=%t since=%t unsafe=%t queued=%d", err, hasGap, hasSince, unsafe, queued)
	}
}

func TestRepairOrdinalSurvivesCompactionAndDuplicate(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	for i := 0; i < 4; i++ {
		if _, err := b.accept(&Message{ID: fmt.Sprintf("ordinal-%d", i), Topic: "t", Payload: []byte{byte(i)}, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}); err != nil {
			t.Fatal(err)
		}
	}
	page, next, valid := b.cache.repairPage(0, 2*messageSize(b.cache.get("ordinal-0")))
	if !valid || len(page) != 2 {
		t.Fatalf("first repair page: valid=%t len=%d", valid, len(page))
	}
	firstOrdinal := page[0].repairOrdinal
	secondOrdinal := page[1].repairOrdinal
	b.cache.mu.Lock()
	b.cache.removeLocked(page[0], true)
	b.cache.removeLocked(page[1], true)
	b.cache.mu.Unlock()
	page, after, valid := b.cache.repairPage(next, 2*messageSize(b.cache.get("ordinal-2")))
	if !valid || len(page) != 2 || page[0].ID != "ordinal-2" || after <= secondOrdinal || firstOrdinal == 0 {
		t.Fatalf("compacted repair page: valid=%t len=%d after=%d second=%d", valid, len(page), after, secondOrdinal)
	}
	duplicate := &Message{ID: "ordinal-2", Topic: "t", Payload: []byte{2}, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if _, err := b.accept(duplicate); err != nil {
		t.Fatal(err)
	}
	_, afterDuplicate, _ := b.cache.repairPage(after, messageSize(duplicate)*2)
	if afterDuplicate != after {
		t.Fatalf("duplicate consumed repair ordinal: before=%d after=%d", after, afterDuplicate)
	}
}

func TestRepairHTTPFailureRetainsOrdinal(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	if _, err := b.accept(&Message{ID: "retry-ordinal", Topic: "t", Payload: []byte("payload"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	p := &peer{url: server.URL}
	p.repairVersion.Store(1)
	if b.repairPeerStep(p) {
		t.Fatal("failed repair reported success")
	}
	if p.repairOrdinal != 0 || p.repairCompleted.Load() != 0 || p.queuedBytes.Load() != 0 {
		t.Fatalf("failed repair advanced state: ordinal=%d completed=%d queued=%d", p.repairOrdinal, p.repairCompleted.Load(), p.queuedBytes.Load())
	}
	if !b.repairPeerStep(p) || p.repairOrdinal == 0 || p.queuedBytes.Load() != 0 {
		t.Fatalf("retry did not advance: ordinal=%d queued=%d", p.repairOrdinal, p.queuedBytes.Load())
	}
}

func TestRepairHTTPRecoversTailAfterCacheCompaction(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-repair", "synthetic-cluster"
	source, target := New(cfg), New(cfg)
	defer source.Close()
	defer target.Close()
	server := httptest.NewServer(target.Handler())
	defer server.Close()
	p := &peer{url: server.URL}
	p.repairVersion.Store(1)
	var messages []*Message
	for i := 0; i < 3; i++ {
		m := &Message{ID: fmt.Sprintf("compact-tail-%d", i), Topic: "t", Payload: make([]byte, 600<<10), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
		if _, err := source.accept(m); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, m)
	}
	for i, m := range messages {
		if !source.repairPeerStep(p) {
			t.Fatalf("repair step %d failed", i)
		}
		source.cache.mu.Lock()
		source.cache.removeLocked(m, true)
		if i == 0 {
			source.cache.tombstones = orderCompactTombstones + 1
		}
		source.cache.mu.Unlock()
	}
	for _, m := range messages {
		if !target.cache.has(m.ID) {
			t.Fatalf("compacted repair lost %s", m.ID)
		}
	}
	if p.repairOrdinal == 0 || p.queuedBytes.Load() != 0 {
		t.Fatalf("repair state after compacted tail: ordinal=%d queued=%d", p.repairOrdinal, p.queuedBytes.Load())
	}
}
