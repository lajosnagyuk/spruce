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
	reorderSince := len(b.cache.reorderSince)
	unsafe := b.cache.topicUnsafeLocked("t", time.Now().UnixMilli())
	cacheBytes := b.cache.bytes
	b.cache.mu.Unlock()
	var expectedBytes int64
	for sequence := uint64(1); sequence <= 4; sequence++ {
		expectedBytes += messageSize(&Message{ID: fmt.Sprintf("chain-%d", sequence), Topic: "t", Key: "k", Origin: "origin", Sequence: sequence, Payload: []byte{byte(sequence), 0}, ExpiresAt: expires})
	}
	if err != nil || through != 4 || reorderBytes != 0 || gap != 0 || reorderSince != 0 || unsafe || cacheBytes != expectedBytes {
		t.Fatalf("repair chain: err=%v through=%d cache_bytes=%d want=%d reorder_bytes=%d gaps=%d since=%d unsafe=%t", err, through, cacheBytes, expectedBytes, reorderBytes, gap, reorderSince, unsafe)
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
	d := readWork(t, s)
	if d.MessageID != second.ID {
		t.Fatalf("successor delivery: %s", d.MessageID)
	}
	b.removeAcks([]string{d.DeliveryID})
	b.cache.mu.Lock()
	before := b.cache.bytes + b.cache.reorderBytes
	b.cache.mu.Unlock()
	b.cache.mu.Lock()
	err = b.acceptBatchLocked([]*Message{first}, true)
	b.cache.mu.Unlock()
	if err != nil {
		t.Fatalf("duplicate repair: %v", err)
	}
	select {
	case d := <-s.ch:
		t.Fatalf("duplicate repair delivery: %+v", d)
	default:
	}
	b.cache.mu.Lock()
	after := b.cache.bytes + b.cache.reorderBytes
	b.cache.mu.Unlock()
	if after != before {
		t.Fatalf("duplicate repair changed accounting: before=%d after=%d", before, after)
	}
}

func TestExpiredBufferedSuccessorIsConsumedWithoutCharge(t *testing.T) {
	for _, repair := range []bool{false, true} {
		for _, allExpired := range []bool{false, true} {
			t.Run(fmt.Sprintf("repair=%t/all-expired=%t", repair, allExpired), func(t *testing.T) {
				b := New(DefaultConfig())
				defer b.Close()
				now := time.Now().UnixMilli()
				origin := fmt.Sprintf("expiry-%t", repair)
				broadcast := &subscriber{id: "broadcast", topic: "t", ch: make(chan Delivery, 8)}
				b.mu.Lock()
				b.addSubscriberLocked(broadcast)
				b.mu.Unlock()
				group := workSubscriber(b, "expiry-group")
				expired := &Message{ID: "expiry-2", Topic: "t", Key: "k2", Origin: origin, Sequence: 2, Payload: []byte("expired"), ExpiresAt: now - 1}
				liveExpiry := now + time.Minute.Milliseconds()
				if allExpired {
					liveExpiry = now - 1
				}
				live := &Message{ID: "expiry-3", Topic: "t", Key: "k3", Origin: origin, Sequence: 3, Payload: []byte("live"), ExpiresAt: liveExpiry}
				for _, m := range []*Message{expired, live} {
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
				cacheBytes := b.cache.bytes
				gapCount := len(b.cache.reorder[origin])
				gapSince := b.cache.reorderSince[origin]
				unsafe := b.cache.topicUnsafeLocked("t", now)
				_, expiredRetained := b.cache.items["expiry-2"]
				_, expiredGap := b.cache.reorder[origin][2]
				b.cache.mu.Unlock()
				wantLive := !allExpired
				var wantBytes = messageSize(first)
				if wantLive {
					wantBytes += messageSize(live)
				}
				if err != nil || through != 3 || queued != 0 || cacheBytes != wantBytes || expiredRetained || expiredGap || gapCount != 0 || gapSince != 0 || unsafe || b.cache.has("expiry-2") || b.cache.has("expiry-3") != wantLive {
					t.Fatalf("expiry transition: err=%v through=%d bytes=%d want=%d queued=%d gaps=%d since=%d expired_cache=%t expired_gap=%t live=%t unsafe=%t", err, through, cacheBytes, wantBytes, queued, gapCount, gapSince, expiredRetained, expiredGap, b.cache.has("expiry-3"), unsafe)
				}
				wantDeliveries := 1
				if wantLive {
					wantDeliveries++
				}
				gotBroadcast := make(map[string]bool)
				for i := 0; i < wantDeliveries; i++ {
					select {
					case d := <-broadcast.ch:
						gotBroadcast[d.MessageID] = true
					case <-time.After(time.Second):
						t.Fatal("broadcast delivery deadline")
					}
				}
				if gotBroadcast[expired.ID] || gotBroadcast[first.ID] != true || gotBroadcast[live.ID] != wantLive || len(gotBroadcast) != wantDeliveries {
					t.Fatalf("broadcast deliveries: %#v", gotBroadcast)
				}
				gotGroup := make(map[string]bool)
				for i := 0; i < wantDeliveries; i++ {
					d := readWork(t, group)
					gotGroup[d.MessageID] = true
					b.removeAcks([]string{d.DeliveryID})
				}
				if gotGroup[expired.ID] || gotGroup[first.ID] != true || gotGroup[live.ID] != wantLive || len(gotGroup) != wantDeliveries {
					t.Fatalf("group deliveries: %#v", gotGroup)
				}
				b.mu.RLock()
				g := b.groupWork[checkpointScope{topic: "t", group: "g"}]
				groupHasExpired := g != nil && g.work[expired.ID] != nil
				b.mu.RUnlock()
				if groupHasExpired {
					t.Fatal("expired message created group work")
				}
			})
		}
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
	b.cache.tombstones = orderCompactTombstones + 1
	b.cache.mu.Unlock()
	page, after, valid := b.cache.repairPage(next, 2*messageSize(b.cache.get("ordinal-2")))
	if !valid || len(page) != 2 || page[0].ID != "ordinal-2" || after <= secondOrdinal || firstOrdinal == 0 {
		t.Fatalf("compacted repair page: valid=%t len=%d after=%d second=%d", valid, len(page), after, secondOrdinal)
	}
	b.cache.mu.Lock()
	compacted := b.cache.orderHead == 0 && len(b.cache.order) == 2 && b.cache.order[0] != nil && b.cache.order[1] != nil
	b.cache.mu.Unlock()
	if !compacted {
		t.Fatal("repair page did not compact the insertion order")
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

func TestRepairPageSkipsExpiryAndEvictionWithBoundedPages(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	expires := time.Now().Add(time.Minute).UnixMilli()
	first := &Message{ID: "page-first", Topic: "page", Payload: []byte("first"), ExpiresAt: expires}
	second := &Message{ID: "page-second", Topic: "page", Payload: []byte("second"), ExpiresAt: expires}
	third := &Message{ID: "page-third", Topic: "page", Payload: []byte("third"), ExpiresAt: expires}
	for _, m := range []*Message{first, second, third} {
		if _, err := b.accept(m); err != nil {
			t.Fatal(err)
		}
	}
	firstSize := messageSize(first)
	if page, next, valid := b.cache.repairPage(0, firstSize-1); valid || page != nil || next != 0 {
		t.Fatalf("undersized page accepted: valid=%t page=%v next=%d", valid, page, next)
	}
	page, next, valid := b.cache.repairPage(0, firstSize)
	if !valid || len(page) != 1 || page[0].ID != first.ID || next != first.repairOrdinal {
		t.Fatalf("byte boundary page: valid=%t page=%v next=%d ordinal=%d", valid, page, next, first.repairOrdinal)
	}
	b.cache.mu.Lock()
	b.cache.removeLocked(first, true)
	b.cache.mu.Unlock()
	page, after, valid := b.cache.repairPage(next, messageSize(second)*2)
	if !valid || len(page) != 2 || page[0].ID != second.ID || after != third.repairOrdinal {
		firstID := ""
		if len(page) > 0 {
			firstID = page[0].ID
		}
		t.Fatalf("evicted page: valid=%t page_len=%d first=%s after=%d want=%d", valid, len(page), firstID, after, third.repairOrdinal)
	}
	// A duplicate must not allocate a new ordinal, and a later insertion must
	// remain visible after the prior continuation.
	duplicate := &Message{ID: second.ID, Topic: second.Topic, Payload: append([]byte(nil), second.Payload...), ExpiresAt: second.ExpiresAt}
	if _, err := b.accept(duplicate); err != nil {
		t.Fatal(err)
	}
	later := &Message{ID: "page-later", Topic: "page", Payload: []byte("later"), ExpiresAt: expires}
	if _, err := b.accept(later); err != nil {
		t.Fatal(err)
	}
	page, afterLater, valid := b.cache.repairPage(after, messageSize(later)*2)
	if !valid || len(page) != 1 || page[0].ID != later.ID || afterLater != later.repairOrdinal {
		t.Fatalf("later insertion page: valid=%t page=%v after=%d want=%d", valid, page, afterLater, later.repairOrdinal)
	}
	empty, emptyAfter, valid := b.cache.repairPage(afterLater, messageSize(later))
	if !valid || len(empty) != 0 || emptyAfter != afterLater {
		t.Fatalf("empty page: valid=%t len=%d after=%d want=%d", valid, len(empty), emptyAfter, afterLater)
	}

	old := &Message{ID: "page-expired", Topic: "page-expired", Payload: []byte("expired"), ExpiresAt: time.Now().Add(-time.Second).UnixMilli()}
	if _, err := b.cache.put(old, time.Now().Add(-2*time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if page, _, valid := b.cache.repairPage(afterLater, messageSize(old)*2); !valid || len(page) != 0 || b.cache.has(old.ID) {
		t.Fatalf("expired page entry survived: valid=%t len=%d cached=%t", valid, len(page), b.cache.has(old.ID))
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
		if i == 2 {
			source.cache.mu.Lock()
			compacted := source.cache.orderHead == 0 && len(source.cache.order) == 1 && source.cache.order[0] != nil
			source.cache.mu.Unlock()
			if !compacted {
				t.Fatal("repair page did not exercise order compaction")
			}
		}
		source.cache.mu.Lock()
		source.cache.removeLocked(m, true)
		if i < 2 {
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

func TestRepairHTTPRecoversLiveTailWithExpiringPages(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-repair", "synthetic-cluster"
	source, target := New(cfg), New(cfg)
	defer source.Close()
	defer target.Close()
	server := httptest.NewServer(target.Handler())
	defer server.Close()
	p := &peer{url: server.URL}
	p.repairVersion.Store(1)
	started := time.Now()
	var messages []*Message
	for i := 0; i < 3; i++ {
		m := &Message{ID: fmt.Sprintf("live-prefix-%d", i), Topic: "mixed-ttl", Payload: make([]byte, 600<<10), ExpiresAt: started.Add(time.Minute).UnixMilli()}
		if _, err := source.accept(m); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, m)
	}
	for i := 0; i < 3; i++ {
		m := &Message{ID: fmt.Sprintf("expiring-page-%d", i), Topic: "mixed-ttl", Payload: make([]byte, 600<<10), ExpiresAt: started.Add(time.Duration(20+20*i) * time.Millisecond).UnixMilli()}
		if _, err := source.accept(m); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, m)
	}
	tail := &Message{ID: "live-tail", Topic: "mixed-ttl", Payload: make([]byte, 600<<10), ExpiresAt: started.Add(500 * time.Millisecond).UnixMilli()}
	if _, err := source.accept(tail); err != nil {
		t.Fatal(err)
	}
	messages = append(messages, tail)
	for step := 0; step < 3; step++ {
		if !source.repairPeerStep(p) {
			t.Fatalf("repair step %d failed before live tail", step)
		}
	}
	time.Sleep(80 * time.Millisecond)
	if !source.repairPeerStep(p) {
		t.Fatalf("repair did not skip expired pages and send live tail")
	}
	for i := 0; i < 3; i++ {
		if !target.cache.has(messages[i].ID) {
			t.Fatalf("live prefix %s was not retained at target", messages[i].ID)
		}
	}
	if !target.cache.has(tail.ID) || time.Now().After(time.UnixMilli(tail.ExpiresAt)) {
		t.Fatalf("live tail was not recovered before expiry: target=%t", target.cache.has(tail.ID))
	}
	if source.metrics.RepairErrors.Load() != 0 || target.metrics.Duplicate.Load() != 0 || p.queuedBytes.Load() != 0 {
		t.Fatalf("repair recovery accounting: source_errors=%d target_duplicates=%d queued=%d", source.metrics.RepairErrors.Load(), target.metrics.Duplicate.Load(), p.queuedBytes.Load())
	}
}
