package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPartitionHealRecoversAfterQueueRetryExhaustion(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	target := New(cfg)
	defer target.Close()
	var isolated atomic.Bool
	isolated.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isolated.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		target.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()
	cfg.Peers = []string{server.URL}
	source := New(cfg)
	defer source.Close()
	expiry := time.Now().Add(time.Minute).UnixMilli()
	m := &Message{ID: "partition-event", Topic: "events", Key: "workspace", Payload: []byte("opaque event"), ExpiresAt: expiry}
	if _, err := source.accept(m); err != nil {
		t.Fatal(err)
	}
	source.enqueuePeers(m)
	deadline := time.Now().Add(2 * time.Second)
	for source.metrics.ReplicationDropped.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if source.metrics.ReplicationDropped.Load() == 0 {
		t.Fatal("test did not exhaust replication attempts")
	}
	if target.cache.has(m.ID) {
		t.Fatal("partition did not isolate the receiver")
	}
	isolated.Store(false)
	deadline = time.Now().Add(4 * time.Second)
	for !target.cache.has(m.ID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !target.cache.has(m.ID) {
		t.Fatal("healed peer never recovered retained work without new publication")
	}
}

func TestRepairPromotesBufferedCopyWithoutDoubleCharging(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	expiry := time.Now().Add(time.Minute).UnixMilli()
	second := &Message{ID: "second", Topic: "t", Key: "k", Origin: "source", Sequence: 2, ExpiresAt: expiry}
	if err := b.acceptReplicatedBatch([]*Message{second}); err != nil {
		t.Fatal(err)
	}
	first := &Message{ID: "first", Topic: "t", Key: "k", Origin: "source", Sequence: 1, ExpiresAt: expiry}
	b.cache.mu.Lock()
	b.cache.maxBytes = messageSize(first) + messageSize(second)
	err := b.acceptBatchLocked([]*Message{first, second}, true)
	queued := b.cache.reorderBytes
	b.cache.mu.Unlock()
	if err != nil || queued != 0 || !b.cache.has("first") || !b.cache.has("second") {
		t.Fatalf("repair lost buffered capacity: %v queued=%d", err, queued)
	}
}

func TestRepairRecoversLiveSuccessorAfterPredecessorExpiry(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	s := workSubscriber(b, "a")
	m := &Message{ID: "live", Topic: "t", Key: "k", Origin: "source", Sequence: 2, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	b.cache.mu.Lock()
	err := b.acceptBatchLocked([]*Message{m}, true)
	b.cache.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if d := readWork(t, s); d.MessageID != m.ID {
		t.Fatal("live successor blocked on expired predecessor")
	}
	b.cache.mu.Lock()
	unsafe := b.cache.topicUnsafeLocked("t", time.Now().UnixMilli())
	b.cache.mu.Unlock()
	if !unsafe {
		t.Fatal("repair skipped a gap without exposing unsafe cursor history")
	}
}

func TestRepairDoesNotClearConcurrentFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	target := New(cfg)
	defer target.Close()
	server := httptest.NewServer(target.Handler())
	defer server.Close()
	source := New(cfg)
	defer source.Close()
	p := &peer{url: server.URL}
	p.repairVersion.Store(1)
	m := &Message{ID: "live", Topic: "t", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if _, err := source.accept(m); err != nil {
		t.Fatal(err)
	}
	if !source.repairPeerStep(p) {
		t.Fatal("repair did not send page")
	}
	p.repairVersion.Add(1)
	source.repairPeerStep(p)
	if p.repairCompleted.Load() != 1 || p.repairVersion.Load() != 2 {
		t.Fatal("concurrent failure cleared by an older repair pass")
	}
	if p.queuedBytes.Load() != 0 {
		t.Fatal("repair reservation leaked")
	}
}

func TestRepairRespectsExistingQueueReservation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicationQueueBytes = 1024
	b := New(cfg)
	defer b.Close()
	p := &peer{url: "http://127.0.0.1:1"}
	p.repairVersion.Store(1)
	p.queuedBytes.Store(512)
	if b.repairPeerStep(p) || p.queuedBytes.Load() != 512 || p.repairCompleted.Load() != 0 {
		t.Fatal("repair exceeded the queue budget or falsely completed")
	}
}

func TestRepairEndpointRequiresPeerAuthentication(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	b := New(cfg)
	defer b.Close()
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/internal/repair", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated repair: %d", w.Code)
	}
}

func TestExpiredPredecessorDoesNotStrandConfirmedSuccessor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	target := New(cfg)
	defer target.Close()
	server := httptest.NewServer(target.Handler())
	defer server.Close()
	cfg.Peers = []string{server.URL}
	source := New(cfg)
	defer source.Close()
	first := &Message{ID: "expired-predecessor", Topic: "t", ExpiresAt: time.Now().Add(time.Millisecond).UnixMilli()}
	if _, err := source.accept(first); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	second := &Message{ID: "live-successor", Topic: "t", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if _, err := source.accept(second); err != nil {
		t.Fatal(err)
	}
	if second.Sequence != 2 {
		t.Fatalf("test requires sequence gap, got %d", second.Sequence)
	}
	source.enqueuePeers(second)
	deadline := time.Now().Add(4 * time.Second)
	for !target.cache.has(second.ID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !target.cache.has(second.ID) {
		t.Fatal("replica confirmed buffered successor but never made it deliverable")
	}
}

func TestTransientSequenceGapDoesNotWalkRetainedCache(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	target := New(cfg)
	defer target.Close()
	server := httptest.NewServer(target.Handler())
	defer server.Close()
	source := New(cfg)
	defer source.Close()
	p := &peer{url: server.URL}
	p.v2.Store(true)
	expiry := time.Now().Add(time.Minute).UnixMilli()
	first := &Message{ID: "first", Topic: "t", ExpiresAt: expiry}
	second := &Message{ID: "second", Topic: "t", ExpiresAt: expiry}
	if err := source.acceptBatch([]*Message{first, second}); err != nil {
		t.Fatal(err)
	}
	if !source.sendPeer(context.Background(), p, second) {
		t.Fatal("buffered copy not retained")
	}
	if p.gapRepairVersion.Load() == 0 {
		t.Fatal("gap notification missing")
	}
	p.gapRepairAfter.Store(0)
	if source.repairPeerStep(p) || p.gapRepairCompleted.Load() != 0 {
		t.Fatal("fresh gap caused immediate repair or was forgotten")
	}
	if !source.sendPeer(context.Background(), p, first) {
		t.Fatal("predecessor failed")
	}
	p.gapRepairAfter.Store(0)
	source.repairPeerStep(p)
	if p.gapRepairCompleted.Load() != p.gapRepairVersion.Load() || source.metrics.RepairPages.Load() != 0 {
		t.Fatal("resolved reorder caused full-cache repair")
	}
}
