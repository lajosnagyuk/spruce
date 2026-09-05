package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOnePeerPublishConvergesWithSlowerReplica(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	fast, slow := New(cfg), New(cfg)
	defer fast.Close()
	defer slow.Close()
	fastServer := httptest.NewServer(fast.Handler())
	defer fastServer.Close()
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/replicate" {
			select {
			case <-time.After(20 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		slow.Handler().ServeHTTP(w, r)
	}))
	defer slowServer.Close()
	cfg.Peers = []string{fastServer.URL, slowServer.URL}
	source := New(cfg)
	defer source.Close()
	r := httptest.NewRequest("POST", "/v1/topics/lifecycle/messages?ack=one-peer", bytes.NewBufferString("event"))
	w := httptest.NewRecorder()
	source.Handler().ServeHTTP(w, r)
	if w.Code != 202 {
		t.Fatalf("publish: %d %s", w.Code, w.Body.String())
	}
	var result struct{ ID string }
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !slow.cache.has(result.ID) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !fast.cache.has(result.ID) || !slow.cache.has(result.ID) {
		t.Fatal("one-peer acknowledgement left the slower healthy replica without a copy")
	}
	for _, b := range []*Broker{fast, slow} {
		b.cache.mu.Lock()
		unsafe := b.cache.topicUnsafeLocked("lifecycle", time.Now().UnixMilli())
		b.cache.mu.Unlock()
		if unsafe {
			t.Fatal("initial replication incorrectly fell back to the legacy protocol")
		}
	}
}

func TestPeerHeaderMatchesEncodedBodyDuringCapabilityChange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	b := New(cfg)
	defer b.Close()
	server := httptest.NewServer(b.Handler())
	defer server.Close()
	p := &peer{url: server.URL}
	var body bytes.Buffer
	m := &Message{ID: "legacy", Topic: "t", Payload: []byte("event"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if err := writePeerBatchV1(&body, []*Message{m}); err != nil {
		t.Fatal(err)
	}
	// Negotiation changes after encoding. The captured version must win.
	p.v2.Store(true)
	if !b.sendPeerBody(context.Background(), p, body.Bytes(), 1, false) {
		t.Fatal("capability change mislabeled an already encoded body")
	}
}

func TestUnavailableCapabilityProbeDoesNotDowngradeReplication(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	target := New(cfg)
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/capabilities" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		target.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()
	source := New(cfg)
	defer source.Close()
	p := &peer{url: server.URL}
	m := &Message{ID: "event", Topic: "t", Payload: []byte("event"), Origin: "source", Sequence: 1, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if !source.sendPeer(context.Background(), p, m) {
		t.Fatal("modern replication failed after transient capability error")
	}
	target.cache.mu.Lock()
	unsafe := target.cache.topicUnsafeLocked("t", time.Now().UnixMilli())
	target.cache.mu.Unlock()
	if unsafe {
		t.Fatal("transient capability failure incorrectly marked topic history unsafe")
	}
}

func TestRejectedAdmissionDoesNotConsumeSequence(t *testing.T) {
	for _, batch := range []bool{false, true} {
		cfg := DefaultConfig()
		cfg.CacheBytes = 1024
		b := New(cfg)
		defer b.Close()
		expiry := time.Now().Add(time.Minute).UnixMilli()
		if batch {
			if err := b.acceptBatch([]*Message{{ID: "bad1", Topic: "t", Payload: make([]byte, 600), ExpiresAt: expiry}, {ID: "bad2", Topic: "t", Payload: make([]byte, 600), ExpiresAt: expiry}}); err == nil {
				t.Fatal("oversized batch accepted")
			}
		} else if _, err := b.accept(&Message{ID: "bad", Topic: "t", Payload: make([]byte, 1024), ExpiresAt: expiry}); err == nil {
			t.Fatal("oversized message accepted")
		}
		m := &Message{ID: "good", Topic: "t", Payload: []byte("event"), ExpiresAt: expiry}
		if _, err := b.accept(m); err != nil || m.Sequence != 1 {
			t.Fatalf("rejected publish created an unfillable sequence hole: batch=%v seq=%d err=%v", batch, m.Sequence, err)
		}
	}
}
