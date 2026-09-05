package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAvailableAcknowledgementTracksSurvivingCopies(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	var unavailable atomic.Int32
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		target := New(cfg)
		defer target.Close()
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if int(unavailable.Load()) > i {
				w.WriteHeader(503)
				return
			}
			target.Handler().ServeHTTP(w, r)
		}))
		defer servers[i].Close()
	}
	cfg.Peers = []string{servers[0].URL, servers[1].URL}
	b := New(cfg)
	defer b.Close()
	publish := func(ack string) (int, int, bool) {
		r := httptest.NewRequest("POST", "/v1/topics/t/messages?ack="+ack, bytes.NewBufferString("event"))
		r.Header.Set("Spruce-Producer-ID", "producer")
		r.Header.Set("Spruce-Idempotency-Key", "same-operation")
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, r)
		var result struct {
			Copies   int  `json:"confirmed_copies"`
			Degraded bool `json:"degraded"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &result)
		return w.Code, result.Copies, result.Degraded
	}
	for down := 0; down <= 2; down++ {
		unavailable.Store(int32(down))
		status, copies, degraded := publish("available")
		if status != 202 || copies != 3-down || degraded != (down > 0) {
			t.Fatalf("down=%d: status=%d copies=%d degraded=%v", down, status, copies, degraded)
		}
	}
	// Historical replicated=true must not bypass current available-mode receipts.
	if len(b.cache.snapshot("t", 0)) != 1 {
		t.Fatal("retry recreated the operation")
	}
	// A cached successful receipt must not bypass a fresh strict requirement.
	if status, _, _ := publish("one-peer"); status != 503 {
		t.Fatalf("strict retry reused historical confirmation: %d", status)
	}
	// A new strict publish must still require a peer; availability is opt-in.
	r := httptest.NewRequest("POST", "/v1/topics/t/messages?ack=one-peer", bytes.NewBufferString("strict"))
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("strict mode silently degraded: %d", w.Code)
	}
	unavailable.Store(0)
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, copies, degraded := publish("available")
		if status == 202 && copies == 3 && !degraded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery: status=%d copies=%d degraded=%v", status, copies, degraded)
		}
		time.Sleep(time.Millisecond)
	}

}

func TestAvailableAcknowledgementWithoutPeers(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	r := httptest.NewRequest("POST", "/v1/topics/t/messages?ack=available", bytes.NewBufferString("event"))
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 202 || !bytes.Contains(w.Body.Bytes(), []byte(`"confirmed_copies":1`)) {
		t.Fatalf("single node: %d %s", w.Code, w.Body.String())
	}
}

func TestAvailableDoesNotWaitForOrClaimUnconfirmedPeer(t *testing.T) {
	gate := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/capabilities" {
			w.Header().Set("Spruce-Peer-Version", "2")
			w.WriteHeader(204)
			return
		}
		<-gate
		w.WriteHeader(204)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.Peers = []string{server.URL}
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	b := New(cfg)
	defer b.Close()
	defer close(gate)
	started := time.Now()
	copies := b.replicateAvailable(context.Background(), &Message{ID: "event", Topic: "t", Payload: []byte("event"), Origin: "source", Sequence: 1, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()})
	if copies != 1 || time.Since(started) > time.Second {
		t.Fatalf("unconfirmed slow replica blocked or counted: copies=%d elapsed=%v", copies, time.Since(started))
	}
}

func TestSynchronousReplicationHasBoundedAdmission(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	cfg.Peers = []string{server.URL}
	b := New(cfg)
	defer b.Close()
	p := b.peers[0]
	for range cap(p.copySlots) {
		p.copySlots <- struct{}{}
	}
	m := &Message{ID: "event", Topic: "t", Payload: []byte("event"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if copies := b.replicateAvailable(context.Background(), m); copies != 1 {
		t.Fatalf("busy peer claimed as confirmed: %d", copies)
	}
	if b.replicateOne(context.Background(), m) {
		t.Fatal("strict confirmation bypassed bounded admission")
	}
	if len(p.copySlots) != cap(p.copySlots) {
		t.Fatal("busy confirmation path changed in-flight accounting")
	}
}
