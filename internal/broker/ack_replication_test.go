package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRoutedAckPropagatesOwnerCheckpoint(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
	replica := New(cfg)
	defer replica.Close()
	replicaServer := httptest.NewServer(replica.Handler())
	defer replicaServer.Close()
	cfg.Peers = []string{replicaServer.URL}
	owner := New(cfg)
	defer owner.Close()
	ownerServer := httptest.NewServer(owner.Handler())
	defer ownerServer.Close()
	cfg.Peers = []string{ownerServer.URL, replicaServer.URL}
	ingress := New(cfg)
	defer ingress.Close()
	sub := &subscriber{id: "owner-member", topic: "t", group: "workers", ch: make(chan Delivery, 1)}
	owner.mu.Lock()
	owner.addSubscriberLocked(sub)
	owner.mu.Unlock()
	m := &Message{ID: "completed", Topic: "t", Payload: []byte("event"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	if !owner.sendDelivery(sub, m, 1) {
		t.Fatal("delivery rejected")
	}
	delivery := <-sub.ch
	body, _ := json.Marshal(ackRequest{DeliveryIDs: []string{delivery.DeliveryID}})
	w := httptest.NewRecorder()
	ingress.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/deliveries/ack", bytes.NewReader(body)))
	if w.Code != 204 {
		t.Fatalf("ACK: %d %s", w.Code, w.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		replica.mu.RLock()
		active := replica.checkpointActiveLocked("t", "workers", m.ID, time.Now().UnixMilli())
		unrelated := replica.checkpointActiveLocked("t", "other", m.ID, time.Now().UnixMilli())
		replica.mu.RUnlock()
		if unrelated {
			t.Fatal("completion leaked to unrelated group")
		}
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("routed ACK failed to propagate owner's group checkpoint")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAckCompletesLocallyWhenCheckpointQueueFull(t *testing.T) {
	for _, path := range []string{"/internal/ack", "/v1/deliveries/ack"} {
		t.Run(path, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.PeerToken, cfg.ClusterID = "synthetic-peer", "synthetic-cluster"
			b := New(cfg)
			defer b.Close()
			// No action worker: an unbuffered queue must reject admission.
			b.peers = []*peer{{acks: make(chan actionBatch)}}
			sub := &subscriber{id: "member", topic: "t", group: "workers", ch: make(chan Delivery, 1)}
			b.mu.Lock()
			b.addSubscriberLocked(sub)
			b.mu.Unlock()
			m := &Message{ID: "event", Topic: "t", Payload: []byte("event"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
			if !b.sendDelivery(sub, m, 1) {
				t.Fatal("delivery rejected")
			}
			delivery := <-sub.ch
			body, _ := json.Marshal(ackRequest{DeliveryIDs: []string{delivery.DeliveryID}})
			r := httptest.NewRequest("POST", path, bytes.NewReader(body))
			r.Header.Set("Spruce-Peer-Token", cfg.PeerToken)
			r.Header.Set("Spruce-Cluster-ID", cfg.ClusterID)
			w := httptest.NewRecorder()
			b.Handler().ServeHTTP(w, r)
			if w.Code != 204 {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if len(b.checkpointsForAcks([]string{delivery.DeliveryID})) != 0 {
				t.Fatal("failed replica prevented local completion")
			}
			b.mu.RLock()
			completed := b.checkpointActiveLocked("t", "workers", "event", time.Now().UnixMilli())
			b.mu.RUnlock()
			if !completed || b.metrics.AckActionDropped.Load() == 0 {
				t.Fatal("local completion or propagation-drop evidence missing")
			}
			if b.peers[0].actionBytes.Load() != 0 {
				t.Fatal("failed admission leaked queue bytes")
			}
		})
	}
}

func TestActionBatchBoundsCheckpointCount(t *testing.T) {
	received := make(chan int, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, err := decodeAckRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		received <- len(a.Checkpoints)
		w.WriteHeader(204)
	}))
	defer server.Close()
	b := New(DefaultConfig())
	p := &peer{url: server.URL, acks: make(chan actionBatch, 2)}
	expiry := time.Now().Add(time.Minute).UnixMilli()
	for i := 0; i < 2; i++ {
		checkpoints := make([]groupCheckpoint, 700)
		for j := range checkpoints {
			checkpoints[j] = groupCheckpoint{Topic: "t", Group: "g", MessageID: fmt.Sprintf("%d-%d", i, j), ExpiresAt: expiry}
		}
		p.acks <- actionBatch{checkpoints: checkpoints, bytes: 1}
	}
	p.actionBytes.Store(2)
	done := make(chan struct{})
	go func() { defer close(done); b.actionLoop(p, "ack", p.acks) }()
	defer func() { b.Close(); <-done }()
	for i := 0; i < 2; i++ {
		select {
		case n := <-received:
			if n != 700 {
				t.Fatalf("unexpected checkpoint batch size %d", n)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("checkpoint batching exceeded receiver's bound")
		}
	}
}
