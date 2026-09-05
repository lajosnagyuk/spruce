package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIdempotencyAcrossReplicatedBrokers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PeerToken, cfg.ClusterID = "synthetic-review-token", "synthetic-review-cluster"
	a, b := New(cfg), New(cfg)
	defer a.Close()
	defer b.Close()
	publish := func(b *Broker) string {
		r := httptest.NewRequest("POST", "/v1/topics/review/messages", bytes.NewBufferString("event"))
		r.Header.Set("Spruce-Producer-ID", "synthetic-producer")
		r.Header.Set("Spruce-Idempotency-Key", "synthetic-operation")
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, r)
		if w.Code != 202 {
			t.Fatalf("publish: %d %s", w.Code, w.Body.String())
		}
		var result struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result.ID
	}
	first := publish(a)
	if publish(a) != first {
		t.Fatal("same-broker retry did not deduplicate")
	}
	var wire bytes.Buffer
	if err := writePeerBatch(&wire, a.cache.snapshot("review", 0)); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/internal/replicate", &wire)
	r.Header.Set("Spruce-Peer-Token", cfg.PeerToken)
	r.Header.Set("Spruce-Cluster-ID", cfg.ClusterID)
	r.Header.Set("Spruce-Peer-Version", "2")
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatalf("replicate: %d %s", w.Code, w.Body.String())
	}
	second := publish(b)
	if second != first {
		t.Fatal("cross-broker retry created a second message ID")
	}
	if got := len(b.cache.snapshot("review", 0)); got != 1 {
		t.Fatalf("expected one logical message, got %d", got)
	}
}

func TestReplicaRetryPreservesExpiryAndRejectsConflict(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	now := time.Now().UnixMilli()
	id := idempotentMessageID("t", "producer", "operation")
	original := &Message{ID: id, Topic: "t", Key: "key", Payload: []byte("event"), Origin: "source", Sequence: 1, CreatedAt: now - 10000, ExpiresAt: now + 50000}
	if err := b.acceptReplicatedBatch([]*Message{original}); err != nil {
		t.Fatal(err)
	}
	retry := &Message{ID: id, Topic: "t", Key: "key", Payload: []byte("event"), CreatedAt: now, ExpiresAt: now + 60000}
	got, inserted, err := b.acceptIdempotent(retry, time.Minute)
	if err != nil || inserted || got.ExpiresAt != original.ExpiresAt || got.Origin != original.Origin || got.Sequence != original.Sequence {
		t.Fatalf("replica retry renewed or changed the original event: got=%+v inserted=%v err=%v", got, inserted, err)
	}
	retry.Payload = []byte("different event")
	if _, _, err := b.acceptIdempotent(retry, time.Minute); !errors.Is(err, errIdempotencyConflict) {
		t.Fatalf("replica accepted conflicting payload: %v", err)
	}
}

func TestIdempotencyIdentitySeparatesFields(t *testing.T) {
	if idempotentMessageID("t", "a\x00b", "c") == idempotentMessageID("t", "a", "b\x00c") {
		t.Fatal("producer and operation field boundaries collided")
	}
	if idempotentMessageID("t", "p", "k") == idempotentMessageID("other", "p", "k") {
		t.Fatal("topic namespace was ignored")
	}
}
