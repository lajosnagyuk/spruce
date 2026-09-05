package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStreamMemoryAdmissionAndRelease(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StreamMemoryBytes = 2 * streamMemoryReservation
	b := New(cfg)
	defer b.Close()
	server := httptest.NewServer(b.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 2 {
		r, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/v1/subscriptions/stream?topic=t", nil)
		resp, err := server.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("stream status=%d", resp.StatusCode)
		}
	}
	r := httptest.NewRequest("GET", "/v1/subscriptions/stream?topic=t", nil)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("exhausted stream memory: %d %s", w.Code, w.Body.String())
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for b.streamMemoryBytes.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if used := b.streamMemoryBytes.Load(); used != 0 {
		t.Fatalf("disconnected streams retain %d reserved bytes", used)
	}
}

func TestReplayIndexAdmissionReleasesReservationOnRejection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StreamMemoryBytes = streamMemoryReservation
	b := New(cfg)
	defer b.Close()
	_, err := b.accept(&Message{ID: "retained", Topic: "t", Payload: []byte("event"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/v1/subscriptions/stream?topic=t", nil)
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 429 || b.streamMemoryBytes.Load() != 0 || len(b.subs) != 0 {
		t.Fatalf("replay rejection leaked state: status=%d memory=%d subscribers=%d", w.Code, b.streamMemoryBytes.Load(), len(b.subs))
	}
}

func TestReplayIDsDoNotKeepEvictedMessagesDeliverable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheBytes = 1024
	b := New(cfg)
	defer b.Close()
	for _, id := range []string{"old", "new"} {
		m := &Message{ID: id, Topic: "t", Payload: make([]byte, 600), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
		if _, err := b.accept(m); err != nil {
			t.Fatal(err)
		}
		if id == "old" {
			b.cache.mu.Lock()
			ids := b.cache.replayIDsLocked("t", nil, false, 0)
			b.cache.mu.Unlock()
			if len(ids) != 1 || ids[0] != id {
				t.Fatalf("unexpected replay IDs: %v", ids)
			}
		}
	}
	if b.replayMessage("old") != nil || b.replayMessage("new") == nil {
		t.Fatal("replay should stop at an evicted ID instead of retaining its payload")
	}
}
