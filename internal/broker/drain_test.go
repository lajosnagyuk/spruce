package broker

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDrainClosesStreamsRejectsNewWorkAndAllowsCompletion(t *testing.T) {
	b := New(DefaultConfig())
	b.Ready()
	defer b.Close()
	server := httptest.NewServer(b.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/v1/subscriptions/stream?topic=t", nil)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("stream: %d", resp.StatusCode)
	}
	b.BeginDrain()
	if _, err = io.ReadAll(resp.Body); err != nil {
		t.Fatalf("stream did not close cleanly: %v", err)
	}
	for _, path := range []string{"/v1/topics/t/messages", "/v1/topics/t/batches", "/v1/subscriptions/stream?topic=t"} {
		method := "POST"
		if path == "/v1/subscriptions/stream?topic=t" {
			method = "GET"
		}
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, httptest.NewRequest(method, path, bytes.NewBufferString("event")))
		if w.Code != 503 {
			t.Fatalf("new work during drain: %s status=%d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/deliveries/ack", bytes.NewBufferString(`{"delivery_ids":["completed"]}`)))
	if w.Code != 204 {
		t.Fatalf("drain rejected completion: %d", w.Code)
	}
	if b.streamMemoryBytes.Load() != 0 {
		t.Fatal("drained stream retained memory reservation")
	}
}
