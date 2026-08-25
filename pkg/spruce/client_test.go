package spruce

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNackAdvancesOrderedCursor(t *testing.T) {
	var acks, nacks atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/deliveries/ack":
			acks.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		case "/v1/deliveries/nack":
			nacks.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		for i := 1; i <= 2; i++ {
			metadata, _ := json.Marshal(Delivery{DeliveryID: string(rune('a' + i)), MessageID: "m", Topic: "t", CreatedAt: int64(i), Cursor: fmt.Sprintf("cursor-%d", i)})
			var sizes [8]byte
			binary.BigEndian.PutUint32(sizes[:4], uint32(len(metadata)))
			_, _ = w.Write(sizes[:])
			_, _ = w.Write(metadata)
		}
	}))
	defer server.Close()
	var handled atomic.Int32
	last, err := New(server.URL).subscribeOnce(context.Background(), SubscribeOptions{Topic: "t"}, func(context.Context, Delivery) error {
		if handled.Add(1) == 1 {
			return context.Canceled
		}
		return nil
	})
	if err == nil || last != "cursor-2" || acks.Load() != 1 || nacks.Load() != 1 {
		t.Fatalf("last=%s acks=%d nacks=%d err=%v", last, acks.Load(), nacks.Load(), err)
	}
}

func TestDeduperStaleOrderEntryDoesNotDeleteRefresh(t *testing.T) {
	d := NewDeduper(2, time.Millisecond)
	if d.Seen("a") {
		t.Fatal("new ID reported seen")
	}
	time.Sleep(2 * time.Millisecond)
	if d.Seen("a") {
		t.Fatal("expired ID reported seen")
	}
	if d.Seen("b") {
		t.Fatal("new ID reported seen")
	}
	if !d.Seen("a") {
		t.Fatal("stale order entry deleted refreshed ID")
	}
}

func TestHandlerDrainTimeoutIsTerminal(t *testing.T) {
	var streams atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		streams.Add(1)
		metadata, _ := json.Marshal(Delivery{DeliveryID: "d", MessageID: "m", Topic: "t", CreatedAt: 1})
		var sizes [8]byte
		binary.BigEndian.PutUint32(sizes[:4], uint32(len(metadata)))
		_, _ = w.Write(sizes[:])
		_, _ = w.Write(metadata)
	}))
	defer server.Close()
	block := make(chan struct{})
	err := New(server.URL).Subscribe(context.Background(), SubscribeOptions{Topic: "t", DrainTimeout: 10 * time.Millisecond}, func(context.Context, Delivery) error { <-block; return nil })
	close(block)
	if !errors.Is(err, ErrHandlerDrainTimeout) || streams.Load() != 1 {
		t.Fatalf("err=%v streams=%d", err, streams.Load())
	}
}

func TestHandlerDrainTimeoutAfterTruncatedFrameIsTerminal(t *testing.T) {
	var streams atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		streams.Add(1)
		metadata, _ := json.Marshal(Delivery{DeliveryID: "d", MessageID: "m", Topic: "t", CreatedAt: 1})
		var sizes [8]byte
		binary.BigEndian.PutUint32(sizes[:4], uint32(len(metadata)))
		_, _ = w.Write(sizes[:])
		_, _ = w.Write(metadata)
		_, _ = w.Write([]byte{1})
	}))
	defer server.Close()
	block := make(chan struct{})
	err := New(server.URL).Subscribe(context.Background(), SubscribeOptions{Topic: "t", DrainTimeout: 10 * time.Millisecond}, func(context.Context, Delivery) error { <-block; return nil })
	close(block)
	if !errors.Is(err, ErrHandlerDrainTimeout) || streams.Load() != 1 {
		t.Fatalf("err=%v streams=%d", err, streams.Load())
	}
}

func TestDefaultSubscribeBatchesAcks(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/deliveries/ack" {
			calls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.spruce.stream")
		for i := 0; i < 32; i++ {
			metadata, _ := json.Marshal(Delivery{DeliveryID: string(rune('a' + i)), MessageID: "m", Topic: "t", CreatedAt: int64(i + 1), Cursor: fmt.Sprintf("cursor-%d", i+1)})
			var sizes [8]byte
			binary.BigEndian.PutUint32(sizes[:4], uint32(len(metadata)))
			_, _ = w.Write(sizes[:])
			_, _ = w.Write(metadata)
		}
	}))
	defer server.Close()
	c := New(server.URL)
	last, err := c.subscribeOnce(context.Background(), SubscribeOptions{Topic: "t"}, func(context.Context, Delivery) error { return nil })
	if err == nil {
		t.Fatal("expected stream EOF")
	}
	if last != "cursor-32" {
		t.Fatalf("cursor=%s", last)
	}
	if calls.Load() >= 32 {
		t.Fatalf("default subscription did not batch acknowledgements: %d requests", calls.Load())
	}
}

func TestSubscribeBoundsOrderedCompletionWindow(t *testing.T) {
	const concurrency = 4
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/deliveries/") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		for i := 0; i < 64; i++ {
			metadata, _ := json.Marshal(Delivery{DeliveryID: fmt.Sprintf("d-%d", i), MessageID: "m", Topic: "t", Cursor: fmt.Sprintf("cursor-%d", i)})
			var sizes [8]byte
			binary.BigEndian.PutUint32(sizes[:4], uint32(len(metadata)))
			_, _ = w.Write(sizes[:])
			_, _ = w.Write(metadata)
		}
	}))
	defer server.Close()

	block := make(chan struct{})
	var started atomic.Int32
	done := make(chan error, 1)
	go func() {
		_, err := New(server.URL).subscribeOnce(context.Background(), SubscribeOptions{Topic: "t", Concurrency: concurrency, DrainTimeout: 20 * time.Millisecond}, func(_ context.Context, delivery Delivery) error {
			started.Add(1)
			if delivery.DeliveryID == "d-0" {
				<-block
			}
			return nil
		})
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if got := started.Load(); got > concurrency {
		t.Fatalf("ordered completion window admitted %d handlers with capacity %d", got, concurrency)
	}
	close(block)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscription did not finish after head handler completed")
	}
}

func TestAckBatcherCoalescesConcurrentAcks(t *testing.T) {
	var calls atomic.Int32
	var received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body struct {
			IDs []string `json:"delivery_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received.Add(int32(len(body.IDs)))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	b := newAckBatcher(ctx, New(server.URL), "ack")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := b.submit(ctx, id); err != nil {
				t.Error(err)
			}
		}(string(rune('a' + i)))
	}
	wg.Wait()
	cancel()
	if received.Load() != 32 {
		t.Fatalf("received %d acknowledgements", received.Load())
	}
	if calls.Load() >= 32 {
		t.Fatalf("acknowledgements were not batched: %d requests", calls.Load())
	}
}

func TestBasicAuthStructuredErrorAndDiagnostics(t *testing.T) {
	var basicOK bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		basicOK = ok && username == "u" && password == "p"
		if r.URL.Path == "/v1/status" {
			_ = json.NewEncoder(w).Encode(Status{Messages: 2, Peers: 1})
			return
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"idempotency_conflict"}`))
	}))
	defer server.Close()
	c := New(server.URL)
	c.HTTP = server.Client()
	c.Username, c.Password = "u", "p"
	status, err := c.Status(context.Background())
	if err != nil || status.Messages != 2 || !basicOK {
		t.Fatalf("status=%+v basic=%v err=%v", status, basicOK, err)
	}
	_, err = c.Publish(context.Background(), "t", []byte("x"), PublishOptions{})
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Code != "idempotency_conflict" {
		t.Fatalf("err=%v", err)
	}
}

func TestPublishRetryRequiresIdempotency(t *testing.T) {
	_, err := New("http://unused").PublishRetry(context.Background(), "t", nil, PublishOptions{}, RetryOptions{})
	if err == nil {
		t.Fatal("retry accepted unsafe publish")
	}
}

func TestPublishRetryAndTelemetry(t *testing.T) {
	var calls, events atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"peer_ack_unavailable"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"id","replicated":true}`))
	}))
	defer server.Close()
	c := New(server.URL)
	c.OnEvent = func(ClientEvent) { events.Add(1) }
	result, err := c.PublishRetry(context.Background(), "t", []byte("x"), PublishOptions{ProducerID: "p", IdempotencyKey: "1"}, RetryOptions{MaxAttempts: 2, MinBackoff: time.Millisecond})
	if err != nil || result.ID != "id" || calls.Load() != 2 || events.Load() != 2 {
		t.Fatalf("result=%+v calls=%d events=%d err=%v", result, calls.Load(), events.Load(), err)
	}
}

func TestPublishRetryHonorsRetryAfterAsMinimum(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"id":"id"}`))
	}))
	defer server.Close()
	started := time.Now()
	result, err := New(server.URL).PublishRetry(context.Background(), "t", nil, PublishOptions{ProducerID: "p", IdempotencyKey: "1"}, RetryOptions{MaxAttempts: 2, MinBackoff: time.Millisecond, MaxBackoff: 25 * time.Millisecond})
	if err != nil || result.ID != "id" || time.Since(started) < 20*time.Millisecond {
		t.Fatalf("result=%+v elapsed=%s err=%v", result, time.Since(started), err)
	}
}

func TestSubscribeRetriesTransientStatusAndHonorsRetryAfter(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	started := time.Now()
	err := New(server.URL).Subscribe(context.Background(), SubscribeOptions{Topic: "t"}, func(context.Context, Delivery) error { return nil })
	if calls.Load() != 2 || time.Since(started) < time.Second || err == nil {
		t.Fatalf("calls=%d elapsed=%s err=%v", calls.Load(), time.Since(started), err)
	}
}

func TestSubscribeEmitsCursorExpiredAndDisconnected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"cursor_expired"}`))
	}))
	defer server.Close()
	var events []ClientEvent
	c := New(server.URL)
	c.OnEvent = func(event ClientEvent) { events = append(events, event) }
	err := c.Subscribe(context.Background(), SubscribeOptions{Topic: "t", Cursor: "expired"}, func(context.Context, Delivery) error { return nil })
	if err == nil || len(events) < 2 || events[len(events)-2].Operation != "subscription_cursor_expired" || events[len(events)-1].Operation != "subscription_disconnected" || events[len(events)-1].StatusCode != http.StatusConflict {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}

func TestConsumableDeliveryControlsAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var acked atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/deliveries/ack" {
			acked.Add(1)
			w.WriteHeader(http.StatusNoContent)
			cancel()
			return
		}
		metadata, _ := json.Marshal(Delivery{DeliveryID: "d", MessageID: "m", Topic: "t", CreatedAt: 1})
		var sizes [8]byte
		binary.BigEndian.PutUint32(sizes[:4], uint32(len(metadata)))
		_, _ = w.Write(sizes[:])
		_, _ = w.Write(metadata)
	}))
	defer server.Close()
	deliveries, _ := New(server.URL).Deliveries(ctx, SubscribeOptions{Topic: "t"})
	item := <-deliveries
	if acked.Load() != 0 {
		t.Fatal("delivery acknowledged before completion")
	}
	item.Complete(nil)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("acknowledgement was not sent")
	}
	if acked.Load() != 1 {
		t.Fatalf("acks=%d", acked.Load())
	}
}

func TestCredentialsRejectHTTPSDowngradeRedirect(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Fatal("credential reached plaintext redirect target")
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer httpServer.Close()
	httpsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, httpServer.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer httpsServer.Close()
	c := New(httpsServer.URL)
	c.HTTP = httpsServer.Client()
	c.Token = "secret"
	if _, err := c.Publish(context.Background(), "t", []byte("x"), PublishOptions{}); err == nil {
		t.Fatal("HTTPS credential downgrade redirect succeeded")
	}
}

func TestHandlerPanicReturnsErrorInsteadOfCrashing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/deliveries/nack" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		metadata, _ := json.Marshal(Delivery{DeliveryID: "d", MessageID: "m", Topic: "t", CreatedAt: 1})
		var sizes [8]byte
		binary.BigEndian.PutUint32(sizes[:4], uint32(len(metadata)))
		_, _ = w.Write(sizes[:])
		_, _ = w.Write(metadata)
	}))
	defer server.Close()
	err := New(server.URL).Subscribe(context.Background(), SubscribeOptions{Topic: "t"}, func(context.Context, Delivery) error { panic("boom") })
	if err == nil || !strings.Contains(err.Error(), "handler panic") {
		t.Fatalf("err=%v", err)
	}
}

func TestSubscriptionMemberIdentityBoundsAndReconnectStability(t *testing.T) {
	if got, err := subscriptionMemberID(strings.Repeat("x", 255)); err != nil || len(got) != 255 {
		t.Fatalf("255-byte identity: got=%q err=%v", got, err)
	}
	if _, err := subscriptionMemberID(strings.Repeat("é", 128)); err == nil {
		t.Fatal("256-byte UTF-8 identity was accepted")
	}
	generated, err := subscriptionMemberID("")
	if err != nil || len(generated) != 32 {
		t.Fatalf("generated identity=%q err=%v", generated, err)
	}
	var mu sync.Mutex
	var members []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		members = append(members, r.URL.Query().Get("member"))
		attempt := len(members)
		mu.Unlock()
		if attempt < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	err = New(server.URL).Subscribe(context.Background(), SubscribeOptions{Topic: "t"}, func(context.Context, Delivery) error { return nil })
	if err == nil || len(members) != 3 || members[0] == "" || members[0] != members[1] || members[1] != members[2] {
		t.Fatalf("members=%v err=%v", members, err)
	}
}
