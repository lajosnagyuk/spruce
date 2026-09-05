package spruce

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestProducerBatcherFlushesByCountAndCopiesPayload(t *testing.T) {
	var mu sync.Mutex
	requests, messages := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		ids := []string{}
		for {
			var keySize [2]byte
			if _, err := io.ReadFull(r.Body, keySize[:]); err != nil {
				break
			}
			key := make([]byte, binary.BigEndian.Uint16(keySize[:]))
			_, _ = io.ReadFull(r.Body, key)
			var size [4]byte
			if _, err := io.ReadFull(r.Body, size[:]); err != nil {
				break
			}
			n := binary.BigEndian.Uint32(size[:])
			payload := make([]byte, n)
			_, _ = io.ReadFull(r.Body, payload)
			if string(payload) != "owned" {
				t.Errorf("payload mutated: %q", payload)
			}
			ids = append(ids, "id")
			mu.Lock()
			messages++
			mu.Unlock()
		}
		_ = json.NewEncoder(w).Encode(BatchResult{IDs: ids})
	}))
	defer server.Close()
	b := NewProducerBatcher(New(server.URL), BatcherOptions{MaxMessages: 4, MaxDelay: time.Second})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := []byte("owned")
			_, err := b.Publish(context.Background(), "topic", payload, PublishOptions{})
			payload[0] = 'X'
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	_ = b.Close(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if requests != 1 || messages != 4 {
		t.Fatalf("requests=%d messages=%d", requests, messages)
	}
}

func TestProducerBatcherCoalescesDistinctKeys(t *testing.T) {
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for {
			var kn [2]byte
			if _, err := io.ReadFull(r.Body, kn[:]); err != nil {
				break
			}
			key := make([]byte, binary.BigEndian.Uint16(kn[:]))
			_, _ = io.ReadFull(r.Body, key)
			keys = append(keys, string(key))
			var pn [4]byte
			_, _ = io.ReadFull(r.Body, pn[:])
			_, _ = io.CopyN(io.Discard, r.Body, int64(binary.BigEndian.Uint32(pn[:])))
		}
		_ = json.NewEncoder(w).Encode(BatchResult{IDs: []string{"1", "2"}})
	}))
	defer server.Close()
	b := NewProducerBatcher(New(server.URL), BatcherOptions{MaxMessages: 2, MaxDelay: time.Second})
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = b.Publish(context.Background(), "t", []byte("x"), PublishOptions{Key: key})
		}()
	}
	wg.Wait()
	_ = b.Close(context.Background())
	if len(keys) != 2 || (keys[0] != "a" && keys[0] != "b") || keys[0] == keys[1] {
		t.Fatalf("keys=%v", keys)
	}
}

func TestProducerBatcherAccountsForV2KeyAtMaxBytesBoundary(t *testing.T) {
	b := NewProducerBatcher(New("http://spruce.invalid"), BatcherOptions{MaxBytes: 10})
	defer b.Close(context.Background())
	if _, err := b.Publish(context.Background(), "t", []byte("xx"), PublishOptions{Key: "key"}); err == nil {
		t.Fatal("accepted 11-byte v2 entry into 10-byte batch")
	}
}

func TestProducerBatcherFlushBarrierAndTimer(t *testing.T) {
	requests := make(chan time.Time, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		_ = json.NewEncoder(w).Encode(BatchResult{IDs: []string{"id"}})
	}))
	defer server.Close()
	b := NewProducerBatcher(New(server.URL), BatcherOptions{MaxDelay: 10 * time.Millisecond})
	started := time.Now()
	done := make(chan error, 1)
	go func() { _, err := b.Publish(context.Background(), "topic", []byte("x"), PublishOptions{}); done <- err }()
	select {
	case at := <-requests:
		if at.Sub(started) < 5*time.Millisecond {
			t.Fatal("timer flushed too early")
		}
	case <-time.After(time.Second):
		t.Fatal("timer did not flush")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestProducerBatcherRejectsShortResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(BatchResult{})
	}))
	defer server.Close()
	b := NewProducerBatcher(New(server.URL), BatcherOptions{MaxMessages: 1})
	if _, err := b.Publish(context.Background(), "topic", []byte("x"), PublishOptions{}); err == nil {
		t.Fatal("short batch result succeeded")
	}
	_ = b.Close(context.Background())
}

func TestProducerBatcherCloseCancelsStalledFlush(t *testing.T) {
	started := make(chan struct{})
	client := New("http://spruce.invalid")
	client.HTTP = &http.Client{Transport: contextTransport{started: started}}
	b := NewProducerBatcher(client, BatcherOptions{MaxMessages: 1})
	go b.Publish(context.Background(), "topic", []byte("x"), PublishOptions{})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error=%v", err)
	}
	select {
	case <-b.done:
	case <-time.After(time.Second):
		t.Fatal("worker remained blocked after close cancellation")
	}
}

type contextTransport struct{ started chan struct{} }

func (t contextTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	close(t.started)
	<-r.Context().Done()
	return nil, r.Context().Err()
}

func TestProducerBatcherAfterCloseNeverWaitsForAbsentWorker(t *testing.T) {
	b := NewProducerBatcher(New("http://spruce.invalid"), BatcherOptions{})
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := b.Publish(ctx, "t", []byte("x"), PublishOptions{})
		cancel()
		if !errors.Is(err, ErrBatcherClosed) {
			t.Fatalf("closed publish waited: %v", err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
		err = b.Flush(ctx)
		cancel()
		if !errors.Is(err, ErrBatcherClosed) {
			t.Fatalf("closed flush waited: %v", err)
		}
	}
}

func TestConcurrentBatcherCloseHonoursEachDeadline(t *testing.T) {
	started := make(chan struct{})
	client := New("http://spruce.invalid")
	client.HTTP = &http.Client{Transport: contextTransport{started: started}}
	b := NewProducerBatcher(client, BatcherOptions{MaxMessages: 1})
	go b.Publish(context.Background(), "t", []byte("x"), PublishOptions{})
	<-started
	first := make(chan error, 1)
	go func() { first <- b.Close(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close deadline: %v", err)
	}
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("concurrent close stranded")
	}
}
