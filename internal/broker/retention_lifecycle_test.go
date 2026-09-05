package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTruncatedV2BatchRejectsWholeRequest(t *testing.T) {
	for _, key := range []string{"", "unfinished"} {
		t.Run(key, func(t *testing.T) {
			b := New(DefaultConfig())
			defer b.Close()
			body := []byte{0, 0, 0, 0, 0, 1, 'x'}
			body = binary.BigEndian.AppendUint16(body, uint16(len(key)))
			body = append(body, key...)
			r := httptest.NewRequest("POST", "/v1/topics/t/batches", bytes.NewReader(body))
			r.Header.Set("Spruce-Batch-Version", "2")
			w := httptest.NewRecorder()
			b.Handler().ServeHTTP(w, r)
			if w.Code != 400 || len(b.cache.snapshot("t", 0)) != 0 {
				t.Fatalf("truncated batch admitted: status=%d cache=%d", w.Code, len(b.cache.snapshot("t", 0)))
			}
		})
	}
}

func TestCheckpointRefreshHistoryRemainsBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CheckpointEntries = 8
	b := New(cfg)
	defer b.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now().UnixMilli()
	for i := range 1000 {
		b.putCheckpointLocked(groupCheckpoint{Topic: "t", Group: "g", MessageID: "m", ExpiresAt: now + int64(i) + 1}, now)
	}
	if len(b.checkpointOrder) > 2*cfg.CheckpointEntries {
		t.Fatalf("one checkpoint retained %d history entries", len(b.checkpointOrder))
	}
	if !b.checkpointActiveLocked("t", "g", "m", now+999) {
		t.Fatal("latest completion lost")
	}
}

func TestIdempotencyRefreshHistoryRemainsBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IdempotencyEntries = 8
	b := New(cfg)
	defer b.Close()
	for range 1000 {
		e, owner, err := b.beginIdempotency(context.Background(), "operation", "stable-id", time.Now().Add(-time.Second).UnixMilli(), [32]byte{})
		if err != nil || !owner {
			t.Fatalf("refresh: %v owner=%v", err, owner)
		}
		b.finishIdempotency("operation", e, true, false)
	}
	if len(b.idempotencyOrder) > 2*cfg.IdempotencyEntries {
		t.Fatalf("one operation retained %d history entries", len(b.idempotencyOrder))
	}
}

func TestReceiveFrontierExpiresWithRetainedHistory(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	now := time.Now()
	for i := range 100 {
		m := &Message{ID: string(rune(i + 100)), Topic: "t", Origin: string(rune(i + 100)), Sequence: 1, ExpiresAt: now.Add(time.Second).UnixMilli()}
		if err := b.acceptBatch([]*Message{m}); err != nil {
			t.Fatal(err)
		}
	}
	b.cache.mu.Lock()
	defer b.cache.mu.Unlock()
	b.cache.expireLocked(now.Add(2 * time.Second).UnixMilli())
	if len(b.cache.receivedThrough) != 0 {
		t.Fatalf("expired origin history retained: %d", len(b.cache.receivedThrough))
	}
}

func TestLongLivedCursorReclaimsExpiredIncarnations(t *testing.T) {
	c := newStreamCursor(map[string]uint64{}, nil)
	for i := range 1000 {
		if c.advance(Delivery{origin: fmt.Sprint(i), sequence: 1, expiresAt: int64(i) + 1}, int64(i)) == "" {
			t.Fatal("expired history exhausted stream cursor")
		}
	}
	if len(c.sequences) != 1 {
		t.Fatalf("expired cursor history retained: %d", len(c.sequences))
	}
}

func TestCursorNeverDiscardsLiveOrUnknownHistory(t *testing.T) {
	c := newStreamCursor(map[string]uint64{"unknown": 9, "live": 5}, map[string]int64{"live": 100})
	c.advance(Delivery{origin: "new", sequence: 1, expiresAt: 100}, 50)
	if c.sequences["unknown"] != 9 || c.sequences["live"] != 5 {
		t.Fatal("cursor discarded unexpired or unknown completion history")
	}
}

func TestCursorNewDeliveryCannotProveUnknownHistoryExpired(t *testing.T) {
	c := newStreamCursor(map[string]uint64{"unknown": 9}, nil)
	c.advance(Delivery{origin: "unknown", sequence: 10, expiresAt: 2}, 1)
	c.advance(Delivery{origin: "new", sequence: 1, expiresAt: 10}, 3)
	if c.sequences["unknown"] != 10 {
		t.Fatal("new short TTL erased unknown predecessor expiry")
	}
}

func FuzzBatchV2Truncation(f *testing.F) {
	f.Add([]byte("opaque"), uint16(3))
	f.Add([]byte{0, 255, 1}, uint16(10))
	f.Fuzz(func(t *testing.T, payload []byte, cut uint16) {
		if len(payload) > 1024 {
			return
		}
		entry := binary.BigEndian.AppendUint16(nil, 1)
		entry = append(entry, 'k')
		entry = binary.BigEndian.AppendUint32(entry, uint32(len(payload)))
		entry = append(entry, payload...)
		body := append(bytes.Clone(entry), entry...)
		end := int(cut) % (len(body) + 1)
		b := New(DefaultConfig())
		defer b.Close()
		r := httptest.NewRequest("POST", "/v1/topics/t/batches", bytes.NewReader(body[:end]))
		r.Header.Set("Spruce-Batch-Version", "2")
		w := httptest.NewRecorder()
		b.Handler().ServeHTTP(w, r)
		messages := len(b.cache.snapshot("t", 0))
		if end == len(entry) || end == len(body) {
			if w.Code != 202 || messages != end/len(entry) {
				t.Fatalf("valid batch rejected: %d", w.Code)
			}
		} else if w.Code == 202 || messages != 0 {
			t.Fatalf("partial entry admitted: cut=%d status=%d cache=%d", end, w.Code, messages)
		}
	})
}

type batchReadFailure struct{}

func (batchReadFailure) Read([]byte) (int, error) {
	return 0, errors.New("synthetic truncated transport")
}

func TestFullBatchWithTransportErrorRejectsWholeRequest(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	r := httptest.NewRequest("POST", "/v1/topics/t/batches", io.MultiReader(bytes.NewReader(make([]byte, maxBatchMessages*4)), batchReadFailure{}))
	w := httptest.NewRecorder()
	b.Handler().ServeHTTP(w, r)
	if w.Code != 400 || len(b.cache.snapshot("t", 0)) != 0 {
		t.Fatalf("transport error admitted batch: %d", w.Code)
	}
}
