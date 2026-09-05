package broker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestGroupMemberCursorDoesNotSkipOtherMembersUnfinishedWork(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	expiry := time.Now().Add(time.Minute).UnixMilli()
	for _, id := range []string{"unfinished", "completed"} {
		if _, err := b.accept(&Message{ID: id, Topic: "t", Origin: "source", Sequence: map[string]uint64{"unfinished": 1, "completed": 2}[id], ExpiresAt: expiry}); err != nil {
			t.Fatal(err)
		}
	}
	b.applyCheckpoints([]groupCheckpoint{{Topic: "t", Group: "g", MessageID: "completed", ExpiresAt: expiry}})
	server := httptest.NewServer(b.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/v1/subscriptions/stream?topic=t&group=g&cursor="+url.QueryEscape(encodeReplayCursor(map[string]uint64{"source": 2})), nil)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("stream status %d", resp.StatusCode)
	}
	var sizes [8]byte
	if _, err = io.ReadFull(resp.Body, sizes[:]); err != nil {
		t.Fatal(err)
	}
	meta := make([]byte, binary.BigEndian.Uint32(sizes[:4]))
	if _, err = io.ReadFull(resp.Body, meta); err != nil {
		t.Fatal(err)
	}
	var d Delivery
	if err = json.Unmarshal(meta, &d); err != nil {
		t.Fatal(err)
	}
	if d.MessageID != "unfinished" {
		t.Fatalf("group replay skipped another member's unacknowledged event: %+v", d)
	}
}

func TestLiveMemberCannotBypassGroupReplay(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	owner := &subscriber{id: "owner", topic: "t", group: "g", replaying: true, ch: make(chan Delivery, 128)}
	live := &subscriber{id: "live", topic: "t", group: "g", ch: make(chan Delivery, 128)}
	b.mu.Lock()
	b.addSubscriberLocked(owner)
	b.addSubscriberLocked(live)
	b.mu.Unlock()
	for i := range 100 {
		b.deliver(&Message{ID: fmt.Sprint(i), Key: fmt.Sprint(i), Topic: "t", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}, "", 1)
	}
	b.mu.RLock()
	count := len(b.groupWork[checkpointScope{topic: "t", group: "g"}].work)
	b.mu.RUnlock()
	if count != 100 || len(live.ch) != 0 || len(owner.ch) != 0 {
		t.Fatalf("new work overtook group replay: deferred=%d live=%d owner=%d", len(owner.deferred), len(live.ch), len(owner.ch))
	}
}

// Inject publication after the replay snapshot, exactly when the stream becomes
// visible to its client. Deferred replay must flush without a subsequent event.
type replayFlushWriter struct {
	http.ResponseWriter
	once    bool
	onFlush func()
}

func (w *replayFlushWriter) Flush() {
	if !w.once {
		w.once = true
		w.onFlush()
	}
	w.ResponseWriter.(http.Flusher).Flush()
}
func TestDeferredReplayFlushesWithoutMoreTraffic(t *testing.T) {
	b := New(DefaultConfig())
	defer b.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.Handler().ServeHTTP(&replayFlushWriter{ResponseWriter: w, onFlush: func() {
			_, err := b.accept(&Message{ID: "deferred", Topic: "t", Payload: []byte("opaque"), ExpiresAt: time.Now().Add(time.Minute).UnixMilli()})
			if err != nil {
				t.Error(err)
			}
		}}, r)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/v1/subscriptions/stream?topic=t", nil)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sizes [8]byte
	if _, err = io.ReadFull(resp.Body, sizes[:]); err != nil {
		t.Fatalf("deferred event remained buffered: %v", err)
	}
	meta := make([]byte, binary.BigEndian.Uint32(sizes[:4]))
	if _, err = io.ReadFull(resp.Body, meta); err != nil {
		t.Fatal(err)
	}
	var d Delivery
	if err = json.Unmarshal(meta, &d); err != nil {
		t.Fatal(err)
	}
	if d.MessageID != "deferred" {
		t.Fatalf("unexpected delivery: %+v", d)
	}
}
