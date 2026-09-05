package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Available acknowledgement favours bounded latency and continued local service.
// It never converts a requested one-peer acknowledgement into a local one.
func (b *Broker) publishAvailable(w http.ResponseWriter, r *http.Request, m *Message, duplicate bool, entry *idempotencyEntry, cacheKey string) {
	copies := b.replicateAvailable(r.Context(), m)
	if entry != nil {
		b.finishIdempotency(cacheKey, entry, true, copies > 1)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": m.ID, "replicated": copies > 1, "confirmed_copies": copies, "degraded": copies < len(b.peers)+1, "deduplicated": duplicate})
}

func (b *Broker) replicateAvailable(parent context.Context, m *Message) int {
	ctx, cancel := context.WithTimeout(parent, 100*time.Millisecond)
	defer cancel()
	result := make(chan *peer, len(b.peers))
	attempted := 0
	for _, p := range b.peers {
		// Background replication still retries these peers and clears the cooldown
		// on success. One dead peer must not impose a timeout on every publication.
		if p.unavailableUntil.Load() > time.Now().UnixNano() {
			continue
		}
		select {
		case p.copySlots <- struct{}{}:
		default:
			continue
		}
		attempted++
		go func(p *peer) {
			defer func() { <-p.copySlots }()
			if b.sendPeer(ctx, p, m) {
				result <- p
			} else {
				result <- nil
			}
		}(p)
	}
	confirmed := make([]*peer, 0, attempted)
	for range attempted {
		select {
		case p := <-result:
			if p != nil {
				confirmed = append(confirmed, p)
			}
		case <-ctx.Done():
			b.enqueuePeerBatchExcept([]*Message{m}, nil, confirmed...)
			return 1 + len(confirmed)
		}
	}
	b.enqueuePeerBatchExcept([]*Message{m}, nil, confirmed...)
	return 1 + len(confirmed)
}
