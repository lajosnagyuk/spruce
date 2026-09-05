package broker

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"io"
	"time"
)

// Stable identity allows replicas and clients to recognise the same logical
// publish after routing changes. Producers must never reuse an operation key
// for a different event. This does not provide cross-broker conflict consensus.
func idempotentMessageID(topic, producer, key string) string {
	h := sha256.New()
	_, _ = io.WriteString(h, "spruce-idempotency-v1")
	for _, value := range []string{topic, producer, key} {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = io.WriteString(h, value)
	}
	return "i" + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

func (b *Broker) acceptIdempotent(m *Message, ttl time.Duration) (*Message, bool, error) {
	b.cache.mu.Lock()
	defer b.cache.mu.Unlock()
	b.cache.expireLocked(time.Now().UnixMilli())
	if existing := b.cache.items[m.ID]; existing != nil {
		if existing.Topic != m.Topic || existing.Key != m.Key || existing.Headers["content-type"] != m.Headers["content-type"] || !bytes.Equal(existing.Payload, m.Payload) || existing.ExpiresAt-existing.CreatedAt != ttl.Milliseconds() {
			return nil, false, errIdempotencyConflict
		}
		// Preserve both the original expiry and origin/sequence on replication.
		return existing, false, nil
	}
	inserted, err := b.acceptLocked(m)
	return m, inserted, err
}
