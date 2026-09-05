package broker

import (
	"sort"
	"time"
)

// Includes the fixed delivery channel, write buffer, cursor maps and framing
// scratch space. Replay indexes are reserved separately before allocation.
const streamMemoryReservation int64 = 256 << 10

func (b *Broker) reserveStreamMemory(bytes int64) bool {
	for {
		used := b.streamMemoryBytes.Load()
		if bytes < 0 || bytes > b.cfg.StreamMemoryBytes-used {
			return false
		}
		if b.streamMemoryBytes.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

// replayIDsLocked intentionally retains only IDs. A slow replay must not pin
// evicted payloads outside the cache and pending-delivery byte budgets.
func (c *cache) replayIDsLocked(topic string, cursor map[string]uint64, legacy bool, since int64) []string {
	ids := make([]string, 0, len(c.topics[topic]))
	for _, m := range c.topics[topic] {
		if m != nil && ((!legacy && m.Sequence > cursor[m.Origin]) || (legacy && m.CreatedAt >= since)) {
			ids = append(ids, m.ID)
		}
	}
	if legacy {
		sort.SliceStable(ids, func(i, j int) bool { return c.items[ids[i]].CreatedAt < c.items[ids[j]].CreatedAt })
	}
	return ids
}

func (b *Broker) replayMessage(id string) *Message {
	m := b.cache.get(id)
	if m == nil || m.ExpiresAt <= time.Now().UnixMilli() {
		return nil
	}
	return m
}
