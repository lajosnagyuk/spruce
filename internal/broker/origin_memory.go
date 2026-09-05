package broker

import "time"

// Receive frontiers only describe retained history. Once that history expires,
// a later noncontiguous copy requests repair rather than pinning every past
// publisher/topic incarnation forever.
func (c *cache) recordReceivedLocked(m *Message) {
	c.receivedThrough[m.Origin] = max(c.receivedThrough[m.Origin], m.Sequence)
	c.receivedUntil[m.Origin] = max(c.receivedUntil[m.Origin], m.ExpiresAt)
	if len(c.reorder[m.Origin]) > 0 {
		c.reorderSince[m.Origin] = time.Now().UnixMilli()
	}
}

func (c *cache) originCapacityLocked(messages []*Message) bool {
	if len(c.receivedThrough)+len(messages) <= c.frontierLimit {
		return true
	}
	added := make(map[string]struct{})
	for _, m := range messages {
		origin := m.Origin
		if origin == "" {
			if state := c.topicSequences[m.Topic]; state != nil {
				origin = state.origin
			} else {
				origin = "local:" + m.Topic
			}
		}
		if _, exists := c.receivedThrough[origin]; !exists {
			added[origin] = struct{}{}
		}
	}
	return len(c.receivedThrough)+len(added) <= c.frontierLimit
}
