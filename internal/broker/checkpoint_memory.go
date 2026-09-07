package broker

// Remove expired and superseded entries, including refresh history for the same
// message. Retained history is bounded independently of the number of keys.
func (b *Broker) compactCheckpointsLocked(now int64) {
	kept := b.checkpointOrder[:0]
	for _, old := range b.checkpointOrder[b.checkpointHead:] {
		entries := b.checkpoints[old.scope]
		if entries[old.messageID] != old.expiresAt {
			continue
		}
		if old.expiresAt <= now {
			delete(entries, old.messageID)
			b.checkpointCount--
			if len(entries) == 0 {
				delete(b.checkpoints, old.scope)
			}
			continue
		}
		kept = append(kept, old)
	}
	clear(b.checkpointOrder[len(kept):])
	b.checkpointOrder = kept
	b.checkpointHead = 0
}
