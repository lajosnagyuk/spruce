package broker

// A long-lived stream need not retain completed, expired origin incarnations.
// Unknown expiry is kept conservatively: it cannot prove history is disposable.
// Only the stream writer accesses these maps, so frames need no frontier clone.
type streamCursor struct {
	sequences map[string]uint64
	expires   map[string]int64
}

func newStreamCursor(sequences map[string]uint64, retainedUntil map[string]int64) *streamCursor {
	c := &streamCursor{sequences: sequences, expires: make(map[string]int64, len(sequences))}
	for origin := range sequences {
		c.expires[origin] = retainedUntil[origin]
	}
	return c
}

func (c *streamCursor) advance(d Delivery, now int64) string {
	_, exists := c.sequences[d.origin]
	if !exists {
		for origin, until := range c.expires {
			if until > 0 && until <= now {
				delete(c.sequences, origin)
				delete(c.expires, origin)
			}
		}
	}
	c.sequences[d.origin] = max(c.sequences[d.origin], d.sequence)
	if !exists || c.expires[d.origin] > 0 {
		c.expires[d.origin] = max(c.expires[d.origin], d.expiresAt)
	}
	return encodeReplayCursor(c.sequences)
}
