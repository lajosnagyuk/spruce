package broker

import (
	"container/heap"
	"errors"
	"strings"
	"time"
)

var errRetentionCapacity = errors.New("retained work capacity")

type groupWork struct {
	id, key    string
	expires    int64
	charge     int64
	attempt    int
	delivery   string
	prev, next *groupWork
}
type groupLane struct{ head, tail *groupWork }
type groupWorkState struct {
	scope     checkpointScope
	lanes     map[string]*groupLane
	work      map[string]*groupWork
	charge    int64
	indexPeak int
}

// All group indexes share the existing stream-memory budget. Payloads remain
// in the TTL cache and bounded in-flight windows; disconnected queue indexes
// cannot pin payload graphs.
func workCharge(m *Message) int64 { return int64(384 + len(m.ID) + len(deliveryAffinity(m))) }
func (b *Broker) registerGroupLocked(topic, group string) bool {
	scope := checkpointScope{topic: topic, group: group}
	if b.groupWork[scope] != nil {
		return true
	}
	if len(b.groupWork) >= b.cfg.MaxStreams {
		return false
	}
	charge := int64(1024 + len(topic) + len(group))
	if b.groupMemoryBytes+charge > b.cfg.StreamMemoryBytes-streamMemoryReservation || !b.reserveStreamMemory(charge) {
		return false
	}
	scope = checkpointScope{topic: strings.Clone(topic), group: strings.Clone(group)}
	b.groupMemoryBytes += charge
	b.groupWork[scope] = &groupWorkState{scope: scope, lanes: make(map[string]*groupLane), work: make(map[string]*groupWork), charge: charge}
	return true
}

// Caller holds cache.mu. Reserve the whole index expansion before accepting a
// message or batch, so pressure never partially admits a public batch.
func (b *Broker) prepareGroupWork(messages []*Message, onlyGroup ...string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now().UnixMilli()
	var charge int64
	for _, g := range b.groupWork {
		if len(onlyGroup) > 0 && onlyGroup[0] != "" && g.scope.group != onlyGroup[0] {
			continue
		}
		for _, m := range messages {
			if m == nil {
				continue
			}
			if m.Topic == g.scope.topic && g.work[m.ID] == nil && !b.checkpointActiveLocked(m.Topic, g.scope.group, m.ID, now) {
				charge += workCharge(m)
			}
		}
	}
	if b.groupMemoryBytes+charge > b.cfg.StreamMemoryBytes-streamMemoryReservation || !b.reserveStreamMemory(charge) {
		return errRetentionCapacity
	}
	var used int64
	for _, g := range b.groupWork {
		if len(onlyGroup) > 0 && onlyGroup[0] != "" && g.scope.group != onlyGroup[0] {
			continue
		}
		for _, m := range messages {
			if m == nil {
				continue
			}
			if m.Topic != g.scope.topic || g.work[m.ID] != nil || b.checkpointActiveLocked(m.Topic, g.scope.group, m.ID, now) {
				continue
			}
			key := deliveryAffinity(m)
			lane := g.lanes[key]
			if lane == nil {
				lane = &groupLane{}
				g.lanes[key] = lane
			}
			w := &groupWork{id: m.ID, key: key, expires: m.ExpiresAt, charge: workCharge(m), attempt: 1, prev: lane.tail}
			if lane.tail != nil {
				lane.tail.next = w
			} else {
				lane.head = w
			}
			lane.tail = w
			g.work[w.id] = w
			g.indexPeak = max(g.indexPeak, len(g.work))
			used += w.charge
		}
	}
	b.streamMemoryBytes.Add(-(charge - used))
	b.groupMemoryBytes += used
	return nil
}

func (b *Broker) wakeGroups() {
	select {
	case b.groupWake <- struct{}{}:
	default:
	}
}

// The cache lock and broker lock are held. A key has at most one outstanding
// delivery. Failed admission leaves its head queued instead of moving it to a
// different member while earlier work is still in flight.
func (b *Broker) dispatchGroupLocked(g *groupWorkState, lane *groupLane, m *Message) {
	w := lane.head
	if w == nil || w.delivery != "" || m == nil || m.ID != w.id || w.expires <= time.Now().UnixMilli() {
		return
	}
	for _, s := range b.topicGroups[g.scope.topic][g.scope.group] {
		if s.replaying {
			return
		}
	}
	var selected *subscriber
	var score uint64
	for _, s := range b.topicGroups[g.scope.topic][g.scope.group] {
		if s.detached || s.replaying {
			continue
		}
		n := hashValue(m.Topic, g.scope.group, w.key, subscriberAffinity(s))
		if selected == nil || n > score {
			selected = s
			score = n
		}
	}
	if selected == nil {
		return
	}
	if id := b.sendDeliveryLocked(selected, m, w.attempt, false); id != "" {
		w.delivery = id
	}
}

func (b *Broker) dispatchGroupMessage(m *Message, onlyGroup string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, g := range b.groupWork {
		if g.scope.topic == m.Topic && (onlyGroup == "" || g.scope.group == onlyGroup) {
			if lane := g.lanes[deliveryAffinity(m)]; lane != nil {
				b.dispatchGroupLocked(g, lane, m)
			}
		}
	}
}

func (b *Broker) removeGroupWorkLocked(g *groupWorkState, w *groupWork) {
	lane := g.lanes[w.key]
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		lane.head = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		lane.tail = w.prev
	}
	delete(g.work, w.id)
	if lane.head == nil {
		delete(g.lanes, w.key)
	}
	b.streamMemoryBytes.Add(-w.charge)
	b.groupMemoryBytes -= w.charge
	// Go maps retain their high-water allocation after deletion. Compact on
	// geometric shrink so repeated backlogs across groups cannot retain every
	// group's historical peak after its work charges have been released.
	if g.indexPeak > 8 && len(g.work) < g.indexPeak/4 {
		work := make(map[string]*groupWork, len(g.work))
		for id, entry := range g.work {
			work[id] = entry
		}
		lanes := make(map[string]*groupLane, len(g.lanes))
		for key, lane := range g.lanes {
			lanes[key] = lane
		}
		g.work, g.lanes, g.indexPeak = work, lanes, len(work)
	}
}

func (b *Broker) completeGroupWorkLocked(topic, group, id string) {
	if g := b.groupWork[checkpointScope{topic: topic, group: group}]; g != nil {
		if w := g.work[id]; w != nil {
			b.removeGroupWorkLocked(g, w)
		}
	}
	b.wakeGroups()
}

// One worker per broker, with bounded indexes and a bounded polling cadence for
// channel pressure. Idle brokers do no cache scanning unless groups exist.
func (b *Broker) groupLoop() {
	timer := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	nextCleanup := time.Time{}
	for {
		select {
		case <-b.stop:
			return
		case <-b.groupWake:
		case <-timer.C:
		}
		b.cache.mu.Lock()
		b.mu.Lock()
		now := time.Now().UnixMilli()
		cleanup := time.Now().After(nextCleanup)
		if cleanup {
			nextCleanup = time.Now().Add(time.Second)
		}
		for scope, g := range b.groupWork {
			if cleanup {
				for _, w := range g.work {
					if w.expires <= now {
						b.metrics.GroupExpired.Add(1)
						b.removeGroupWorkLocked(g, w)
						continue
					}
					if b.checkpointActiveLocked(scope.topic, scope.group, w.id, now) {
						b.removeGroupWorkLocked(g, w)
					}
				}
			}
			for _, lane := range g.lanes {
				for lane.head != nil && lane.head.expires <= now {
					b.metrics.GroupExpired.Add(1)
					b.removeGroupWorkLocked(g, lane.head)
				}
				if lane.head == nil {
					continue
				}
				b.dispatchGroupLocked(g, lane, b.cache.items[lane.head.id])
			}
			if len(g.work) == 0 && len(b.topicGroups[scope.topic][scope.group]) == 0 {
				delete(b.groupWork, scope)
				b.streamMemoryBytes.Add(-g.charge)
				b.groupMemoryBytes -= g.charge
			}
		}
		b.mu.Unlock()
		b.cache.mu.Unlock()
	}
}

// Called after the timed-out pending delivery is removed. Preserve the key head
// beyond MaxAttempts: expiry, not retry exhaustion, releases outstanding work.
func (b *Broker) retryGroup(p *pending) {
	b.mu.Lock()
	if g := b.groupWork[checkpointScope{topic: p.message.Topic, group: p.group}]; g != nil {
		if w := g.work[p.message.ID]; w != nil && w.delivery == p.deliveryID {
			w.delivery = ""
			w.attempt = min(w.attempt+1, b.cfg.MaxAttempts)
		}
	}
	b.mu.Unlock()
	b.wakeGroups()
}

// Shared pending admission. Caller holds mu; grouped pressure queues the key
// rather than cancelling its stream. Broadcast keeps its existing behaviour.
func (b *Broker) sendDeliveryLocked(s *subscriber, m *Message, attempt int, cancelOnPressure bool) string {
	bytes := messageSize(m)
	if s.detached || b.pendingBytes+bytes > b.cfg.MaxInflightBytes || s.inflightBytes+bytes > b.cfg.MaxSubscriberInflightBytes || len(s.ch) == cap(s.ch) {
		if cancelOnPressure && s.cancel != nil {
			s.cancel()
		}
		if cancelOnPressure {
			b.metrics.Dropped.Add(1)
		}
		return ""
	}
	id := b.nextID()
	now := time.Now()
	p := acquirePending()
	*p = pending{deliveryID: id, message: m, attempt: attempt, group: s.group, subscriberID: s.id, expiresAt: m.ExpiresAt, bytes: bytes, deliveredAt: now, next: now.Add(b.cfg.AckDeadline)}
	p.deadline = pendingDeadline{id: id, at: p.next}
	b.pending[id] = p
	b.linkTopicPendingLocked(p)
	b.pendingBytes += bytes
	s.inflightBytes += bytes
	heap.Push(&b.pendingDeadlines, &p.deadline)
	s.ch <- Delivery{DeliveryID: id, MessageID: m.ID, Topic: m.Topic, Key: m.Key, Headers: m.Headers, CreatedAt: m.CreatedAt, Attempt: attempt, Payload: m.Payload, origin: m.Origin, sequence: m.Sequence}
	b.metrics.Delivered.Add(1)
	return id
}

func (b *Broker) groupWorkCountsLocked() (entries, keys int) {
	for _, g := range b.groupWork {
		entries += len(g.work)
		keys += len(g.lanes)
	}
	return
}
