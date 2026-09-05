package broker

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxHeaders             = 8 << 10
	topicCompactTombstones = 256
	orderCompactTombstones = 1024
)

type Config struct {
	CacheBytes                 int64
	DefaultTTL                 time.Duration
	MaxTTL                     time.Duration
	MaxMessage                 int64
	QueueDepth                 int
	ReplicationQueueBytes      int64
	ActionQueueBytes           int64
	MaxInflightBytes           int64
	MaxSubscriberInflightBytes int64
	AckDeadline                time.Duration
	MaxAttempts                int
	Peers                      []string
	PeerToken                  string
	PreviousPeerToken          string
	ClusterID                  string
	PeerCAFile                 string
	ClientToken                string
	PreviousClientToken        string
	AdminToken                 string
	PreviousAdminToken         string
	BasicUsername              string
	BasicPassword              string
	AdminBasicUsername         string
	AdminBasicPassword         string
	RequireClientAuth          bool
	RequireAdminAuth           bool
	IdempotencyEntries         int
	CheckpointEntries          int
	MaxConcurrentRequests      int
	MaxStreams                 int
	StreamMemoryBytes          int64
	MaxInternalRequests        int
	PublishAdmissionBytes      int64
	PublishAdmissionWait       time.Duration
	DeliveryLagLimit           time.Duration
	Logger                     *slog.Logger
}

func DefaultConfig() Config {
	return Config{CacheBytes: 256 << 20, DefaultTTL: time.Minute, MaxTTL: 24 * time.Hour,
		MaxMessage: 1 << 20, QueueDepth: 4096, ReplicationQueueBytes: 64 << 20, ActionQueueBytes: 4 << 20,
		MaxInflightBytes: 64 << 20, MaxSubscriberInflightBytes: 16 << 20, AckDeadline: 30 * time.Second, MaxAttempts: 8,
		IdempotencyEntries: 65536, CheckpointEntries: 65536, MaxConcurrentRequests: 4096, MaxStreams: 1024, MaxInternalRequests: 1024,
		PublishAdmissionBytes: 32 << 20, PublishAdmissionWait: 100 * time.Millisecond, DeliveryLagLimit: time.Second, StreamMemoryBytes: 16 << 20}
}

type Message struct {
	ID            string            `json:"id"`
	Topic         string            `json:"topic"`
	Key           string            `json:"key,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Payload       []byte            `json:"-"`
	CreatedAt     int64             `json:"created_at"`
	ExpiresAt     int64             `json:"expires_at"`
	Origin        string            `json:"origin,omitempty"`
	Sequence      uint64            `json:"sequence,omitempty"`
	expiry        *expiryItem
	expiryPrev    *Message
	expiryNext    *Message
	orderIndex    int
	topicIndex    int
	accountedSize int64
}

type Delivery struct {
	DeliveryID string            `json:"delivery_id"`
	MessageID  string            `json:"message_id"`
	Topic      string            `json:"topic"`
	Key        string            `json:"key,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	CreatedAt  int64             `json:"created_at"`
	Attempt    int               `json:"attempt"`
	Cursor     string            `json:"cursor,omitempty"`
	Payload    []byte            `json:"-"`
	origin     string
	sequence   uint64
}

type cache struct {
	mu              sync.Mutex
	items           map[string]*Message
	order           []*Message
	orderHead       int
	expiry          expiryHeap
	expiryItems     map[int64]*expiryItem
	tombstones      int
	topics          map[string][]*Message
	topicTombstones map[string]int
	frontiers       map[string]*topicFrontier
	frontierLimit   int
	unsafe          map[string]map[string]int64
	topicSequences  map[string]*topicSequence
	receivedThrough map[string]uint64
	reorder         map[string]map[uint64]*Message
	reorderBytes    int64
	reorderLimit    int
	bytes           int64
	maxBytes        int64
	rejectPressure  bool
	evicted         atomic.Uint64
	expired         atomic.Uint64
}

type topicSequence struct {
	origin    string
	next      uint64
	expiresAt int64
}

type topicFrontier struct {
	Sequences map[string]uint64 `json:"sequences"`
	ExpiresAt int64             `json:"expires_at"`
}

type replayCursor struct {
	Sequences map[string]uint64 `json:"sequences"`
}

type expiryItem struct {
	at    int64
	index int
	head  *Message
	count int
}
type expiryHeap []*expiryItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].at < h[j].at }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *expiryHeap) Push(v any) {
	item := v.(*expiryItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	v := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	v.index = -1
	return v
}

func newCache(max int64) *cache {
	limit := int(max / 128)
	if limit < 1024 {
		limit = 1024
	}
	return &cache{items: make(map[string]*Message), expiryItems: make(map[int64]*expiryItem), topics: make(map[string][]*Message), topicTombstones: make(map[string]int), frontiers: make(map[string]*topicFrontier), frontierLimit: limit, unsafe: make(map[string]map[string]int64), topicSequences: make(map[string]*topicSequence), receivedThrough: make(map[string]uint64), reorder: make(map[string]map[uint64]*Message), reorderLimit: 4096, maxBytes: max}
}

func (c *cache) markUnsafeLocked(topic, source string, until int64) {
	if until <= 0 {
		return
	}
	entries := c.unsafe[topic]
	if entries == nil {
		if len(c.unsafe) >= c.frontierLimit {
			topic, source = "*", "capacity"
			entries = c.unsafe[topic]
		}
		if entries == nil {
			entries = make(map[string]int64)
			c.unsafe[topic] = entries
		}
	}
	if len(entries) >= 256 {
		source = "capacity"
	}
	entries[source] = max(entries[source], until)
}

func (c *cache) clearUnsafeLocked(topic, source string) {
	if entries := c.unsafe[topic]; entries != nil {
		delete(entries, source)
		if len(entries) == 0 {
			delete(c.unsafe, topic)
		}
	}
}

func (c *cache) topicUnsafeLocked(topic string, now int64) bool {
	unsafe := false
	for _, key := range []string{"*", topic} {
		if entries := c.unsafe[key]; entries != nil {
			for source, until := range entries {
				if until <= now {
					delete(entries, source)
				} else {
					unsafe = true
				}
			}
			if len(entries) == 0 {
				delete(c.unsafe, key)
			}
		}
	}
	return unsafe
}

func encodeReplayCursor(sequences map[string]uint64) string {
	if len(sequences) > 256 {
		return ""
	}
	b, _ := json.Marshal(replayCursor{Sequences: sequences})
	if len(b) > 12<<10 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeReplayCursor(value string) (map[string]uint64, error) {
	if value == "" {
		return make(map[string]uint64), nil
	}
	b, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(b) > 16<<10 {
		return nil, errors.New("invalid cursor")
	}
	var cursor replayCursor
	if json.Unmarshal(b, &cursor) != nil || cursor.Sequences == nil || len(cursor.Sequences) > 256 {
		return nil, errors.New("invalid cursor")
	}
	for origin := range cursor.Sequences {
		if origin == "" || len(origin) > 64 {
			return nil, errors.New("invalid cursor")
		}
	}
	return cursor.Sequences, nil
}

func cloneFrontier(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func frontierBehind(cursor map[string]uint64, frontier *topicFrontier) bool {
	if frontier == nil {
		return false
	}
	for origin, sequence := range frontier.Sequences {
		if cursor[origin] < sequence {
			return true
		}
	}
	return false
}

func messageSize(m *Message) int64 {
	if m.accountedSize > 0 {
		return m.accountedSize
	}
	// CacheBytes is an RSS safety budget, not just a payload limit. Include a
	// conservative allowance for map/list/interface and allocator-class overhead
	// so a large population of tiny messages cannot outrun the cgroup boundary.
	n := int64(256 + len(m.ID) + len(m.Topic) + len(m.Key) + len(m.Payload))
	for k, v := range m.Headers {
		n += int64(len(k) + len(v) + 8)
	}
	m.accountedSize = n
	return n
}

func (c *cache) put(m *Message, now int64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.putLocked(m, now)
}

func (c *cache) putLocked(m *Message, now int64) (bool, error) {
	c.expireLocked(now)
	return c.insertLocked(m)
}

func (c *cache) insertLocked(m *Message) (bool, error) {
	if _, ok := c.items[m.ID]; ok {
		return false, nil
	}
	sz := messageSize(m)
	if sz > c.maxBytes {
		return false, errors.New("message exceeds cache capacity")
	}
	if c.rejectPressure && c.bytes+sz > c.maxBytes {
		return false, errRetentionCapacity
	}
	for c.bytes+sz > c.maxBytes && c.orderHead < len(c.order) {
		c.dropOldestLocked()
	}
	c.items[m.ID] = m
	m.orderIndex = len(c.order)
	c.order = append(c.order, m)
	m.topicIndex = len(c.topics[m.Topic])
	c.topics[m.Topic] = append(c.topics[m.Topic], m)
	item := c.expiryItems[m.ExpiresAt]
	if item == nil {
		item = &expiryItem{at: m.ExpiresAt, index: -1}
		c.expiryItems[m.ExpiresAt] = item
		heap.Push(&c.expiry, item)
	}
	m.expiry = item
	m.expiryNext = item.head
	if item.head != nil {
		item.head.expiryPrev = m
	}
	item.head = m
	item.count++
	c.bytes += sz
	return true, nil
}

func (c *cache) putBatch(messages []*Message, now int64) ([]bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.putBatchLocked(messages, now)
}

func (c *cache) putBatchLocked(messages []*Message, now int64) ([]bool, error) {
	c.expireLocked(now)
	var total int64
	for _, m := range messages {
		sz := messageSize(m)
		if sz > c.maxBytes || total > c.maxBytes-sz {
			return nil, errors.New("message exceeds cache capacity")
		}
		total += sz
	}
	return c.insertBatchLocked(messages), nil
}

// The caller holds c.mu and has already validated the aggregate byte budget.
func (c *cache) insertBatchLocked(messages []*Message) []bool {
	inserted := make([]bool, len(messages))
	for i, m := range messages {
		inserted[i], _ = c.insertLocked(m)
	}
	return inserted
}

func (c *cache) dropOldestLocked() {
	for c.orderHead < len(c.order) {
		m := c.order[c.orderHead]
		c.order[c.orderHead] = nil
		c.orderHead++
		c.tombstones++
		if m == nil {
			continue
		}
		if _, ok := c.items[m.ID]; !ok {
			continue
		}
		c.removeLocked(m, true)
		return
	}
}

func (c *cache) removeLocked(m *Message, pressure bool) {
	if _, ok := c.items[m.ID]; ok {
		if pressure && m.Origin != "" {
			frontier := c.frontiers[m.Topic]
			if frontier == nil {
				if len(c.frontiers) >= c.frontierLimit {
					for topic, candidate := range c.frontiers {
						if candidate.ExpiresAt <= time.Now().UnixMilli() {
							delete(c.frontiers, topic)
						}
					}
				}
				if len(c.frontiers) >= c.frontierLimit {
					c.markUnsafeLocked(m.Topic, "frontier-capacity", m.ExpiresAt)
				} else {
					frontier = &topicFrontier{Sequences: make(map[string]uint64)}
					c.frontiers[m.Topic] = frontier
				}
			}
			if frontier != nil {
				if _, exists := frontier.Sequences[m.Origin]; !exists && len(frontier.Sequences) >= 256 {
					c.markUnsafeLocked(m.Topic, "frontier-origins", m.ExpiresAt)
					frontier = nil
				}
			}
			if frontier != nil {
				frontier.Sequences[m.Origin] = max(frontier.Sequences[m.Origin], m.Sequence)
				frontier.ExpiresAt = max(frontier.ExpiresAt, m.ExpiresAt)
			}
		}
		delete(c.items, m.ID)
		if m.orderIndex >= 0 && m.orderIndex < len(c.order) && c.order[m.orderIndex] == m {
			c.order[m.orderIndex] = nil
			c.tombstones++
		}
		if indexed := c.topics[m.Topic]; m.topicIndex >= 0 && m.topicIndex < len(indexed) && indexed[m.topicIndex] == m {
			indexed[m.topicIndex] = nil
			c.topics[m.Topic] = indexed
			c.topicTombstones[m.Topic]++
			if c.topicTombstones[m.Topic] == len(indexed) {
				delete(c.topics, m.Topic)
				delete(c.topicTombstones, m.Topic)
			} else if c.topicTombstones[m.Topic] > topicCompactTombstones && c.topicTombstones[m.Topic]*2 > len(indexed) {
				kept := indexed[:0]
				for _, live := range indexed {
					if live != nil {
						live.topicIndex = len(kept)
						kept = append(kept, live)
					}
				}
				for i := len(kept); i < len(indexed); i++ {
					indexed[i] = nil
				}
				if cap(kept) > 512 && len(kept) < cap(kept)/4 {
					kept = append([]*Message(nil), kept...)
				}
				if len(kept) == 0 {
					delete(c.topics, m.Topic)
				} else {
					c.topics[m.Topic] = kept
				}
				delete(c.topicTombstones, m.Topic)
			}
		}
		c.unlinkExpiryLocked(m)
		c.bytes -= messageSize(m)
		if pressure {
			c.evicted.Add(1)
		} else {
			c.expired.Add(1)
		}
	}
}

func (c *cache) unlinkExpiryLocked(m *Message) {
	item := m.expiry
	if item == nil {
		return
	}
	if m.expiryPrev != nil {
		m.expiryPrev.expiryNext = m.expiryNext
	} else {
		item.head = m.expiryNext
	}
	if m.expiryNext != nil {
		m.expiryNext.expiryPrev = m.expiryPrev
	}
	m.expiry, m.expiryPrev, m.expiryNext = nil, nil, nil
	item.count--
	if item.count == 0 {
		delete(c.expiryItems, item.at)
		if item.index >= 0 {
			heap.Remove(&c.expiry, item.index)
		}
	}
}

func (c *cache) expire(now int64) { c.mu.Lock(); c.expireLocked(now); c.mu.Unlock() }
func (c *cache) expireLocked(now int64) {
	for topic, frontier := range c.frontiers {
		if frontier.ExpiresAt <= now {
			delete(c.frontiers, topic)
		}
	}
	for topic := range c.unsafe {
		c.topicUnsafeLocked(topic, now)
	}
	for topic, sequence := range c.topicSequences {
		if sequence.expiresAt <= now && len(c.topics[topic]) == 0 {
			delete(c.topicSequences, topic)
		}
	}
	for origin, gaps := range c.reorder {
		topic := ""
		for sequence, m := range gaps {
			topic = m.Topic
			if m.ExpiresAt <= now {
				c.reorderBytes -= messageSize(m)
				delete(gaps, sequence)
			}
		}
		if len(gaps) == 0 {
			delete(c.reorder, origin)
			if topic != "" {
				c.clearUnsafeLocked(topic, "gap:"+origin)
			}
		}
	}
	for c.expiry.Len() > 0 && c.expiry[0].at <= now {
		item := heap.Pop(&c.expiry).(*expiryItem)
		delete(c.expiryItems, item.at)
		for m := item.head; m != nil; {
			next := m.expiryNext
			m.expiry, m.expiryPrev, m.expiryNext = nil, nil, nil
			if c.items[m.ID] == m && m.ExpiresAt == item.at {
				c.removeLocked(m, false)
			}
			m = next
		}
		item.head, item.count = nil, 0
	}
	if c.tombstones > orderCompactTombstones && c.tombstones*2 > len(c.order) {
		kept := c.order[:0]
		for _, m := range c.order {
			if m != nil {
				if _, ok := c.items[m.ID]; ok {
					m.orderIndex = len(kept)
					kept = append(kept, m)
				}
			}
		}
		for i := len(kept); i < len(c.order); i++ {
			c.order[i] = nil
		}
		c.order = kept
		c.orderHead = 0
		c.tombstones = 0
		if cap(c.order) > 1024 && len(c.order) < cap(c.order)/4 {
			c.order = append([]*Message(nil), c.order...)
		}
	}
}
func (c *cache) snapshot(topic string, since int64) []*Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UnixMilli()
	out := make([]*Message, 0)
	for _, m := range c.topics[topic] {
		if m == nil {
			continue
		}
		if m.Topic == topic && m.CreatedAt >= since && m.ExpiresAt > now {
			out = append(out, m)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out
}

func (c *cache) replayLocked(topic string, cursor map[string]uint64) []*Message {
	out := make([]*Message, 0, len(c.topics[topic]))
	for _, m := range c.topics[topic] {
		if m != nil && m.Sequence > cursor[m.Origin] {
			out = append(out, m)
		}
	}
	return out
}

func (c *cache) replaySinceLocked(topic string, since int64) []*Message {
	out := make([]*Message, 0, len(c.topics[topic]))
	for _, m := range c.topics[topic] {
		if m != nil && m.CreatedAt >= since {
			out = append(out, m)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out
}

func (c *cache) page(after string, maxBytes int64) ([]*Message, string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(time.Now().UnixMilli())
	start := c.orderHead
	if after != "" {
		found := false
		for i := c.orderHead; i < len(c.order); i++ {
			if c.order[i] != nil && c.order[i].ID == after {
				start, found = i+1, true
				break
			}
		}
		if !found {
			return nil, "", false
		}
	}
	out := make([]*Message, 0, 256)
	var bytes int64
	next := after
	for i := start; i < len(c.order) && len(out) < maxBatchMessages; i++ {
		if m := c.order[i]; m != nil {
			sz := messageSize(m)
			if bytes+sz > maxBytes {
				break
			}
			out, bytes, next = append(out, m), bytes+sz, m.ID
		}
	}
	return out, next, true
}

func (c *cache) has(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[id]
	return ok
}

func (c *cache) get(id string) *Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.items[id]
}

type subscriber struct {
	id, topic, group string
	affinityID       string
	ch               chan Delivery
	inflightBytes    int64
	cancel           context.CancelFunc
	detached         bool
	replaying        bool
	deferred         []string
}

type deliveryCandidate struct {
	subscriber *subscriber
	group      string
	score      uint64
}

var deliveryCandidates = sync.Pool{New: func() any { return make([]deliveryCandidate, 0, 32) }}
var deliveryMetadata = sync.Pool{New: func() any { return make([]byte, 0, 256) }}

type pending struct {
	deliveryID   string
	message      *Message
	attempt      int
	group        string
	subscriberID string
	expiresAt    int64
	bytes        int64
	next         time.Time
	deadline     pendingDeadline
	deliveredAt  time.Time
	topicPrev    *pending
	topicNext    *pending
}

type topicPendingState struct {
	head, tail *pending
	count      int
}

var pendingPool = sync.Pool{New: func() any { return new(pending) }}

func acquirePending() *pending { return pendingPool.Get().(*pending) }
func resetPending(p *pending)  { *p = pending{} }
func releasePending(p *pending) {
	resetPending(p)
	pendingPool.Put(p)
}

var latencyBoundsUS = [...]uint64{100, 250, 500, 1000, 2500, 5000, 10000, 50000}

type durationHistogram struct {
	buckets [len(latencyBoundsUS)]atomic.Uint64
	count   atomic.Uint64
	sumUS   atomic.Uint64
}

func (h *durationHistogram) observe(elapsed time.Duration) {
	us := uint64(max(0, elapsed.Microseconds()))
	for i, bound := range latencyBoundsUS {
		if us <= bound {
			h.buckets[i].Add(1)
		}
	}
	h.count.Add(1)
	h.sumUS.Add(us)
}

type pendingDeadline struct {
	id    string
	at    time.Time
	index int
}
type pendingHeap []*pendingDeadline

func (h pendingHeap) Len() int           { return len(h) }
func (h pendingHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h pendingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *pendingHeap) Push(v any) {
	item := v.(*pendingDeadline)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	v := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	v.index = -1
	return v
}

type peer struct {
	url              string
	ch               chan []*Message
	queuedBytes      atomic.Int64
	actionBytes      atomic.Int64
	acks             chan actionBatch
	nacks            chan actionBatch
	v2               atomic.Bool
	legacy           atomic.Bool
	probeMu          sync.Mutex
	lastV2Probe      atomic.Int64
	unavailableUntil atomic.Int64
	copySlots        chan struct{}
}

func writePeerBatchV1(w io.Writer, messages []*Message) error {
	if len(messages) == 0 || len(messages) > maxBatchMessages {
		return errors.New("invalid peer batch size")
	}
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(messages)))
	if _, err := w.Write(count[:]); err != nil {
		return err
	}
	for _, m := range messages {
		if err := writePeerString(w, m.ID); err != nil {
			return err
		}
		if err := writePeerString(w, m.Topic); err != nil {
			return err
		}
		if err := writePeerString(w, m.Key); err != nil {
			return err
		}
		var fixed [22]byte
		binary.BigEndian.PutUint16(fixed[:2], uint16(len(m.Headers)))
		binary.BigEndian.PutUint32(fixed[2:6], uint32(len(m.Payload)))
		binary.BigEndian.PutUint64(fixed[6:14], uint64(m.CreatedAt))
		binary.BigEndian.PutUint64(fixed[14:22], uint64(m.ExpiresAt))
		if _, err := w.Write(fixed[:]); err != nil {
			return err
		}
		for k, v := range m.Headers {
			if err := writePeerString(w, k); err != nil {
				return err
			}
			if err := writePeerString(w, v); err != nil {
				return err
			}
		}
		if _, err := w.Write(m.Payload); err != nil {
			return err
		}
	}
	return nil
}

func readPeerBatchV1(r io.Reader, maxMessage int64) ([]*Message, error) {
	var countBytes [4]byte
	if _, err := io.ReadFull(r, countBytes[:]); err != nil {
		return nil, err
	}
	count := int(binary.BigEndian.Uint32(countBytes[:]))
	if count == 0 || count > maxBatchMessages {
		return nil, errors.New("invalid peer batch size")
	}
	messages := make([]*Message, 0, count)
	for range count {
		id, err := readPeerString(r, 64)
		if err != nil {
			return nil, err
		}
		topic, err := readPeerString(r, 255)
		if err != nil {
			return nil, err
		}
		key, err := readPeerString(r, maxHeaders)
		if err != nil {
			return nil, err
		}
		var fixed [22]byte
		if _, err = io.ReadFull(r, fixed[:]); err != nil {
			return nil, err
		}
		headerCount := int(binary.BigEndian.Uint16(fixed[:2]))
		payloadSize := int64(binary.BigEndian.Uint32(fixed[2:6]))
		if headerCount > 256 || payloadSize > maxMessage {
			return nil, errors.New("peer message exceeds limit")
		}
		headers := make(map[string]string, headerCount)
		headerBytes := 0
		for range headerCount {
			k, e := readPeerString(r, maxHeaders)
			if e != nil {
				return nil, e
			}
			v, e := readPeerString(r, maxHeaders)
			if e != nil {
				return nil, e
			}
			headerBytes += len(k) + len(v)
			if headerBytes > maxHeaders {
				return nil, errors.New("peer headers exceed limit")
			}
			headers[k] = v
		}
		payload := make([]byte, payloadSize)
		if _, err = io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		messages = append(messages, &Message{ID: id, Topic: topic, Key: key, Headers: headers, Payload: payload, CreatedAt: int64(binary.BigEndian.Uint64(fixed[6:14])), ExpiresAt: int64(binary.BigEndian.Uint64(fixed[14:22]))})
	}
	return messages, nil
}

type actionBatch struct {
	ids         []string
	checkpoints []groupCheckpoint
	bytes       int64
}

type groupCheckpoint struct {
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	MessageID string `json:"message_id"`
	ExpiresAt int64  `json:"expires_at"`
}

type checkpointScope struct{ topic, group string }
type checkpointOrderEntry struct {
	scope     checkpointScope
	messageID string
	expiresAt int64
}

type idempotencyEntry struct {
	id          string
	expiresAt   int64
	accepted    bool
	replicated  bool
	ready       chan struct{}
	fingerprint [32]byte
	inProgress  bool
}
type idempotencyOrderEntry struct{ key, id string }

type Metrics struct {
	GroupExpired                                                                                      atomic.Uint64
	Published, PublishBytes, Rejected, OverloadRejected, CursorExpired, Replicated, ReplicationErrors atomic.Uint64
	ReplicationDropped, ReplicationPressureRejected, DeliveryPressureRejected                         atomic.Uint64
	AckActionDropped, NackActionDropped                                                               atomic.Uint64
	Delivered, Redelivered, Acked, Dropped, Duplicate                                                 atomic.Uint64
	PublishLatency, ReplicationLatency, AckLatency                                                    durationHistogram
	PublicAuthRejected, AdminAuthRejected, PeerAuthRejected                                           atomic.Uint64
}

type Broker struct {
	cfg                   Config
	cache                 *cache
	mux                   *http.ServeMux
	client                *http.Client
	boot                  [8]byte
	seq                   atomic.Uint64
	messageSeq            atomic.Uint64
	origin                string
	metrics               Metrics
	mu                    sync.RWMutex
	subs                  map[string]*subscriber
	topicBroadcast        map[string]map[string]*subscriber
	topicGroups           map[string]map[string]map[string]*subscriber
	groupWork             map[checkpointScope]*groupWorkState
	groupWake             chan struct{}
	groupMemoryBytes      int64
	pending               map[string]*pending
	topicPending          map[string]*topicPendingState
	pendingBytes          int64
	pendingDeadlines      pendingHeap
	idempotency           map[string]*idempotencyEntry
	idempotencyOrder      []idempotencyOrderEntry
	checkpoints           map[checkpointScope]map[string]int64
	checkpointOrder       []checkpointOrderEntry
	checkpointHead        int
	checkpointCount       int
	peers                 []*peer
	stop                  chan struct{}
	closeOnce             sync.Once
	requestSlots          chan struct{}
	publishAdmissionBytes atomic.Int64
	admissionMu           sync.Mutex
	admissionWaiters      []*publishAdmissionWaiter
	replicationFreed      chan struct{}
	streamSlots           chan struct{}
	streamMemoryBytes     atomic.Int64
	internalSlots         chan struct{}
	digestSlot            chan struct{}
	ready                 atomic.Bool
	draining              atomic.Bool
}

type publishAdmissionWaiter struct {
	bytes    int64
	ready    chan struct{}
	granted  bool
	bypassed uint8
}

func New(cfg Config) *Broker {
	if cfg.RequireClientAuth && cfg.ClientToken == "" && cfg.BasicUsername == "" {
		panic("spruce: client authentication is required but no credential is configured")
	}
	if cfg.RequireAdminAuth && cfg.AdminToken == "" && cfg.AdminBasicUsername == "" {
		panic("spruce: admin authentication is required but no credential is configured")
	}
	if cfg.CacheBytes <= 0 {
		cfg.CacheBytes = DefaultConfig().CacheBytes
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = DefaultConfig().DefaultTTL
	}
	if cfg.MaxTTL <= 0 {
		cfg.MaxTTL = DefaultConfig().MaxTTL
	}
	if cfg.MaxMessage <= 0 {
		cfg.MaxMessage = DefaultConfig().MaxMessage
	}
	if cfg.MaxMessage > 23<<20 {
		panic("spruce: max message exceeds the snapshot wire limit")
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = DefaultConfig().QueueDepth
	}
	if cfg.ReplicationQueueBytes <= 0 {
		cfg.ReplicationQueueBytes = DefaultConfig().ReplicationQueueBytes
	}
	if cfg.ActionQueueBytes <= 0 {
		cfg.ActionQueueBytes = DefaultConfig().ActionQueueBytes
	}
	if cfg.MaxInflightBytes <= 0 {
		cfg.MaxInflightBytes = DefaultConfig().MaxInflightBytes
	}
	if cfg.MaxSubscriberInflightBytes <= 0 {
		cfg.MaxSubscriberInflightBytes = DefaultConfig().MaxSubscriberInflightBytes
	}
	if cfg.AckDeadline <= 0 {
		cfg.AckDeadline = DefaultConfig().AckDeadline
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultConfig().MaxAttempts
	}
	if cfg.IdempotencyEntries <= 0 {
		cfg.IdempotencyEntries = DefaultConfig().IdempotencyEntries
	}
	if cfg.CheckpointEntries <= 0 {
		cfg.CheckpointEntries = DefaultConfig().CheckpointEntries
	}
	if cfg.MaxConcurrentRequests <= 0 {
		cfg.MaxConcurrentRequests = DefaultConfig().MaxConcurrentRequests
	}
	if cfg.MaxStreams <= 0 {
		cfg.MaxStreams = DefaultConfig().MaxStreams
	}
	if cfg.StreamMemoryBytes <= 0 {
		cfg.StreamMemoryBytes = DefaultConfig().StreamMemoryBytes
	}
	if cfg.StreamMemoryBytes < streamMemoryReservation {
		panic("spruce: stream memory budget must fit at least one stream")
	}
	if cfg.MaxInternalRequests <= 0 {
		cfg.MaxInternalRequests = DefaultConfig().MaxInternalRequests
	}
	if cfg.PublishAdmissionBytes <= 0 {
		cfg.PublishAdmissionBytes = DefaultConfig().PublishAdmissionBytes
	}
	if cfg.PublishAdmissionWait <= 0 {
		cfg.PublishAdmissionWait = DefaultConfig().PublishAdmissionWait
	}
	if cfg.DeliveryLagLimit <= 0 {
		cfg.DeliveryLagLimit = DefaultConfig().DeliveryLagLimit
	}
	if cfg.PublishAdmissionBytes < cfg.MaxMessage || cfg.PublishAdmissionBytes < maxBatchBytes {
		panic("spruce: publish admission budget must fit the maximum message and batch")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	peerTransport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.PeerCAFile != "" {
		pem, err := os.ReadFile(cfg.PeerCAFile)
		if err != nil {
			panic("spruce: read peer CA: " + err.Error())
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			panic("spruce: peer CA contains no certificates")
		}
		peerTransport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	}
	b := &Broker{cfg: cfg, cache: newCache(cfg.CacheBytes), mux: http.NewServeMux(),
		client: &http.Client{Timeout: 5 * time.Second, Transport: peerTransport}, subs: make(map[string]*subscriber),
		topicBroadcast: make(map[string]map[string]*subscriber), topicGroups: make(map[string]map[string]map[string]*subscriber),
		pending: make(map[string]*pending), topicPending: make(map[string]*topicPendingState), idempotency: make(map[string]*idempotencyEntry), checkpoints: make(map[checkpointScope]map[string]int64), stop: make(chan struct{}),
		requestSlots: make(chan struct{}, cfg.MaxConcurrentRequests), streamSlots: make(chan struct{}, cfg.MaxStreams), digestSlot: make(chan struct{}, 1), replicationFreed: make(chan struct{}, 1)}
	b.cache.rejectPressure = true
	b.groupWork = make(map[checkpointScope]*groupWorkState)
	b.groupWake = make(chan struct{}, 1)
	b.internalSlots = make(chan struct{}, cfg.MaxInternalRequests)
	if _, err := rand.Read(b.boot[:]); err != nil {
		panic("spruce: generate boot identity: " + err.Error())
	}
	b.origin = base64.RawURLEncoding.EncodeToString(b.boot[:])
	for _, u := range cfg.Peers {
		u = strings.TrimRight(strings.TrimSpace(u), "/")
		if u != "" {
			p := &peer{url: u, ch: make(chan []*Message, cfg.QueueDepth), acks: make(chan actionBatch, 1024), nacks: make(chan actionBatch, 1024), copySlots: make(chan struct{}, 32)}
			b.peers = append(b.peers, p)
		}
	}
	if len(b.peers) > 0 && (cfg.PeerToken == "" || cfg.ClusterID == "") {
		panic("spruce: peer token and cluster ID are required when peers are configured")
	}
	b.routes()
	for _, p := range b.peers {
		go b.peerLoop(p)
		go b.actionLoop(p, "ack", p.acks)
		go b.actionLoop(p, "nack", p.nacks)
	}
	go b.maintenance()
	go b.groupLoop()
	return b
}

func (b *Broker) Handler() http.Handler { return http.HandlerFunc(b.serveHTTP) }
func (b *Broker) Close()                { b.closeOnce.Do(func() { close(b.stop) }) }
func (b *Broker) BeginDrain() {
	b.ready.Store(false)
	b.draining.Store(true)
	b.mu.RLock()
	for _, s := range b.subs {
		if s.cancel != nil {
			s.cancel()
		}
	}
	b.mu.RUnlock()
}
func (b *Broker) Ready() { b.ready.Store(true) }

func (b *Broker) acquirePublishAdmission(ctx context.Context, bytes int64) bool {
	if bytes <= 0 || bytes > b.cfg.PublishAdmissionBytes {
		return false
	}
	b.admissionMu.Lock()
	if len(b.admissionWaiters) == 0 && b.publishAdmissionBytes.Load()+bytes <= b.cfg.PublishAdmissionBytes {
		b.publishAdmissionBytes.Add(bytes)
		b.admissionMu.Unlock()
		return true
	}
	waiter := &publishAdmissionWaiter{bytes: bytes, ready: make(chan struct{})}
	b.admissionWaiters = append(b.admissionWaiters, waiter)
	b.admissionMu.Unlock()
	timer := time.NewTimer(b.cfg.PublishAdmissionWait)
	defer timer.Stop()
	select {
	case <-waiter.ready:
		return true
	case <-ctx.Done():
	case <-timer.C:
	}
	b.admissionMu.Lock()
	if waiter.granted {
		b.admissionMu.Unlock()
		return true
	}
	for i, queued := range b.admissionWaiters {
		if queued == waiter {
			copy(b.admissionWaiters[i:], b.admissionWaiters[i+1:])
			b.admissionWaiters[len(b.admissionWaiters)-1] = nil
			b.admissionWaiters = b.admissionWaiters[:len(b.admissionWaiters)-1]
			break
		}
	}
	b.grantPublishAdmissionLocked()
	b.admissionMu.Unlock()
	return false
}

func (b *Broker) grantPublishAdmissionLocked() {
	for len(b.admissionWaiters) > 0 {
		available := b.cfg.PublishAdmissionBytes - b.publishAdmissionBytes.Load()
		selected := -1
		for i, waiter := range b.admissionWaiters {
			if waiter.bytes <= available {
				selected = i
				break
			}
			if i == 0 && waiter.bypassed >= 8 {
				break
			}
		}
		if selected < 0 {
			return
		}
		waiter := b.admissionWaiters[selected]
		for i := 0; i < selected; i++ {
			if b.admissionWaiters[i].bypassed < 8 {
				b.admissionWaiters[i].bypassed++
			}
		}
		copy(b.admissionWaiters[selected:], b.admissionWaiters[selected+1:])
		b.admissionWaiters[len(b.admissionWaiters)-1] = nil
		b.admissionWaiters = b.admissionWaiters[:len(b.admissionWaiters)-1]
		b.publishAdmissionBytes.Add(waiter.bytes)
		waiter.granted = true
		close(waiter.ready)
	}
}

func (b *Broker) releasePublishAdmission(bytes int64) {
	b.admissionMu.Lock()
	b.publishAdmissionBytes.Add(-bytes)
	b.grantPublishAdmissionLocked()
	b.admissionMu.Unlock()
}

func (b *Broker) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if b.draining.Load() && (r.URL.Path == "/v1/subscriptions/stream" || strings.HasPrefix(r.URL.Path, "/v1/topics/")) {
		w.Header().Set("Retry-After", "0")
		problem(w, http.StatusServiceUnavailable, "broker_draining")
		return
	}

	if strings.HasPrefix(r.URL.Path, "/health/") {
		b.mux.ServeHTTP(w, r)
		return
	}
	slots := b.requestSlots
	if r.URL.Path == "/v1/subscriptions/stream" {
		slots = b.streamSlots
	}
	if strings.HasPrefix(r.URL.Path, "/internal/") {
		slots = b.internalSlots
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		problem(w, 429, "concurrency_limit")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/internal/") {
		b.mux.ServeHTTP(w, r)
		return
	}
	required, previous := b.cfg.ClientToken, b.cfg.PreviousClientToken
	basicUsername, basicPassword := b.cfg.BasicUsername, b.cfg.BasicPassword
	if r.URL.Path == "/metrics" || strings.HasPrefix(r.URL.Path, "/v1/status") {
		if b.cfg.AdminToken != "" || b.cfg.AdminBasicUsername != "" {
			required, previous, basicUsername, basicPassword = b.cfg.AdminToken, b.cfg.PreviousAdminToken, b.cfg.AdminBasicUsername, b.cfg.AdminBasicPassword
		}
	}
	if required != "" || basicUsername != "" {
		authorization := r.Header.Get("Authorization")
		allowed := false
		if required != "" && strings.HasPrefix(authorization, "Bearer ") {
			provided := strings.TrimPrefix(authorization, "Bearer ")
			allowed = tokenEqual(provided, required) || tokenEqual(provided, previous)
		}
		if !allowed && basicUsername != "" {
			username, password, ok := r.BasicAuth()
			allowed = ok && len(username) == len(basicUsername) && len(password) == len(basicPassword) && subtle.ConstantTimeCompare([]byte(username), []byte(basicUsername)) == 1 && subtle.ConstantTimeCompare([]byte(password), []byte(basicPassword)) == 1
		}
		if !allowed {
			if r.URL.Path == "/metrics" || strings.HasPrefix(r.URL.Path, "/v1/status") {
				b.metrics.AdminAuthRejected.Add(1)
			} else {
				b.metrics.PublicAuthRejected.Add(1)
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="spruce", charset="UTF-8"`)
			problem(w, http.StatusUnauthorized, "unauthorized")
			return
		}
	}
	b.mux.ServeHTTP(w, r)
}

func (b *Broker) routes() {
	b.mux.HandleFunc("POST /v1/topics/{topic}/messages", b.publish)
	b.mux.HandleFunc("POST /v1/topics/{topic}/batches", b.publishBatch)
	b.mux.HandleFunc("GET /v1/subscriptions/stream", b.stream)
	b.mux.HandleFunc("POST /v1/deliveries/ack", b.ack)
	b.mux.HandleFunc("POST /v1/deliveries/nack", b.nack)
	b.mux.HandleFunc("POST /internal/replicate", b.replicate)
	b.mux.HandleFunc("GET /internal/snapshot", b.snapshot)
	b.mux.HandleFunc("GET /internal/checkpoints", b.checkpointSnapshot)
	b.mux.HandleFunc("GET /internal/replay-frontiers", b.replayFrontierSnapshot)
	b.mux.HandleFunc("GET /internal/capabilities", b.peerCapabilities)
	b.mux.HandleFunc("GET /internal/cache-digest", b.cacheDigest)
	b.mux.HandleFunc("POST /internal/ack", b.internalAck)
	b.mux.HandleFunc("POST /internal/nack", b.internalNack)
	b.mux.HandleFunc("GET /metrics", b.prometheus)
	b.mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	b.mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !b.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	b.mux.HandleFunc("GET /v1/status", b.status)
	b.mux.HandleFunc("GET /v1/status/messages/{id}", b.messageStatus)
}

const (
	maxBatchMessages = 4096
	maxBatchBytes    = 16 << 20
)

var batchReaders = sync.Pool{New: func() any {
	return bufio.NewReaderSize(nil, 64<<10)
}}

// Batch v1 is [4 byte payload length][payload] and shares Spruce-Key.
// Batch v2 is [2 byte key length][key][4 byte payload length][payload].
// The version is selected by Spruce-Batch-Version; retaining v1 keeps existing clients compatible.
func (b *Broker) publishBatch(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	defer func() { b.metrics.PublishLatency.observe(time.Since(started)) }()
	reserved := r.ContentLength
	if reserved <= 0 {
		reserved = maxBatchBytes
	}
	if !b.acquirePublishAdmission(r.Context(), reserved) {
		b.metrics.Rejected.Add(1)
		b.metrics.OverloadRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, "overloaded")
		return
	}
	defer b.releasePublishAdmission(reserved)
	if r.ContentLength > maxBatchBytes {
		problem(w, 413, "batch_too_large")
		return
	}
	topic := r.PathValue("topic")
	if !validTopic(topic) {
		problem(w, 400, "invalid_topic")
		return
	}
	if b.topicDeliveryPressured(topic, time.Now()) {
		b.metrics.Rejected.Add(1)
		b.metrics.OverloadRejected.Add(1)
		b.metrics.DeliveryPressureRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, "delivery_overloaded")
		return
	}
	if len(r.Header.Get("Spruce-Key")) > maxHeaders || len(r.Header.Get("Content-Type")) > maxHeaders {
		problem(w, 400, "invalid_metadata")
		return
	}
	if ack := r.Header.Get("Spruce-Ack"); ack != "" && ack != "local" {
		problem(w, 400, "invalid_ack_mode")
		return
	}
	if r.Header.Get("Spruce-Idempotency-Key") != "" {
		problem(w, 400, "batch_idempotency_unsupported")
		return
	}
	ttl, err := b.parseTTL(r.Header.Get("Spruce-TTL"))
	if err != nil {
		problem(w, 400, "invalid_ttl")
		return
	}
	reader := batchReaders.Get().(*bufio.Reader)
	reader.Reset(io.LimitReader(r.Body, maxBatchBytes+1))
	defer func() {
		reader.Reset(nil)
		batchReaders.Put(reader)
	}()
	payloads := make([][]byte, 0, 256)
	keys := make([]string, 0, 256)
	version := r.Header.Get("Spruce-Batch-Version")
	if version != "" && version != "1" && version != "2" {
		problem(w, 400, "invalid_batch_version")
		return
	}
	var total int64
	for len(payloads) < maxBatchMessages {
		var key string
		if version == "2" {
			var keySize [2]byte
			_, err := io.ReadFull(reader, keySize[:])
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				problem(w, 400, "invalid_batch")
				return
			}
			kn := int64(binary.BigEndian.Uint16(keySize[:]))
			total += 2 + kn
			if kn > maxHeaders || total > maxBatchBytes {
				problem(w, 413, "batch_too_large")
				return
			}
			keyBytes := make([]byte, kn)
			if _, err = io.ReadFull(reader, keyBytes); err != nil {
				problem(w, 400, "invalid_batch")
				return
			}
			key = string(keyBytes)
		}
		var size [4]byte
		_, err := io.ReadFull(reader, size[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			b.metrics.Rejected.Add(1)
			problem(w, 400, "invalid_batch")
			return
		}
		n := int64(binary.BigEndian.Uint32(size[:]))
		total += 4 + n
		if n > b.cfg.MaxMessage || total > maxBatchBytes {
			b.metrics.Rejected.Add(1)
			problem(w, 413, "batch_too_large")
			return
		}
		payload := make([]byte, n)
		if _, err = io.ReadFull(reader, payload); err != nil {
			b.metrics.Rejected.Add(1)
			problem(w, 400, "invalid_batch")
			return
		}
		payloads = append(payloads, payload)
		keys = append(keys, key)
	}
	if len(payloads) == maxBatchMessages {
		if _, err := reader.Peek(1); err == nil {
			problem(w, 413, "too_many_batch_messages")
			return
		}
	}
	if len(payloads) == 0 {
		problem(w, 400, "empty_batch")
		return
	}
	now := time.Now()
	ids := make([]string, 0, len(payloads))
	messages := make([]*Message, 0, len(payloads))
	sharedKey, contentType := r.Header.Get("Spruce-Key"), r.Header.Get("Content-Type")
	for i, payload := range payloads {
		key := sharedKey
		if version == "2" {
			key = keys[i]
		}
		m := &Message{ID: b.nextID(), Topic: topic, Key: key, Payload: payload,
			CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(ttl).UnixMilli()}
		if contentType != "" {
			m.Headers = map[string]string{"content-type": contentType}
		}
		if messageSize(m) > b.cfg.CacheBytes {
			problem(w, 507, "cache_capacity")
			return
		}

		messages = append(messages, m)
		ids = append(ids, m.ID)
	}
	replicationBytes := batchBytes(messages)
	reservedPeer := b.waitReplicationAdmission(r.Context(), replicationBytes)
	if len(b.peers) > 0 && reservedPeer == nil {
		b.metrics.Rejected.Add(1)
		b.metrics.OverloadRejected.Add(1)
		b.metrics.ReplicationPressureRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, "replication_overloaded")
		return
	}
	if err := b.acceptBatch(messages); err != nil {
		b.releaseReplicationReservation(reservedPeer, replicationBytes)
		if errors.Is(err, errRetentionCapacity) {
			w.Header().Set("Retry-After", "1")
			problem(w, 429, "retention_capacity")
		} else {
			problem(w, 507, "cache_capacity")
		}
		return
	}
	for _, m := range messages {
		b.metrics.Published.Add(1)
		b.metrics.PublishBytes.Add(uint64(len(m.Payload)))
	}
	b.enqueuePeerBatch(messages, reservedPeer)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(202)
	_ = json.NewEncoder(w).Encode(map[string]any{"ids": ids})
}

func (b *Broker) parseTTL(value string) (time.Duration, error) {
	if value == "" {
		return b.cfg.DefaultTTL, nil
	}
	ttl, err := time.ParseDuration(value)
	if err != nil || ttl <= 0 || ttl > b.cfg.MaxTTL {
		return 0, errors.New("invalid TTL")
	}
	return ttl, nil
}

func validTopic(s string) bool {
	if len(s) == 0 || len(s) > 255 {
		return false
	}
	for _, r := range s {
		if !(r == '.' || r == '-' || r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func (b *Broker) nextID() string {
	var raw [16]byte
	copy(raw[:8], b.boot[:])
	binary.BigEndian.PutUint64(raw[8:], b.seq.Add(1))
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

var errIdempotencyConflict = errors.New("idempotency key reused with different message")

func messageFingerprint(payload []byte, key, contentType string, ttl time.Duration) [32]byte {
	h := sha256.New()
	_, _ = h.Write(payload)
	_, _ = io.WriteString(h, "\x00"+key+"\x00"+contentType+"\x00"+ttl.String())
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func (b *Broker) beginIdempotency(ctx context.Context, key, id string, expiresAt int64, fingerprint [32]byte) (*idempotencyEntry, bool, error) {
	for {
		b.mu.Lock()
		if existing := b.idempotency[key]; existing != nil && (existing.inProgress || existing.expiresAt > time.Now().UnixMilli()) {
			if existing.fingerprint != fingerprint {
				b.mu.Unlock()
				return nil, false, errIdempotencyConflict
			}
			ready := existing.ready
			b.mu.Unlock()
			select {
			case <-ready:
				if existing.accepted {
					return existing, false, nil
				}
				continue
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}
		if len(b.idempotency) >= b.cfg.IdempotencyEntries {
			evicted := false
			for i, candidate := range b.idempotencyOrder {
				current := b.idempotency[candidate.key]
				if current == nil || current.id != candidate.id {
					continue
				}
				if current.inProgress {
					continue
				}
				delete(b.idempotency, candidate.key)
				b.idempotencyOrder = append(b.idempotencyOrder[:i], b.idempotencyOrder[i+1:]...)
				evicted = true
				break
			}
			if !evicted {
				b.mu.Unlock()
				return nil, false, errors.New("idempotency capacity exhausted")
			}
		}
		entry := &idempotencyEntry{id: id, expiresAt: expiresAt, ready: make(chan struct{}), fingerprint: fingerprint, inProgress: true}
		b.idempotency[key] = entry
		b.idempotencyOrder = append(b.idempotencyOrder, idempotencyOrderEntry{key: key, id: id})
		if len(b.idempotencyOrder) > 2*b.cfg.IdempotencyEntries {
			compacted := b.idempotencyOrder[:0]
			for _, item := range b.idempotencyOrder {
				if current := b.idempotency[item.key]; current != nil && current.id == item.id {
					compacted = append(compacted, item)
				}
			}
			b.idempotencyOrder = compacted
		}
		b.mu.Unlock()
		return entry, true, nil
	}
}

func (b *Broker) finishIdempotency(key string, entry *idempotencyEntry, accepted, replicated bool) {
	if entry == nil {
		return
	}
	b.mu.Lock()
	entry.accepted, entry.replicated = accepted, replicated
	entry.inProgress = false
	if !accepted {
		if b.idempotency[key] == entry {
			delete(b.idempotency, key)
		}
	}
	close(entry.ready)
	b.mu.Unlock()
}

func (b *Broker) markIdempotencyReplicated(entry *idempotencyEntry) {
	b.mu.Lock()
	entry.replicated = true
	b.mu.Unlock()
}
func (b *Broker) idempotencyReplicated(entry *idempotencyEntry) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return entry.replicated
}

func (b *Broker) publish(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	defer func() { b.metrics.PublishLatency.observe(time.Since(started)) }()
	reserved := r.ContentLength
	if reserved <= 0 {
		reserved = b.cfg.MaxMessage
	}
	if !b.acquirePublishAdmission(r.Context(), reserved) {
		b.metrics.Rejected.Add(1)
		b.metrics.OverloadRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, "overloaded")
		return
	}
	defer b.releasePublishAdmission(reserved)
	if r.ContentLength > b.cfg.MaxMessage {
		problem(w, 413, "message_too_large")
		return
	}
	topic := r.PathValue("topic")
	if !validTopic(topic) {
		problem(w, 400, "invalid_topic")
		return
	}
	if b.topicDeliveryPressured(topic, time.Now()) {
		b.metrics.Rejected.Add(1)
		b.metrics.OverloadRejected.Add(1)
		b.metrics.DeliveryPressureRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, "delivery_overloaded")
		return
	}
	if len(r.Header.Get("Spruce-Key")) > maxHeaders || len(r.Header.Get("Content-Type")) > maxHeaders {
		problem(w, 400, "invalid_metadata")
		return
	}
	ack := r.URL.Query().Get("ack")
	if ack == "" {
		ack = r.Header.Get("Spruce-Ack")
	}
	if ack == "" {
		ack = "local"
	}
	if ack != "local" && ack != "one-peer" && ack != "available" {
		problem(w, 400, "invalid_ack_mode")
		return
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, b.cfg.MaxMessage+1))
	if err != nil || int64(len(payload)) > b.cfg.MaxMessage {
		b.metrics.Rejected.Add(1)
		problem(w, 413, "message_too_large")
		return
	}
	idempotencyKey := r.Header.Get("Spruce-Idempotency-Key")
	producerID := r.Header.Get("Spruce-Producer-ID")
	if len(idempotencyKey) > 128 || len(producerID) > 128 || (idempotencyKey != "" && producerID == "") {
		problem(w, 400, "invalid_idempotency_key")
		return
	}
	ttl, err := b.parseTTL(r.Header.Get("Spruce-TTL"))
	if err != nil {
		problem(w, 400, "invalid_ttl")
		return
	}
	now := time.Now()
	id := b.nextID()
	var idempotency *idempotencyEntry
	idempotencyCacheKey := ""
	if idempotencyKey != "" {
		id = idempotentMessageID(topic, producerID, idempotencyKey)
		idempotencyCacheKey = id
		var owner bool
		fingerprint := messageFingerprint(payload, r.Header.Get("Spruce-Key"), r.Header.Get("Content-Type"), ttl)
		idempotency, owner, err = b.beginIdempotency(r.Context(), idempotencyCacheKey, id, now.Add(ttl).UnixMilli(), fingerprint)
		if err != nil {
			if errors.Is(err, errIdempotencyConflict) {
				problem(w, 409, "idempotency_conflict")
			} else {
				problem(w, 429, "idempotency_capacity")
			}
			return
		}
		if !owner {
			id = idempotency.id
			if ack == "available" {
				m := b.cache.get(id)
				if m == nil {
					problem(w, 409, "idempotency_history_expired")
					return
				}
				b.publishAvailable(w, r, m, true, nil, "")
				return
			}
			replicated := b.idempotencyReplicated(idempotency)
			if ack == "one-peer" {
				m := b.cache.get(id)
				if m == nil || !b.replicateOne(r.Context(), m) {
					problem(w, 503, "peer_ack_unavailable")
					return
				}
				b.markIdempotencyReplicated(idempotency)
				replicated = true
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(202)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "replicated": replicated, "deduplicated": true})
			return
		}
	}
	m := &Message{ID: id, Topic: topic, Key: r.Header.Get("Spruce-Key"), Payload: payload,
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(ttl).UnixMilli()}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		m.Headers = map[string]string{"content-type": ct}
	}
	replicationBytes := messageSize(m)
	var reservedPeer *peer
	if ack == "local" {
		reservedPeer = b.waitReplicationAdmission(r.Context(), replicationBytes)
	}
	if ack == "local" && len(b.peers) > 0 && reservedPeer == nil {
		b.finishIdempotency(idempotencyCacheKey, idempotency, false, false)
		b.metrics.Rejected.Add(1)
		b.metrics.OverloadRejected.Add(1)
		b.metrics.ReplicationPressureRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, "replication_overloaded")
		return
	}
	var inserted bool
	if idempotencyKey != "" {
		m, inserted, err = b.acceptIdempotent(m, ttl)
	} else {
		inserted, err = b.accept(m)
	}
	if err != nil {
		b.releaseReplicationReservation(reservedPeer, replicationBytes)
		b.finishIdempotency(idempotencyCacheKey, idempotency, false, false)
		b.metrics.Rejected.Add(1)
		if errors.Is(err, errIdempotencyConflict) {
			problem(w, 409, "idempotency_conflict")
		} else {
			if errors.Is(err, errRetentionCapacity) {
				w.Header().Set("Retry-After", "1")
				problem(w, 429, "retention_capacity")
			} else {
				problem(w, 507, "cache_capacity")
			}
		}
		return
	}
	if idempotency != nil {
		b.mu.Lock()
		idempotency.expiresAt = m.ExpiresAt
		b.mu.Unlock()
	}
	if inserted {
		b.metrics.Published.Add(1)
		b.metrics.PublishBytes.Add(uint64(len(payload)))
	}
	if ack == "available" {
		b.publishAvailable(w, r, m, !inserted, idempotency, idempotencyCacheKey)
		return
	}
	replicated := false
	if ack == "one-peer" && len(b.peers) > 0 {
		replicated = b.replicateOne(r.Context(), m)
	}
	if ack == "one-peer" && !replicated {
		b.finishIdempotency(idempotencyCacheKey, idempotency, true, false)
		problem(w, 503, "peer_ack_unavailable")
		return
	}
	if ack != "one-peer" || !replicated {
		b.enqueuePeerBatch([]*Message{m}, reservedPeer)
	}
	b.finishIdempotency(idempotencyCacheKey, idempotency, true, replicated)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(202)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": m.ID, "replicated": replicated, "deduplicated": !inserted})
}

func (b *Broker) accept(m *Message) (bool, error) {
	b.cache.mu.Lock()
	defer b.cache.mu.Unlock()
	return b.acceptLocked(m)
}

func (b *Broker) acceptLocked(m *Message) (bool, error) {
	b.cache.expireLocked(time.Now().UnixMilli())
	if messageSize(m) > b.cache.maxBytes {
		return false, errors.New("message exceeds cache capacity")
	}
	if _, exists := b.cache.items[m.ID]; exists {
		b.metrics.Duplicate.Add(1)
		return false, nil
	}
	if b.cache.bytes+messageSize(m) > b.cache.maxBytes {
		return false, errRetentionCapacity
	}
	if m.Origin == "" && b.cache.topicSequences[m.Topic] == nil && len(b.cache.topicSequences) >= b.cache.frontierLimit {
		return false, errors.New("topic sequence capacity")
	}
	if err := b.prepareGroupWork([]*Message{m}); err != nil {
		return false, err
	}
	if m.Origin == "" {
		state := b.cache.topicSequences[m.Topic]
		if state == nil {
			if len(b.cache.topicSequences) >= b.cache.frontierLimit {
				b.cache.markUnsafeLocked(m.Topic, "sequence-capacity", m.ExpiresAt)
				return false, errors.New("topic sequence capacity")
			}
			state = &topicSequence{origin: b.origin + "." + strconv.FormatUint(b.seq.Add(1), 36)}
			b.cache.topicSequences[m.Topic] = state
		}
		state.next++
		state.expiresAt = max(state.expiresAt, m.ExpiresAt)
		m.Origin, m.Sequence = state.origin, state.next
		b.cache.receivedThrough[m.Origin] = m.Sequence
	}
	inserted, err := b.cache.insertLocked(m)
	if err != nil || !inserted {
		if !inserted && err == nil {
			b.metrics.Duplicate.Add(1)
		}
		return inserted, err
	}
	b.deliver(m, "", 1)
	return true, nil
}

func (b *Broker) acceptBatch(messages []*Message) error {
	b.cache.mu.Lock()
	defer b.cache.mu.Unlock()
	b.cache.expireLocked(time.Now().UnixMilli())
	// Admission must precede sequence assignment: rejected batches must not
	// leave holes that replicas can never fill.
	var total int64
	newTopics := make(map[string]struct{})
	for _, m := range messages {
		size := messageSize(m)
		if size > b.cache.maxBytes || total > b.cache.maxBytes-size {
			return errors.New("message exceeds cache capacity")
		}
		total += size
		if m.Origin == "" && b.cache.topicSequences[m.Topic] == nil {
			newTopics[m.Topic] = struct{}{}
		}
	}
	if len(b.cache.topicSequences)+len(newTopics) > b.cache.frontierLimit {
		return errors.New("topic sequence capacity")
	}
	var added int64
	for _, m := range messages {
		if b.cache.items[m.ID] == nil {
			added += messageSize(m)
		}
	}
	if b.cache.bytes+added > b.cache.maxBytes {
		return errRetentionCapacity
	}
	if err := b.prepareGroupWork(messages); err != nil {
		return err
	}
	for _, m := range messages {
		if m.Origin == "" {
			state := b.cache.topicSequences[m.Topic]
			if state == nil {
				if len(b.cache.topicSequences) >= b.cache.frontierLimit {
					b.cache.markUnsafeLocked(m.Topic, "sequence-capacity", m.ExpiresAt)
					return errors.New("topic sequence capacity")
				}
				state = &topicSequence{origin: b.origin + "." + strconv.FormatUint(b.seq.Add(1), 36)}
				b.cache.topicSequences[m.Topic] = state
			}
			state.next++
			state.expiresAt = max(state.expiresAt, m.ExpiresAt)
			m.Origin, m.Sequence = state.origin, state.next
			b.cache.receivedThrough[m.Origin] = m.Sequence
		} else {
			b.cache.receivedThrough[m.Origin] = max(b.cache.receivedThrough[m.Origin], m.Sequence)
		}
	}
	inserted := b.cache.insertBatchLocked(messages)
	for i, m := range messages {
		if inserted[i] {
			b.deliver(m, "", 1)
		} else {
			b.metrics.Duplicate.Add(1)
		}
	}
	return nil
}

func (b *Broker) acceptReplicatedBatch(messages []*Message) error {
	b.cache.mu.Lock()
	defer b.cache.mu.Unlock()
	now := time.Now().UnixMilli()
	b.cache.expireLocked(now)
	for _, m := range messages {
		if m.Origin == "" || m.Sequence == 0 || messageSize(m) > b.cache.maxBytes {
			return errors.New("invalid replicated sequence")
		}
	}
	var added int64
	for _, m := range messages {
		if b.cache.items[m.ID] == nil {
			added += messageSize(m)
		}
	}
	if b.cache.bytes+b.cache.reorderBytes+added > b.cache.maxBytes {
		return errRetentionCapacity
	}
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].Origin == messages[j].Origin {
			return messages[i].Sequence < messages[j].Sequence
		}
		return messages[i].Origin < messages[j].Origin
	})
	for _, m := range messages {
		fromGap := false
		through := b.cache.receivedThrough[m.Origin]
		if m.Sequence <= through {
			if _, exists := b.cache.items[m.ID]; exists {
				b.metrics.Duplicate.Add(1)
			}
			next := b.cache.reorder[m.Origin][through+1]
			if next == nil {
				continue
			}
			m = next
			fromGap = true
			delete(b.cache.reorder[m.Origin], m.Sequence)
			b.cache.reorderBytes -= messageSize(m)
		}
		if m.Sequence > through+1 {
			gaps := b.cache.reorder[m.Origin]
			if gaps == nil {
				gaps = make(map[uint64]*Message)
				b.cache.reorder[m.Origin] = gaps
			}
			if _, exists := gaps[m.Sequence]; exists {
				b.metrics.Duplicate.Add(1)
				continue
			}
			if len(gaps) >= b.cache.reorderLimit || b.cache.reorderBytes+messageSize(m) > b.cache.maxBytes/4 {
				b.cache.markUnsafeLocked(m.Topic, "overflow:"+m.Origin, m.ExpiresAt)
				b.metrics.ReplicationDropped.Add(1)
				continue
			}
			gaps[m.Sequence] = m
			b.cache.reorderBytes += messageSize(m)
			b.cache.markUnsafeLocked(m.Topic, "gap:"+m.Origin, m.ExpiresAt)
			continue
		}
		for current := m; current != nil; {
			if err := b.prepareGroupWork([]*Message{current}); err != nil {
				if current != m || fromGap {
					if b.cache.reorder[current.Origin] == nil {
						b.cache.reorder[current.Origin] = make(map[uint64]*Message)
					}
					b.cache.reorder[current.Origin][current.Sequence] = current
					b.cache.reorderBytes += messageSize(current)
					b.cache.markUnsafeLocked(current.Topic, "gap:"+current.Origin, current.ExpiresAt)
				}
				return err
			}
			inserted, err := b.cache.insertLocked(current)
			if err != nil {
				return err
			}
			b.cache.receivedThrough[current.Origin] = current.Sequence
			if inserted {
				b.deliver(current, "", 1)
			} else {
				b.metrics.Duplicate.Add(1)
			}
			next := b.cache.reorder[current.Origin][current.Sequence+1]
			if next != nil {
				delete(b.cache.reorder[current.Origin], current.Sequence+1)
				b.cache.reorderBytes -= messageSize(next)
			}
			if len(b.cache.reorder[current.Origin]) == 0 {
				delete(b.cache.reorder, current.Origin)
				b.cache.clearUnsafeLocked(current.Topic, "gap:"+current.Origin)
			}
			current = next
		}
	}
	return nil
}

func (b *Broker) enqueuePeers(m *Message) {
	b.enqueuePeerBatch([]*Message{m}, nil)
}

func (b *Broker) enqueuePeerBatch(messages []*Message, reservedPeer *peer) {
	b.enqueuePeerBatchExcept(messages, reservedPeer, nil)
}

func (b *Broker) enqueuePeerBatchExcept(messages []*Message, reservedPeer *peer, confirmedPeers ...*peer) {
	var bytes int64
	for _, m := range messages {
		bytes += messageSize(m)
	}
peerLoop:
	for _, p := range b.peers {
		for _, confirmed := range confirmedPeers {
			if p == confirmed {
				continue peerLoop
			}
		}
		if p == reservedPeer {
			select {
			case p.ch <- messages:
			case <-b.stop:
				b.releaseReplicationReservation(p, bytes)
			}
			continue
		}
		for {
			queued := p.queuedBytes.Load()
			if queued+bytes > b.cfg.ReplicationQueueBytes {
				b.metrics.ReplicationDropped.Add(uint64(len(messages)))
				break
			}
			if !p.queuedBytes.CompareAndSwap(queued, queued+bytes) {
				continue
			}
			select {
			case p.ch <- messages:
			default:
				p.queuedBytes.Add(-bytes)
				b.metrics.ReplicationDropped.Add(uint64(len(messages)))
			}
			break
		}
	}
}

func (b *Broker) replicationHighWaterBytes() int64 {
	highWater := b.cfg.ReplicationQueueBytes * 3 / 4
	if highWater == 0 {
		highWater = 1
	}
	return highWater
}

func (b *Broker) reserveReplicationCapacity(bytes int64) *peer {
	highWater := b.replicationHighWaterBytes()
	for _, p := range b.peers {
		for {
			queued := p.queuedBytes.Load()
			fitsWatermark := queued+bytes <= highWater
			fitsEmptyQueue := queued == 0 && bytes <= b.cfg.ReplicationQueueBytes
			if !fitsWatermark && !fitsEmptyQueue {
				break
			}
			if p.queuedBytes.CompareAndSwap(queued, queued+bytes) {
				return p
			}
		}
	}
	return nil
}

func (b *Broker) waitReplicationAdmission(ctx context.Context, bytes int64) *peer {
	if len(b.peers) == 0 {
		return nil
	}
	if p := b.reserveReplicationCapacity(bytes); p != nil {
		return p
	}
	timer := time.NewTimer(b.cfg.PublishAdmissionWait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			return b.reserveReplicationCapacity(bytes)
		case <-b.replicationFreed:
			if p := b.reserveReplicationCapacity(bytes); p != nil {
				return p
			}
		}
	}
}

func (b *Broker) releaseReplicationReservation(p *peer, bytes int64) {
	if p == nil {
		return
	}
	p.queuedBytes.Add(-bytes)
	b.signalReplicationFreed()
}

func (b *Broker) signalReplicationFreed() {
	select {
	case b.replicationFreed <- struct{}{}:
	default:
	}
}

func writePeerString(w io.Writer, value string) error {
	if len(value) > 65535 {
		return errors.New("peer string too large")
	}
	var size [2]byte
	binary.BigEndian.PutUint16(size[:], uint16(len(value)))
	if _, err := w.Write(size[:]); err != nil {
		return err
	}
	_, err := io.WriteString(w, value)
	return err
}

func writePeerBatch(w io.Writer, messages []*Message) error {
	if len(messages) == 0 || len(messages) > maxBatchMessages {
		return errors.New("invalid peer batch size")
	}
	if buffer, ok := w.(*bytes.Buffer); ok {
		encoded, err := appendPeerBatch(buffer.AvailableBuffer(), messages)
		if err != nil {
			return err
		}
		_, err = buffer.Write(encoded)
		return err
	}
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(messages)))
	if _, err := w.Write(count[:]); err != nil {
		return err
	}
	for _, m := range messages {
		if err := writePeerString(w, m.ID); err != nil {
			return err
		}
		if err := writePeerString(w, m.Topic); err != nil {
			return err
		}
		if err := writePeerString(w, m.Key); err != nil {
			return err
		}
		if err := writePeerString(w, m.Origin); err != nil {
			return err
		}
		if len(m.Headers) > 65535 || len(m.Payload) > int(^uint32(0)) {
			return errors.New("peer message too large")
		}
		var fixed [30]byte
		binary.BigEndian.PutUint16(fixed[:2], uint16(len(m.Headers)))
		binary.BigEndian.PutUint32(fixed[2:6], uint32(len(m.Payload)))
		binary.BigEndian.PutUint64(fixed[6:14], uint64(m.CreatedAt))
		binary.BigEndian.PutUint64(fixed[14:22], uint64(m.ExpiresAt))
		binary.BigEndian.PutUint64(fixed[22:30], m.Sequence)
		if _, err := w.Write(fixed[:]); err != nil {
			return err
		}
		for k, v := range m.Headers {
			if err := writePeerString(w, k); err != nil {
				return err
			}
			if err := writePeerString(w, v); err != nil {
				return err
			}
		}
		if _, err := w.Write(m.Payload); err != nil {
			return err
		}
	}
	return nil
}

func appendPeerBatch(out []byte, messages []*Message) ([]byte, error) {
	if len(messages) == 0 || len(messages) > maxBatchMessages {
		return out, errors.New("invalid peer batch size")
	}
	out = binary.BigEndian.AppendUint32(out, uint32(len(messages)))
	for _, m := range messages {
		var err error
		if out, err = appendPeerString(out, m.ID); err != nil {
			return out, err
		}
		if out, err = appendPeerString(out, m.Topic); err != nil {
			return out, err
		}
		if out, err = appendPeerString(out, m.Key); err != nil {
			return out, err
		}
		if out, err = appendPeerString(out, m.Origin); err != nil {
			return out, err
		}
		if len(m.Headers) > 65535 || len(m.Payload) > int(^uint32(0)) {
			return out, errors.New("peer message too large")
		}
		out = binary.BigEndian.AppendUint16(out, uint16(len(m.Headers)))
		out = binary.BigEndian.AppendUint32(out, uint32(len(m.Payload)))
		out = binary.BigEndian.AppendUint64(out, uint64(m.CreatedAt))
		out = binary.BigEndian.AppendUint64(out, uint64(m.ExpiresAt))
		out = binary.BigEndian.AppendUint64(out, m.Sequence)
		for key, value := range m.Headers {
			if out, err = appendPeerString(out, key); err != nil {
				return out, err
			}
			if out, err = appendPeerString(out, value); err != nil {
				return out, err
			}
		}
		out = append(out, m.Payload...)
	}
	return out, nil
}

func appendPeerString(out []byte, value string) ([]byte, error) {
	if len(value) > 65535 {
		return out, errors.New("peer string too large")
	}
	out = binary.BigEndian.AppendUint16(out, uint16(len(value)))
	return append(out, value...), nil
}

func readPeerString(r io.Reader, maximum int) (string, error) {
	var size [2]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return "", err
	}
	n := int(binary.BigEndian.Uint16(size[:]))
	if n > maximum {
		return "", errors.New("peer string exceeds limit")
	}
	value := make([]byte, n)
	if _, err := io.ReadFull(r, value); err != nil {
		return "", err
	}
	return string(value), nil
}

func readPeerBatch(r io.Reader, maxMessage int64) ([]*Message, error) {
	var countBytes [4]byte
	if _, err := io.ReadFull(r, countBytes[:]); err != nil {
		return nil, err
	}
	count := int(binary.BigEndian.Uint32(countBytes[:]))
	if count == 0 || count > maxBatchMessages {
		return nil, errors.New("invalid peer batch size")
	}
	messages := make([]*Message, 0, count)
	for range count {
		id, err := readPeerString(r, 64)
		if err != nil {
			return nil, err
		}
		topic, err := readPeerString(r, 255)
		if err != nil {
			return nil, err
		}
		key, err := readPeerString(r, maxHeaders)
		if err != nil {
			return nil, err
		}
		origin, err := readPeerString(r, 64)
		if err != nil {
			return nil, err
		}
		var fixed [30]byte
		if _, err = io.ReadFull(r, fixed[:]); err != nil {
			return nil, err
		}
		headerCount := int(binary.BigEndian.Uint16(fixed[:2]))
		payloadSize := int64(binary.BigEndian.Uint32(fixed[2:6]))
		if headerCount > 256 || payloadSize > maxMessage {
			return nil, errors.New("peer message exceeds limit")
		}
		var headers map[string]string
		if headerCount > 0 {
			headers = make(map[string]string, headerCount)
		}
		headerBytes := 0
		for range headerCount {
			k, e := readPeerString(r, maxHeaders)
			if e != nil {
				return nil, e
			}
			v, e := readPeerString(r, maxHeaders)
			if e != nil {
				return nil, e
			}
			headerBytes += len(k) + len(v)
			if headerBytes > maxHeaders {
				return nil, errors.New("peer headers exceed limit")
			}
			headers[k] = v
		}
		payload := make([]byte, payloadSize)
		if _, err = io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		messages = append(messages, &Message{ID: id, Topic: topic, Key: key, Headers: headers, Payload: payload,
			CreatedAt: int64(binary.BigEndian.Uint64(fixed[6:14])), ExpiresAt: int64(binary.BigEndian.Uint64(fixed[14:22])), Origin: origin, Sequence: binary.BigEndian.Uint64(fixed[22:30])})
	}
	return messages, nil
}

func (b *Broker) sendPeer(ctx context.Context, p *peer, m *Message) bool {
	return b.sendPeerBatch(ctx, p, []*Message{m})
}

func (b *Broker) sendPeerBatch(ctx context.Context, p *peer, messages []*Message) bool {
	b.probePeerV2(ctx, p)
	var body bytes.Buffer
	var err error
	v2 := p.v2.Load() || !p.legacy.Load()
	if v2 {
		err = writePeerBatch(&body, messages)
	} else {
		err = writePeerBatchV1(&body, messages)
	}
	if err != nil {
		b.metrics.ReplicationErrors.Add(uint64(len(messages)))
		return false
	}
	return b.sendPeerBody(ctx, p, body.Bytes(), len(messages), v2)
}

func (b *Broker) sendPeerBody(ctx context.Context, p *peer, body []byte, count int, v2 bool) bool {
	started := time.Now()
	defer func() { b.metrics.ReplicationLatency.observe(time.Since(started)) }()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/internal/replicate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/vnd.spruce.peer")
	req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
	req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
	if v2 {
		req.Header.Set("Spruce-Peer-Version", "2")
	}
	resp, err := b.client.Do(req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			p.unavailableUntil.Store(time.Now().Add(time.Second).UnixNano())
			b.metrics.ReplicationErrors.Add(1)
		}
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		p.unavailableUntil.Store(time.Now().Add(time.Second).UnixNano())
		b.metrics.ReplicationErrors.Add(1)
		return false
	}
	p.unavailableUntil.Store(0)
	b.metrics.Replicated.Add(uint64(count))
	return true
}

func (b *Broker) peerLoop(p *peer) {
	var encoded bytes.Buffer
	for {
		select {
		case <-b.stop:
			return
		case batch := <-p.ch:
			b.probePeerV2(context.Background(), p)
			encoded.Reset()
			var err error
			v2 := p.v2.Load() || !p.legacy.Load()
			if v2 {
				err = writePeerBatch(&encoded, batch)
			} else {
				err = writePeerBatchV1(&encoded, batch)
			}
			if err != nil {
				b.metrics.ReplicationErrors.Add(uint64(len(batch)))
				p.queuedBytes.Add(-batchBytes(batch))
				b.signalReplicationFreed()
				continue
			}
			replicated := false
			for attempt := 0; attempt < 3; attempt++ {
				if b.sendPeerBody(context.Background(), p, encoded.Bytes(), len(batch), v2) {
					replicated = true
					break
				}
				if attempt == 2 {
					break
				}
				timer := time.NewTimer(time.Duration(1<<attempt) * 50 * time.Millisecond)
				select {
				case <-b.stop:
					if !timer.Stop() {
						<-timer.C
					}
					p.queuedBytes.Add(-batchBytes(batch))
					b.signalReplicationFreed()
					return
				case <-timer.C:
				}
			}
			if !replicated {
				b.metrics.ReplicationDropped.Add(uint64(len(batch)))
			}
			p.queuedBytes.Add(-batchBytes(batch))
			b.signalReplicationFreed()
			if encoded.Cap() > 1<<20 {
				encoded = bytes.Buffer{}
			}
		}
	}
}

func (b *Broker) probePeerV2(ctx context.Context, p *peer) {
	if p.v2.Load() {
		return
	}
	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	if p.v2.Load() {
		return
	}
	now := time.Now().UnixMilli()
	previous := p.lastV2Probe.Load()
	if now-previous < 30000 || !p.lastV2Probe.CompareAndSwap(previous, now) {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(probeCtx, http.MethodGet, p.url+"/internal/capabilities", nil)
	req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
	req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
	resp, err := b.client.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent && resp.Header.Get("Spruce-Peer-Version") == "2" {
		p.v2.Store(true)
	} else if resp.StatusCode == http.StatusNotFound {
		// Only an explicit unsupported endpoint permits legacy fallback.
		// A timeout or a restarting modern peer must not poison replay safety.
		p.legacy.Store(true)
	}
}

func batchBytes(batch []*Message) int64 {
	var bytes int64
	for _, m := range batch {
		bytes += messageSize(m)
	}
	return bytes
}

func (b *Broker) replicateOne(ctx context.Context, m *Message) bool {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	result := make(chan *peer, len(b.peers))
	for _, p := range b.peers {
		select {
		case p.copySlots <- struct{}{}:
		default:
			result <- nil
			continue
		}
		go func(p *peer) {
			defer func() { <-p.copySlots }()
			if b.sendPeer(ctx, p, m) {
				result <- p
			} else {
				result <- nil
			}
		}(p)
	}
	for range b.peers {
		if confirmed := <-result; confirmed != nil {
			// Returning cancels the other synchronous writes. Queue their
			// copies so cancellation cannot leave permanent sequence holes.
			b.enqueuePeerBatchExcept([]*Message{m}, nil, confirmed)
			return true
		}
	}
	return false
}

func (b *Broker) peerAllowed(r *http.Request) bool {
	provided := r.Header.Get("Spruce-Peer-Token")
	if b.cfg.PeerToken == "" || (!tokenEqual(provided, b.cfg.PeerToken) && !tokenEqual(provided, b.cfg.PreviousPeerToken)) {
		b.metrics.PeerAuthRejected.Add(1)
		return false
	}
	allowed := r.Header.Get("Spruce-Cluster-ID") == b.cfg.ClusterID
	if !allowed {
		b.metrics.PeerAuthRejected.Add(1)
	}
	return allowed
}

func tokenEqual(provided, expected string) bool {
	return expected != "" && len(provided) == len(expected) && subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
}

func (b *Broker) snapshot(w http.ResponseWriter, r *http.Request) {
	if !b.peerAllowed(r) {
		problem(w, http.StatusUnauthorized, "invalid_peer")
		return
	}
	messages, next, valid := b.cache.page(r.URL.Query().Get("after"), 24<<20)
	if !valid {
		problem(w, http.StatusConflict, "snapshot_cursor_expired")
		return
	}
	if len(messages) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	v2 := r.Header.Get("Spruce-Peer-Version") == "2"
	if v2 {
		w.Header().Set("Content-Type", "application/vnd.spruce.peer.v2")
		w.Header().Set("Spruce-Peer-Version", "2")
	} else {
		w.Header().Set("Content-Type", "application/vnd.spruce.peer")
	}
	w.Header().Set("Spruce-Next-Cursor", next)
	var err error
	if v2 {
		err = writePeerBatch(w, messages)
	} else {
		err = writePeerBatchV1(w, messages)
	}
	if err != nil {
		b.cfg.Logger.Error("encode snapshot", "error", err)
	}
}

func (b *Broker) checkpointSnapshot(w http.ResponseWriter, r *http.Request) {
	if !b.peerAllowed(r) {
		problem(w, http.StatusUnauthorized, "invalid_peer")
		return
	}
	now := time.Now().UnixMilli()
	b.mu.RLock()
	checkpoints := make([]groupCheckpoint, 0, b.checkpointCount)
	for scope, entries := range b.checkpoints {
		for messageID, expiresAt := range entries {
			if expiresAt > now {
				checkpoints = append(checkpoints, groupCheckpoint{Topic: scope.topic, Group: scope.group, MessageID: messageID, ExpiresAt: expiresAt})
			}
		}
	}
	b.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(checkpoints)
}

type replayFrontierSnapshot struct {
	Topics      map[string]*topicFrontier   `json:"topics"`
	UnsafeUntil int64                       `json:"unsafe_until,omitempty"`
	Unsafe      map[string]map[string]int64 `json:"unsafe,omitempty"`
}

func (b *Broker) peerCapabilities(w http.ResponseWriter, r *http.Request) {
	if !b.peerAllowed(r) {
		problem(w, http.StatusUnauthorized, "invalid_peer")
		return
	}
	w.Header().Set("Spruce-Peer-Version", "2")
	w.WriteHeader(http.StatusNoContent)
}

func (b *Broker) replayFrontierSnapshot(w http.ResponseWriter, r *http.Request) {
	if !b.peerAllowed(r) {
		problem(w, http.StatusUnauthorized, "invalid_peer")
		return
	}
	b.cache.mu.Lock()
	now := time.Now().UnixMilli()
	snapshot := replayFrontierSnapshot{Topics: make(map[string]*topicFrontier, len(b.cache.frontiers)), Unsafe: make(map[string]map[string]int64, len(b.cache.unsafe))}
	for topic, frontier := range b.cache.frontiers {
		snapshot.Topics[topic] = &topicFrontier{Sequences: cloneFrontier(frontier.Sequences), ExpiresAt: frontier.ExpiresAt}
	}
	for topic, entries := range b.cache.unsafe {
		copied := make(map[string]int64, len(entries))
		for source, until := range entries {
			if until > now {
				copied[source] = until
			}
		}
		if len(copied) > 0 {
			snapshot.Unsafe[topic] = copied
		}
	}
	b.cache.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snapshot)
}

func (b *Broker) syncReplayFrontiersFromPeer(ctx context.Context, p *peer) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.url+"/internal/replay-frontiers", nil)
	req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
	req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		p.v2.Store(false)
		b.cache.mu.Lock()
		b.cache.markUnsafeLocked("*", "legacy-frontier:"+p.url, time.Now().Add(b.cfg.MaxTTL).UnixMilli())
		b.cache.mu.Unlock()
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("replay frontier peer returned %s", resp.Status)
	}
	p.v2.Store(true)
	var snapshot replayFrontierSnapshot
	if json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&snapshot) != nil || len(snapshot.Topics) > b.cache.frontierLimit || len(snapshot.Unsafe) > b.cache.frontierLimit {
		return errors.New("invalid replay frontier snapshot")
	}
	now := time.Now().UnixMilli()
	maxExpiry := now + int64(b.cfg.MaxTTL/time.Millisecond)
	if snapshot.UnsafeUntil < 0 || snapshot.UnsafeUntil > maxExpiry {
		return errors.New("invalid replay frontier snapshot")
	}
	for topic, entries := range snapshot.Unsafe {
		if (topic != "*" && !validTopic(topic)) || len(entries) > 256 {
			return errors.New("invalid replay frontier snapshot")
		}
		for source, until := range entries {
			if source == "" || until < 0 || until > maxExpiry {
				return errors.New("invalid replay frontier snapshot")
			}
		}
	}
	for topic, incoming := range snapshot.Topics {
		if !validTopic(topic) || incoming == nil || len(incoming.Sequences) > 256 || incoming.ExpiresAt > maxExpiry {
			return errors.New("invalid replay frontier snapshot")
		}
		for origin := range incoming.Sequences {
			if origin == "" || len(origin) > 64 {
				return errors.New("invalid replay frontier snapshot")
			}
		}
	}
	b.cache.mu.Lock()
	defer b.cache.mu.Unlock()
	if snapshot.UnsafeUntil > 0 {
		b.cache.markUnsafeLocked("*", "legacy-snapshot", snapshot.UnsafeUntil)
	}
	for topic, entries := range snapshot.Unsafe {
		for source, until := range entries {
			b.cache.markUnsafeLocked(topic, "peer:"+source, until)
		}
	}
	for topic, incoming := range snapshot.Topics {
		if incoming.ExpiresAt <= now {
			continue
		}
		current := b.cache.frontiers[topic]
		if current == nil {
			current = &topicFrontier{Sequences: make(map[string]uint64)}
			b.cache.frontiers[topic] = current
		}
		combined := cloneFrontier(current.Sequences)
		for origin, sequence := range incoming.Sequences {
			combined[origin] = max(combined[origin], sequence)
		}
		if encodeReplayCursor(combined) == "" {
			b.cache.markUnsafeLocked(topic, "frontier-merge", incoming.ExpiresAt)
			continue
		}
		for origin, sequence := range incoming.Sequences {
			current.Sequences[origin] = max(current.Sequences[origin], sequence)
		}
		current.ExpiresAt = max(current.ExpiresAt, incoming.ExpiresAt)
	}
	return nil
}

func (b *Broker) syncCheckpointsFromPeer(ctx context.Context, p *peer) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.url+"/internal/checkpoints", nil)
	req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
	req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("checkpoint peer returned %s", resp.Status)
	}
	var checkpoints []groupCheckpoint
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
	if err := decoder.Decode(&checkpoints); err != nil {
		return err
	}
	if len(checkpoints) > b.cfg.CheckpointEntries {
		return errors.New("checkpoint snapshot exceeds configured limit")
	}
	now := time.Now().UnixMilli()
	maxExpiry := now + int64(b.cfg.MaxTTL/time.Millisecond)
	for _, checkpoint := range checkpoints {
		if !validTopic(checkpoint.Topic) || len(checkpoint.Group) == 0 || len(checkpoint.Group) > 255 || len(checkpoint.MessageID) == 0 || len(checkpoint.MessageID) > 64 || checkpoint.ExpiresAt > maxExpiry {
			return errors.New("invalid checkpoint snapshot")
		}
	}
	b.applyCheckpoints(checkpoints)
	return nil
}

var ErrNoPeerSnapshot = errors.New("no peer snapshot available")

func (b *Broker) SyncFromPeers(ctx context.Context) error {
	var lastErr error
	available := false
	for _, p := range b.peers {
		cursor := ""
		peerAvailable := false
		var peerErr error
		for {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.url+"/internal/snapshot?after="+url.QueryEscape(cursor), nil)
			req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
			req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
			req.Header.Set("Spruce-Peer-Version", "2")
			resp, err := b.client.Do(req)
			if err != nil {
				lastErr, peerErr = err, err
				break
			}
			peerAvailable, available = true, true
			if resp.StatusCode == http.StatusNoContent {
				resp.Body.Close()
				break
			}
			if resp.StatusCode != http.StatusOK {
				lastErr = fmt.Errorf("snapshot peer returned %s", resp.Status)
				peerErr = lastErr
				resp.Body.Close()
				break
			}
			v2 := resp.Header.Get("Spruce-Peer-Version") == "2"
			var messages []*Message
			if v2 {
				messages, err = readPeerBatch(io.LimitReader(resp.Body, 25<<20), b.cfg.MaxMessage)
			} else {
				messages, err = readPeerBatchV1(io.LimitReader(resp.Body, 25<<20), b.cfg.MaxMessage)
			}
			resp.Body.Close()
			if err != nil {
				lastErr, peerErr = err, err
				break
			}
			if !v2 {
				for _, m := range messages {
					b.cache.mu.Lock()
					b.cache.markUnsafeLocked(m.Topic, "legacy-bootstrap:"+p.url, m.ExpiresAt)
					b.cache.mu.Unlock()
				}
			}
			if err := b.acceptBatch(messages); err != nil {
				return err
			}
			next := resp.Header.Get("Spruce-Next-Cursor")
			if next == "" || next == cursor {
				return errors.New("invalid snapshot cursor")
			}
			cursor = next
		}
		if peerAvailable && peerErr != nil {
			return peerErr
		}
		if peerAvailable {
			if err := b.syncReplayFrontiersFromPeer(ctx, p); err != nil {
				return err
			}
			if err := b.syncCheckpointsFromPeer(ctx, p); err != nil {
				return err
			}
		}
	}
	if available {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrNoPeerSnapshot, lastErr)
}

func (b *Broker) replicate(w http.ResponseWriter, r *http.Request) {
	if !b.peerAllowed(r) {
		problem(w, 401, "invalid_peer")
		return
	}
	v2 := r.Header.Get("Spruce-Peer-Version") == "2"
	var messages []*Message
	var err error
	if v2 {
		messages, err = readPeerBatch(io.LimitReader(r.Body, 32<<20), b.cfg.MaxMessage)
	} else {
		messages, err = readPeerBatchV1(io.LimitReader(r.Body, 32<<20), b.cfg.MaxMessage)
	}
	if err != nil {
		problem(w, 400, "invalid_message")
		return
	}
	now := time.Now().UnixMilli()
	if !v2 {
		b.cache.mu.Lock()
		for _, m := range messages {
			b.cache.markUnsafeLocked(m.Topic, "legacy-live", m.ExpiresAt)
		}
		b.cache.mu.Unlock()
	}
	for _, m := range messages {
		if !validTopic(m.Topic) || m.ExpiresAt > time.Now().Add(b.cfg.MaxTTL).UnixMilli() || messageSize(m) > b.cfg.CacheBytes {
			problem(w, 400, "invalid_message")
			return
		}
	}
	live := messages[:0]
	for _, m := range messages {
		if m.ExpiresAt > now {
			live = append(live, m)
		}
	}
	var acceptErr error
	if v2 {
		acceptErr = b.acceptReplicatedBatch(live)
	} else {
		acceptErr = b.acceptBatch(live)
	}
	if acceptErr != nil {
		if errors.Is(acceptErr, errRetentionCapacity) {
			w.Header().Set("Retry-After", "1")
			problem(w, 429, "retention_capacity")
		} else {
			problem(w, 507, "cache_capacity")
		}
		return
	}
	w.WriteHeader(204)
}

func hashValue(parts ...string) uint64 {
	var h uint64 = 14695981039346656037
	for _, s := range parts {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= 1099511628211
		}
		h ^= 0xff
		h *= 1099511628211
	}
	return h
}

func deliveryAffinity(m *Message) string {
	if m.Key != "" {
		return m.Key
	}
	return m.ID
}

func subscriberAffinity(s *subscriber) string {
	if s.affinityID != "" {
		return s.affinityID
	}
	return s.id
}

func (b *Broker) deliver(m *Message, onlyGroup string, attempt int) {
	_ = b.prepareGroupWork([]*Message{m}, onlyGroup)
	b.dispatchGroupMessage(m, onlyGroup)
	b.mu.Lock()
	if len(b.topicBroadcast[m.Topic]) == 0 && len(b.topicGroups[m.Topic]) == 0 {
		b.mu.Unlock()
		return
	}
	candidates := deliveryCandidates.Get().([]deliveryCandidate)[:0]
	if onlyGroup == "" {
		for _, s := range b.topicBroadcast[m.Topic] {
			if s.replaying {
				if len(s.deferred) >= 256 {
					s.cancel()
					continue
				}
				s.deferred = append(s.deferred, m.ID)
				continue
			}
			candidates = append(candidates, deliveryCandidate{subscriber: s})
		}
	}
	b.mu.Unlock()
	if len(candidates) > 1 {
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].group != candidates[j].group {
				return candidates[i].group < candidates[j].group
			}
			return candidates[i].score > candidates[j].score
		})
	}
	completedGroup := ""
	hasCompletedGroup := false
	for _, candidate := range candidates {
		if candidate.group == "" {
			b.sendDelivery(candidate.subscriber, m, attempt)
			continue
		}
		if hasCompletedGroup && candidate.group == completedGroup {
			continue
		}
		if b.sendDelivery(candidate.subscriber, m, attempt) {
			completedGroup = candidate.group
			hasCompletedGroup = true
		}
	}
	for i := range candidates {
		candidates[i] = deliveryCandidate{}
	}
	if cap(candidates) <= 4096 {
		deliveryCandidates.Put(candidates[:0])
	}
}

func (b *Broker) addSubscriberLocked(s *subscriber) {
	if s.group != "" {
		b.registerGroupLocked(s.topic, s.group)
	}
	b.subs[s.id] = s
	if s.group == "" {
		if b.topicBroadcast[s.topic] == nil {
			b.topicBroadcast[s.topic] = make(map[string]*subscriber)
		}
		b.topicBroadcast[s.topic][s.id] = s
		return
	}
	if b.topicGroups[s.topic] == nil {
		b.topicGroups[s.topic] = make(map[string]map[string]*subscriber)
	}
	if b.topicGroups[s.topic][s.group] == nil {
		b.topicGroups[s.topic][s.group] = make(map[string]*subscriber)
	}
	b.topicGroups[s.topic][s.group][s.id] = s
}

func (b *Broker) fenceAffinityMemberLocked(s *subscriber) {
	if s.group == "" || s.affinityID == "" {
		return
	}
	for _, existing := range b.topicGroups[s.topic][s.group] {
		if existing.affinityID == s.affinityID {
			existing.replaying = false
			if existing.cancel != nil {
				existing.cancel()
			}
			b.removeSubscriberLocked(existing)
		}
	}
}

func (b *Broker) removeSubscriberLocked(s *subscriber) {
	s.detached = true
	delete(b.subs, s.id)
	if s.group == "" {
		delete(b.topicBroadcast[s.topic], s.id)
		if len(b.topicBroadcast[s.topic]) == 0 {
			delete(b.topicBroadcast, s.topic)
		}
		return
	}
	delete(b.topicGroups[s.topic][s.group], s.id)
	if len(b.topicGroups[s.topic][s.group]) == 0 {
		delete(b.topicGroups[s.topic], s.group)
	}
	if len(b.topicGroups[s.topic]) == 0 {
		delete(b.topicGroups, s.topic)
	}
}

func (b *Broker) linkTopicPendingLocked(p *pending) {
	topic := p.message.Topic
	state := b.topicPending[topic]
	if state == nil {
		state = &topicPendingState{}
		b.topicPending[topic] = state
	}
	p.topicPrev = state.tail
	if state.tail != nil {
		state.tail.topicNext = p
	} else {
		state.head = p
	}
	state.tail = p
	state.count++
}

func (b *Broker) unlinkTopicPendingLocked(p *pending) {
	topic := p.message.Topic
	state := b.topicPending[topic]
	if state == nil {
		return
	}
	if p.topicPrev != nil {
		p.topicPrev.topicNext = p.topicNext
	} else {
		state.head = p.topicNext
	}
	if p.topicNext != nil {
		p.topicNext.topicPrev = p.topicPrev
	} else {
		state.tail = p.topicPrev
	}
	p.topicPrev, p.topicNext = nil, nil
	state.count--
	if state.count == 0 {
		delete(b.topicPending, topic)
	}
}

// Grouped work uses byte admission and key completion gates. A slow key must
// not impose the legacy topic-wide broadcast lag rejection on unrelated keys.
func (b *Broker) broadcastDeliveryPressuredLocked(state *topicPendingState, now time.Time) bool {
	if state == nil {
		return false
	}
	for p := state.head; p != nil; p = p.topicNext {
		if p.group == "" {
			return now.Sub(p.deliveredAt) >= b.cfg.DeliveryLagLimit
		}
	}
	return false
}

func (b *Broker) topicDeliveryPressured(topic string, now time.Time) bool {
	b.mu.RLock()
	state := b.topicPending[topic]
	pressured := b.broadcastDeliveryPressuredLocked(state, now)
	b.mu.RUnlock()
	return pressured
}

func (b *Broker) sendDelivery(s *subscriber, m *Message, attempt int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sendDeliveryLocked(s, m, attempt, true) != ""
}

func writeFrame(w io.Writer, d Delivery) error {
	meta := appendDeliveryJSON(deliveryMetadata.Get().([]byte)[:0], d)
	release := func() {
		if cap(meta) <= 64<<10 {
			deliveryMetadata.Put(meta[:0])
		}
	}
	var sizes [8]byte
	binary.BigEndian.PutUint32(sizes[:4], uint32(len(meta)))
	binary.BigEndian.PutUint32(sizes[4:], uint32(len(d.Payload)))
	if _, err := w.Write(sizes[:]); err != nil {
		release()
		return err
	}
	if _, err := w.Write(meta); err != nil {
		release()
		return err
	}
	_, err := w.Write(d.Payload)
	release()
	return err
}

func appendDeliveryJSON(out []byte, d Delivery) []byte {
	out = append(out, '{')
	out = appendJSONField(out, "delivery_id", d.DeliveryID, false)
	out = appendJSONField(out, "message_id", d.MessageID, true)
	out = appendJSONField(out, "topic", d.Topic, true)
	if d.Key != "" {
		out = appendJSONField(out, "key", d.Key, true)
	}
	if len(d.Headers) > 0 {
		out = append(out, ',', '"', 'h', 'e', 'a', 'd', 'e', 'r', 's', '"', ':', '{')
		first := true
		for key, value := range d.Headers {
			if !first {
				out = append(out, ',')
			}
			first = false
			out = strconv.AppendQuote(out, key)
			out = append(out, ':')
			out = strconv.AppendQuote(out, value)
		}
		out = append(out, '}')
	}
	out = append(out, ',', '"', 'c', 'r', 'e', 'a', 't', 'e', 'd', '_', 'a', 't', '"', ':')
	out = strconv.AppendInt(out, d.CreatedAt, 10)
	out = append(out, ',', '"', 'a', 't', 't', 'e', 'm', 'p', 't', '"', ':')
	out = strconv.AppendInt(out, int64(d.Attempt), 10)
	if d.Cursor != "" {
		out = appendJSONField(out, "cursor", d.Cursor, true)
	}
	return append(out, '}')
}

func appendJSONField(out []byte, name, value string, comma bool) []byte {
	if comma {
		out = append(out, ',')
	}
	out = strconv.AppendQuote(out, name)
	out = append(out, ':')
	return strconv.AppendQuote(out, value)
}

func (b *Broker) stream(w http.ResponseWriter, r *http.Request) {
	topic, group := r.URL.Query().Get("topic"), r.URL.Query().Get("group")
	member := r.URL.Query().Get("member")
	if !validTopic(topic) {
		problem(w, 400, "invalid_topic")
		return
	}
	if len(group) > 255 {
		problem(w, 400, "invalid_group")
		return
	}
	if len(member) > 255 {
		problem(w, 400, "invalid_member")
		return
	}
	legacy := r.URL.Query().Has("since")
	if legacy && r.URL.Query().Has("cursor") {
		problem(w, 400, "ambiguous_cursor")
		return
	}
	var legacySince int64
	if legacy {
		parsed, parseErr := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		if parseErr != nil || parsed < 0 {
			problem(w, 400, "invalid_since")
			return
		}
		legacySince = parsed
	}
	cursor, err := decodeReplayCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		problem(w, 400, "invalid_cursor")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		problem(w, 500, "stream_unsupported")
		return
	}
	streamCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	if !b.reserveStreamMemory(streamMemoryReservation) {
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, "stream_memory_capacity")
		return
	}
	reservedMemory := streamMemoryReservation
	defer func() { b.streamMemoryBytes.Add(-reservedMemory) }()
	s := &subscriber{id: b.nextID(), affinityID: member, topic: topic, group: group, ch: make(chan Delivery, 256), cancel: cancel, replaying: true}
	b.cache.mu.Lock()
	b.cache.expireLocked(time.Now().UnixMilli())
	frontier := b.cache.frontiers[topic]
	unsafe := b.cache.topicUnsafeLocked(topic, time.Now().UnixMilli())
	legacyLost := legacy && (unsafe || (frontier != nil && len(frontier.Sequences) > 0))
	if legacyLost || (!legacy && r.URL.Query().Get("cursor") != "" && (unsafe || frontierBehind(cursor, frontier))) {
		b.cache.mu.Unlock()
		b.metrics.CursorExpired.Add(1)
		problem(w, http.StatusConflict, "cursor_expired")
		return
	}
	b.mu.Lock()
	if b.draining.Load() {
		b.mu.Unlock()
		b.cache.mu.Unlock()
		problem(w, http.StatusServiceUnavailable, "broker_draining")
		return
	}
	if group != "" {
		if !b.registerGroupLocked(topic, group) {
			b.mu.Unlock()
			b.cache.mu.Unlock()
			problem(w, 429, "group_memory_capacity")
			return
		}
		b.mu.Unlock()
		if err := b.prepareGroupWork(b.cache.topics[topic]); err != nil {
			b.cache.mu.Unlock()
			problem(w, 429, "group_memory_capacity")
			return
		}
		b.mu.Lock()
	}
	groupReplayOwner := group == ""
	var replay []string
	if groupReplayOwner {
		indexBytes := int64(len(b.cache.topics[topic])) * 16
		for _, m := range b.cache.topics[topic] {
			if m != nil {
				indexBytes += int64(len(m.ID))
			}
		}
		if !b.reserveStreamMemory(indexBytes) {
			b.mu.Unlock()
			b.cache.mu.Unlock()
			w.Header().Set("Retry-After", "1")
			problem(w, http.StatusTooManyRequests, "replay_memory_capacity")
			return
		}
		reservedMemory += indexBytes
		replayCursor := cursor
		if group != "" && !legacy {
			// A member's cursor says nothing about work assigned to another
			// member. Recover the group's retained backlog using checkpoints.
			replayCursor = nil
		}
		replay = b.cache.replayIDsLocked(topic, replayCursor, legacy, legacySince)
	}
	b.fenceAffinityMemberLocked(s)
	if !groupReplayOwner {
		s.replaying = false
	}
	b.addSubscriberLocked(s)
	b.wakeGroups()
	if group != "" && groupReplayOwner {
		kept := replay[:0]
		now := time.Now().UnixMilli()
		for _, id := range replay {
			if !b.checkpointActiveLocked(topic, group, id, now) {
				kept = append(kept, id)
			}
		}
		replay = kept
	}
	b.mu.Unlock()
	b.cache.mu.Unlock()
	w.Header().Set("Spruce-Cursor", encodeReplayCursor(cursor))
	defer func() {
		b.mu.Lock()
		takeover := s.replaying && s.group != ""
		b.removeSubscriberLocked(s)
		for _, p := range b.pending {
			if p.subscriberID == s.id {
				p.next = time.Now()
				p.deadline.at = p.next
				heap.Fix(&b.pendingDeadlines, p.deadline.index)
			}
		}
		b.mu.Unlock()
		if takeover {
			// Reconnect members so one takes over bounded replay. Do not retain
			// another full payload snapshot in a disconnected handler.
			b.mu.RLock()
			for _, member := range b.topicGroups[s.topic][s.group] {
				member.cancel()
			}
			b.mu.RUnlock()
		}
	}()
	w.Header().Set("Content-Type", "application/vnd.spruce.stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(200)
	flusher.Flush()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	// Keep small-message delivery syscall-efficient without allowing a slow
	// subscriber to accumulate an unbounded response buffer. Large payloads
	// bypass the buffer directly; small frames share a bounded 64 KiB write.
	bw := bufio.NewWriterSize(w, 64<<10)
	replayFrames, replayBytes := 0, int64(0)
	flushReplay := func() bool {
		if replayFrames == 0 {
			return true
		}
		if err := bw.Flush(); err != nil {
			return false
		}
		flusher.Flush()
		replayFrames, replayBytes = 0, 0
		return true
	}
	writeReplay := func(id string) bool {
		m := b.replayMessage(id)
		if m == nil {
			// Close rather than silently skip an evicted/expired replay entry.
			// An existing cursor will be checked against the loss frontier on reconnect.
			return false
		}
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
		if !b.sendDelivery(s, m, 1) {
			return false
		}
		var d Delivery
		select {
		case d = <-s.ch:
		case <-streamCtx.Done():
			return false
		}
		nextCursor := cloneFrontier(cursor)
		nextCursor[d.origin] = max(nextCursor[d.origin], d.sequence)
		encoded := encodeReplayCursor(nextCursor)
		if encoded == "" {
			b.cache.mu.Lock()
			b.cache.markUnsafeLocked(m.Topic, "cursor-capacity", m.ExpiresAt)
			b.cache.mu.Unlock()
			return false
		}
		cursor, d.Cursor = nextCursor, encoded
		if err := writeFrame(bw, d); err != nil {
			return false
		}
		replayFrames++
		replayBytes += messageSize(m)
		return true
	}
	for _, id := range replay {
		if !writeReplay(id) {
			return
		}
		if replayFrames >= 128 || replayBytes >= 256<<10 {
			if !flushReplay() {
				return
			}
		}
	}
	if !flushReplay() {
		return
	}
	replay = nil
	for {
		b.mu.Lock()
		deferred := s.deferred
		s.deferred = nil
		if len(deferred) == 0 {
			s.replaying = false
			b.mu.Unlock()
			break
		}
		b.mu.Unlock()
		for _, id := range deferred {
			if !writeReplay(id) {
				return
			}
			if replayFrames >= 128 || replayBytes >= 256<<10 {
				if !flushReplay() {
					return
				}
			}
		}
		if !flushReplay() {
			return
		}
	}
	for {
		select {
		case <-streamCtx.Done():
			return
		case d := <-s.ch:
			nextCursor := cloneFrontier(cursor)
			nextCursor[d.origin] = max(nextCursor[d.origin], d.sequence)
			d.Cursor = encodeReplayCursor(nextCursor)
			if d.Cursor == "" {
				return
			}
			cursor = nextCursor
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := writeFrame(bw, d); err != nil {
				return
			}
			for range 127 {
				select {
				case next := <-s.ch:
					nextCursor := cloneFrontier(cursor)
					nextCursor[next.origin] = max(nextCursor[next.origin], next.sequence)
					next.Cursor = encodeReplayCursor(nextCursor)
					if next.Cursor == "" {
						return
					}
					cursor = nextCursor
					if err := writeFrame(bw, next); err != nil {
						return
					}
				default:
					goto flush
				}
			}
		flush:
			if err := bw.Flush(); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := writeFrame(bw, Delivery{}); err != nil {
				return
			}
			if err := bw.Flush(); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

type ackRequest struct {
	DeliveryIDs []string          `json:"delivery_ids,omitempty"`
	Checkpoints []groupCheckpoint `json:"checkpoints,omitempty"`
}

func decodeAckRequest(r io.Reader) (ackRequest, error) {
	var a ackRequest
	decoder := json.NewDecoder(io.LimitReader(r, 1<<20))
	if err := decoder.Decode(&a); err != nil {
		return a, err
	}
	if len(a.DeliveryIDs) > 1024 || len(a.Checkpoints) > 1024 || len(a.DeliveryIDs)+len(a.Checkpoints) == 0 {
		return a, errors.New("invalid delivery ID count")
	}
	for _, id := range a.DeliveryIDs {
		if len(id) == 0 || len(id) > 64 {
			return a, errors.New("invalid delivery ID")
		}
	}
	for _, checkpoint := range a.Checkpoints {
		if !validTopic(checkpoint.Topic) || len(checkpoint.Group) == 0 || len(checkpoint.Group) > 255 || len(checkpoint.MessageID) == 0 || len(checkpoint.MessageID) > 64 || checkpoint.ExpiresAt <= 0 {
			return a, errors.New("invalid checkpoint")
		}
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return a, errors.New("trailing JSON")
	}
	return a, nil
}

func (b *Broker) checkpointActiveLocked(topic, group, messageID string, now int64) bool {
	scope := checkpointScope{topic: topic, group: group}
	expiresAt := b.checkpoints[scope][messageID]
	return expiresAt > now
}

func (b *Broker) putCheckpointLocked(checkpoint groupCheckpoint, now int64) {
	if checkpoint.ExpiresAt <= now {
		return
	}
	scope := checkpointScope{topic: checkpoint.Topic, group: checkpoint.Group}
	indexed := b.checkpoints[scope]
	if indexed == nil {
		indexed = make(map[string]int64)
		b.checkpoints[scope] = indexed
	}
	if current := indexed[checkpoint.MessageID]; current >= checkpoint.ExpiresAt {
		return
	}
	if indexed[checkpoint.MessageID] == 0 {
		b.checkpointCount++
	}
	indexed[checkpoint.MessageID] = checkpoint.ExpiresAt
	b.completeGroupWorkLocked(checkpoint.Topic, checkpoint.Group, checkpoint.MessageID)
	b.checkpointOrder = append(b.checkpointOrder, checkpointOrderEntry{scope: scope, messageID: checkpoint.MessageID, expiresAt: checkpoint.ExpiresAt})
	for b.checkpointCount > b.cfg.CheckpointEntries && b.checkpointHead < len(b.checkpointOrder) {
		old := b.checkpointOrder[b.checkpointHead]
		b.checkpointOrder[b.checkpointHead] = checkpointOrderEntry{}
		b.checkpointHead++
		if entries := b.checkpoints[old.scope]; entries != nil && entries[old.messageID] == old.expiresAt {
			delete(entries, old.messageID)
			b.checkpointCount--
			if len(entries) == 0 {
				delete(b.checkpoints, old.scope)
			}
		}
	}
	if b.checkpointHead > b.cfg.CheckpointEntries && b.checkpointHead*2 > len(b.checkpointOrder) {
		copy(b.checkpointOrder, b.checkpointOrder[b.checkpointHead:])
		b.checkpointOrder = b.checkpointOrder[:len(b.checkpointOrder)-b.checkpointHead]
		b.checkpointHead = 0
	}
}

func (b *Broker) applyCheckpoints(checkpoints []groupCheckpoint) {
	now := time.Now().UnixMilli()
	b.mu.Lock()
	for _, checkpoint := range checkpoints {
		if checkpoint.ExpiresAt <= now+int64(b.cfg.MaxTTL/time.Millisecond) {
			b.putCheckpointLocked(checkpoint, now)
		}
	}
	b.mu.Unlock()
}

func (b *Broker) checkpointsForAcks(ids []string) []groupCheckpoint {
	checkpoints := make([]groupCheckpoint, 0, len(ids))
	b.mu.RLock()
	for _, id := range ids {
		if p := b.pending[id]; p != nil && p.group != "" {
			checkpoints = append(checkpoints, groupCheckpoint{Topic: p.message.Topic, Group: p.group, MessageID: p.message.ID, ExpiresAt: p.expiresAt})
		}
	}
	b.mu.RUnlock()
	return checkpoints
}

func (b *Broker) removeAcks(ids []string) []groupCheckpoint {
	checkpoints := make([]groupCheckpoint, 0, len(ids))
	now := time.Now().UnixMilli()
	b.mu.Lock()
	for _, id := range ids {
		if p, ok := b.pending[id]; ok {
			if p.group != "" {
				checkpoint := groupCheckpoint{Topic: p.message.Topic, Group: p.group, MessageID: p.message.ID, ExpiresAt: p.expiresAt}
				b.putCheckpointLocked(checkpoint, now)
				checkpoints = append(checkpoints, checkpoint)
			}
			delete(b.pending, id)
			b.unlinkTopicPendingLocked(p)
			if p.deadline.index >= 0 {
				heap.Remove(&b.pendingDeadlines, p.deadline.index)
			}
			b.pendingBytes -= p.bytes
			if s := b.subs[p.subscriberID]; s != nil {
				s.inflightBytes -= p.bytes
			}
			b.metrics.Acked.Add(1)
			b.metrics.AckLatency.observe(time.Since(p.deliveredAt))
			releasePending(p)
		}
	}
	b.mu.Unlock()
	return checkpoints
}
func (b *Broker) ack(w http.ResponseWriter, r *http.Request) {
	a, err := decodeAckRequest(r.Body)
	if err != nil {
		problem(w, 400, "invalid_ack")
		return
	}
	if len(a.DeliveryIDs) == 0 {
		problem(w, 400, "invalid_ack")
		return
	}
	// A local completion must not depend on a failed replica's queue. Delivery
	// IDs include the boot identity, so repeated local ACKs remain recognisable
	// after pending state has been removed. Unknown owners still need forwarding.
	local := true
	for _, id := range a.DeliveryIDs {
		raw, err := base64.RawURLEncoding.DecodeString(id)
		if err != nil || len(raw) != 16 || !bytes.Equal(raw[:8], b.boot[:]) {
			local = false
			break
		}
	}
	if local {
		checkpoints := b.removeAcks(a.DeliveryIDs)
		if len(checkpoints) > 0 {
			b.broadcastAction(r.Context(), "ack", ackRequest{Checkpoints: checkpoints})
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	a.Checkpoints = b.checkpointsForAcks(a.DeliveryIDs)
	if !b.broadcastAction(r.Context(), "ack", a) {
		problem(w, 503, "peer_ack_unavailable")
		return
	}
	b.removeAcks(a.DeliveryIDs)
	w.WriteHeader(204)
}
func (b *Broker) internalAck(w http.ResponseWriter, r *http.Request) {
	if !b.peerAllowed(r) {
		problem(w, 401, "invalid_peer")
		return
	}
	a, err := decodeAckRequest(r.Body)
	if err != nil {
		problem(w, 400, "invalid_ack")
		return
	}
	// Complete locally before best-effort checkpoint propagation. A partition
	// must not turn a successful handler into repeated local delivery. A later
	// loss of this owner can still replay work whose checkpoint did not survive.
	derived := b.removeAcks(a.DeliveryIDs)
	if len(derived) > 0 {
		b.broadcastAction(r.Context(), "ack", ackRequest{Checkpoints: derived})
	}
	b.applyCheckpoints(a.Checkpoints)
	w.WriteHeader(204)
}
func (b *Broker) broadcastAction(ctx context.Context, action string, a ackRequest) bool {
	ok := true
	for _, p := range b.peers {
		ids := append([]string(nil), a.DeliveryIDs...)
		checkpoints := append([]groupCheckpoint(nil), a.Checkpoints...)
		bytes := int64(32)
		for _, id := range ids {
			bytes += int64(len(id) + 4)
		}
		for _, checkpoint := range checkpoints {
			bytes += int64(len(checkpoint.Topic) + len(checkpoint.Group) + len(checkpoint.MessageID) + 48)
		}
		reserved := false
		for {
			queued := p.actionBytes.Load()
			if queued+bytes > b.cfg.ActionQueueBytes {
				ok = false
				b.countDroppedAction(action, max(len(ids), len(checkpoints)))
				break
			}
			if p.actionBytes.CompareAndSwap(queued, queued+bytes) {
				reserved = true
				break
			}
		}
		if !reserved {
			continue
		}
		var ch chan actionBatch
		if action == "ack" {
			ch = p.acks
		} else {
			ch = p.nacks
		}
		select {
		case ch <- actionBatch{ids: ids, checkpoints: checkpoints, bytes: bytes}:
		case <-ctx.Done():
			p.actionBytes.Add(-bytes)
			return false
		default:
			p.actionBytes.Add(-bytes)
			b.countDroppedAction(action, max(len(ids), len(checkpoints)))
			ok = false
		}
	}
	return ok
}

func (b *Broker) countDroppedAction(action string, count int) {
	if action == "ack" {
		b.metrics.AckActionDropped.Add(uint64(count))
	} else {
		b.metrics.NackActionDropped.Add(uint64(count))
	}
}

func (b *Broker) actionLoop(p *peer, action string, ch <-chan actionBatch) {
	var carry *actionBatch
	for {
		var first actionBatch
		if carry != nil {
			first, carry = *carry, nil
		} else {
			select {
			case <-b.stop:
				return
			case first = <-ch:
			}
		}
		ids := append([]string(nil), first.ids...)
		checkpoints := append([]groupCheckpoint(nil), first.checkpoints...)
		queuedBytes := first.bytes
		draining := true
		for len(ids) < 1024 && len(checkpoints) < 1024 && draining {
			select {
			case more := <-ch:
				room := 1024 - len(ids)
				if len(more.ids) > room || len(checkpoints)+len(more.checkpoints) > 1024 {
					carry = &more
					draining = false
					continue
				}
				ids = append(ids, more.ids...)
				checkpoints = append(checkpoints, more.checkpoints...)
				queuedBytes += more.bytes
			default:
				draining = false
			}
		}
		body, _ := json.Marshal(ackRequest{DeliveryIDs: ids, Checkpoints: checkpoints})
		backoff := 50 * time.Millisecond
		deadline := time.Now().Add(b.cfg.AckDeadline)
		sent := false
		for {
			req, _ := http.NewRequest(http.MethodPost, p.url+"/internal/"+action, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
			req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
			resp, err := b.client.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode/100 == 2 {
					sent = true
					break
				}
			}
			if time.Now().Add(backoff).After(deadline) {
				break
			}
			timer := time.NewTimer(backoff)
			select {
			case <-b.stop:
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
		if !sent {
			b.countDroppedAction(action, max(len(ids), len(checkpoints)))
		}
		p.actionBytes.Add(-queuedBytes)
	}
}
func (b *Broker) nack(w http.ResponseWriter, r *http.Request) {
	a, err := decodeAckRequest(r.Body)
	if err != nil {
		problem(w, 400, "invalid_nack")
		return
	}
	if len(a.DeliveryIDs) == 0 || len(a.Checkpoints) != 0 {
		problem(w, 400, "invalid_nack")
		return
	}
	b.applyNacks(a.DeliveryIDs)
	if !b.broadcastAction(r.Context(), "nack", a) {
		problem(w, 503, "peer_nack_unavailable")
		return
	}
	w.WriteHeader(204)
}

func (b *Broker) applyNacks(ids []string) {
	b.mu.Lock()
	for _, id := range ids {
		if p := b.pending[id]; p != nil {
			p.next = time.Now()
			p.deadline.at = p.next
			heap.Fix(&b.pendingDeadlines, p.deadline.index)
		}
	}
	b.mu.Unlock()
}

func (b *Broker) internalNack(w http.ResponseWriter, r *http.Request) {
	if !b.peerAllowed(r) {
		problem(w, 401, "invalid_peer")
		return
	}
	a, err := decodeAckRequest(r.Body)
	if err != nil {
		problem(w, 400, "invalid_nack")
		return
	}
	if len(a.DeliveryIDs) == 0 || len(a.Checkpoints) != 0 {
		problem(w, 400, "invalid_nack")
		return
	}
	b.applyNacks(a.DeliveryIDs)
	w.WriteHeader(204)
}

func (b *Broker) maintenance() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			return
		case now := <-t.C:
			b.cache.expire(now.UnixMilli())
			var retry []*pending
			b.mu.Lock()
			for b.pendingDeadlines.Len() > 0 && !b.pendingDeadlines[0].at.After(now) {
				deadline := heap.Pop(&b.pendingDeadlines).(*pendingDeadline)
				p := b.pending[deadline.id]
				if p == nil || !p.next.Equal(deadline.at) {
					continue
				}
				delete(b.pending, deadline.id)
				b.unlinkTopicPendingLocked(p)
				b.pendingBytes -= p.bytes
				if s := b.subs[p.subscriberID]; s != nil {
					s.inflightBytes -= p.bytes
				}
				retry = append(retry, p)
			}
			b.mu.Unlock()
			for _, p := range retry {
				if p.group != "" {
					b.retryGroup(p)
					b.metrics.Redelivered.Add(1)
					releasePending(p)
					continue
				}
				if p.attempt >= b.cfg.MaxAttempts {
					b.metrics.Dropped.Add(1)
					releasePending(p)
					continue
				}
				p.attempt++
				b.metrics.Redelivered.Add(1)
				if p.expiresAt <= now.UnixMilli() {
					b.metrics.Dropped.Add(1)
					releasePending(p)
					continue
				}
				m := p.message
				if p.group != "" {
					b.deliver(m, p.group, p.attempt)
					releasePending(p)
					continue
				}
				b.mu.RLock()
				s := b.subs[p.subscriberID]
				b.mu.RUnlock()
				if s != nil {
					b.sendDelivery(s, m, p.attempt)
				} else {
					b.metrics.Dropped.Add(1)
				}
				releasePending(p)
			}
		}
	}
}

func problem(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}
func (b *Broker) status(w http.ResponseWriter, _ *http.Request) {
	b.cache.mu.Lock()
	v := map[string]any{"messages": len(b.cache.items), "cache_accounted_bytes": b.cache.bytes, "cache_limit_bytes": b.cache.maxBytes, "peers": len(b.peers)}
	b.cache.mu.Unlock()
	b.mu.RLock()
	v["consumers"] = len(b.subs)
	v["pending_deliveries"] = len(b.pending)
	v["pending_bytes"] = b.pendingBytes
	v["consumer_checkpoints"] = b.checkpointCount
	groupEntries, groupKeys := b.groupWorkCountsLocked()
	v["registered_groups"] = len(b.groupWork)
	v["group_outstanding_messages"] = groupEntries
	v["group_active_keys"] = groupKeys
	v["group_memory_bytes"] = b.groupMemoryBytes
	v["group_expired_messages"] = b.metrics.GroupExpired.Load()
	pressuredTopics := 0
	now := time.Now()
	for _, state := range b.topicPending {
		if b.broadcastDeliveryPressuredLocked(state, now) {
			pressuredTopics++
		}
	}
	v["delivery_pressured_topics"] = pressuredTopics
	b.mu.RUnlock()
	var replicationQueueBytes, replicationQueueMaxPeerBytes, actionQueueBytes int64
	for _, p := range b.peers {
		peerBytes := p.queuedBytes.Load()
		replicationQueueBytes += peerBytes
		replicationQueueMaxPeerBytes = max(replicationQueueMaxPeerBytes, peerBytes)
		actionQueueBytes += p.actionBytes.Load()
	}
	v["replication_queue_bytes"] = replicationQueueBytes
	v["replication_queue_max_peer_bytes"] = replicationQueueMaxPeerBytes
	v["replication_queue_capacity_bytes"] = b.cfg.ReplicationQueueBytes * int64(len(b.peers))
	v["replication_queue_high_water_bytes"] = b.replicationHighWaterBytes()
	v["action_queue_bytes"] = actionQueueBytes
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (b *Broker) cacheDigest(w http.ResponseWriter, r *http.Request) {
	if !b.peerAllowed(r) {
		problem(w, http.StatusUnauthorized, "invalid_peer")
		return
	}
	select {
	case b.digestSlot <- struct{}{}:
		defer func() { <-b.digestSlot }()
	default:
		problem(w, http.StatusTooManyRequests, "digest_in_progress")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, digestMessages(b.cache.digestSnapshot()))
}

func (c *cache) digestSnapshot() []*Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	messages := make([]*Message, 0, len(c.items))
	for _, message := range c.items {
		messages = append(messages, message)
	}
	return messages
}

func digestMessages(messages []*Message) string {
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID < messages[j].ID })
	digest := sha256.New()
	for _, message := range messages {
		messageDigest := sha256.New()
		_ = writePeerString(messageDigest, message.ID)
		_ = writePeerString(messageDigest, message.Topic)
		_ = writePeerString(messageDigest, message.Key)
		var fixed [22]byte
		binary.BigEndian.PutUint32(fixed[:4], uint32(len(message.Payload)))
		binary.BigEndian.PutUint64(fixed[4:12], uint64(message.CreatedAt))
		binary.BigEndian.PutUint64(fixed[12:20], uint64(message.ExpiresAt))
		binary.BigEndian.PutUint16(fixed[20:22], uint16(len(message.Headers)))
		_, _ = messageDigest.Write(fixed[:])
		headerKeys := make([]string, 0, len(message.Headers))
		for key := range message.Headers {
			headerKeys = append(headerKeys, key)
		}
		sort.Strings(headerKeys)
		for _, key := range headerKeys {
			_ = writePeerString(messageDigest, key)
			_ = writePeerString(messageDigest, message.Headers[key])
		}
		_, _ = messageDigest.Write(message.Payload)
		_, _ = digest.Write(messageDigest.Sum(nil))
	}
	return base64.RawURLEncoding.EncodeToString(digest.Sum(nil))
}

func (b *Broker) messageStatus(w http.ResponseWriter, r *http.Request) {
	if b.cache.has(r.PathValue("id")) {
		w.WriteHeader(204)
		return
	}
	problem(w, 404, "message_not_cached")
}
func (b *Broker) prometheus(w http.ResponseWriter, _ *http.Request) {
	b.cache.mu.Lock()
	entries, bytes, evicted, expired := len(b.cache.items), b.cache.bytes, b.cache.evicted.Load(), b.cache.expired.Load()
	b.cache.mu.Unlock()
	b.mu.RLock()
	consumers, inflight, pendingBytes, checkpoints := len(b.subs), len(b.pending), b.pendingBytes, b.checkpointCount
	groupEntries, groupKeys := b.groupWorkCountsLocked()
	registeredGroups := len(b.groupWork)
	groupMemoryBytes := b.groupMemoryBytes
	pressuredTopics := 0
	now := time.Now()
	for _, state := range b.topicPending {
		if b.broadcastDeliveryPressuredLocked(state, now) {
			pressuredTopics++
		}
	}
	b.mu.RUnlock()
	var replicationQueueBytes, replicationQueueMaxPeerBytes, actionQueueBytes int64
	for _, p := range b.peers {
		peerBytes := p.queuedBytes.Load()
		replicationQueueBytes += peerBytes
		replicationQueueMaxPeerBytes = max(replicationQueueMaxPeerBytes, peerBytes)
		actionQueueBytes += p.actionBytes.Load()
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	vals := []struct {
		name  string
		value uint64
	}{{"spruce_publish_total", b.metrics.Published.Load()}, {"spruce_publish_bytes_total", b.metrics.PublishBytes.Load()}, {"spruce_publish_rejected_total", b.metrics.Rejected.Load()}, {"spruce_publish_overload_rejected_total", b.metrics.OverloadRejected.Load()}, {"spruce_replication_pressure_rejected_total", b.metrics.ReplicationPressureRejected.Load()}, {"spruce_delivery_pressure_rejected_total", b.metrics.DeliveryPressureRejected.Load()}, {"spruce_subscription_cursor_expired_total", b.metrics.CursorExpired.Load()}, {"spruce_publish_admission_bytes", uint64(b.publishAdmissionBytes.Load())}, {"spruce_publish_admission_capacity_bytes", uint64(b.cfg.PublishAdmissionBytes)}, {"spruce_public_auth_rejected_total", b.metrics.PublicAuthRejected.Load()}, {"spruce_admin_auth_rejected_total", b.metrics.AdminAuthRejected.Load()}, {"spruce_peer_auth_rejected_total", b.metrics.PeerAuthRejected.Load()}, {"spruce_replication_total", b.metrics.Replicated.Load()}, {"spruce_replication_errors_total", b.metrics.ReplicationErrors.Load()}, {"spruce_replication_dropped_messages_total", b.metrics.ReplicationDropped.Load()}, {"spruce_ack_propagation_dropped_ids_total", b.metrics.AckActionDropped.Load()}, {"spruce_nack_propagation_dropped_ids_total", b.metrics.NackActionDropped.Load()}, {"spruce_deliveries_total", b.metrics.Delivered.Load()}, {"spruce_redeliveries_total", b.metrics.Redelivered.Load()}, {"spruce_acks_total", b.metrics.Acked.Load()}, {"spruce_delivery_dropped_total", b.metrics.Dropped.Load()}, {"spruce_cache_evictions_total", evicted}, {"spruce_cache_expired_total", expired}, {"spruce_cache_messages", uint64(entries)}, {"spruce_cache_accounted_bytes", uint64(bytes)}, {"spruce_consumers", uint64(consumers)}, {"spruce_consumer_checkpoints", uint64(checkpoints)}, {"spruce_consumer_inflight", uint64(inflight)}, {"spruce_consumer_inflight_bytes", uint64(pendingBytes)}, {"spruce_delivery_pressured_topics", uint64(pressuredTopics)}, {"spruce_replication_queue_bytes", uint64(replicationQueueBytes)}, {"spruce_replication_queue_max_peer_bytes", uint64(replicationQueueMaxPeerBytes)}, {"spruce_replication_queue_capacity_bytes", uint64(b.cfg.ReplicationQueueBytes * int64(len(b.peers)))}, {"spruce_replication_queue_high_water_bytes", uint64(b.replicationHighWaterBytes())}, {"spruce_action_queue_bytes", uint64(actionQueueBytes)}}
	for _, v := range vals {
		kind := "counter"
		if v.name == "spruce_cache_messages" || v.name == "spruce_cache_accounted_bytes" || v.name == "spruce_consumers" || v.name == "spruce_consumer_checkpoints" || v.name == "spruce_consumer_inflight" || v.name == "spruce_delivery_pressured_topics" || strings.Contains(v.name, "_queue_") || strings.Contains(v.name, "_admission_") || v.name == "spruce_publish_admission_bytes" || v.name == "spruce_consumer_inflight_bytes" {
			kind = "gauge"
		}
		_, _ = fmt.Fprintf(w, "# TYPE %s %s\n%s %d\n", v.name, kind, v.name, v.value)
	}
	_, _ = fmt.Fprintf(w, "# TYPE spruce_group_outstanding_messages gauge\nspruce_group_outstanding_messages %d\n# TYPE spruce_group_active_keys gauge\nspruce_group_active_keys %d\n# TYPE spruce_registered_groups gauge\nspruce_registered_groups %d\n", groupEntries, groupKeys, registeredGroups)
	_, _ = fmt.Fprintf(w, "# TYPE spruce_group_memory_bytes gauge\nspruce_group_memory_bytes %d\n", groupMemoryBytes)
	_, _ = fmt.Fprintf(w, "# TYPE spruce_group_expired_messages_total counter\nspruce_group_expired_messages_total %d\n", b.metrics.GroupExpired.Load())
	b.writeHistogram(w, "spruce_publish_request_duration_microseconds", &b.metrics.PublishLatency)
	_, _ = fmt.Fprintf(w, "# TYPE spruce_stream_memory_bytes gauge\nspruce_stream_memory_bytes %d\n# TYPE spruce_stream_memory_capacity_bytes gauge\nspruce_stream_memory_capacity_bytes %d\n", b.streamMemoryBytes.Load(), b.cfg.StreamMemoryBytes)
	b.writeHistogram(w, "spruce_replication_request_duration_microseconds", &b.metrics.ReplicationLatency)
	b.writeHistogram(w, "spruce_delivery_ack_duration_microseconds", &b.metrics.AckLatency)
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	_, _ = fmt.Fprintf(w, "# TYPE spruce_process_heap_bytes gauge\nspruce_process_heap_bytes %d\n# TYPE spruce_process_gc_cycles_total counter\nspruce_process_gc_cycles_total %d\n# TYPE spruce_process_goroutines gauge\nspruce_process_goroutines %d\n", memory.HeapAlloc, memory.NumGC, runtime.NumGoroutine())
}

func (b *Broker) writeHistogram(w io.Writer, name string, h *durationHistogram) {
	_, _ = fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	for i, bound := range latencyBoundsUS {
		_, _ = fmt.Fprintf(w, "%s_bucket{le=\"%d\"} %d\n", name, bound, h.buckets[i].Load())
	}
	_, _ = fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %d\n%s_count %d\n", name, h.count.Load(), name, h.sumUS.Load(), name, h.count.Load())
}
