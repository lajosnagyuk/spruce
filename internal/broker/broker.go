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
	MaxConcurrentRequests      int
	MaxStreams                 int
	MaxInternalRequests        int
	Logger                     *slog.Logger
}

func DefaultConfig() Config {
	return Config{CacheBytes: 256 << 20, DefaultTTL: time.Minute, MaxTTL: 24 * time.Hour,
		MaxMessage: 1 << 20, QueueDepth: 4096, ReplicationQueueBytes: 64 << 20, ActionQueueBytes: 4 << 20,
		MaxInflightBytes: 64 << 20, MaxSubscriberInflightBytes: 16 << 20, AckDeadline: 30 * time.Second, MaxAttempts: 8,
		IdempotencyEntries: 65536, MaxConcurrentRequests: 4096, MaxStreams: 1024, MaxInternalRequests: 1024}
}

type Message struct {
	ID            string            `json:"id"`
	Topic         string            `json:"topic"`
	Key           string            `json:"key,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Payload       []byte            `json:"-"`
	CreatedAt     int64             `json:"created_at"`
	ExpiresAt     int64             `json:"expires_at"`
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
	Payload    []byte            `json:"-"`
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
	bytes           int64
	maxBytes        int64
	evicted         atomic.Uint64
	expired         atomic.Uint64
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
	return &cache{items: make(map[string]*Message), expiryItems: make(map[int64]*expiryItem), topics: make(map[string][]*Message), topicTombstones: make(map[string]int), maxBytes: max}
}

func messageSize(m *Message) int64 {
	if m.accountedSize > 0 {
		return m.accountedSize
	}
	n := int64(96 + len(m.ID) + len(m.Topic) + len(m.Key) + len(m.Payload))
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
	c.expireLocked(now)
	var total int64
	for _, m := range messages {
		sz := messageSize(m)
		if sz > c.maxBytes || total > c.maxBytes-sz {
			return nil, errors.New("message exceeds cache capacity")
		}
		total += sz
	}
	inserted := make([]bool, len(messages))
	for i, m := range messages {
		inserted[i], _ = c.insertLocked(m)
	}
	return inserted, nil
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
	ch               chan Delivery
	inflightBytes    int64
	cancel           context.CancelFunc
	detached         bool
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
	url         string
	ch          chan []*Message
	queuedBytes atomic.Int64
	actionBytes atomic.Int64
	acks        chan actionBatch
	nacks       chan actionBatch
}

type actionBatch struct {
	ids   []string
	bytes int64
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
	Published, PublishBytes, Rejected, Replicated, ReplicationErrors atomic.Uint64
	ReplicationDropped                                               atomic.Uint64
	AckActionDropped, NackActionDropped                              atomic.Uint64
	Delivered, Redelivered, Acked, Dropped, Duplicate                atomic.Uint64
	PublishLatency, ReplicationLatency, AckLatency                   durationHistogram
	PublicAuthRejected, AdminAuthRejected, PeerAuthRejected          atomic.Uint64
}

type Broker struct {
	cfg              Config
	cache            *cache
	mux              *http.ServeMux
	client           *http.Client
	boot             [8]byte
	seq              atomic.Uint64
	metrics          Metrics
	mu               sync.RWMutex
	subs             map[string]*subscriber
	topicBroadcast   map[string]map[string]*subscriber
	topicGroups      map[string]map[string]map[string]*subscriber
	pending          map[string]*pending
	pendingBytes     int64
	pendingDeadlines pendingHeap
	idempotency      map[string]*idempotencyEntry
	idempotencyOrder []idempotencyOrderEntry
	peers            []*peer
	stop             chan struct{}
	closeOnce        sync.Once
	requestSlots     chan struct{}
	streamSlots      chan struct{}
	internalSlots    chan struct{}
	digestSlot       chan struct{}
	ready            atomic.Bool
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
	if cfg.MaxConcurrentRequests <= 0 {
		cfg.MaxConcurrentRequests = DefaultConfig().MaxConcurrentRequests
	}
	if cfg.MaxStreams <= 0 {
		cfg.MaxStreams = DefaultConfig().MaxStreams
	}
	if cfg.MaxInternalRequests <= 0 {
		cfg.MaxInternalRequests = DefaultConfig().MaxInternalRequests
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
		pending: make(map[string]*pending), idempotency: make(map[string]*idempotencyEntry), stop: make(chan struct{}),
		requestSlots: make(chan struct{}, cfg.MaxConcurrentRequests), streamSlots: make(chan struct{}, cfg.MaxStreams), digestSlot: make(chan struct{}, 1)}
	b.internalSlots = make(chan struct{}, cfg.MaxInternalRequests)
	if _, err := rand.Read(b.boot[:]); err != nil {
		panic("spruce: generate boot identity: " + err.Error())
	}
	for _, u := range cfg.Peers {
		u = strings.TrimRight(strings.TrimSpace(u), "/")
		if u != "" {
			b.peers = append(b.peers, &peer{url: u, ch: make(chan []*Message, cfg.QueueDepth), acks: make(chan actionBatch, 1024), nacks: make(chan actionBatch, 1024)})
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
	return b
}

func (b *Broker) Handler() http.Handler { return http.HandlerFunc(b.serveHTTP) }
func (b *Broker) Close()                { b.closeOnce.Do(func() { close(b.stop) }) }
func (b *Broker) BeginDrain()           { b.ready.Store(false) }
func (b *Broker) Ready()                { b.ready.Store(true) }

func (b *Broker) serveHTTP(w http.ResponseWriter, r *http.Request) {
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

// Batch wire format is a sequence of [4 byte big-endian payload length][payload].
// Topic, TTL, key, and content type are shared by the batch. This intentionally
// keeps the fast producer path trivial to encode and decode.
func (b *Broker) publishBatch(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	defer func() { b.metrics.PublishLatency.observe(time.Since(started)) }()
	if r.ContentLength > maxBatchBytes {
		problem(w, 413, "batch_too_large")
		return
	}
	topic := r.PathValue("topic")
	if !validTopic(topic) {
		problem(w, 400, "invalid_topic")
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
	var total int64
	for len(payloads) < maxBatchMessages {
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
	key, contentType := r.Header.Get("Spruce-Key"), r.Header.Get("Content-Type")
	for _, payload := range payloads {
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
	if err := b.acceptBatch(messages); err != nil {
		problem(w, 507, "cache_capacity")
		return
	}
	for _, m := range messages {
		b.metrics.Published.Add(1)
		b.metrics.PublishBytes.Add(uint64(len(m.Payload)))
	}
	b.enqueuePeerBatch(messages)
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
	if r.ContentLength > b.cfg.MaxMessage {
		problem(w, 413, "message_too_large")
		return
	}
	topic := r.PathValue("topic")
	if !validTopic(topic) {
		problem(w, 400, "invalid_topic")
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
	if ack != "local" && ack != "one-peer" {
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
		idempotencyCacheKey = topic + "\x00" + producerID + "\x00" + idempotencyKey
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
			replicated := b.idempotencyReplicated(idempotency)
			if ack == "one-peer" && !replicated {
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
	if _, err = b.accept(m); err != nil {
		b.finishIdempotency(idempotencyCacheKey, idempotency, false, false)
		b.metrics.Rejected.Add(1)
		problem(w, 507, "cache_capacity")
		return
	}
	b.metrics.Published.Add(1)
	b.metrics.PublishBytes.Add(uint64(len(payload)))
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
		b.enqueuePeers(m)
	}
	b.finishIdempotency(idempotencyCacheKey, idempotency, true, replicated)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(202)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": m.ID, "replicated": replicated})
}

func (b *Broker) accept(m *Message) (bool, error) {
	inserted, err := b.cache.put(m, time.Now().UnixMilli())
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
	inserted, err := b.cache.putBatch(messages, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	for i, m := range messages {
		if inserted[i] {
			b.deliver(m, "", 1)
		} else {
			b.metrics.Duplicate.Add(1)
		}
	}
	return nil
}

func (b *Broker) enqueuePeers(m *Message) {
	b.enqueuePeerBatch([]*Message{m})
}

func (b *Broker) enqueuePeerBatch(messages []*Message) {
	var bytes int64
	for _, m := range messages {
		bytes += messageSize(m)
	}
	for _, p := range b.peers {
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
		if len(m.Headers) > 65535 || len(m.Payload) > int(^uint32(0)) {
			return errors.New("peer message too large")
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
		if len(m.Headers) > 65535 || len(m.Payload) > int(^uint32(0)) {
			return out, errors.New("peer message too large")
		}
		out = binary.BigEndian.AppendUint16(out, uint16(len(m.Headers)))
		out = binary.BigEndian.AppendUint32(out, uint32(len(m.Payload)))
		out = binary.BigEndian.AppendUint64(out, uint64(m.CreatedAt))
		out = binary.BigEndian.AppendUint64(out, uint64(m.ExpiresAt))
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
		var fixed [22]byte
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
			CreatedAt: int64(binary.BigEndian.Uint64(fixed[6:14])), ExpiresAt: int64(binary.BigEndian.Uint64(fixed[14:22]))})
	}
	return messages, nil
}

func (b *Broker) sendPeer(ctx context.Context, p *peer, m *Message) bool {
	return b.sendPeerBatch(ctx, p, []*Message{m})
}

func (b *Broker) sendPeerBatch(ctx context.Context, p *peer, messages []*Message) bool {
	var body bytes.Buffer
	if err := writePeerBatch(&body, messages); err != nil {
		b.metrics.ReplicationErrors.Add(uint64(len(messages)))
		return false
	}
	return b.sendPeerBody(ctx, p, body.Bytes(), len(messages))
}

func (b *Broker) sendPeerBody(ctx context.Context, p *peer, body []byte, count int) bool {
	started := time.Now()
	defer func() { b.metrics.ReplicationLatency.observe(time.Since(started)) }()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/internal/replicate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/vnd.spruce.peer")
	req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
	req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
	resp, err := b.client.Do(req)
	if err != nil {
		b.metrics.ReplicationErrors.Add(1)
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b.metrics.ReplicationErrors.Add(1)
		return false
	}
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
			encoded.Reset()
			if err := writePeerBatch(&encoded, batch); err != nil {
				b.metrics.ReplicationErrors.Add(uint64(len(batch)))
				p.queuedBytes.Add(-batchBytes(batch))
				continue
			}
			replicated := false
			for attempt := 0; attempt < 3; attempt++ {
				if b.sendPeerBody(context.Background(), p, encoded.Bytes(), len(batch)) {
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
					return
				case <-timer.C:
				}
			}
			if !replicated {
				b.metrics.ReplicationDropped.Add(uint64(len(batch)))
			}
			p.queuedBytes.Add(-batchBytes(batch))
			if encoded.Cap() > 1<<20 {
				encoded = bytes.Buffer{}
			}
		}
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
	result := make(chan bool, len(b.peers))
	for _, p := range b.peers {
		go func(p *peer) { result <- b.sendPeer(ctx, p, m) }(p)
	}
	for range b.peers {
		if <-result {
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
	w.Header().Set("Content-Type", "application/vnd.spruce.peer")
	w.Header().Set("Spruce-Next-Cursor", next)
	if err := writePeerBatch(w, messages); err != nil {
		b.cfg.Logger.Error("encode snapshot", "error", err)
	}
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
			messages, err := readPeerBatch(io.LimitReader(resp.Body, 25<<20), b.cfg.MaxMessage)
			resp.Body.Close()
			if err != nil {
				lastErr, peerErr = err, err
				break
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
	messages, err := readPeerBatch(io.LimitReader(r.Body, 32<<20), b.cfg.MaxMessage)
	if err != nil {
		problem(w, 400, "invalid_message")
		return
	}
	now := time.Now().UnixMilli()
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
	if err := b.acceptBatch(live); err != nil {
		problem(w, 507, "cache_capacity")
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

func (b *Broker) deliver(m *Message, onlyGroup string, attempt int) {
	b.mu.RLock()
	if len(b.topicBroadcast[m.Topic]) == 0 && len(b.topicGroups[m.Topic]) == 0 {
		b.mu.RUnlock()
		return
	}
	candidates := deliveryCandidates.Get().([]deliveryCandidate)[:0]
	if onlyGroup == "" {
		for _, s := range b.topicBroadcast[m.Topic] {
			candidates = append(candidates, deliveryCandidate{subscriber: s})
		}
	}
	for group, indexed := range b.topicGroups[m.Topic] {
		if onlyGroup != "" && group != onlyGroup {
			continue
		}
		for _, s := range indexed {
			candidates = append(candidates, deliveryCandidate{subscriber: s, group: group, score: hashValue(m.ID, s.id)})
		}
	}
	b.mu.RUnlock()
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

func (b *Broker) sendDelivery(s *subscriber, m *Message, attempt int) bool {
	deliveryID := b.nextID()
	d := Delivery{DeliveryID: deliveryID, MessageID: m.ID, Topic: m.Topic, Key: m.Key, Headers: m.Headers, CreatedAt: m.CreatedAt, Attempt: attempt, Payload: m.Payload}
	bytes := messageSize(m)
	now := time.Now()
	p := acquirePending()
	*p = pending{deliveryID: deliveryID, message: m, attempt: attempt, group: s.group, subscriberID: s.id, expiresAt: m.ExpiresAt, bytes: bytes, deliveredAt: now, next: now.Add(b.cfg.AckDeadline)}
	p.deadline = pendingDeadline{id: deliveryID, at: p.next}
	b.mu.Lock()
	if s.detached || b.pendingBytes+bytes > b.cfg.MaxInflightBytes || s.inflightBytes+bytes > b.cfg.MaxSubscriberInflightBytes {
		b.mu.Unlock()
		releasePending(p)
		if s.cancel != nil {
			s.cancel()
		}
		b.metrics.Dropped.Add(1)
		return false
	}
	b.pending[deliveryID] = p
	b.pendingBytes += bytes
	s.inflightBytes += bytes
	heap.Push(&b.pendingDeadlines, &p.deadline)
	select {
	case s.ch <- d:
		b.mu.Unlock()
		b.metrics.Delivered.Add(1)
		return true
	default:
		delete(b.pending, deliveryID)
		b.pendingBytes -= bytes
		s.inflightBytes -= bytes
		if p.deadline.index >= 0 {
			heap.Remove(&b.pendingDeadlines, p.deadline.index)
		}
		b.mu.Unlock()
		releasePending(p)
		if s.cancel != nil {
			s.cancel()
		}
		b.metrics.Dropped.Add(1)
		return false
	}
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
	if !validTopic(topic) {
		problem(w, 400, "invalid_topic")
		return
	}
	if len(group) > 255 {
		problem(w, 400, "invalid_group")
		return
	}
	var since int64
	if value := r.URL.Query().Get("since"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			problem(w, 400, "invalid_since")
			return
		}
		since = parsed
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		problem(w, 500, "stream_unsupported")
		return
	}
	streamCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	s := &subscriber{id: b.nextID(), topic: topic, group: group, ch: make(chan Delivery, 256), cancel: cancel}
	b.cache.mu.Lock()
	b.cache.expireLocked(time.Now().UnixMilli())
	replay := make([]*Message, 0)
	for _, m := range b.cache.topics[topic] {
		if m == nil {
			continue
		}
		if m.Topic == topic && m.CreatedAt >= since {
			replay = append(replay, m)
		}
	}
	b.mu.Lock()
	b.addSubscriberLocked(s)
	b.mu.Unlock()
	b.cache.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.removeSubscriberLocked(s)
		for _, p := range b.pending {
			if p.subscriberID == s.id {
				p.next = time.Now()
				p.deadline.at = p.next
				heap.Fix(&b.pendingDeadlines, p.deadline.index)
			}
		}
		b.mu.Unlock()
	}()
	for _, m := range replay {
		if group == "" {
			b.sendDelivery(s, m, 1)
		} else {
			b.deliver(m, group, 1)
		}
	}
	w.Header().Set("Content-Type", "application/vnd.spruce.stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(200)
	flusher.Flush()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	bw := bufio.NewWriterSize(w, 32<<10)
	for {
		select {
		case <-streamCtx.Done():
			return
		case d := <-s.ch:
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := writeFrame(bw, d); err != nil {
				return
			}
			for range 63 {
				select {
				case next := <-s.ch:
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
	DeliveryIDs []string `json:"delivery_ids"`
}

func decodeAckRequest(r io.Reader) (ackRequest, error) {
	var a ackRequest
	decoder := json.NewDecoder(io.LimitReader(r, 64<<10))
	if err := decoder.Decode(&a); err != nil {
		return a, err
	}
	if len(a.DeliveryIDs) == 0 || len(a.DeliveryIDs) > 1024 {
		return a, errors.New("invalid delivery ID count")
	}
	for _, id := range a.DeliveryIDs {
		if len(id) == 0 || len(id) > 64 {
			return a, errors.New("invalid delivery ID")
		}
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return a, errors.New("trailing JSON")
	}
	return a, nil
}

func (b *Broker) removeAcks(ids []string) {
	b.mu.Lock()
	for _, id := range ids {
		if p, ok := b.pending[id]; ok {
			delete(b.pending, id)
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
}
func (b *Broker) ack(w http.ResponseWriter, r *http.Request) {
	a, err := decodeAckRequest(r.Body)
	if err != nil {
		problem(w, 400, "invalid_ack")
		return
	}
	b.removeAcks(a.DeliveryIDs)
	if !b.broadcastAction(r.Context(), "ack", a) {
		problem(w, 503, "peer_ack_unavailable")
		return
	}
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
	b.removeAcks(a.DeliveryIDs)
	w.WriteHeader(204)
}
func (b *Broker) broadcastAction(ctx context.Context, action string, a ackRequest) bool {
	ok := true
	for _, p := range b.peers {
		ids := append([]string(nil), a.DeliveryIDs...)
		bytes := int64(32)
		for _, id := range ids {
			bytes += int64(len(id) + 4)
		}
		reserved := false
		for {
			queued := p.actionBytes.Load()
			if queued+bytes > b.cfg.ActionQueueBytes {
				ok = false
				b.countDroppedAction(action, len(ids))
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
		case ch <- actionBatch{ids: ids, bytes: bytes}:
		case <-ctx.Done():
			p.actionBytes.Add(-bytes)
			return false
		default:
			p.actionBytes.Add(-bytes)
			b.countDroppedAction(action, len(ids))
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
		queuedBytes := first.bytes
		draining := true
		for len(ids) < 1024 && draining {
			select {
			case more := <-ch:
				room := 1024 - len(ids)
				if len(more.ids) > room {
					carry = &more
					draining = false
					continue
				}
				ids = append(ids, more.ids...)
				queuedBytes += more.bytes
			default:
				draining = false
			}
		}
		body, _ := json.Marshal(ackRequest{DeliveryIDs: ids})
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
			b.countDroppedAction(action, len(ids))
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
				b.pendingBytes -= p.bytes
				if s := b.subs[p.subscriberID]; s != nil {
					s.inflightBytes -= p.bytes
				}
				retry = append(retry, p)
			}
			b.mu.Unlock()
			for _, p := range retry {
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
	b.mu.RUnlock()
	var replicationQueueBytes, actionQueueBytes int64
	for _, p := range b.peers {
		replicationQueueBytes += p.queuedBytes.Load()
		actionQueueBytes += p.actionBytes.Load()
	}
	v["replication_queue_bytes"] = replicationQueueBytes
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
	consumers, inflight, pendingBytes := len(b.subs), len(b.pending), b.pendingBytes
	b.mu.RUnlock()
	var replicationQueueBytes, actionQueueBytes int64
	for _, p := range b.peers {
		replicationQueueBytes += p.queuedBytes.Load()
		actionQueueBytes += p.actionBytes.Load()
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	vals := []struct {
		name  string
		value uint64
	}{{"spruce_publish_total", b.metrics.Published.Load()}, {"spruce_publish_bytes_total", b.metrics.PublishBytes.Load()}, {"spruce_publish_rejected_total", b.metrics.Rejected.Load()}, {"spruce_public_auth_rejected_total", b.metrics.PublicAuthRejected.Load()}, {"spruce_admin_auth_rejected_total", b.metrics.AdminAuthRejected.Load()}, {"spruce_peer_auth_rejected_total", b.metrics.PeerAuthRejected.Load()}, {"spruce_replication_total", b.metrics.Replicated.Load()}, {"spruce_replication_errors_total", b.metrics.ReplicationErrors.Load()}, {"spruce_replication_dropped_messages_total", b.metrics.ReplicationDropped.Load()}, {"spruce_ack_propagation_dropped_ids_total", b.metrics.AckActionDropped.Load()}, {"spruce_nack_propagation_dropped_ids_total", b.metrics.NackActionDropped.Load()}, {"spruce_deliveries_total", b.metrics.Delivered.Load()}, {"spruce_redeliveries_total", b.metrics.Redelivered.Load()}, {"spruce_acks_total", b.metrics.Acked.Load()}, {"spruce_delivery_dropped_total", b.metrics.Dropped.Load()}, {"spruce_cache_evictions_total", evicted}, {"spruce_cache_expired_total", expired}, {"spruce_cache_messages", uint64(entries)}, {"spruce_cache_accounted_bytes", uint64(bytes)}, {"spruce_consumers", uint64(consumers)}, {"spruce_consumer_inflight", uint64(inflight)}, {"spruce_consumer_inflight_bytes", uint64(pendingBytes)}, {"spruce_replication_queue_bytes", uint64(replicationQueueBytes)}, {"spruce_action_queue_bytes", uint64(actionQueueBytes)}}
	for _, v := range vals {
		kind := "counter"
		if v.name == "spruce_cache_messages" || v.name == "spruce_cache_accounted_bytes" || v.name == "spruce_consumers" || v.name == "spruce_consumer_inflight" || strings.HasSuffix(v.name, "_queue_bytes") || v.name == "spruce_consumer_inflight_bytes" {
			kind = "gauge"
		}
		_, _ = fmt.Fprintf(w, "# TYPE %s %s\n%s %d\n", v.name, kind, v.name, v.value)
	}
	b.writeHistogram(w, "spruce_publish_request_duration_microseconds", &b.metrics.PublishLatency)
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
