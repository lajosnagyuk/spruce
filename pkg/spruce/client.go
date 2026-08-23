package spruce

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	BaseURL                  string
	Token                    string
	Username                 string
	Password                 string
	HTTP                     *http.Client
	OnEvent                  func(ClientEvent)
	AllowInsecureCredentials bool
}
type ClientEvent struct {
	Operation  string
	Duration   time.Duration
	StatusCode int
	Err        error
}
type HandlerPanicError struct{ Value any }

func (e *HandlerPanicError) Error() string { return fmt.Sprintf("spruce: handler panic: %v", e.Value) }

type PublishOptions struct {
	Key, ContentType, ProducerID, IdempotencyKey, Ack string
	TTL                                               time.Duration
}
type PublishResult struct {
	ID         string `json:"id"`
	Replicated bool   `json:"replicated"`
}

type BatchResult struct {
	IDs []string `json:"ids"`
}
type BatchEntry struct {
	Payload []byte
	Key     string
}
type Delivery struct {
	DeliveryID string            `json:"delivery_id"`
	MessageID  string            `json:"message_id"`
	Topic      string            `json:"topic"`
	Key        string            `json:"key,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	CreatedAt  int64             `json:"created_at"`
	Cursor     string            `json:"cursor,omitempty"`
	Attempt    int               `json:"attempt"`
	Payload    []byte            `json:"-"`
}

func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{Timeout: 30 * time.Second}}
}
func (c *Client) CloseIdleConnections() { c.httpClient().CloseIdleConnections() }
func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}
func (c *Client) doWith(base *http.Client, req *http.Request) (*http.Response, error) {
	copy := *base
	previous := copy.CheckRedirect
	copy.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) > 0 && via[0].Header.Get("Authorization") != "" && next.URL.Scheme != "https" && !c.AllowInsecureCredentials {
			return errors.New("spruce: refusing credential redirect to plaintext HTTP")
		}
		if previous != nil {
			return previous(next, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return copy.Do(req)
}
func (c *Client) do(req *http.Request) (*http.Response, error) { return c.doWith(c.httpClient(), req) }
func (c *Client) authorize(req *http.Request) error {
	if (c.Token != "" || c.Username != "") && req.URL.Scheme != "https" && !c.AllowInsecureCredentials {
		return errors.New("spruce: credentials require HTTPS; enable AllowInsecureCredentials only for isolated development")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	} else if c.Username != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}
	return nil
}
func (c *Client) emit(operation string, started time.Time, status int, err error) {
	if c.OnEvent != nil {
		func() {
			defer func() { _ = recover() }()
			c.OnEvent(ClientEvent{Operation: operation, Duration: time.Since(started), StatusCode: status, Err: err})
		}()
	}
}

type Error struct {
	StatusCode         int
	Status, Code, Body string
	RetryAfter         time.Duration
}

func (e *Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("spruce: %s: %s", e.Status, e.Code)
	}
	return fmt.Sprintf("spruce: %s: %s", e.Status, e.Body)
}
func responseError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var problem struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &problem)
	return &Error{StatusCode: resp.StatusCode, Status: resp.Status, Code: problem.Error, Body: strings.TrimSpace(string(body)), RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := time.ParseDuration(strings.TrimSpace(value) + "s"); err == nil && seconds > 0 {
		return seconds
	}
	if deadline, err := http.ParseTime(value); err == nil && deadline.After(now) {
		return deadline.Sub(now)
	}
	return 0
}

func retryDelay(backoff, maximum, retryAfter time.Duration) time.Duration {
	delay := backoff/2 + time.Duration(rand.Int64N(max(1, int64(backoff))))
	if delay > maximum {
		delay = maximum
	}
	if delay < retryAfter {
		delay = retryAfter
	}
	return delay
}
func (c *Client) Publish(ctx context.Context, topic string, payload []byte, o PublishOptions) (PublishResult, error) {
	started := time.Now()
	status := 0
	var finalErr error
	defer func() { c.emit("publish", started, status, finalErr) }()
	var out PublishResult
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/topics/"+url.PathEscape(topic)+"/messages", bytes.NewReader(payload))
	if err != nil {
		return out, err
	}
	if o.Key != "" {
		req.Header.Set("Spruce-Key", o.Key)
	}
	if o.ContentType != "" {
		req.Header.Set("Content-Type", o.ContentType)
	}
	if o.TTL > 0 {
		req.Header.Set("Spruce-TTL", o.TTL.String())
	}
	if o.Ack != "" {
		req.Header.Set("Spruce-Ack", o.Ack)
	}
	if o.IdempotencyKey != "" {
		req.Header.Set("Spruce-Idempotency-Key", o.IdempotencyKey)
	}
	if o.ProducerID != "" {
		req.Header.Set("Spruce-Producer-ID", o.ProducerID)
	}
	if err = c.authorize(req); err != nil {
		finalErr = err
		return out, err
	}
	resp, err := c.do(req)
	if err != nil {
		finalErr = err
		return out, err
	}
	status = resp.StatusCode
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		finalErr = responseError(resp)
		return out, finalErr
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	finalErr = err
	return out, err
}

type RetryOptions struct {
	MaxAttempts            int
	MinBackoff, MaxBackoff time.Duration
}

func (c *Client) PublishRetry(ctx context.Context, topic string, payload []byte, o PublishOptions, retry RetryOptions) (PublishResult, error) {
	if o.ProducerID == "" || o.IdempotencyKey == "" {
		return PublishResult{}, errors.New("retry requires producer ID and idempotency key")
	}
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 3
	}
	if retry.MinBackoff <= 0 {
		retry.MinBackoff = 50 * time.Millisecond
	}
	if retry.MaxBackoff <= 0 {
		retry.MaxBackoff = 2 * time.Second
	}
	backoff := retry.MinBackoff
	var result PublishResult
	var err error
	for attempt := 0; attempt < retry.MaxAttempts; attempt++ {
		result, err = c.Publish(ctx, topic, payload, o)
		if err == nil {
			return result, nil
		}
		var apiErr *Error
		if errors.As(err, &apiErr) && apiErr.StatusCode != 408 && apiErr.StatusCode != 429 && apiErr.StatusCode != 503 {
			return result, err
		}
		if attempt+1 == retry.MaxAttempts {
			break
		}
		retryAfter := time.Duration(0)
		if apiErr != nil {
			retryAfter = apiErr.RetryAfter
		}
		t := time.NewTimer(retryDelay(backoff, retry.MaxBackoff, retryAfter))
		select {
		case <-ctx.Done():
			t.Stop()
			return result, ctx.Err()
		case <-t.C:
		}
		backoff *= 2
		if backoff > retry.MaxBackoff {
			backoff = retry.MaxBackoff
		}
	}
	return result, err
}

func (c *Client) PublishBatch(ctx context.Context, topic string, payloads [][]byte, o PublishOptions) (BatchResult, error) {
	entries := make([]BatchEntry, len(payloads))
	for i, payload := range payloads {
		entries[i] = BatchEntry{Payload: payload, Key: o.Key}
	}
	return c.PublishBatchEntries(ctx, topic, entries, o)
}

func (c *Client) PublishBatchEntries(ctx context.Context, topic string, entries []BatchEntry, o PublishOptions) (BatchResult, error) {
	started := time.Now()
	status := 0
	var finalErr error
	defer func() { c.emit("publish_batch", started, status, finalErr) }()
	var out BatchResult
	if len(entries) == 0 || len(entries) > 4096 {
		if len(entries) == 0 {
			return out, errors.New("batch is empty")
		}
		return out, errors.New("batch exceeds 4096 messages")
	}
	var body bytes.Buffer
	total := 0
	for _, entry := range entries {
		key := []byte(entry.Key)
		if len(key) > 8<<10 || len(entry.Payload) > 1<<20 || total > (16<<20)-6-len(key)-len(entry.Payload) {
			return out, errors.New("batch exceeds protocol limits")
		}
		total += 6 + len(key) + len(entry.Payload)
	}
	body.Grow(total)
	for _, entry := range entries {
		payload, key := entry.Payload, []byte(entry.Key)
		if len(payload) > int(^uint32(0)) {
			return out, errors.New("payload is too large")
		}
		var keySize [2]byte
		binary.BigEndian.PutUint16(keySize[:], uint16(len(key)))
		_, _ = body.Write(keySize[:])
		_, _ = body.Write(key)
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
		_, _ = body.Write(size[:])
		_, _ = body.Write(payload)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/topics/"+url.PathEscape(topic)+"/batches", &body)
	if err != nil {
		return out, err
	}
	req.Header.Set("Spruce-Batch-Version", "2")
	if o.ContentType != "" {
		req.Header.Set("Content-Type", o.ContentType)
	}
	if o.TTL > 0 {
		req.Header.Set("Spruce-TTL", o.TTL.String())
	}
	if o.Ack != "" && o.Ack != "local" {
		return out, errors.New("batch only supports local acknowledgement")
	}
	if o.IdempotencyKey != "" {
		return out, errors.New("batch idempotency is not supported")
	}
	if err = c.authorize(req); err != nil {
		finalErr = err
		return out, err
	}
	resp, err := c.do(req)
	if err != nil {
		finalErr = err
		return out, err
	}
	status = resp.StatusCode
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		finalErr = responseError(resp)
		return out, finalErr
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	finalErr = err
	return out, err
}

type SubscribeOptions struct {
	Topic, Group    string
	Cursor          string
	Since           int64 // Deprecated: timestamp cursors are rejected.
	Concurrency     int
	MaxPayloadBytes int
	DrainTimeout    time.Duration
}
type Handler func(context.Context, Delivery) error

type ackItem struct {
	id   string
	done chan error
}

type ackBatcher struct {
	c     *Client
	kind  string
	items chan ackItem
}

func newAckBatcher(ctx context.Context, c *Client, kind string) *ackBatcher {
	b := &ackBatcher{c: c, kind: kind, items: make(chan ackItem, 1024)}
	go b.run(ctx)
	return b
}

func (b *ackBatcher) submit(ctx context.Context, id string) error {
	done := make(chan error, 1)
	item := ackItem{id: id, done: done}
	select {
	case b.items <- item:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-item.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *ackBatcher) run(ctx context.Context) {
	for {
		var first ackItem
		select {
		case first = <-b.items:
		case <-ctx.Done():
			return
		}
		batch := []ackItem{first}
		timer := time.NewTimer(500 * time.Microsecond)
	collect:
		for len(batch) < 256 {
			select {
			case item := <-b.items:
				batch = append(batch, item)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				for _, item := range batch {
					item.done <- ctx.Err()
				}
				return
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		ids := make([]string, len(batch))
		for i := range batch {
			ids[i] = batch[i].id
		}
		err := b.c.ack(ctx, b.kind, ids)
		for _, item := range batch {
			item.done <- err
		}
	}
}

func (c *Client) Subscribe(ctx context.Context, o SubscribeOptions, handler Handler) error {
	if o.Topic == "" {
		return errors.New("topic is required")
	}
	if o.Since != 0 {
		return errors.New("timestamp subscription cursors are no longer supported; use Cursor")
	}
	backoff := 50 * time.Millisecond
	cursor := o.Cursor
	for ctx.Err() == nil {
		o.Cursor = cursor
		connectedAt := time.Now()
		last, err := c.subscribeOnce(ctx, o, handler)
		status := 0
		var apiErr *Error
		if errors.As(err, &apiErr) {
			status = apiErr.StatusCode
			if apiErr.Code == "cursor_expired" {
				c.emit("subscription_cursor_expired", connectedAt, status, err)
			}
		}
		c.emit("subscription_disconnected", connectedAt, status, err)
		if last != cursor || time.Since(connectedAt) >= 5*time.Second {
			backoff = 50 * time.Millisecond
		}
		if last != "" {
			cursor = last
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrHandlerDrainTimeout) {
			return err
		}
		var panicErr *HandlerPanicError
		if errors.As(err, &panicErr) {
			return err
		}
		if apiErr != nil && apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 && apiErr.StatusCode != http.StatusRequestTimeout && apiErr.StatusCode != http.StatusTooManyRequests {
			return err
		}
		retryAfter := time.Duration(0)
		if apiErr != nil {
			retryAfter = apiErr.RetryAfter
		}
		wait := retryDelay(backoff, 2*time.Second, retryAfter)
		c.emit("subscription_reconnecting", time.Now(), 0, err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	return ctx.Err()
}

var ErrHandlerDrainTimeout = errors.New("spruce: handlers did not stop before drain timeout")

type Status struct {
	Messages            int   `json:"messages"`
	CacheAccountedBytes int64 `json:"cache_accounted_bytes"`
	CacheLimitBytes     int64 `json:"cache_limit_bytes"`
	Peers               int   `json:"peers"`
	Consumers           int   `json:"consumers"`
	PendingDeliveries   int   `json:"pending_deliveries"`
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		c.emit("get", started, 0, err)
		return err
	}
	if err = c.authorize(req); err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		c.emit("get", started, 0, err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		err = responseError(resp)
		c.emit("get", started, resp.StatusCode, err)
		return err
	}
	if out != nil {
		err = json.NewDecoder(resp.Body).Decode(out)
		c.emit("get", started, resp.StatusCode, err)
		return err
	}
	c.emit("get", started, resp.StatusCode, nil)
	return nil
}
func (c *Client) Status(ctx context.Context) (Status, error) {
	var out Status
	err := c.get(ctx, "/v1/status", &out)
	return out, err
}
func (c *Client) Healthy(ctx context.Context) error { return c.get(ctx, "/health/ready", nil) }
func (c *Client) MessageCached(ctx context.Context, id string) (bool, error) {
	err := c.get(ctx, "/v1/status/messages/"+url.PathEscape(id), nil)
	var apiErr *Error
	if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
		return false, nil
	}
	return err == nil, err
}
func (c *Client) Metrics(ctx context.Context) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/metrics", nil)
	if err := c.authorize(req); err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, responseError(resp)
	}
	return io.ReadAll(resp.Body)
}

type ConsumableDelivery struct {
	Delivery Delivery
	once     sync.Once
	done     chan error
}

func (d *ConsumableDelivery) Complete(err error) { d.once.Do(func() { d.done <- err }) }
func (c *Client) Deliveries(ctx context.Context, o SubscribeOptions) (<-chan *ConsumableDelivery, <-chan error) {
	deliveries, errs := make(chan *ConsumableDelivery, max(1, o.Concurrency)), make(chan error, 1)
	go func() {
		defer close(deliveries)
		defer close(errs)
		err := c.Subscribe(ctx, o, func(ctx context.Context, delivery Delivery) error {
			item := &ConsumableDelivery{Delivery: delivery, done: make(chan error, 1)}
			select {
			case deliveries <- item:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case err := <-item.done:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			errs <- err
		}
	}()
	return deliveries, errs
}

func (c *Client) subscribeOnce(ctx context.Context, o SubscribeOptions, handler Handler) (string, error) {
	if o.Concurrency <= 0 {
		o.Concurrency = 16
	}
	if o.Concurrency > 1024 {
		return o.Cursor, errors.New("subscription concurrency exceeds 1024")
	}
	if o.MaxPayloadBytes <= 0 {
		o.MaxPayloadBytes = 1 << 20
	}
	if o.MaxPayloadBytes > 64<<20 {
		return o.Cursor, errors.New("subscription payload limit exceeds 64 MiB")
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = time.Second
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	v := url.Values{"topic": []string{o.Topic}}
	if o.Group != "" {
		v.Set("group", o.Group)
	}
	if o.Cursor != "" {
		v.Set("cursor", o.Cursor)
	}
	req, _ := http.NewRequestWithContext(streamCtx, http.MethodGet, c.BaseURL+"/v1/subscriptions/stream?"+v.Encode(), nil)
	if err := c.authorize(req); err != nil {
		return o.Cursor, err
	}
	streamHTTP := *c.httpClient()
	streamHTTP.Timeout = 0
	resp, err := c.doWith(&streamHTTP, req)
	if err != nil {
		return o.Cursor, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return o.Cursor, responseError(resp)
	}
	c.emit("subscription_connected", time.Now(), http.StatusOK, nil)
	br := bufio.NewReaderSize(resp.Body, 32<<10)
	var cursor atomic.Value
	initial := resp.Header.Get("Spruce-Cursor")
	if initial == "" {
		initial = o.Cursor
	}
	cursor.Store(initial)
	var sequence atomic.Uint64
	sem := make(chan struct{}, o.Concurrency)
	progressWindow := make(chan struct{}, o.Concurrency)
	workerErrors := make(chan error, 1)
	acks := newAckBatcher(streamCtx, c, "ack")
	nacks := newAckBatcher(streamCtx, c, "nack")
	var progressMu sync.Mutex
	nextProgress := uint64(1)
	completed := make(map[uint64]string)
	markComplete := func(index uint64, value string) {
		progressMu.Lock()
		completed[index] = value
		advanced := 0
		for {
			value, ok := completed[nextProgress]
			if !ok {
				break
			}
			delete(completed, nextProgress)
			if value != "" {
				cursor.Store(value)
			}
			nextProgress++
			advanced++
		}
		progressMu.Unlock()
		for range advanced {
			<-progressWindow
		}
	}
	drainWorkers := func() bool {
		deadline := time.NewTimer(o.DrainTimeout)
		defer deadline.Stop()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			if len(sem) == 0 {
				return true
			}
			select {
			case <-ticker.C:
			case <-deadline.C:
				cancel()
				return false
			}
		}
	}
	for {
		d, err := readFrame(br, o.MaxPayloadBytes)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if !drainWorkers() {
					return cursor.Load().(string), ErrHandlerDrainTimeout
				}
			} else {
				cancel()
				if !drainWorkers() {
					return cursor.Load().(string), ErrHandlerDrainTimeout
				}
			}
			select {
			case workerErr := <-workerErrors:
				return cursor.Load().(string), workerErr
			default:
				return cursor.Load().(string), err
			}
		}
		if d.DeliveryID == "" {
			continue
		}
		select {
		case progressWindow <- struct{}{}:
		case <-streamCtx.Done():
			cancel()
			if !drainWorkers() {
				return cursor.Load().(string), ErrHandlerDrainTimeout
			}
			return cursor.Load().(string), streamCtx.Err()
		}
		select {
		case sem <- struct{}{}:
		case <-streamCtx.Done():
			<-progressWindow
			cancel()
			if !drainWorkers() {
				return cursor.Load().(string), ErrHandlerDrainTimeout
			}
			return cursor.Load().(string), streamCtx.Err()
		}
		index := sequence.Add(1)
		go func() {
			defer func() { <-sem }()
			var handlerErr error
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						handlerErr = &HandlerPanicError{Value: recovered}
					}
				}()
				handlerErr = handler(streamCtx, d)
			}()
			if handlerErr != nil {
				if e := nacks.submit(streamCtx, d.DeliveryID); e != nil {
					select {
					case workerErrors <- e:
						cancel()
					default:
					}
					return
				}
				var panicErr *HandlerPanicError
				if errors.As(handlerErr, &panicErr) {
					select {
					case workerErrors <- handlerErr:
						cancel()
					default:
					}
					return
				}
				markComplete(index, d.Cursor)
				return
			}
			if e := acks.submit(streamCtx, d.DeliveryID); e != nil {
				select {
				case workerErrors <- e:
					cancel()
				default:
				}
				return
			}
			markComplete(index, d.Cursor)
		}()
	}
}
func readFrame(r io.Reader, maxPayloadBytes int) (Delivery, error) {
	var d Delivery
	var sizes [8]byte
	if _, e := io.ReadFull(r, sizes[:]); e != nil {
		return d, e
	}
	ml, pl := binary.BigEndian.Uint32(sizes[:4]), binary.BigEndian.Uint32(sizes[4:])
	if ml > 64<<10 || uint64(pl) > uint64(maxPayloadBytes) {
		return d, errors.New("invalid spruce frame size")
	}
	meta := make([]byte, ml)
	if _, e := io.ReadFull(r, meta); e != nil {
		return d, e
	}
	if e := json.Unmarshal(meta, &d); e != nil {
		return d, e
	}
	d.Payload = make([]byte, pl)
	_, e := io.ReadFull(r, d.Payload)
	return d, e
}
func (c *Client) ack(ctx context.Context, kind string, ids []string) error {
	body, _ := json.Marshal(map[string][]string{"delivery_ids": ids})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/deliveries/"+kind, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if e := c.authorize(req); e != nil {
		return e
	}
	resp, e := c.do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("spruce %s: %s", kind, resp.Status)
	}
	return nil
}

type Deduper struct {
	mu    sync.Mutex
	ttl   time.Duration
	max   int
	seen  map[string]time.Time
	order []dedupeEntry
}

type dedupeEntry struct {
	id    string
	until time.Time
}

func NewDeduper(max int, ttl time.Duration) *Deduper {
	if max <= 0 {
		max = 65536
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Deduper{ttl: ttl, max: max, seen: make(map[string]time.Time, max)}
}
func (d *Deduper) Seen(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if until, ok := d.seen[id]; ok && until.After(now) {
		return true
	}
	until := now.Add(d.ttl)
	d.seen[id] = until
	d.order = append(d.order, dedupeEntry{id: id, until: until})
	for len(d.order) > d.max {
		old := d.order[0]
		d.order = d.order[1:]
		if current, ok := d.seen[old.id]; ok && current.Equal(old.until) {
			delete(d.seen, old.id)
		}
	}
	return false
}
