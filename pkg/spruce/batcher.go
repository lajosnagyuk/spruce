package spruce

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrBatcherClosed = errors.New("spruce: producer batcher is closed")

type BatcherOptions struct {
	MaxMessages int
	MaxBytes    int
	MaxDelay    time.Duration
	QueueDepth  int
}

type batchPublish struct {
	ctx     context.Context
	topic   string
	payload []byte
	options PublishOptions
	done    chan batchPublishResult
}
type batchPublishResult struct {
	result PublishResult
	err    error
}
type batchCommand struct {
	publish *batchPublish
	barrier chan error
}

type ProducerBatcher struct {
	client    *Client
	opts      BatcherOptions
	queue     chan batchCommand
	slots     chan struct{}
	enqueueMu sync.Mutex
	stop      chan struct{}
	done      chan struct{}
	once      sync.Once
	closeErr  error
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewProducerBatcher(client *Client, options BatcherOptions) *ProducerBatcher {
	if options.MaxMessages <= 0 {
		options.MaxMessages = 256
	}
	if options.MaxMessages > 4096 {
		options.MaxMessages = 4096
	}
	if options.MaxBytes <= 0 {
		options.MaxBytes = 1 << 20
	}
	if options.MaxBytes > 16<<20 {
		options.MaxBytes = 16 << 20
	}
	if options.MaxDelay <= 0 {
		options.MaxDelay = 250 * time.Microsecond
	}
	if options.QueueDepth <= 0 {
		options.QueueDepth = 4096
	}
	options.QueueDepth = min(options.QueueDepth, 65536)
	workerCtx, cancel := context.WithCancel(context.Background())
	b := &ProducerBatcher{client: client, opts: options, queue: make(chan batchCommand, options.QueueDepth), slots: make(chan struct{}, options.QueueDepth), stop: make(chan struct{}), done: make(chan struct{}), ctx: workerCtx, cancel: cancel}
	go b.run()
	return b
}

func (b *ProducerBatcher) Publish(ctx context.Context, topic string, payload []byte, options PublishOptions) (PublishResult, error) {
	if topic == "" {
		return PublishResult{}, errors.New("topic is required")
	}
	if batchEntrySize(payload, options.Key) > b.opts.MaxBytes || len(payload) > 1<<20 || len(options.Key) > 8<<10 {
		return PublishResult{}, errors.New("payload exceeds batcher limits")
	}
	if options.IdempotencyKey != "" || (options.Ack != "" && options.Ack != "local") {
		return PublishResult{}, errors.New("option is incompatible with batch publishing")
	}
	if err := b.reserve(ctx); err != nil {
		return PublishResult{}, err
	}
	item := &batchPublish{ctx: ctx, topic: topic, payload: append([]byte(nil), payload...), options: options, done: make(chan batchPublishResult, 1)}
	if err := b.enqueueReserved(batchCommand{publish: item}); err != nil {
		return PublishResult{}, err
	}
	select {
	case result := <-item.done:
		return result.result, result.err
	case <-ctx.Done():
		return PublishResult{}, ctx.Err()
	}
}

func (b *ProducerBatcher) Flush(ctx context.Context) error {
	if err := b.reserve(ctx); err != nil {
		return err
	}
	barrier := make(chan error, 1)
	if err := b.enqueueReserved(batchCommand{barrier: barrier}); err != nil {
		return err
	}
	select {
	case err := <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Reserve before copying payloads so blocked callers cannot grow an owned queue
// outside QueueDepth. Closing and submission share a short, nonblocking lock.
func (b *ProducerBatcher) reserve(ctx context.Context) error {
	select {
	case <-b.stop:
		return ErrBatcherClosed
	default:
	}
	select {
	case b.slots <- struct{}{}:
		return nil
	case <-b.stop:
		return ErrBatcherClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *ProducerBatcher) enqueueReserved(cmd batchCommand) error {
	b.enqueueMu.Lock()
	defer b.enqueueMu.Unlock()
	select {
	case <-b.stop:
		<-b.slots
		return ErrBatcherClosed
	default:
		b.queue <- cmd
		return nil
	}
}

func (b *ProducerBatcher) Close(ctx context.Context) error {
	b.once.Do(func() {
		go func() {
			b.closeErr = b.Flush(ctx)
			b.enqueueMu.Lock()
			close(b.stop)
			b.enqueueMu.Unlock()
		}()
	})
	select {
	case <-b.done:
		return b.closeErr
	case <-ctx.Done():
		b.cancel()
		return ctx.Err()
	}
}

func (b *ProducerBatcher) run() {
	defer close(b.done)
	defer b.cancel()
	var pending []*batchPublish
	pendingBytes := 0
	var timer *time.Timer
	var timerC <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	reset := func() {
		clear(pending)
		pending = pending[:0]
		pendingBytes = 0
		if timer != nil {
			timer.Stop()
		}
		timerC = nil
	}
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		entries := make([]BatchEntry, 0, len(pending))
		active := make([]*batchPublish, 0, len(pending))
		for i := range pending {
			if pending[i].ctx.Err() == nil {
				entries = append(entries, BatchEntry{Payload: pending[i].payload, Key: pending[i].options.Key})
				active = append(active, pending[i])
			} else {
				pending[i].done <- batchPublishResult{err: pending[i].ctx.Err()}
			}
		}
		if len(entries) == 0 {
			reset()
			return nil
		}
		shared := pending[0].options
		shared.Key = ""
		result, err := b.client.PublishBatchEntries(b.ctx, pending[0].topic, entries, shared)
		if err == nil && len(result.IDs) != len(active) {
			err = errors.New("spruce: invalid batch result count")
		}
		for i, item := range active {
			logical := PublishResult{}
			if err == nil && i < len(result.IDs) {
				logical.ID = result.IDs[i]
			}
			item.done <- batchPublishResult{result: logical, err: err}
		}
		reset()
		return err
	}
	compatible := func(a, c *batchPublish) bool {
		ao, co := a.options, c.options
		ao.Key, co.Key = "", ""
		return a.topic == c.topic && ao == co
	}

	for {
		select {
		case <-b.stop:
			_ = flush()
			for {
				select {
				case cmd := <-b.queue:
					<-b.slots
					if cmd.publish != nil {
						cmd.publish.done <- batchPublishResult{err: ErrBatcherClosed}
					} else {
						cmd.barrier <- ErrBatcherClosed
					}
				default:
					return
				}
			}
		case <-timerC:
			_ = flush()
		case cmd := <-b.queue:
			<-b.slots
			if cmd.barrier != nil {
				cmd.barrier <- flush()
				continue
			}
			item := cmd.publish
			if item.ctx.Err() != nil {
				item.done <- batchPublishResult{err: item.ctx.Err()}
				continue
			}
			if len(pending) > 0 && (!compatible(pending[0], item) || len(pending) >= b.opts.MaxMessages || pendingBytes+batchEntrySize(item.payload, item.options.Key) > b.opts.MaxBytes) {
				_ = flush()
			}
			pending = append(pending, item)
			pendingBytes += batchEntrySize(item.payload, item.options.Key)
			if len(pending) == 1 {
				if timer == nil {
					timer = time.NewTimer(b.opts.MaxDelay)
				} else {
					timer.Reset(b.opts.MaxDelay)
				}
				timerC = timer.C
			}
			if len(pending) >= b.opts.MaxMessages || pendingBytes >= b.opts.MaxBytes {
				_ = flush()
			}
		}
	}
}

func batchEntrySize(payload []byte, key string) int { return 6 + len(key) + len(payload) }
