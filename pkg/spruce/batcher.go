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
	client *Client
	opts   BatcherOptions
	queue  chan batchCommand
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
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
	workerCtx, cancel := context.WithCancel(context.Background())
	b := &ProducerBatcher{client: client, opts: options, queue: make(chan batchCommand, options.QueueDepth), stop: make(chan struct{}), done: make(chan struct{}), ctx: workerCtx, cancel: cancel}
	go b.run()
	return b
}

func (b *ProducerBatcher) Publish(ctx context.Context, topic string, payload []byte, options PublishOptions) (PublishResult, error) {
	if topic == "" {
		return PublishResult{}, errors.New("topic is required")
	}
	if len(payload)+4 > b.opts.MaxBytes || len(payload) > 1<<20 {
		return PublishResult{}, errors.New("payload exceeds batcher limits")
	}
	if options.IdempotencyKey != "" || (options.Ack != "" && options.Ack != "local") {
		return PublishResult{}, errors.New("option is incompatible with batch publishing")
	}
	item := &batchPublish{ctx: ctx, topic: topic, payload: append([]byte(nil), payload...), options: options, done: make(chan batchPublishResult, 1)}
	select {
	case b.queue <- batchCommand{publish: item}:
	case <-b.stop:
		return PublishResult{}, ErrBatcherClosed
	case <-ctx.Done():
		return PublishResult{}, ctx.Err()
	}
	select {
	case result := <-item.done:
		return result.result, result.err
	case <-ctx.Done():
		return PublishResult{}, ctx.Err()
	}
}

func (b *ProducerBatcher) Flush(ctx context.Context) error {
	barrier := make(chan error, 1)
	select {
	case b.queue <- batchCommand{barrier: barrier}:
	case <-b.stop:
		return ErrBatcherClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *ProducerBatcher) Close(ctx context.Context) error {
	var err error
	b.once.Do(func() { err = b.Flush(ctx); close(b.stop) })
	select {
	case <-b.done:
		return err
	case <-ctx.Done():
		b.cancel()
		return ctx.Err()
	}
}

func (b *ProducerBatcher) run() {
	defer close(b.done)
	var pending []*batchPublish
	var timer *time.Timer
	var timerC <-chan time.Time
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		payloads := make([][]byte, len(pending))
		for i := range pending {
			payloads[i] = pending[i].payload
		}
		result, err := b.client.PublishBatch(b.ctx, pending[0].topic, payloads, pending[0].options)
		if err == nil && len(result.IDs) != len(pending) {
			err = errors.New("spruce: invalid batch result count")
		}
		for i, item := range pending {
			logical := PublishResult{}
			if err == nil && i < len(result.IDs) {
				logical.ID = result.IDs[i]
			}
			item.done <- batchPublishResult{result: logical, err: err}
		}
		pending = pending[:0]
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerC = nil
		}
		return err
	}
	compatible := func(a, c *batchPublish) bool { return a.topic == c.topic && a.options == c.options }
	bytes := func(items []*batchPublish) int {
		n := 0
		for _, item := range items {
			n += 4 + len(item.payload)
		}
		return n
	}
	for {
		select {
		case <-b.stop:
			_ = flush()
			for {
				select {
				case cmd := <-b.queue:
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
			if cmd.barrier != nil {
				cmd.barrier <- flush()
				continue
			}
			item := cmd.publish
			if item.ctx.Err() != nil {
				item.done <- batchPublishResult{err: item.ctx.Err()}
				continue
			}
			if len(pending) > 0 && (!compatible(pending[0], item) || len(pending) >= b.opts.MaxMessages || bytes(pending)+4+len(item.payload) > b.opts.MaxBytes) {
				_ = flush()
			}
			pending = append(pending, item)
			if len(pending) == 1 {
				timer = time.NewTimer(b.opts.MaxDelay)
				timerC = timer.C
			}
			if len(pending) >= b.opts.MaxMessages || bytes(pending) >= b.opts.MaxBytes {
				_ = flush()
			}
		}
	}
}
