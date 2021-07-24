// Package memory contains an in-memory implementation of a pub/sub queue.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/badgerodon/pubsub"
)

type queue struct {
	cfg *config

	closeCtx    context.Context
	closeCancel context.CancelFunc
}

// New creates a new pub/sub queue backed by in-memory channels.
func New(ctx context.Context, opts ...Option) pubsub.Queue {
	cfg := getConfig(opts...)
	q := &queue{
		cfg: cfg,
	}
	q.closeCtx, q.closeCancel = context.WithCancel(ctx)
	return q
}

func (q *queue) Close() error {
	q.closeCancel()
	return nil
}

func (q *queue) Topic(ctx context.Context, name string) (pubsub.Topic, error) {
	return newTopic(ctx, q, name)
}

type topic struct {
	q *queue

	in chan pubsub.Message

	mu            sync.RWMutex
	subscriptions map[string]*subscription
}

func newTopic(ctx context.Context, q *queue, name string) (pubsub.Topic, error) {
	t := &topic{
		q:             q,
		in:            make(chan pubsub.Message),
		subscriptions: make(map[string]*subscription),
	}
	go t.run(ctx)
	return t, nil
}

func (t *topic) run(ctx context.Context) {
	for {
		var msg pubsub.Message
		select {
		case <-t.q.closeCtx.Done():
			return
		case <-ctx.Done():
			return
		case msg = <-t.in:
		}

		t.mu.RLock()
		ss := make([]*subscription, 0, len(t.subscriptions))
		for _, s := range t.subscriptions {
			ss = append(ss, s)
		}
		t.mu.RUnlock()

		for _, s := range ss {
			select {
			case <-t.q.closeCtx.Done():
				return
			case <-ctx.Done():
				return
			case s.in <- msg:
			}
		}
	}
}

func (t *topic) Publish(ctx context.Context, msg pubsub.Message) error {
	select {
	case <-t.q.closeCtx.Done():
		return t.q.closeCtx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case t.in <- msg:
	}
	return nil
}

func (t *topic) Subscribe(ctx context.Context, name string, opts ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	t.mu.RLock()
	s, ok := t.subscriptions[name]
	t.mu.RUnlock()
	if ok {
		return s, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	s, ok = t.subscriptions[name]
	if ok {
		return s, nil
	}

	s, err := newSubscription(ctx, t, name, opts...)
	if err != nil {
		return nil, err
	}
	t.subscriptions[name] = s
	return s, nil
}

type subscription struct {
	cfg *pubsub.SubscribeConfig
	t   *topic

	ack chan struct{}
	in  chan pubsub.Message
	out chan queueMessage
}

func newSubscription(ctx context.Context, t *topic, name string, opts ...pubsub.SubscribeOption) (*subscription, error) {
	s := &subscription{
		cfg: pubsub.NewSubscribeConfig(opts...),
		t:   t,
		ack: make(chan struct{}),
		in:  make(chan pubsub.Message),
		out: make(chan queueMessage),
	}
	go s.run(ctx)
	return s, nil
}

func (s *subscription) run(ctx context.Context) {
	var messages []queueMessage
	var pending int
	for {
		if len(messages) > 0 && pending < s.t.q.cfg.parallelism {
			qmsg := messages[0]
			select {
			case <-s.t.q.closeCtx.Done():
				return
			case <-ctx.Done():
				return
			case <-s.ack:
				pending--
			case msg := <-s.in:
				messages = append(messages, newQueueMessage(msg))
			case s.out <- qmsg:
				messages = messages[1:]
			}
		} else {
			select {
			case <-s.t.q.closeCtx.Done():
				return
			case <-ctx.Done():
				return
			case msg := <-s.in:
				messages = append(messages, newQueueMessage(msg))
			case <-s.ack:
				pending--
			}
		}
	}
}

func (s *subscription) Receive(ctx context.Context, handler pubsub.SubscribeHandler) error {
	for {
		select {
		case <-s.t.q.closeCtx.Done():
			return nil
		case <-ctx.Done():
			return nil
		case msg := <-s.out:
			if msg.attempts > s.cfg.RetryPolicy.MaxAttempts {
				// drop the message as we've exceeded the max attempts
				continue
			}
			// run the handler in a goroutine, possibly delaying based on the retry policy
			go func() {
				select {
				case <-s.t.q.closeCtx.Done():
				case <-ctx.Done():
				case <-time.After(s.cfg.RetryPolicy.BackoffDuration(msg.attempts)):
					handler(ctx, newSubscriberMessage(ctx, s, msg))
				}
			}()
		}
	}
}

type queueMessage struct {
	pubsub.Message
	attempts int
}

func newQueueMessage(msg pubsub.Message) queueMessage {
	return queueMessage{Message: msg}
}

type subscriberMessage struct {
	queueMessage
	ctx         context.Context
	s           *subscription
	ackNackOnce sync.Once
}

func newSubscriberMessage(ctx context.Context, s *subscription, msg queueMessage) *subscriberMessage {
	return &subscriberMessage{
		queueMessage: msg,
		ctx:          ctx,
		s:            s,
	}
}

func (smsg *subscriberMessage) Ack() {
	smsg.ackNackOnce.Do(func() {
		select {
		case <-smsg.s.t.q.closeCtx.Done():
		case <-smsg.ctx.Done():
		case smsg.s.ack <- struct{}{}:
		}
	})
}

func (smsg *subscriberMessage) Nack() {
	qmsg := smsg.queueMessage
	qmsg.attempts++
	// push the message back onto the queue
	smsg.ackNackOnce.Do(func() {
		select {
		case <-smsg.s.t.q.closeCtx.Done():
		case <-smsg.ctx.Done():
		case smsg.s.out <- qmsg:
		}
	})
}
