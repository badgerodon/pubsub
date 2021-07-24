package pubsub

import (
	"context"
	"io"
	"time"
)

// A Message is a pub/sub message.
type Message interface {
	ID() string
	Data() []byte
}

type basicMessage struct {
	id   string
	data []byte
}

func (msg basicMessage) ID() string {
	return msg.id
}

func (msg basicMessage) Data() []byte {
	return msg.data
}

// NewMessage creates a new message.
func NewMessage(data []byte) Message {
	return basicMessage{id: NewID(), data: data}
}

// A Queue is a pub/sub queue. It contains topics.
type Queue interface {
	io.Closer
	Topic(ctx context.Context, name string) (Topic, error)
}

// A Topic is a named, logical channel which messages can be published to.
type Topic interface {
	// Publish publishes a message to the topic.
	Publish(ctx context.Context, msg Message) error
	// Subscribe creates (or resumes) a subscription to a topic. All messages
	// published to the topic will be sent to all subscribers.
	Subscribe(ctx context.Context, name string, opts ...SubscribeOption) (Subscription, error)
}

type (
	// The SubscribeConfig customizes how subscription works.
	SubscribeConfig struct {
		RetryPolicy RetryPolicy
	}
	// A SubscribeOption customizes the SubscribeConfig.
	SubscribeOption = func(config *SubscribeConfig)
	// A SubscribeHandler is a function invoked for pub/sub messages sent to a topic.
	SubscribeHandler = func(ctx context.Context, msg SubscriberMessage)
	// A Subscription is a single subscription to a topic.
	Subscription interface {
		// Receive receives messages sent to the subscription. The given handler
		// will be invoked for each message in a goroutine. To stop `Receive`
		// cancel the context.
		Receive(ctx context.Context, handler SubscribeHandler) error
	}
	// A SubscriberMessage is a pub sub message with Ack and Nack methods.
	SubscriberMessage interface {
		Message
		// Ack signals completion of a message which will then be removed from the pub/sub topic.
		Ack()
		// Nack signals failure of message processing. The message will be retried according to
		// the retry policy of the subscription.
		Nack()
	}
)

// WithMaxAttempts sets the max attempts in the subscribe config.
func WithMaxAttempts(maxAttempts int) SubscribeOption {
	return func(cfg *SubscribeConfig) {
		cfg.RetryPolicy.MaxAttempts = maxAttempts
	}
}

// WithMinBackoff sets the min backoff in the subscribe config.
func WithMinBackoff(minBackoff time.Duration) SubscribeOption {
	return func(cfg *SubscribeConfig) {
		cfg.RetryPolicy.MinBackoff = minBackoff
	}
}

// WithMaxBackoff sets the max backoff in the subscribe config.
func WithMaxBackoff(maxBackoff time.Duration) SubscribeOption {
	return func(cfg *SubscribeConfig) {
		cfg.RetryPolicy.MaxBackoff = maxBackoff
	}
}

// NewSubscribeConfig creates a new SubscribeConfig from the given options.
func NewSubscribeConfig(opts ...SubscribeOption) *SubscribeConfig {
	cfg := new(SubscribeConfig)
	WithMaxAttempts(5)(cfg)
	WithMinBackoff(time.Second * 10)(cfg)
	WithMaxBackoff(time.Second * 600)(cfg)
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
