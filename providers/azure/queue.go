package azure

import (
	"context"

	"github.com/badgerodon/pubsub"
)

type queue struct {
}

// New creates a new pub/sub queue backed by Azure Storage Queues.
func New() pubsub.Queue {
	return new(queue)
}

func (q *queue) Close() error {
	panic("implement me")
}

func (q *queue) Topic(ctx context.Context, name string) (pubsub.Topic, error) {
	return newTopic(q, name), nil
}

type topic struct {
	q    *queue
	name string
}

func newTopic(q *queue, name string) pubsub.Topic {
	return &topic{
		q:    q,
		name: name,
	}
}

func (t *topic) Publish(ctx context.Context, msg pubsub.Message) error {
	panic("implement me")
}

func (t *topic) Subscribe(ctx context.Context, name string, options ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	panic("implement me")
}
