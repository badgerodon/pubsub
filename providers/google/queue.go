// Package google implements a Pub/Sub queue using Google Cloud Pub/Sub.
//
// See: https://cloud.google.com/pubsub
package google

import (
	"context"
	"fmt"

	googlepubsub "cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/badgerodon/pubsub"
)

type queue struct {
	cfg    *config
	client *googlepubsub.Client
}

// New creates a new pub/sub queue backed by Google Cloud Pub/Sub.
func New(ctx context.Context, options ...Option) (pubsub.Queue, error) {
	q := &queue{cfg: getConfig(options...)}

	var err error
	q.client, err = googlepubsub.NewClient(ctx, q.cfg.projectID)
	if err != nil {
		return nil, fmt.Errorf("google: error create new client (project=%s): %w",
			q.cfg.projectID, err)
	}

	return q, nil
}

func (q *queue) Close() error {
	return q.client.Close()
}

func (q *queue) Topic(ctx context.Context, name string) (pubsub.Topic, error) {
	return newTopic(ctx, q, name)
}

type topic struct {
	q     *queue
	topic *googlepubsub.Topic
}

func newTopic(ctx context.Context, q *queue, name string) (pubsub.Topic, error) {
	t := &topic{q: q}

	t.topic = q.client.Topic(name)
	exists, err := t.topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("google: error getting topic (project=%s topic=%s): %w",
			q.cfg.projectID, name, err)
	}

	if !exists {
		// create the topic if it doesn't exist
		t.topic, err = q.client.CreateTopic(ctx, name)
		if status.Code(err) == codes.AlreadyExists {
			t.topic = q.client.Topic(name)
		} else if err != nil {
			return nil, fmt.Errorf("google: error creating topic (project=%s topic=%s): %w",
				q.cfg.projectID, name, err)
		}
	}

	return t, nil
}

func (t *topic) Publish(ctx context.Context, msg pubsub.Message) error {
	_, err := t.topic.Publish(ctx, &googlepubsub.Message{
		ID:   msg.ID(),
		Data: msg.Data(),
	}).Get(ctx)
	if err != nil {
		return fmt.Errorf("google: error creating topic (project=%s topic=%s message_id=%s): %w",
			t.q.cfg.projectID, t.topic.ID(), msg.ID(), err)
	}

	return nil
}

func (t *topic) Subscribe(ctx context.Context, name string, opts ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	return newSubscription(ctx, t, name, opts...)
}

type subscription struct {
	t            *topic
	subscription *googlepubsub.Subscription
}

func newSubscription(ctx context.Context, t *topic, name string, opts ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	id := getSubscriptionID(t.topic, name)
	s := &subscription{t: t}
	s.subscription = t.q.client.Subscription(id)
	exists, err := s.subscription.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("google: error getting subscription (project=%s subscription=%s): %w",
			t.q.cfg.projectID, id, err)
	}

	if exists {
		// update any existing subscription
		_, err = s.subscription.Update(ctx, getSubscriptionConfigToUpdate(opts...))
		if err != nil {
			return nil, fmt.Errorf("google: error updating subscription (project=%s subscription=%s): %w",
				t.q.cfg.projectID, id, err)
		}
	} else {
		// create the subscription if it doesn't exist
		s.subscription, err = t.q.client.CreateSubscription(ctx, getSubscriptionID(t.topic, name),
			getSubscriptionConfig(t.topic, opts...))
		if status.Code(err) == codes.AlreadyExists {
			s.subscription = t.q.client.Subscription(getSubscriptionID(t.topic, name))
		} else if err != nil {
			return nil, fmt.Errorf("google: error creating subscription (project=%s subscription=%s): %w",
				t.q.cfg.projectID, id, err)
		}
	}
	return s, nil
}

func (s *subscription) Receive(ctx context.Context, handler pubsub.SubscribeHandler) error {
	err := s.subscription.Receive(ctx, func(ctx context.Context, msg *googlepubsub.Message) {
		handler(ctx, subscriberMessage{msg})
	})
	if err != nil {
		return fmt.Errorf("google: error receiving messages (project=%s subscription=%s): %w",
			s.t.q.cfg.projectID, s.subscription.ID(), err)
	}
	return nil
}

type subscriberMessage struct {
	*googlepubsub.Message
}

func (s subscriberMessage) ID() string {
	return s.Message.ID
}

func (s subscriberMessage) Data() []byte {
	return s.Message.Data
}

func getSubscriptionID(topic *googlepubsub.Topic, name string) string {
	return topic.ID() + "-" + name
}

func getSubscriptionConfig(topic *googlepubsub.Topic, opts ...pubsub.SubscribeOption) googlepubsub.SubscriptionConfig {
	subscribeConfig := pubsub.NewSubscribeConfig(opts...)
	return googlepubsub.SubscriptionConfig{
		Topic: topic,
		DeadLetterPolicy: &googlepubsub.DeadLetterPolicy{
			MaxDeliveryAttempts: subscribeConfig.RetryPolicy.MaxAttempts,
		},
		RetryPolicy: &googlepubsub.RetryPolicy{
			MinimumBackoff: subscribeConfig.RetryPolicy.MinBackoff,
			MaximumBackoff: subscribeConfig.RetryPolicy.MaxBackoff,
		},
	}
}
func getSubscriptionConfigToUpdate(opts ...pubsub.SubscribeOption) googlepubsub.SubscriptionConfigToUpdate {
	subscribeConfig := pubsub.NewSubscribeConfig(opts...)
	return googlepubsub.SubscriptionConfigToUpdate{
		DeadLetterPolicy: &googlepubsub.DeadLetterPolicy{
			MaxDeliveryAttempts: subscribeConfig.RetryPolicy.MaxAttempts,
		},
		RetryPolicy: &googlepubsub.RetryPolicy{
			MinimumBackoff: subscribeConfig.RetryPolicy.MinBackoff,
			MaximumBackoff: subscribeConfig.RetryPolicy.MaxBackoff,
		},
	}
}
