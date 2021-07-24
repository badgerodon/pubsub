package testutil

import (
	"context"
	"testing"

	"github.com/badgerodon/pubsub"
	"github.com/stretchr/testify/assert"
)

func Run(t *testing.T, q pubsub.Queue) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic, err := q.Topic(ctx, "TOPIC-1")
	if !assert.NoError(t, err) {
		return
	}


	subscription, err := topic.Subscribe(ctx, "SUBSCRIPTION-1")
	if !assert.NoError(t, err) {
		return
	}

	recv := make(chan pubsub.Message, 4)
	go func() {
		err := subscription.Receive(ctx, func(ctx context.Context, msg pubsub.SubscriberMessage) {
			recv <-msg
			msg.Ack()
		})
		assert.NoError(t, err)
	}()

	msgs := []pubsub.Message{
		pubsub.NewMessage([]byte("1")),
		pubsub.NewMessage([]byte("2")),
		pubsub.NewMessage([]byte("3")),
		pubsub.NewMessage([]byte("4")),
	}
	for _, msg := range msgs {
		err = topic.Publish(ctx, msg)
		if !assert.NoError(t, err) {
			return
		}
	}

	seen := map[string]struct{}{}
	for i := 0; i < len(msgs); i++ {
		msg := <-recv
		seen[msg.ID()] = struct{}{}
	}
	assert.Equal(t, map[string]struct{}{
		"1":{}, "2":{}, "3":{}, "4":{},
	}, seen)
}
