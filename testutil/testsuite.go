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

	topic, err := q.Topic(ctx, "EXAMPLE-1")
	if !assert.NoError(t, err) {
		return
	}

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
}
