package google

import (
	"context"
	"testing"
	"time"

	"github.com/badgerodon/pubsub/testutil"
	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	ctx, clearTimeout := context.WithTimeout(context.Background(), time.Second*30)
	defer clearTimeout()

	assert.NoError(t, testutil.WithGooglePubSubEmulator(ctx, func() {
		q, err := New(ctx, WithProjectID("PROJECT-1"))
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = q.Close() }()

		testutil.RunTestSuite(t, q)
	}))
}
