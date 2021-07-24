package memory

import (
	"context"
	"testing"
	"time"

	"github.com/badgerodon/pubsub/testutil"
)

func TestQueue(t *testing.T) {
	ctx, clearTimeout := context.WithTimeout(context.Background(), time.Second*10)
	defer clearTimeout()

	testutil.Run(t, New(ctx))
}
