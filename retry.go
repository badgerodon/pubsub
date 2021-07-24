package pubsub

import (
	"math/rand"
	"sync"
	"time"
)

var (
	jitterRandLock sync.Mutex
	jitterRand     = rand.New(rand.NewSource(time.Now().UnixNano()))
)

const (
	multiplier = 1.6
	jitter     = 0.2
)

// A RetryPolicy configures how messages in the queue should be retried.
type RetryPolicy struct {
	MaxAttempts            int
	MinBackoff, MaxBackoff time.Duration
}

// BackoffDuration returns the backoff duration for the given number of retries.
func (retryPolicy *RetryPolicy) BackoffDuration(retries int) time.Duration {
	if retries == 0 {
		return 0
	}

	backoff, max := float64(retryPolicy.MinBackoff), float64(retryPolicy.MaxBackoff)
	for backoff < max && retries > 0 {
		backoff *= multiplier
		retries--
	}
	if backoff > max {
		backoff = max
	}

	// Randomize backoff delays so that if a cluster of requests start at
	// the same time, they won't operate in lockstep.
	jitterRandLock.Lock()
	r := jitterRand.Float64()
	jitterRandLock.Unlock()
	backoff *= 1 + jitter*(r*2-1)
	if backoff < 0 {
		return 0
	}

	return time.Duration(backoff)
}
