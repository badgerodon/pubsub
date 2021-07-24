package pubsub

import (
	"crypto/rand"

	"github.com/oklog/ulid/v2"
)

var entropy = ulid.Monotonic(rand.Reader, 0)

// NewID creates a new, random, sequential id.
var NewID = func() string {
	return ulid.MustNew(ulid.Now(), entropy).String()
}
