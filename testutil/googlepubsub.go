package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
)

// WithGooglePubSubEmulator runs a gcloud pubsub emulator in a docker container,
// sets the expected environment variable, and calls the given callback.
func WithGooglePubSubEmulator(ctx context.Context, callback func()) error {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		return fmt.Errorf("error creating dockertest pool: %w", err)
	}

	resource, err := dockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "fixl/google-cloud-sdk-emulators",
		Tag:          "349.0.0-pubsub",
		Cmd:          []string{"start", "--host-port=0.0.0.0:8085"},
		ExposedPorts: []string{"8085"},
	})
	if err != nil {
		return fmt.Errorf("error running pubsub emulator: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = resource.Expire(uint(time.Until(deadline).Seconds()))
	} else {
		_ = resource.Expire(60 * 10)
	}
	go TailLogs(ctx, dockerPool, resource)

	addr := resource.GetHostPort("8085/tcp")

	if err = dockerPool.Retry(func() error {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return err
		}
		_ = conn.Close()
		return nil
	}); err != nil {
		return fmt.Errorf("error starting pubsub emulator: %w", err)
	}

	current := os.Getenv("PUBSUB_EMULATOR_HOST")
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", addr)
	callback()
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", current)

	if err = dockerPool.Purge(resource); err != nil {
		return fmt.Errorf("error purging dockertest pool: %w", err)
	}

	return nil
}

func TailLogs(ctx context.Context, pool *dockertest.Pool, resource *dockertest.Resource) {
	_ = pool.Client.Logs(docker.LogsOptions{
		Context:      ctx,
		Stderr:       true,
		Stdout:       true,
		Follow:       true,
		Container:    resource.Container.ID,
		OutputStream: os.Stderr,
	})
}
