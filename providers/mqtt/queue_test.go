package mqtt

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/badgerodon/pubsub/testutil"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/stretchr/testify/require"
)

func TestMQTT(t *testing.T) {
	ctx, clearTimeout := context.WithTimeout(context.Background(), time.Second*10)
	defer clearTimeout()

	li, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := li.Addr().String()
	require.NoError(t, li.Close())

	srv := server.New()
	err = srv.AddListener(listeners.NewTCP("0", addr), nil)
	require.NoError(t, err)
	err = srv.Serve()
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()

	clientOptions := mqtt.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetConnectRetry(true)
	q := New(ctx, WithClientOptions(clientOptions))
	testutil.RunTestSuite(ctx, t, q)
}
