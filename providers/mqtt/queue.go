// Package pubsub contains a Pub/Sub queue implementation backed by MQTT.
package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/badgerodon/pubsub"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
)

type queue struct {
	cfg *config

	closeOnce   sync.Once
	closeErr    error
	closeCtx    context.Context
	closeCancel context.CancelFunc

	publishLock   sync.Mutex
	publishClient mqtt.Client
}

// New creates a new Pub/Sub queue backed by MQTT.
func New(ctx context.Context, opts ...Option) pubsub.Queue {
	q := &queue{}
	q.cfg = getConfig(opts...)
	q.closeCtx, q.closeCancel = context.WithCancel(ctx)
	return q
}

func (q *queue) Close() error {
	q.closeOnce.Do(func() {
		q.closeCancel()

		q.publishLock.Lock()
		if q.publishClient != nil {
			go q.publishClient.Disconnect(0)
			q.publishClient = nil
		}
		q.publishLock.Unlock()
	})
	return q.closeErr
}

func (q *queue) Topic(ctx context.Context, name string) (pubsub.Topic, error) {
	return newTopic(ctx, q, name), nil
}

func (q *queue) getPublishClient(ctx context.Context) (mqtt.Client, error) {
	q.publishLock.Lock()
	defer q.publishLock.Unlock()

	client := q.publishClient
	if client != nil {
		return client, nil
	}

	client = mqtt.NewClient(q.cfg.clientOptions)
	err := q.wait(ctx, client.Connect())
	if err != nil {
		go client.Disconnect(0)
		return nil, fmt.Errorf("mqtt: error connecting to broker: %w", err)
	}

	q.publishClient = client
	return q.publishClient, nil
}

func (q *queue) wait(ctx context.Context, tok mqtt.Token) error {
	return wait(q.closeCtx, ctx, newTokenContext(tok))
}

type topic struct {
	ctx  context.Context
	q    *queue
	name string
}

func newTopic(ctx context.Context, q *queue, name string) *topic {
	t := new(topic)
	t.ctx = ctx
	t.q = q
	t.name = name
	return t
}

func (t *topic) Publish(ctx context.Context, msg pubsub.Message) error {
	client, err := t.q.getPublishClient(ctx)
	if err != nil {
		return err
	}

	bs, err := json.Marshal(newMessage(msg.ID(), msg.Data()))
	if err != nil {
		return fmt.Errorf("mqtt: error marshaling message: %w", err)
	}

	err = t.wait(ctx, client.Publish(t.name, 2, false, bs))
	if err != nil {
		return fmt.Errorf("mqtt: error publishing message (topic=%s id=%s): %w",
			t.name, msg.ID(), err)
	}

	return nil
}

func (t *topic) Subscribe(ctx context.Context, name string, opts ...pubsub.SubscribeOption) (pubsub.Subscription, error) {
	return newSubscription(ctx, t, name), nil
}

func (t *topic) wait(ctx context.Context, tok mqtt.Token) error {
	return wait(t.q.closeCtx, t.ctx, ctx, newTokenContext(tok))
}

type subscription struct {
	ctx  context.Context
	t    *topic
	name string
}

func newSubscription(ctx context.Context, t *topic, name string) *subscription {
	s := new(subscription)
	s.ctx = ctx
	s.t = t
	s.name = name
	return s
}

func (s *subscription) Receive(ctx context.Context, handler pubsub.SubscribeHandler) error {
	clientOptions := new(mqtt.ClientOptions)
	*clientOptions = *s.t.q.cfg.clientOptions
	clientOptions = clientOptions.SetClientID(s.name)

	client := mqtt.NewClient(clientOptions)
	err := s.wait(ctx, client.Connect())
	if err != nil {
		return fmt.Errorf("mqtt: error connecting to broker: %w", err)
	}
	defer func() {
		go client.Disconnect(0)
	}()

	err = s.wait(ctx, client.Subscribe(s.t.name, 2, func(_ mqtt.Client, msg mqtt.Message) {
		smsg, err := newSubscriberMessage(ctx, msg)
		if err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("mqtt: discarding invalid message")
			msg.Ack()
			return
		}
		go handler(ctx, smsg)
	}))
	if err != nil {
		return fmt.Errorf("mqtt: error subscribing to topic (topic=%s): %w",
			s.name, err)
	}

	_ = wait(s.t.q.closeCtx, s.t.ctx, s.ctx, ctx)

	return nil
}

func (s *subscription) wait(ctx context.Context, tok mqtt.Token) error {
	return wait(s.t.q.closeCtx, s.t.ctx, s.ctx, ctx, newTokenContext(tok))
}

type message struct {
	id   string
	data []byte
}

func newMessage(id string, data []byte) pubsub.Message {
	return message{id, data}
}

func (msg message) ID() string {
	return msg.id
}

func (msg message) Data() []byte {
	return msg.data
}

func (msg message) MarshalJSON() ([]byte, error) {
	var asJSON struct {
		ID   string `json:"id"`
		Data []byte `json:"data"`
	}
	asJSON.ID = msg.id
	asJSON.Data = msg.data
	return json.Marshal(asJSON)
}

func (msg *message) UnmarshalJSON(bytes []byte) error {
	var asJSON struct {
		ID   string `json:"id"`
		Data []byte `json:"data"`
	}
	err := json.Unmarshal(bytes, &asJSON)
	if err != nil {
		return err
	}
	msg.id = asJSON.ID
	msg.data = asJSON.Data
	return nil
}

type subscriberMessage struct {
	message
	ctx         context.Context
	mqttMessage mqtt.Message
}

func newSubscriberMessage(ctx context.Context, msg mqtt.Message) (pubsub.SubscriberMessage, error) {
	smsg := new(subscriberMessage)
	err := json.Unmarshal(msg.Payload(), &smsg.message)
	if err != nil {
		return nil, err
	}
	smsg.ctx = ctx
	smsg.mqttMessage = msg
	return smsg, nil
}

func (s *subscriberMessage) Ack() {
	s.mqttMessage.Ack()
}

func (s *subscriberMessage) Nack() {
	log.Ctx(s.ctx).Warn().Msg("mqtt: nack is not implemented, ignoring")
}

type tokenContext struct {
	mqtt.Token
}

func newTokenContext(tok mqtt.Token) context.Context {
	return tokenContext{tok}
}

func (t tokenContext) Deadline() (deadline time.Time, ok bool) {
	return deadline, false
}

func (t tokenContext) Err() error {
	return t.Error()
}

func (t tokenContext) Value(key interface{}) interface{} {
	return nil
}

func wait(ctxs ...context.Context) error {
	cases := make([]reflect.SelectCase, len(ctxs))
	for i, ctx := range ctxs {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
	}
	chosen, _, _ := reflect.Select(cases)
	return ctxs[chosen].Err()
}
