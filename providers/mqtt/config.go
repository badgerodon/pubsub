package mqtt

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type config struct {
	clientOptions *mqtt.ClientOptions
}

type Option func(cfg *config)

func WithClientOptions(clientOptions *mqtt.ClientOptions) Option {
	return func(cfg *config) {
		cfg.clientOptions = clientOptions
	}
}

func getConfig(opts ...Option) *config {
	cfg := new(config)
	WithClientOptions(mqtt.NewClientOptions())(cfg)
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}