package memory

import (
	"runtime"
)

type config struct {
	parallelism int
}

type Option = func(cfg *config)

func getConfig(options ...Option) *config {
	cfg := new(config)
	WithParallelism(runtime.NumCPU())(cfg)
	for _, option := range options {
		option(cfg)
	}
	return cfg
}

func WithParallelism(parallelism int) Option {
	return func(cfg *config) {
		cfg.parallelism = parallelism
	}
}
