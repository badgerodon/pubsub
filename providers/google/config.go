package google

import "os"

var (
	DefaultProjectID = os.Getenv("PUBSUB_PROJECT_ID")
)

type config struct {
	projectID      string
}

// An Option customizes the config.
type Option = func(cfg *config)

func getConfig(options ...Option) *config {
	cfg := new(config)
	WithProjectID(DefaultProjectID)(cfg)
	for _, option := range options {
		option(cfg)
	}
	return cfg
}

// WithProjectID customizes the project id.
func WithProjectID(projectID string) Option {
	return func(cfg *config) {
		cfg.projectID = projectID
	}
}
