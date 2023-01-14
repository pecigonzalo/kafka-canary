package canary

import "time"

type Config struct {
	Topic                       string        `mapstructure:"topic"`
	ClientID                    string        `mapstructure:"client-id"`
	ReconcileInterval           time.Duration `mapstructure:"reconcile-interval"`
	StatusCheckInterval         time.Duration `mapstructure:"status-check-interval"`
	BootstrapBackoffMaxAttempts int           `mapstructure:"bootstrap-backoff-max-attempts"`
	BootstrapBackoffScale       time.Duration `mapstructure:"bootstrap-backoff-scale"`
	ProducerLatencyBuckets      []float64     `mapstructure:"producer-latency-buckets"`
	EndToEndLatencyBuckets      []float64     `mapstructure:"endtoend-latency-buckets"`
	ConsumerGroupID             string        `mapstructure:"consumer-group-id"`
}
