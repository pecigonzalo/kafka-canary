package canary

import "time"

type Config struct {
	Topic                  string        `mapstructure:"topic"`
	ClientID               string        `mapstructure:"client-id"`
	ReconcileInterval      time.Duration `mapstructure:"reconcile-interval"`
	ProducerLatencyBuckets []float64     `mapstructure:"producer-latency-buckets"`
	EndToEndLatencyBuckets []float64     `mapstructure:"endtoend-latency-buckets"`
	ConsumerGroupID        string        `mapstructure:"consumer-group-id"`
}
