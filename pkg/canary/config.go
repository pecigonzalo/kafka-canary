package canary

import "time"

type Config struct {
	Topic                       string
	ClientID                    string
	ReconcileInterval           time.Duration
	StatusCheckInterval         time.Duration
	BootstrapBackoffMaxAttempts int
	BootstrapBackoffScale       time.Duration
	ProducerLatencyBuckets      []float64
}
