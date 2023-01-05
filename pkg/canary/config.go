package canary

import "time"

type Config struct {
	ReconcileInterval           time.Duration
	StatusCheckInterval         time.Duration
	BootstrapBackoffMaxAttempts int
	BootstrapBackoffScale       time.Duration
}
