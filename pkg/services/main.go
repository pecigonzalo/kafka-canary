package services

import (
	"context"
	"net/http"
)

// ErrExpectedClusterSize defines the error raised when the expected cluster size is not met
type ErrExpectedClusterSize struct{}

func (e *ErrExpectedClusterSize) Error() string {
	return "Current cluster size differs from the expected size"
}

type StatusService interface {
	Open()
	Close()
	StatusHandler() http.Handler
}

type ConnectionService interface {
	Open()
	Close()
}

type TopicService interface {
	Reconcile() (TopicReconcileResult, error)
	Close()
}

type ProducerService interface {
	Send(partitionsAssignments []int)
	Refresh()
	Close()
}

type ConsumerService interface {
	Consume()
	Refresh()
	Leaders(context.Context) (map[int]int, error)
	Close()
}
