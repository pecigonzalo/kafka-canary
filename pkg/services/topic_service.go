package services

import (
	"github.com/pecigonzalo/kafka-canary/pkg/canary"
	"github.com/pecigonzalo/kafka-canary/pkg/client"
)

// TopicReconcileResult contains the result of a topic reconcile
type TopicReconcileResult struct {
	// new partitions assignments across brokers
	Assignments map[int32][]int32
	// partition to leader assignments
	Leaders map[int32]int32
	// if a refresh metadata is needed
	RefreshProducerMetadata bool
}

type topicService struct{}

func NewTopicService(canary canary.Config, client client.Config) TopicService {
	return &topicService{}
}

func (s topicService) Close() {}
func (s topicService) Reconcile() (TopicReconcileResult, error) {
	return TopicReconcileResult{}, nil
}
