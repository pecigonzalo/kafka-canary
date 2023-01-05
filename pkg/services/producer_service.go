package services

import (
	"github.com/pecigonzalo/kafka-canary/pkg/canary"
	"github.com/pecigonzalo/kafka-canary/pkg/client"
)

type producerService struct{}

func NewProducerService(canary canary.Config, producer *client.Client) ProducerService {
	return &producerService{}
}

func (s *producerService) Send(partitionAssignments map[int32][]int32) {}
func (s *producerService) Close()                                      {}
func (s *producerService) Refresh()                                    {}
