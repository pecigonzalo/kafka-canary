package services

import (
	"github.com/pecigonzalo/kafka-canary/pkg/canary"
	"github.com/pecigonzalo/kafka-canary/pkg/client"
)

type consumerService struct{}

func NewConsumerService(canary canary.Config, consumer *client.Client) ConsumerService {
	return &consumerService{}
}

func (s *consumerService) Consume() {}
func (s *consumerService) Leaders() (map[int32]int32, error) {
	return map[int32]int32{}, nil
}
func (s *consumerService) Refresh() {}
func (s *consumerService) Close()   {}
