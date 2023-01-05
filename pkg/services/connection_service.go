package services

import (
	"github.com/pecigonzalo/kafka-canary/pkg/canary"
	"github.com/pecigonzalo/kafka-canary/pkg/client"
)

type connectionService struct{}

func NewConnectionService(canary canary.Config, client client.Config) ConnectionService {
	return &connectionService{}
}

func (s *connectionService) Open()  {}
func (s *connectionService) Close() {}
