package services

import (
	"github.com/pecigonzalo/kafka-canary/pkg/canary"
	"github.com/pecigonzalo/kafka-canary/pkg/client"
)

type connectionService struct{}

func NewConnectionService(canary canary.Config, connectorConfig client.ConnectorConfig) ConnectionService {
	return &connectionService{}
}

func (s *connectionService) Open()  {}
func (s *connectionService) Close() {}
