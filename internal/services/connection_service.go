package services

import (
	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
)

type ConnectionService interface {
	Open()
	Close()
}

type connectionService struct{}

func NewConnectionService(canary canary.Config, connectorConfig client.ConnectorConfig) ConnectionService {
	return &connectionService{}
}

func (s *connectionService) Open()  {}
func (s *connectionService) Close() {}
