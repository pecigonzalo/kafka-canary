package client

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// KafkaClient is a kafka.Client compatible interface
type KafkaClient interface {
	Metadata(ctx context.Context, req *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	CreateTopics(ctx context.Context, req *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error)
	IncrementalAlterConfigs(ctx context.Context, req *kafka.IncrementalAlterConfigsRequest) (*kafka.IncrementalAlterConfigsResponse, error)
	CreatePartitions(ctx context.Context, req *kafka.CreatePartitionsRequest) (*kafka.CreatePartitionsResponse, error)
	AlterPartitionReassignments(ctx context.Context, req *kafka.AlterPartitionReassignmentsRequest) (*kafka.AlterPartitionReassignmentsResponse, error)
	ElectLeaders(ctx context.Context, req *kafka.ElectLeadersRequest) (*kafka.ElectLeadersResponse, error)
}

var _ KafkaClient = &kafka.Client{}
