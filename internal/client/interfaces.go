package client

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// KafkaAdminClient is a kafka.Client compatible interface
type KafkaAdminClient interface {
	Metadata(ctx context.Context, req *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	CreateTopics(ctx context.Context, req *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error)
	IncrementalAlterConfigs(ctx context.Context, req *kafka.IncrementalAlterConfigsRequest) (*kafka.IncrementalAlterConfigsResponse, error)
	CreatePartitions(ctx context.Context, req *kafka.CreatePartitionsRequest) (*kafka.CreatePartitionsResponse, error)
	AlterPartitionReassignments(ctx context.Context, req *kafka.AlterPartitionReassignmentsRequest) (*kafka.AlterPartitionReassignmentsResponse, error)
	ElectLeaders(ctx context.Context, req *kafka.ElectLeadersRequest) (*kafka.ElectLeadersResponse, error)
}

var _ KafkaAdminClient = &kafka.Client{}

// KafkaReaderClient is a kafka.Reader compatible interface
type KafkaReaderClient interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

var _ KafkaReaderClient = &kafka.Reader{}

// KafkaWriterClient is a kafka.Writer compatible interface
type KafkaWriterClient interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

var _ KafkaWriterClient = &kafka.Writer{}
