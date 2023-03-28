package client

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// A Kafka admin client that uses the Brokers API
//
//go:generate mockery --name Admin --with-expecter --output ../services/mocks
type Admin interface {
	GetTopic(ctx context.Context, name string) (TopicInfo, error)
	CreateTopic(ctx context.Context, name string, assignments []PartitionAssignment, configs map[string]string) error
	UpdateTopicConfig(ctx context.Context, name string, configs map[string]string) error
	GetBrokers(ctx context.Context) ([]BrokerInfo, error)
	AddPartitions(ctx context.Context, name string, assignments []PartitionAssignment) error
	AssignPartitions(ctx context.Context, name string, assignments []PartitionAssignment) error
	RunLeaderElection(ctx context.Context, name string, partitions []int) error
}

var _ Admin = (*BrokerAdmin)(nil)

type BrokerAdmin struct {
	client KafkaAdminClient
}

func NewBrokerAdmin(config ConnectorConfig) (*BrokerAdmin, error) {
	connector, err := NewConnector(config)
	if err != nil {
		return nil, err
	}

	return &BrokerAdmin{
		client: connector.KafkaClient,
	}, nil
}

// GetTopic returns information about a topic
func (c *BrokerAdmin) GetTopic(ctx context.Context, name string) (TopicInfo, error) {
	topicInfo := TopicInfo{}

	resp, err := c.client.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{name},
	})
	if err != nil {
		return topicInfo, err
	}

	// Check the topic lenght
	if topicCount := len(resp.Topics); topicCount != 1 {
		return topicInfo, fmt.Errorf("unexpected topic count: %d", topicCount)
	}

	topic := resp.Topics[0]
	if topic.Error != nil {
		return topicInfo, topic.Error
	}

	topicInfo.Name = topic.Name
	for _, p := range topic.Partitions {
		var replicas []int
		for _, r := range p.Replicas {
			replicas = append(replicas, r.ID)
		}
		topicInfo.Partitions = append(topicInfo.Partitions, PartitionAssignment{
			ID:       p.ID,
			Replicas: replicas,
		})
	}

	return topicInfo, nil
}

// CreateTopic creates a topic with the given name, assignments and config
func (c *BrokerAdmin) CreateTopic(
	ctx context.Context,
	name string,
	assignments []PartitionAssignment,
	configs map[string]string,
) error {
	var configEntries []kafka.ConfigEntry
	for k, v := range configs {
		configEntries = append(configEntries, kafka.ConfigEntry{
			ConfigName:  k,
			ConfigValue: v,
		})
	}

	replicaAssignments := []kafka.ReplicaAssignment{}
	for _, partition := range assignments {
		replicaAssignments = append(replicaAssignments, kafka.ReplicaAssignment{
			Partition: partition.ID,
			Replicas:  partition.Replicas,
		})
	}

	resp, err := c.client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:              name,
			NumPartitions:      -1,
			ReplicationFactor:  -1,
			ReplicaAssignments: replicaAssignments,
			ConfigEntries:      configEntries,
		}},
	})
	if err != nil {
		return err
	}

	if err = KafkaErrorsToErr(resp.Errors); err != nil {
		return err
	}

	return nil
}

func (c *BrokerAdmin) UpdateTopicConfig(ctx context.Context, name string, configs map[string]string) error {
	var configEntries []kafka.ConfigEntry
	for k, v := range configs {
		configEntries = append(configEntries, kafka.ConfigEntry{
			ConfigName:  k,
			ConfigValue: v,
		})
	}

	resp, err := c.client.IncrementalAlterConfigs(ctx, &kafka.IncrementalAlterConfigsRequest{
		Resources: []kafka.IncrementalAlterConfigsRequestResource{{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: name,
			Configs:      configEntriesToAPIConfigs(configEntries),
		}},
	})
	if err != nil {
		return err
	}
	if err = IncrementalAlterConfigsResponseResourcesError(resp.Resources); err != nil {
		return err
	}

	return nil
}

// GetBrokers gets matadata about all brokers
func (c *BrokerAdmin) GetBrokers(ctx context.Context) ([]BrokerInfo, error) {
	var brokerInfo []BrokerInfo

	resp, err := c.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return brokerInfo, nil
	}

	for _, b := range resp.Brokers {
		brokerInfo = append(brokerInfo, BrokerInfo{
			ID:   b.ID,
			Rack: b.Rack,
		})
	}

	return brokerInfo, nil
}

// AddPartitions adds a list of partitions to a topic
func (c *BrokerAdmin) AddPartitions(ctx context.Context, name string, assignments []PartitionAssignment) error {
	topicInfo, err := c.GetTopic(ctx, name)
	if err != nil {
		return err
	}

	// Information about the partitions to add
	topicPartitions := kafka.TopicPartitionsConfig{
		Name:  name,
		Count: int32(len(assignments)) + int32(len(topicInfo.Partitions)),
	}

	var partitionAssignments []kafka.TopicPartitionAssignment
	for _, assignment := range assignments {
		brokerIDsInt32 := []int32{}
		for _, replica := range assignment.Replicas {
			brokerIDsInt32 = append(brokerIDsInt32, int32(replica))
		}

		partitionAssignments = append(
			partitionAssignments,
			kafka.TopicPartitionAssignment{
				BrokerIDs: brokerIDsInt32,
			},
		)
	}
	topicPartitions.TopicPartitionAssignments = partitionAssignments

	resp, err := c.client.CreatePartitions(ctx, &kafka.CreatePartitionsRequest{
		Topics: []kafka.TopicPartitionsConfig{topicPartitions},
	})
	if err != nil {
		return err
	}
	if err = KafkaErrorsToErr(resp.Errors); err != nil {
		return err
	}

	return nil
}

// AssignPartitions assigns topic partitions replicas to brokers
func (c *BrokerAdmin) AssignPartitions(ctx context.Context, name string, assignments []PartitionAssignment) error {
	var requestAssignments []kafka.AlterPartitionReassignmentsRequestAssignment

	for _, assignment := range assignments {
		requestAssignments = append(requestAssignments, kafka.AlterPartitionReassignmentsRequestAssignment{
			PartitionID: assignment.ID,
			BrokerIDs:   assignment.Replicas,
		})
	}

	resp, err := c.client.AlterPartitionReassignments(ctx, &kafka.AlterPartitionReassignmentsRequest{
		Topic:       name,
		Assignments: requestAssignments,
	})
	if err != nil {
		return err
	}
	if err = resp.Error; err != nil {
		return err
	}
	if err = AlterPartitionReassignmentsRequestAssignmentError(resp.PartitionResults); err != nil {
		return err
	}

	return nil
}

// RunLeaderElection performs a leader election on the given topic name and partition IDs
func (c *BrokerAdmin) RunLeaderElection(ctx context.Context, name string, partitions []int) error {
	resp, err := c.client.ElectLeaders(ctx, &kafka.ElectLeadersRequest{
		Topic:      name,
		Partitions: partitions,
	})
	if err != nil {
		return err
	}
	if err = resp.Error; err != nil {
		return err
	}

	return nil
}

func configEntriesToAPIConfigs(
	configEntries []kafka.ConfigEntry,
) []kafka.IncrementalAlterConfigsRequestConfig {
	var apiConfigs []kafka.IncrementalAlterConfigsRequestConfig

	for _, entry := range configEntries {
		var op kafka.ConfigOperation

		if entry.ConfigValue == "" {
			op = kafka.ConfigOperationDelete
		} else {
			op = kafka.ConfigOperationSet
		}

		apiConfigs = append(
			apiConfigs,
			kafka.IncrementalAlterConfigsRequestConfig{
				Name:            entry.ConfigName,
				Value:           entry.ConfigValue,
				ConfigOperation: op,
			},
		)
	}

	return apiConfigs
}
