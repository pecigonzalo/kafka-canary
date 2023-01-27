package services

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"

	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
)

var (
	cleanupPolicy    string = "delete"
	metricsNamespace        = "kafka_canary"

	topicCreationFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "topic_creation_failed_total",
		Namespace: metricsNamespace,
		Help:      "Total number of errors while creating the canary topic",
	}, []string{"topic"})

	// describeClusterError = promauto.NewCounterVec(prometheus.CounterOpts{
	// 	Name:      "topic_describe_cluster_error_total",
	// 	Namespace: metricsNamespace,
	// 	Help:      "Total number of errors while describing cluster",
	// }, nil)

	describeTopicError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "topic_describe_error_total",
		Namespace: metricsNamespace,
		Help:      "Total number of errors while getting canary topic metadata",
	}, []string{"topic"})

	// alterTopicAssignmentsError = promauto.NewCounterVec(prometheus.CounterOpts{
	// 	Name:      "topic_alter_assignments_error_total",
	// 	Namespace: metricsNamespace,
	// 	Help:      "Total number of errors while altering partitions assignments for the canary topic",
	// }, []string{"topic"})

	alterTopicConfigurationError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "topic_alter_configuration_error_total",
		Namespace: metricsNamespace,
		Help:      "Total number of errors while altering configuration for the canary topic",
	}, []string{"topic"})
)

// TopicReconcileResult contains the result of a topic reconcile
type TopicReconcileResult struct {
	// new partitions assignments across brokers
	Assignments []int
	// partition to leader assignments
	Leaders map[int32]int32
	// if a refresh metadata is needed
	RefreshProducerMetadata bool
}

type topicService struct {
	logger          *zerolog.Logger
	admin           client.Client
	canaryConfig    canary.Config
	connectorConfig client.ConnectorConfig
	initialized     bool
}

func NewTopicService(canaryConfig canary.Config, connectorConfig client.ConnectorConfig, logger *zerolog.Logger) TopicService {
	return &topicService{
		logger:          logger,
		canaryConfig:    canaryConfig,
		connectorConfig: connectorConfig,
		initialized:     false,
	}
}

func (s *topicService) Reconcile() (TopicReconcileResult, error) {
	result := TopicReconcileResult{}

	ctx := context.Background()

	if s.admin == nil {
		a, err := client.NewBrokerAdminClient(ctx,
			client.BrokerAdminClientConfig{
				ConnectorConfig: s.connectorConfig,
			}, s.logger)
		if err != nil {
			s.logger.Error().Err(err).Msg("Error creating cluster admin client")
			return result, err
		}
		s.admin = a
	}

	_, err := s.admin.GetTopic(ctx, s.canaryConfig.Topic, false)

	// If we lost the connection, reset
	if client.IsTransientNetworkError(err) {
		s.Close()
		return result, err
	}

	// assignment := s.requestAssignments()

	// Create the topic if missing
	// TODO: Update parition config if missmatch
	if err == client.ErrTopicDoesNotExist {
		err = s.admin.CreateTopic(ctx, kafka.TopicConfig{
			Topic:             s.canaryConfig.Topic,
			NumPartitions:     3,
			ReplicationFactor: 3,
			// ReplicaAssignments: assignment,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "cleanup.policy", ConfigValue: cleanupPolicy},
				{ConfigName: "min.insync.replicas", ConfigValue: strconv.Itoa(3)}, // TODO: Get broker count from Kafka
			},
		})
		if err != nil {
			labels := prometheus.Labels{
				"topic": s.canaryConfig.Topic,
			}
			topicCreationFailed.With(labels).Inc()
			s.logger.Error().Str("topic", s.canaryConfig.Topic).Err(err).Msg("Error creating the topic")
			return result, err
		}
		s.logger.Info().Str("topic", s.canaryConfig.Topic).Msg("The canary topic was created")
	}
	topic, err := s.admin.GetTopic(ctx, s.canaryConfig.Topic, false)

	// If cant describe we can't proceed
	if err != nil {
		labels := prometheus.Labels{
			"topic": s.canaryConfig.Topic,
		}
		describeTopicError.With(labels).Inc()
		s.logger.Error().Err(err).Str("topic", s.canaryConfig.Topic).Msg("Error describing topic")
		return result, err
	}

	// Configure the topic if first run
	if !s.initialized {
		_, err := s.admin.UpdateTopicConfig(ctx, s.canaryConfig.Topic, []kafka.ConfigEntry{
			{},
		}, true)
		if err != nil {
			labels := prometheus.Labels{
				"topic": s.canaryConfig.Topic,
			}
			alterTopicConfigurationError.With(labels).Inc()
			s.logger.Error().Err(err).Str("topic", s.canaryConfig.Topic).Msg("Error altering topic configuration")
			return result, err
		}
		s.initialized = true
	}

	result.Assignments = topic.PartitionIDs()

	return result, nil
}

func (s topicService) Close() {
	s.logger.Info().Msg("Closing topic service")

	if s.admin == nil {
		return
	}
	if err := s.admin.Close(); err != nil {
		s.logger.Fatal().Err(err).Msg("Error closing cluster admin")
	}
}

// TODO: Implement
// func (s *topicService) requestAssignments() {
// 	brokers, err := s.admin.GetBrokerIDs(ctx)
// 	if err != nil {
// 		s.logger.Fatal().Err(err).Msg("Error getting broker information")
// 	}
//
// 	brokersNumber := len(brokers)
// 	partitions := max(currentPartitions, brokersNumber)
// 	replicationFactor := min(brokersNumber, 3)
// 	minISR := max(1, replicationFactor-1)
//
// 	assignments := kafka.PartitionAssignment{}
// }
//
// func max(x, y int) int {
// 	if x < y {
// 		return y
// 	}
// 	return x
// }
//
// func min(x, y int) int {
// 	if x > y {
// 		return y
// 	}
// 	return x
// }
