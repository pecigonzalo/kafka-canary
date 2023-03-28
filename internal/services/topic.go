package services

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"

	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
)

var (
	cleanupPolicy    = "delete"
	metricsNamespace = "kafka_canary"

	topicCreationFailed          *prometheus.CounterVec
	describeClusterError         *prometheus.CounterVec
	describeTopicError           *prometheus.CounterVec
	alterTopicAssignmentsError   *prometheus.CounterVec
	alterTopicConfigurationError *prometheus.CounterVec
)

type TopicService interface {
	Reconcile(context.Context) (TopicReconcileResult, error)
	Close()
}

// TopicReconcileResult contains the result of a topic reconcile
type TopicReconcileResult struct {
	// new partitions assignments across brokers
	Assignments []int
	// partition to leader assignments
	Leaders map[int]int
	// if a refresh metadata is needed
	RefreshProducerMetadata bool
}

type topicService struct {
	initialized  bool
	admin        client.Admin
	canaryConfig *canary.Config
	logger       *zerolog.Logger
}

func NewTopicService(admin client.Admin, canaryConfig canary.Config, logger *zerolog.Logger) TopicService {
	topicCreationFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "topic_creation_failed_total",
		Namespace:   metricsNamespace,
		Help:        "Total number of errors while creating the canary topic",
		ConstLabels: prometheus.Labels{"topic": canaryConfig.Topic},
	}, nil)

	describeClusterError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "topic_describe_cluster_error_total",
		Namespace: metricsNamespace,
		Help:      "Total number of errors while describing cluster",
	}, nil)

	describeTopicError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "topic_describe_error_total",
		Namespace:   metricsNamespace,
		Help:        "Total number of errors while getting canary topic metadata",
		ConstLabels: prometheus.Labels{"topic": canaryConfig.Topic},
	}, nil)

	alterTopicAssignmentsError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "topic_alter_assignments_error_total",
		Namespace:   metricsNamespace,
		Help:        "Total number of errors while altering partitions assignments for the canary topic",
		ConstLabels: prometheus.Labels{"topic": canaryConfig.Topic},
	}, nil)

	alterTopicConfigurationError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "topic_alter_configuration_error_total",
		Namespace:   metricsNamespace,
		Help:        "Total number of errors while altering configuration for the canary topic",
		ConstLabels: prometheus.Labels{"topic": canaryConfig.Topic},
	}, nil)

	serviceLogger := logger.With().
		Str("canaryService", "topic").
		Str("topic", canaryConfig.Topic).
		Logger()

	return &topicService{
		initialized:  false,
		admin:        admin,
		canaryConfig: &canaryConfig,
		logger:       &serviceLogger,
	}
}

func (s *topicService) Reconcile(ctx context.Context) (TopicReconcileResult, error) {
	result := TopicReconcileResult{}

	brokers, err := s.admin.GetBrokers(ctx)
	if err != nil {
		describeClusterError.WithLabelValues().Inc()
		s.logger.Error().Err(err).Msg("Error getting broker information")
		return result, err
	}

	topic, err := s.getTopic(ctx)
	if err != nil {
		if errors.Is(err, kafka.UnknownTopicOrPartition) {
			// If the topic is missing, create it
			if err = s.createTopic(ctx, brokers); err != nil {
				return result, err
			}
		}
		// Return and restart the reconciliation
		return result, err
	}

	// Configure the topic if first run
	if !s.initialized {
		if err = s.reconcileConfiguration(ctx, brokers); err != nil {
			return result, err
		}
	}

	// Refresh metadata if the topic partitions are not equal to the broker count
	// as this means we are going to reconcile
	result.RefreshProducerMetadata = len(brokers) != len(topic.Partitions)

	if result.RefreshProducerMetadata {
		if err = s.reconcilePartitions(ctx, topic.Partitions, brokers); err != nil {
			return result, err
		}
	}

	// Refresh topic info
	topic, err = s.getTopic(ctx)
	if err != nil {
		return result, err
	}

	var partitionIDs []int
	for _, partition := range topic.Partitions {
		partitionIDs = append(partitionIDs, partition.ID)
	}
	result.Assignments = partitionIDs

	leaders := map[int]int{}
	for _, partition := range topic.Partitions {
		leaders[partition.ID] = partition.Leader
	}
	result.Leaders = leaders

	s.initialized = true

	return result, nil
}

func (s topicService) Close() {
	s.logger.Info().Msg("Service closed")
}

func (s *topicService) reconcileConfiguration(ctx context.Context, brokers []client.BrokerInfo) error {
	brokersNumber := len(brokers)
	replicationFactor := min(brokersNumber, 3)
	configs := map[string]string{
		"cleanup.policy":      cleanupPolicy,
		"min.insync.replicas": strconv.Itoa(replicationFactor),
	}

	err := s.admin.UpdateTopicConfig(ctx, s.canaryConfig.Topic, configs)
	if err != nil {
		alterTopicConfigurationError.WithLabelValues().Inc()
		s.logger.Error().Err(err).Msg("Error altering topic configuration")
		return err
	}
	return nil
}

func (s *topicService) reconcilePartitions(ctx context.Context, currentPartitions []client.PartitionAssignment, brokers []client.BrokerInfo) error {
	brokersNumber := len(brokers)
	currentPartitionCount := len(currentPartitions)
	assignments := s.requestAssignments(ctx, currentPartitionCount, brokers)

	// If we have less partitions than brokers scale up, else assign
	if currentPartitionCount < brokersNumber {
		if err := s.admin.AssignPartitions(ctx, s.canaryConfig.Topic, assignments[:currentPartitionCount]); err != nil {
			alterTopicAssignmentsError.WithLabelValues().Inc()
			s.logger.Error().Err(err).Msg("Unable to assign partitions")
			return err
		} else {
			s.logger.Info().Msg("Added missing partition assignments")
		}

		finished := false
		for !finished {
			if err := s.admin.AddPartitions(ctx, s.canaryConfig.Topic, assignments[currentPartitionCount:]); err != nil {
				if errors.Is(err, kafka.ReassignmentInProgress) {
					s.logger.Warn().Msg("Unable to assign new partitions, existing modification in progress")
					time.Sleep(5 * time.Second)
					continue
				} else {
					alterTopicAssignmentsError.WithLabelValues().Inc()
					s.logger.Error().Err(err).Msg("Unable to add partitions")
					return err
				}
			}
			s.logger.Info().Msg("Added missing partitions")
			finished = true
		}
	} else {
		if err := s.admin.AssignPartitions(ctx, s.canaryConfig.Topic, assignments); err != nil {
			alterTopicAssignmentsError.WithLabelValues().Inc()
			s.logger.Error().Err(err).Msg("Unable to assign partitions")
			return err
		} else {
			s.logger.Info().Msg("Added missing partition assignments")
		}
	}
	s.logger.Info().Msg("Updated partition count and assignment")

	topic, err := s.getTopic(ctx)
	if err != nil {
		return err
	}

	var currentPartitionIDs []int
	for _, partition := range topic.Partitions {
		currentPartitionIDs = append(currentPartitionIDs, partition.ID)
	}

	// Run election to balanance leaders
	if err := s.admin.RunLeaderElection(ctx, s.canaryConfig.Topic, currentPartitionIDs); err != nil {
		s.logger.Error().Err(err).Msg("Error running leader election")
		return err
	} else {
		s.logger.Info().Msg("Ran partition leader election")
	}
	return nil
}

func (s *topicService) getTopic(ctx context.Context) (*client.TopicInfo, error) {
	topicInfo, err := s.admin.GetTopic(ctx, s.canaryConfig.Topic)
	if err != nil {
		describeTopicError.WithLabelValues().Inc()
		s.logger.Warn().Err(err).Msg("Error describing topic")
		return nil, err
	}

	return &topicInfo, nil
}

func (s *topicService) createTopic(ctx context.Context, brokers []client.BrokerInfo) error {
	brokersNumber := len(brokers)
	replicationFactor := min(brokersNumber, 3)

	assignments := s.requestAssignments(ctx, 0, brokers)
	configs := map[string]string{
		"cleanup.policy":      cleanupPolicy,
		"min.insync.replicas": strconv.Itoa(replicationFactor),
	}

	if err := s.admin.CreateTopic(ctx, s.canaryConfig.Topic, assignments, configs); err != nil {
		topicCreationFailed.WithLabelValues().Inc()
		s.logger.Error().Err(err).Msg("Error creating the topic")
		return err
	}

	s.logger.Info().Msg("The canary topic was created")
	return nil
}

func (s *topicService) requestAssignments(ctx context.Context, currentPartitions int, brokers []client.BrokerInfo) []client.PartitionAssignment {
	brokersNumber := len(brokers)
	partitions := max(currentPartitions, brokersNumber)
	replicationFactor := min(brokersNumber, 3)
	minISR := max(1, replicationFactor-1)

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID < brokers[j].ID
	})

	rackMap := make(map[string][]client.BrokerInfo)
	var rackNames []string
	brokersWithRack := 0

	for _, broker := range brokers {
		if broker.Rack != "" {
			brokersWithRack++
			if _, ok := rackMap[broker.Rack]; !ok {
				rackMap[broker.Rack] = make([]client.BrokerInfo, 0)
				rackNames = append(rackNames, broker.Rack)
			}
			rackMap[broker.Rack] = append(rackMap[broker.Rack], broker)
		}
	}

	if len(brokers) != brokersWithRack {
		if brokersWithRack > 0 {
			s.logger.Warn().
				Str("brokersWithRack", fmt.Sprintf("%d/%d", brokersNumber, brokersWithRack)).
				Msg("Some brokers lack rack assignment, topic will not use rack awareness")
		} else {
			index := 0
			for {
				again := false

				for _, rackName := range rackNames {
					brokerList := rackMap[rackName]
					if len(brokerList) > 0 {
						var head client.BrokerInfo
						head, rackMap[rackName] = brokerList[0], brokerList[1:]
						brokers[index] = head
						index++
						again = true
					}
				}

				if !again {
					break
				}
			}
		}
	}

	assignments := []client.PartitionAssignment{}
	for p := 0; p < partitions; p++ {
		replicas := []int{}
		for r := 0; r < replicationFactor; r++ {
			replicas = append(replicas, r+1)
		}
		assignments = append(assignments, client.PartitionAssignment{
			ID:       p,
			Replicas: replicas,
		})
	}

	s.logger.Info().
		Int("minISR", minISR).
		Str("assignment", fmt.Sprintf("%v", assignments)).
		Msg("Requested partition assignment")

	return assignments
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}
