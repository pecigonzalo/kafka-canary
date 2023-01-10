package services

import (
	"context"
	"time"

	"github.com/pecigonzalo/kafka-canary/pkg/canary"
	"github.com/pecigonzalo/kafka-canary/pkg/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
)

var (
	RecordsProducedCounter uint64 = 0

	recordsProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "records_produced_total",
		Namespace: metrics_namespace,
		Help:      "The total number of records produced",
	}, []string{"clientid", "partition"})

	recordsProducedFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "records_produced_failed_total",
		Namespace: metrics_namespace,
		Help:      "The total number of records failed to produce",
	}, []string{"clientid", "partition"})

	// it's defined when the service is created because buckets are configurable
	recordsProducedLatency *prometheus.HistogramVec

	refreshProducerMetadataError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "producer_refresh_metadata_error_total",
		Namespace: metrics_namespace,
		Help:      "Total number of errors while refreshing producer metadata",
	}, []string{"clientid"})
)

type producerService struct {
	client          client.Connector
	producer        kafka.Writer
	canaryConfig    *canary.Config
	connectorConfig client.ConnectorConfig
	logger          *zerolog.Logger
	// index of the next message to send
	index int
}

func NewProducerService(canaryConfig canary.Config, connectorConfig client.ConnectorConfig, logger *zerolog.Logger) ProducerService {
	recordsProducedLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "records_produced_latency",
		Namespace: metrics_namespace,
		Help:      "Records produced latency in milliseconds",
		Buckets:   canaryConfig.ProducerLatencyBuckets,
	}, []string{"clientid", "partition"})

	client, err := client.NewConnector(connectorConfig)
	if err != nil {
		logger.Fatal().Msg("Error creating producer service client")
	}
	logger.Info().Msg("Created producer service client")

	producer := &kafka.Writer{
		Addr:      kafka.TCP(connectorConfig.BrokerAddr),
		Transport: client.KafkaClient.Transport,
		Topic:     canaryConfig.Topic,
	}
	logger.Info().Msg("Created producer service writer")

	return &producerService{
		client:          *client,
		producer:        *producer,
		canaryConfig:    &canaryConfig,
		connectorConfig: connectorConfig,
		logger:          logger,
	}
}

func (s *producerService) Send(partitionAssignments []int) {
	numPartitions := len(partitionAssignments)
	for i := 0; i < numPartitions; i++ {
		value := s.newCanaryMessage()
		msg := kafka.Message{
			Partition: i,
			Value:     []byte(value.Json()),
		}
		s.logger.Info().
			Str("value", value.Json()).
			Int("partition", i).
			Msgf("Sending message: value=%s on partition=%s", msg.Value, msg.Partition)

		err := s.producer.WriteMessages(context.Background(), msg)
		timestamp := time.Now().UnixMilli()
		labels := prometheus.Labels{
			"clientid":  s.canaryConfig.ClientID,
			"partition": string(i),
		}
		recordsProduced.With(labels).Inc()
		RecordsProducedCounter++

		if err != nil {
			s.logger.Warn().Msgf("Error sending message: %v", err)
			recordsProducedFailed.With(labels).Inc()
		} else {
			duration := timestamp - value.Timestamp
			s.logger.Info().
				Int("partition", i).
				Msgf("Message sent: partition=%d, duration=%d ms", i, duration)
			recordsProducedLatency.With(labels).Observe(float64(duration))
		}
	}
}

func (s *producerService) Refresh() {
	// TODO: Implement
	s.logger.Info().Msg("Producer refreshing metadata")
}

func (s *producerService) Close() {
	s.logger.Info().Msg("Closing producer")
	err := s.producer.Close()
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Error closing the kafka producer")
	}
	s.logger.Info().Msg("Producer closed")
}

func (s *producerService) newCanaryMessage() CanaryMessage {
	s.index++
	timestamp := time.Now().UnixMilli()
	cm := CanaryMessage{
		ProducerID: s.canaryConfig.ClientID,
		MessageID:  s.index,
		Timestamp:  timestamp,
	}
	return cm
}
