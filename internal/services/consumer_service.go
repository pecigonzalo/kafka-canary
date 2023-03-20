package services

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"

	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
)

var (
	RecordsConsumedCounter uint64 = 0

	recordsConsumed              *prometheus.CounterVec
	recordsConsumerFailed        *prometheus.CounterVec
	recordsEndToEndLatency       *prometheus.HistogramVec
	refreshConsumerMetadataError *prometheus.CounterVec
)

type consumerService struct {
	client          *client.Connector
	consumer        *kafka.Reader
	canaryConfig    *canary.Config
	connectorConfig *client.ConnectorConfig
	logger          *zerolog.Logger
}

func NewConsumerService(canaryConfig canary.Config, connectorConfig client.ConnectorConfig, logger *zerolog.Logger) ConsumerService {
	recordsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "records_consumed_total",
		Namespace:   metricsNamespace,
		Help:        "The total number of records consumed",
		ConstLabels: prometheus.Labels{"consumergroup": canaryConfig.ConsumerGroupID},
	}, []string{"partition"})

	recordsConsumerFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "consumer_error_total",
		Namespace:   metricsNamespace,
		Help:        "Total number of errors reported by the consumer",
		ConstLabels: prometheus.Labels{"consumergroup": canaryConfig.ConsumerGroupID},
	}, nil)

	recordsEndToEndLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "records_consumed_latency",
		Namespace:   metricsNamespace,
		Help:        "Records end-to-end latency in milliseconds",
		Buckets:     canaryConfig.EndToEndLatencyBuckets,
		ConstLabels: prometheus.Labels{"consumergroup": canaryConfig.ConsumerGroupID},
	}, []string{"partition"})

	refreshConsumerMetadataError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "consumer_refresh_metadata_error_total",
		Namespace:   metricsNamespace,
		Help:        "Total number of errors while refreshing consumer metadata",
		ConstLabels: prometheus.Labels{"consumergroup": canaryConfig.ConsumerGroupID},
	}, nil)

	serviceLogger := logger.With().
		Str("canaryService", "consumer").
		Str("topic", canaryConfig.Topic).
		Str("consumerGroup", canaryConfig.ConsumerGroupID).
		Logger()

	client, err := client.NewConnector(connectorConfig)
	if err != nil {
		serviceLogger.Fatal().Err(err).Msg("Error creating consumer service client")
	}
	client.Dialer.ClientID = canaryConfig.ClientID
	serviceLogger.Info().Msg("Created consumer service client")

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     client.Config.BrokerAddrs,
		Dialer:      client.Dialer,
		GroupID:     canaryConfig.ConsumerGroupID,
		Topic:       canaryConfig.Topic,
		StartOffset: kafka.LastOffset,
	})
	logger.Info().Msg("Created consumer service reader")

	return &consumerService{
		client:          client,
		consumer:        consumer,
		canaryConfig:    &canaryConfig,
		connectorConfig: &connectorConfig,
		logger:          &serviceLogger,
	}
}

func (s *consumerService) Consume(ctx context.Context) {
	go func() {
		defer s.Close()
		for {
			message, err := s.consumer.ReadMessage(ctx)
			if err != nil {
				partition := s.consumer.Config().Partition

				if strings.Contains(err.Error(), "connection reset") ||
					strings.Contains(err.Error(), "broken pipe") ||
					strings.Contains(err.Error(), "i/o timeout") {
					// These errors are recoverable, just try again
					s.logger.Warn().Err(err).Msgf(
						"Got connection error reading from partition %d, retrying: %+v",
						partition,
						err,
					)
					continue
				} else if ctx.Err() != nil {
					s.logger.Info().Msg("Context cancelled")
					return
				} else {
					s.logger.Error().Err(err).Msg("Error consuming topic")
					recordsConsumerFailed.WithLabelValues().Inc()
				}
			}

			if ctx.Err() != nil {
				s.logger.Info().Msg("Consumer Groups context cancelled")
				return
			}
			canaryMessage, err := NewCanaryMessage(message.Value)
			if err != nil {
				s.logger.Err(err).Msg("Error creating new canary message")
				return
			}
			partitionString := strconv.Itoa(message.Partition)

			timestamp := time.Now().UnixMilli()
			duration := timestamp - canaryMessage.Timestamp

			recordsEndToEndLatency.WithLabelValues(partitionString).Observe(float64(duration))
			recordsConsumed.WithLabelValues(partitionString).Inc()
			RecordsConsumedCounter++

			s.logger.Info().
				Int64("duration", duration).
				Int("partition", message.Partition).
				Int64("offset", message.Offset).
				Msg("Message read")
			if s.logger.Debug().Enabled() {
				s.logger.Debug().
					Int64("duration", duration).
					Int("partition", message.Partition).
					Int64("offset", message.Offset).
					Bytes("value", message.Value).
					Msg("Message content")
			}
		}
	}()
}

func (s *consumerService) Refresh(ctx context.Context) {
	s.logger.Info().Msg("Refreshing metadata")
	_, err := s.getMetadata(ctx)
	if err != nil {
		refreshConsumerMetadataError.WithLabelValues().Inc()
		s.logger.Error().Err(err).Msg("Error refreshing metadata")
	}
}

func (s *consumerService) Leaders(ctx context.Context) (map[int]int, error) {
	s.logger.Info().Msg("Getting leaders")
	topic, err := s.getMetadata(ctx)
	if err != nil {
		s.logger.Err(err).Msg("Error getting leaders")
		return nil, err
	}

	leaders := make(map[int]int, len(topic.Partitions))

	for _, p := range topic.Partitions {
		leaders[p.ID] = p.Leader.ID
	}

	return leaders, nil
}

func (s *consumerService) Close() {
	s.logger.Info().Msg("Service closing")
	err := s.consumer.Close()
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Error while closing")
	}
	s.logger.Info().Msg("Service closed")
}

func (s *consumerService) getMetadata(ctx context.Context) (kafka.Topic, error) {
	resp, err := s.client.KafkaClient.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{s.canaryConfig.Topic},
	})
	if err != nil {
		return kafka.Topic{}, err
	}

	topic := resp.Topics[0]
	if topic.Error != nil {
		return kafka.Topic{}, err
	}

	return topic, nil
}
