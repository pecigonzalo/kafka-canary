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
	client          *client.BrokerAdminClient
	consumer        *kafka.Reader
	canaryConfig    *canary.Config
	connectorConfig client.ConnectorConfig
	// reference to the function for cancelling the consumer group context
	// in order to ending the session and allowing a rejoin with rebalancing
	cancel context.CancelFunc
	logger *zerolog.Logger
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
	}, []string{})

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
	}, []string{})

	// NOTE: Shuold we get this context from the caller?
	ctx := context.Background()

	serviceLogger := logger.With().
		Str("topic", canaryConfig.Topic).
		Str("consumerGroup", canaryConfig.ConsumerGroupID).
		Logger()

	client, err := client.NewBrokerAdminClient(ctx, client.BrokerAdminClientConfig{
		ConnectorConfig: connectorConfig,
	}, &serviceLogger)
	if err != nil {
		serviceLogger.Fatal().Err(err).Msg("Error creating consumer service client")
	}
	serviceLogger.Info().Msg("Created consumer service client")

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     client.GetConnector().Config.BrokerAddrs,
		Dialer:      client.GetConnector().Dialer,
		GroupID:     canaryConfig.ConsumerGroupID,
		Topic:       canaryConfig.Topic,
		StartOffset: kafka.LastOffset,
	})
	logger.Info().Msg("Created consumer service reader")

	return &consumerService{
		client:          client,
		consumer:        consumer,
		canaryConfig:    &canaryConfig,
		connectorConfig: connectorConfig,
		logger:          &serviceLogger,
	}
}

func (s *consumerService) Consume(ctx context.Context) {
	// creating new context with cancellation, for exiting Consume when metadata refresh is needed
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
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
					s.logger.Info().Msg("Consumer context cancelled")
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
	s.logger.Info().Msg("Consumer refreshing metadata")
	resp, err := s.client.GetConnector().KafkaClient.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{s.canaryConfig.Topic},
	})
	if err != nil {
		refreshConsumerMetadataError.WithLabelValues().Inc()
		s.logger.Error().Err(err).Msg("Error freshing metadata in consumer")
	}
	for _, topic := range resp.Topics {
		if topic.Error != nil {
			refreshConsumerMetadataError.WithLabelValues().Inc()
			s.logger.Error().Err(topic.Error).Msg("Error freshing metadata in consumer")
		}
	}
}

func (s *consumerService) Leaders(ctx context.Context) (map[int]int, error) {
	topic, err := s.client.GetTopic(ctx, s.canaryConfig.Topic, false)
	if err != nil {
		return map[int]int{}, nil
	}

	leaders := make(map[int]int, len(topic.Partitions))

	for _, p := range topic.Partitions {
		leaders[p.ID] = p.Leader
	}

	return leaders, nil
}

func (s *consumerService) Close() {
	s.logger.Info().Msg("Closing consumer")
	s.cancel()
	err := s.consumer.Close()
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Error closing the kafka consumer")
	}
	s.logger.Info().Msg("Consumer closed")
}
