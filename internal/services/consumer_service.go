package services

import (
	"context"
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
	RecordsConsumedCounter uint64 = 0

	recordsConsumed        *prometheus.CounterVec
	recordsConsumerFailed  *prometheus.CounterVec
	recordsEndToEndLatency *prometheus.HistogramVec
)

type consumerService struct {
	client          *client.Connector
	consumer        *kafka.Reader
	canaryConfig    *canary.Config
	connectorConfig *client.ConnectorConfig
	logger          *zerolog.Logger
	cancel          context.CancelFunc
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
		Brokers:               client.Config.BrokerAddrs,
		Dialer:                client.Dialer,
		GroupID:               canaryConfig.ConsumerGroupID,
		Topic:                 canaryConfig.Topic,
		StartOffset:           kafka.LastOffset,
		WatchPartitionChanges: true,
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
	ctx, s.cancel = context.WithCancel(ctx)
	go func() {
		for {
			message, err := s.consumer.FetchMessage(ctx)
			timestamp := time.Now().UnixMilli()
			s.logger.Debug().
				Int("partition", message.Partition).
				Int64("offset", message.Offset).
				Msg("Message read")
			if err != nil {
				partition := s.consumer.Config().Partition

				if client.IsDisconnection(err) {
					// These errors are recoverable, just try again
					s.logger.Warn().Err(err).Msgf(
						"Got connection error reading from partition %d, retrying: %+v",
						partition,
						err,
					)
					continue
				} else if ctx.Err() != nil {
					s.logger.Info().Msg("Context closed")
					return
				} else {
					recordsConsumerFailed.WithLabelValues().Inc()
					s.logger.Error().Err(err).Msg("Error consuming message")
					continue
				}
			}

			if err = s.consumer.CommitMessages(ctx, message); err != nil {
				s.logger.Error().Err(err).Msg("Error commiting message")
				continue
			}

			partitionString := strconv.Itoa(message.Partition)
			recordsConsumed.WithLabelValues(partitionString).Inc()

			canaryMessage, err := NewCanaryMessage(message.Value)
			if err != nil {
				recordsConsumerFailed.WithLabelValues().Inc()
				s.logger.Err(err).Msg("Error parsing canary message")
				continue
			}

			duration := timestamp - canaryMessage.Timestamp
			recordsEndToEndLatency.WithLabelValues(partitionString).Observe(float64(duration))

			s.logger.Info().
				Int64("duration", duration).
				Int("partition", message.Partition).
				Int64("offset", message.Offset).
				Msg("Message processed")

			if s.logger.Debug().Enabled() {
				s.logger.Debug().
					Int64("duration", duration).
					Int("partition", message.Partition).
					Int64("offset", message.Offset).
					Str("value", canaryMessage.String()).
					Msg("Message content")
			}
		}
	}()
}

func (s *consumerService) Close() {
	s.logger.Info().Msg("Service closing")
	s.cancel()
	err := s.consumer.Close()
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Error while closing")
	}
	s.logger.Info().Msg("Service closed")
}
