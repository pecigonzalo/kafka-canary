package services

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
)

var (
	RecordsConsumedCounter uint64 = 0

	recordsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "records_consumed_total",
		Namespace: metricsNamespace,
		Help:      "The total number of records consumed",
	}, []string{"clientid", "partition"})

	recordsConsumerFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "consumer_error_total",
		Namespace: metricsNamespace,
		Help:      "Total number of errors reported by the consumer",
	}, []string{"clientid"})

	// it's defined when the service is created because buckets are configurable
	recordsEndToEndLatency *prometheus.HistogramVec

	// refreshConsumerMetadataError = promauto.NewCounterVec(prometheus.CounterOpts{
	// 	Name:      "consumer_refresh_metadata_error_total",
	// 	Namespace: metricsNamespace,
	// 	Help:      "Total number of errors while refreshing consumer metadata",
	// }, []string{"clientid"})
)

type consumerService struct {
	client          client.BrokerAdminClient
	consumer        kafka.Reader
	canaryConfig    *canary.Config
	connectorConfig client.ConnectorConfig
	// reference to the function for cancelling the Sarama consumer group context
	// in order to ending the session and allowing a rejoin with rebalancing
	cancel context.CancelFunc
	logger *zerolog.Logger
}

func NewConsumerService(canaryConfig canary.Config, connectorConfig client.ConnectorConfig, logger *zerolog.Logger) ConsumerService {
	recordsEndToEndLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "records_consumed_latency",
		Namespace: metricsNamespace,
		Help:      "Records end-to-end latency in milliseconds",
		Buckets:   canaryConfig.EndToEndLatencyBuckets,
	}, []string{"clientid", "partition"})

	ctx := context.Background()

	client, err := client.NewBrokerAdminClient(ctx, client.BrokerAdminClientConfig{
		ConnectorConfig: connectorConfig,
	}, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Error creating consumer service client")
	}
	logger.Info().Msg("Created consumer service client")

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     client.GetConnector().Config.BrokerAddrs,
		Dialer:      client.GetConnector().Dialer,
		GroupID:     canaryConfig.ConsumerGroupID,
		Topic:       canaryConfig.Topic,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.LastOffset,
	})
	logger.Info().Msg("Created consumer service reader")

	return &consumerService{
		client:          *client,
		consumer:        *consumer,
		canaryConfig:    &canaryConfig,
		connectorConfig: connectorConfig,
		logger:          logger,
	}
}

func (s *consumerService) Consume() {
	// creating new context with cancellation, for exiting Consume when metadata refresh is needed
	ctx, cancel := context.WithCancel(context.Background())
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
				} else {
					s.logger.Error().Err(err).Str("topic", s.canaryConfig.Topic).Msg("Error consuming topic")
					labels := prometheus.Labels{
						"clientid": s.canaryConfig.ClientID,
					}
					recordsConsumerFailed.With(labels).Inc()
				}
			}
			s.logger.Debug().Msg("Read canary message")

			if ctx.Err() != nil {
				s.logger.Info().Msg("Consumer Groups context cancelled")
				return
			}
			canaryMessage := NewCanaryMessage(message.Value)
			timestamp := time.Now().UnixMilli()
			duration := timestamp - canaryMessage.Timestamp
			labels := prometheus.Labels{
				"clientid":  s.canaryConfig.ClientID,
				"partition": strconv.Itoa(int(message.Partition)),
			}
			recordsEndToEndLatency.With(labels).Observe(float64(duration))
			recordsConsumed.With(labels).Inc()
			RecordsConsumedCounter++
			s.logger.Info().
				Int64("duration", duration).
				Int("partition", message.Partition).
				Int64("offset", message.Offset).
				Bytes("message", message.Value).
				Msg("Read message")
		}
	}()
}

func (s *consumerService) Refresh() {
	// TODO: Implement
	s.logger.Info().Msg("Producer refreshing metadata")
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
