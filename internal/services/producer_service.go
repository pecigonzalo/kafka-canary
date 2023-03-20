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
	RecordsProducedCounter uint64 = 0

	recordsProduced              *prometheus.CounterVec
	recordsProducedFailed        *prometheus.CounterVec
	recordsProducedLatency       *prometheus.HistogramVec
	refreshProducerMetadataError *prometheus.CounterVec
)

type producerService struct {
	client          *client.Connector
	producer        *kafka.Writer
	canaryConfig    *canary.Config
	connectorConfig *client.ConnectorConfig
	logger          *zerolog.Logger
	// index of the next message to send
	index int
}

func NewProducerService(canaryConfig canary.Config, connectorConfig client.ConnectorConfig, logger *zerolog.Logger) ProducerService {
	recordsProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "records_produced_total",
		Namespace:   metricsNamespace,
		Help:        "The total number of records produced",
		ConstLabels: prometheus.Labels{"clientid": canaryConfig.ClientID},
	}, []string{"partition"})

	recordsProducedFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "records_produced_failed_total",
		Namespace:   metricsNamespace,
		Help:        "The total number of records failed to produce",
		ConstLabels: prometheus.Labels{"clientid": canaryConfig.ClientID},
	}, []string{"partition"})

	recordsProducedLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "records_produced_latency",
		Namespace:   metricsNamespace,
		Help:        "Records produced latency in milliseconds",
		Buckets:     canaryConfig.ProducerLatencyBuckets,
		ConstLabels: prometheus.Labels{"clientid": canaryConfig.ClientID},
	}, []string{"partition"})

	refreshProducerMetadataError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "producer_refresh_metadata_error_total",
		Namespace:   metricsNamespace,
		Help:        "Total number of errors while refreshing producer metadata",
		ConstLabels: prometheus.Labels{"clientid": canaryConfig.ClientID},
	}, nil)

	serviceLogger := logger.With().
		Str("canaryService", "producer").
		Str("topic", canaryConfig.Topic).
		Str("clientId", canaryConfig.ClientID).
		Logger()

	client, err := client.NewConnector(connectorConfig)
	if err != nil {
		serviceLogger.Fatal().Msg("Error creating producer service client")
	}
	serviceLogger.Info().Msg("Created producer service client")

	producer := &kafka.Writer{
		Addr:      kafka.TCP(client.Config.BrokerAddrs...),
		Transport: client.KafkaClient.Transport,
		Topic:     canaryConfig.Topic,
	}
	serviceLogger.Info().Msg("Created producer service writer")

	return &producerService{
		client:          client,
		producer:        producer,
		canaryConfig:    &canaryConfig,
		connectorConfig: &connectorConfig,
		logger:          &serviceLogger,
	}
}

func (s *producerService) Send(ctx context.Context, partitionAssignments []int) {
	numPartitions := len(partitionAssignments)
	for i := 0; i < numPartitions; i++ {
		value := s.newCanaryMessage()
		msg := kafka.Message{
			Partition: i,
			Value:     []byte(value.JSON()),
		}
		s.logger.Debug().
			Str("value", value.String()).
			Int("partition", i).
			Msg("Sending message")
		partitionString := strconv.Itoa(i)

		err := s.producer.WriteMessages(ctx, msg)
		timestamp := time.Now().UnixMilli()
		recordsProduced.WithLabelValues(partitionString).Inc()
		RecordsProducedCounter++

		if err != nil {
			s.logger.Warn().Msgf("Error sending message: %v", err)
			recordsProducedFailed.WithLabelValues(partitionString).Inc()
		} else {
			duration := timestamp - value.Timestamp
			s.logger.Info().
				Int("partition", i).
				Int64("duration", duration).
				Msg("Message sent")
			recordsProducedLatency.WithLabelValues(partitionString).Observe(float64(duration))
		}
	}
}

func (s *producerService) Refresh(ctx context.Context) {
	s.logger.Info().Msg("Producer refreshing metadata")
	_, err := s.getMetadata(ctx)
	if err != nil {
		refreshProducerMetadataError.WithLabelValues().Inc()
		s.logger.Error().Err(err).Msg("Error refreshing metadata")
	}
}

func (s *producerService) Close() {
	s.logger.Info().Msg("Closing producer")
	err := s.producer.Close()
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Error closing the kafka producer")
	}
	s.logger.Info().Msg("Producer closed")
}

func (s *producerService) getMetadata(ctx context.Context) (kafka.Topic, error) {
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
