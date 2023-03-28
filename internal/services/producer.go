package services

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
)

var (
	recordsProduced        *prometheus.CounterVec
	recordsProducedFailed  *prometheus.CounterVec
	recordsProducedLatency *prometheus.HistogramVec
)

type ProducerService interface {
	Send(context.Context, []int)
	Close()
}

type producerService struct {
	client       client.Producer
	canaryConfig *canary.Config
	logger       *zerolog.Logger
	// index of the next message to send
	index int
}

func NewProducerService(client client.Producer, canaryConfig canary.Config, logger *zerolog.Logger) ProducerService {
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

	serviceLogger := logger.With().
		Str("canaryService", "producer").
		Str("topic", canaryConfig.Topic).
		Str("clientId", canaryConfig.ClientID).
		Logger()

	serviceLogger.Info().Msg("Created producer service writer")

	return &producerService{
		client:       client,
		canaryConfig: &canaryConfig,
		logger:       &serviceLogger,
	}
}

func (s *producerService) Send(ctx context.Context, partitionAssignments []int) {
	numPartitions := len(partitionAssignments)
	for i := 0; i < numPartitions; i++ {
		go func(i int) {
			value := s.newCanaryMessage()
			msg := client.Message{
				Partition: i,
				Value:     []byte(value.JSON()),
			}
			s.logger.Debug().
				Str("value", value.String()).
				Int("partition", i).
				Msg("Sending message")
			partitionString := strconv.Itoa(i)

			err := s.client.Write(ctx, msg)
			timestamp := time.Now().UnixMilli()
			recordsProduced.WithLabelValues(partitionString).Inc()

			if err != nil {
				recordsProducedFailed.WithLabelValues(partitionString).Inc()
				s.logger.Warn().Msgf("Error sending message: %v", err)
			} else {
				duration := timestamp - value.Timestamp
				recordsProducedLatency.WithLabelValues(partitionString).Observe(float64(duration))
				s.logger.Info().
					Int("partition", i).
					Int64("duration", duration).
					Msg("Message sent")
			}
		}(i)
	}
}

func (s *producerService) Close() {
	s.logger.Info().Msg("Service closing")
	err := s.client.Close()
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Error closing the kafka producer")
	}
	s.logger.Info().Msg("Service closed")
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
