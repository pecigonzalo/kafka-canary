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
	RecordsConsumedCounter uint64 = 0

	recordsConsumed        *prometheus.CounterVec
	recordsConsumerFailed  *prometheus.CounterVec
	recordsEndToEndLatency *prometheus.HistogramVec
)

type ConsumerService interface {
	Consume(context.Context)
	Close()
}

type consumerService struct {
	client       client.Consumer
	canaryConfig *canary.Config
	logger       *zerolog.Logger
	cancel       context.CancelFunc
}

func NewConsumerService(client client.Consumer, canaryConfig canary.Config, logger *zerolog.Logger) ConsumerService {
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

	return &consumerService{
		client:       client,
		canaryConfig: &canaryConfig,
		logger:       &serviceLogger,
	}
}

func (s *consumerService) Consume(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	go func() {
		for {
			message, err := s.client.Fetch(ctx)
			timestamp := time.Now().UnixMilli()
			s.logger.Debug().
				Int("partition", message.Partition).
				Int64("offset", message.Offset).
				Msg("Message read")
			if err != nil {
				if client.IsDisconnection(err) {
					// These errors are recoverable, just try again
					s.logger.Warn().Err(err).Msgf(
						"Got connection error reading message retrying: %+v",
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

			if err = s.client.Commit(ctx, message); err != nil {
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
	err := s.client.Close()
	if err != nil {
		s.logger.Fatal().Err(err).Msg("Error while closing")
	}
	s.logger.Info().Msg("Service closed")
}
