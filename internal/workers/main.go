// Package workers defines an interface for canary workers and related implementations
package workers

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/services"
	"github.com/rs/zerolog"
)

// Worker interface exposing main operations on canary workers
type Worker interface {
	Start()
	Stop()
}

// CanaryManager defines the manager driving the different producer, consumer and topic services
type CanaryManager struct {
	canaryConfig      *canary.Config
	topicService      services.TopicService
	producerService   services.ProducerService
	consumerService   services.ConsumerService
	connectionService services.ConnectionService
	statusService     services.StatusService
	stop              chan struct{}
	syncStop          sync.WaitGroup
	logger            *zerolog.Logger
}

var (
//	expectedClusterSizeError = promauto.NewCounterVec(prometheus.CounterOpts{
//		Name:      "expected_cluster_size_error_total",
//		Namespace: "strimzi_canary",
//		Help:      "Total number of errors while waiting the Kafka cluster having the expected size",
//	}, nil)
)

// NewCanaryManager returns an instance of the cananry manager worker
func NewCanaryManager(canaryConfig canary.Config,
	topicService services.TopicService, producerService services.ProducerService,
	consumerService services.ConsumerService, connectionService services.ConnectionService,
	statusService services.StatusService, logger *zerolog.Logger) Worker {
	cm := CanaryManager{
		canaryConfig:      &canaryConfig,
		topicService:      topicService,
		producerService:   producerService,
		consumerService:   consumerService,
		connectionService: connectionService,
		statusService:     statusService,
		logger:            logger,
	}
	return &cm
}

// Start runs a first reconcile and start a timer for periodic reconciling
func (cm *CanaryManager) Start() {
	cm.logger.Info().Msg("Starting canary manager")

	cm.stop = make(chan struct{})
	cm.syncStop.Add(1)

	cm.connectionService.Open()
	cm.statusService.Open()

	result, err := cm.topicService.Reconcile()
	if err != nil {
		cm.logger.Fatal().Err(err).Msg("Error starting canary manager")
	}
	cm.logger.Info().Msg("Consume and produce")
	// consumer will subscribe to the topic so all partitions (even if we have less brokers)
	cm.consumerService.Consume()
	// producer has to send to partitions assigned to brokers
	cm.producerService.Send(result.Assignments)

	cm.logger.Info().Dur("interval", cm.canaryConfig.ReconcileInterval).Msg("Running reconciliation loop")
	ticker := time.NewTicker(cm.canaryConfig.ReconcileInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				cm.reconcile()
			case <-cm.stop:
				ticker.Stop()
				defer cm.syncStop.Done()
				cm.logger.Info().Msg("Stopping canary manager reconcile loop")
				return
			}
		}
	}()
}

// Stop stops the services and the reconcile timer
func (cm *CanaryManager) Stop() {
	cm.logger.Info().Msg("Stopping canary manager")

	// ask to stop the ticker reconcile loop and wait
	close(cm.stop)
	cm.syncStop.Wait()

	cm.producerService.Close()
	cm.consumerService.Close()
	cm.topicService.Close()
	cm.connectionService.Close()
	cm.statusService.Close()

	cm.logger.Info().Msg("Canary manager closed")
}

func (cm *CanaryManager) reconcile() {
	cm.logger.Info().Msg("Canary manager reconcile")

	if result, err := cm.topicService.Reconcile(); err == nil {
		if result.RefreshProducerMetadata {
			cm.producerService.Refresh()
		}

		leaders, err := cm.consumerService.Leaders(context.Background())
		if err != nil || !reflect.DeepEqual(result.Leaders, leaders) {
			cm.consumerService.Refresh()
		}
		// producer has to send to partitions assigned to brokers
		cm.producerService.Send(result.Assignments)
	}
}
