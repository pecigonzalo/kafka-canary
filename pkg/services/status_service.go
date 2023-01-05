package services

import (
	"encoding/json"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/pecigonzalo/kafka-canary/pkg/canary"
	"github.com/pecigonzalo/kafka-canary/pkg/services/internal/util"
	"github.com/rs/zerolog"
)

// Status defines useful status related information
type Status struct {
	Consuming ConsumingStatus
}

// ConsumingStatus defines consuming related status information
type ConsumingStatus struct {
	TimeWindow time.Duration
	Percentage float64
}

type statusService struct {
	canaryConfig           *canary.Config
	producedRecordsSamples util.TimeWindowRing
	consumedRecordsSamples util.TimeWindowRing
	stop                   chan struct{}
	syncStop               sync.WaitGroup
	logger                 *zerolog.Logger
}

func NewStatusServiceService(canary canary.Config, logger *zerolog.Logger) StatusService {
	return &statusService{
		canaryConfig: &canary,
		logger:       logger,
	}
}

func (s *statusService) Open()  {}
func (s *statusService) Close() {}
func (s *statusService) StatusHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		status := Status{}

		// update consuming related status section
		status.Consuming = ConsumingStatus{
			TimeWindow: s.canaryConfig.StatusCheckInterval * time.Duration(s.consumedRecordsSamples.Count()),
		}
		consumedPercentage, err := s.consumedPercentage()
		if e, ok := err.(*util.ErrNoDataSamples); ok {
			status.Consuming.Percentage = -1
			s.logger.Error().Err(err).Msgf("Error processing consumed records percentage: %v", e)
		} else {
			status.Consuming.Percentage = consumedPercentage
		}

		json, _ := json.Marshal(status)
		rw.Header().Add("Content-Type", "application/json")
		rw.Write(json)
	})
}

// consumedPercentage function processes the percentage of consumed messages in the specified time window
func (s *statusService) consumedPercentage() (float64, error) {
	// sampling for produced (and consumed records) not done yet
	if s.producedRecordsSamples.IsEmpty() {
		return 0, &util.ErrNoDataSamples{}
	}

	// get number of records consumed and produced since the beginning of the time window (tail of ring buffers)
	consumed := s.consumedRecordsSamples.Head() - s.consumedRecordsSamples.Tail()
	produced := s.producedRecordsSamples.Head() - s.producedRecordsSamples.Tail()

	if produced == 0 {
		return 0, &util.ErrNoDataSamples{}
	}

	percentage := float64(consumed*100) / float64(produced)
	// rounding to two decimal digits
	percentage = math.Round(percentage*100) / 100
	s.logger.Info().Msgf("Status consumed percentage = %f", percentage)
	return percentage, nil
}
