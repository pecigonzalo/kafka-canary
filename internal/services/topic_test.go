package services

import (
	"context"
	"errors"
	"testing"

	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
	"github.com/pecigonzalo/kafka-canary/internal/services/mocks"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test_topicServiceMetrics tests the service expected metrics
// we can change the mocks freely, but not the test expectations
func Test_topicServiceMetrics(t *testing.T) {
	mockTopic := "fake-topic"

	topicCreationFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_creation_failed_total",
	}, nil)
	describeClusterError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_describe_cluster_error_total",
	}, nil)
	describeTopicError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_describe_error_total",
	}, nil)
	alterTopicAssignmentsError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_alter_assignments_error_total",
	}, nil)
	alterTopicConfigurationError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_alter_configuration_error_total",
	}, nil)

	type fields struct {
		initialized  bool
		admin        *mocks.Admin
		canaryConfig *canary.Config
		logger       *zerolog.Logger
	}
	type args struct {
		ctx context.Context
	}
	type metrics struct {
		name   string
		metric *prometheus.CounterVec
		value  int
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		mocks   func(t *testing.T) *mocks.Admin
		metrics []metrics
	}{
		{
			name: "NoErrors",
			fields: fields{
				initialized:  false,
				canaryConfig: &canary.Config{},
				logger:       &zerolog.Logger{},
			},
			args: args{context.Background()},
			mocks: func(t *testing.T) *mocks.Admin {
				m := mocks.NewAdmin(t)

				m.On("GetBrokers", mock.Anything).
					Return([]client.BrokerInfo{
						{ID: 1, Rack: "rack1"},
					}, nil)

				m.On("GetTopic", mock.Anything, mock.Anything).
					Return(client.TopicInfo{
						Name: mockTopic,
					}, nil)

				m.On("UpdateTopicConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil)

				m.On("AssignPartitions", mock.Anything, mock.Anything, mock.Anything).Return(nil)

				m.On("AddPartitions", mock.Anything, mock.Anything, mock.Anything).Return(nil)

				m.On("RunLeaderElection", mock.Anything, mock.Anything, mock.Anything).Return(nil)

				m.On("GetTopic", mock.Anything, mock.Anything).
					Return(client.TopicInfo{
						Name: mockTopic,
					}, nil)

				return m
			},
			metrics: []metrics{
				{name: "topicCreationFailed", metric: topicCreationFailed, value: 0},
				{name: "describeClusterError", metric: describeClusterError, value: 0},
				{name: "describeTopicError", metric: describeTopicError, value: 0},
				{name: "alterTopicAssignmentsError", metric: alterTopicAssignmentsError, value: 0},
				{name: "alterTopicConfigurationError", metric: alterTopicConfigurationError, value: 0},
			},
		},
		{
			name: "TopicCreationFailed",
			fields: fields{
				initialized:  false,
				canaryConfig: &canary.Config{},
				logger:       &zerolog.Logger{},
			},
			args: args{context.Background()},
			mocks: func(t *testing.T) *mocks.Admin {
				m := mocks.NewAdmin(t)

				m.On("GetBrokers", mock.Anything).
					Return([]client.BrokerInfo{
						{ID: 1, Rack: "rack1"},
					}, nil)

				m.On("GetTopic", mock.Anything, mock.Anything).
					Return(client.TopicInfo{}, kafka.UnknownTopicOrPartition)

				m.On("CreateTopic", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(errors.New("Some error"))

				return m
			},
			metrics: []metrics{
				{name: "topicCreationFailed", metric: topicCreationFailed, value: 1},
				{name: "describeClusterError", metric: describeClusterError, value: 0},
				{name: "describeTopicError", metric: describeTopicError, value: 1},
				{name: "alterTopicAssignmentsError", metric: alterTopicAssignmentsError, value: 0},
				{name: "alterTopicConfigurationError", metric: alterTopicConfigurationError, value: 0},
			},
		},
		{
			name: "DescribeClusterError",
			fields: fields{
				initialized:  false,
				canaryConfig: &canary.Config{},
				logger:       &zerolog.Logger{},
			},
			args: args{context.Background()},
			mocks: func(t *testing.T) *mocks.Admin {
				m := mocks.NewAdmin(t)
				m.On("GetBrokers", mock.Anything).
					Return([]client.BrokerInfo{
						{ID: 1, Rack: "rack1"},
					}, errors.New("Some error"))

				return m
			},
			metrics: []metrics{
				{name: "topicCreationFailed", metric: topicCreationFailed, value: 0},
				{name: "describeClusterError", metric: describeClusterError, value: 1},
				{name: "describeTopicError", metric: describeTopicError, value: 0},
				{name: "alterTopicAssignmentsError", metric: alterTopicAssignmentsError, value: 0},
				{name: "alterTopicConfigurationError", metric: alterTopicConfigurationError, value: 0},
			},
		},
		{
			name: "DescribeTopicError",
			fields: fields{
				initialized:  false,
				canaryConfig: &canary.Config{},
				logger:       &zerolog.Logger{},
			},
			args: args{context.Background()},
			mocks: func(t *testing.T) *mocks.Admin {
				m := mocks.NewAdmin(t)
				m.On("GetBrokers", mock.Anything).
					Return([]client.BrokerInfo{
						{ID: 1, Rack: "rack1"},
					}, nil)

				m.On("GetTopic", mock.Anything, mock.Anything).
					Return(client.TopicInfo{
						Name: mockTopic,
					}, errors.New("Some error"))
				return m
			},
			metrics: []metrics{
				{name: "topicCreationFailed", metric: topicCreationFailed, value: 0},
				{name: "describeClusterError", metric: describeClusterError, value: 0},
				{name: "describeTopicError", metric: describeTopicError, value: 1},
				{name: "alterTopicAssignmentsError", metric: alterTopicAssignmentsError, value: 0},
				{name: "alterTopicConfigurationError", metric: alterTopicConfigurationError, value: 0},
			},
		},
		{
			name: "AlterTopicAssignmentError",
			fields: fields{
				initialized:  true,
				canaryConfig: &canary.Config{},
				logger:       &zerolog.Logger{},
			},
			args: args{context.Background()},
			mocks: func(t *testing.T) *mocks.Admin {
				m := mocks.NewAdmin(t)

				m.On("GetBrokers", mock.Anything).
					Return([]client.BrokerInfo{
						{ID: 1, Rack: "rack1"},
						{ID: 2, Rack: "rack2"},
					}, nil)

				m.On("GetTopic", mock.Anything, mock.Anything).
					Return(client.TopicInfo{
						Name: mockTopic,
						Partitions: []client.PartitionAssignment{{
							ID:       1,
							Leader:   0,
							Replicas: []int{},
						}},
					}, nil)

				m.On("AssignPartitions", mock.Anything, mock.Anything, mock.Anything).
					Return(errors.New("Some error"))

				return m
			},
			metrics: []metrics{
				{name: "topicCreationFailed", metric: topicCreationFailed, value: 0},
				{name: "describeClusterError", metric: describeClusterError, value: 0},
				{name: "describeTopicError", metric: describeTopicError, value: 0},
				{name: "alterTopicAssignmentsError", metric: alterTopicAssignmentsError, value: 1},
				{name: "alterTopicConfigurationError", metric: alterTopicConfigurationError, value: 0},
			},
		},
		{
			name: "AlterTopicAssignmentError",
			fields: fields{
				initialized:  false,
				canaryConfig: &canary.Config{},
				logger:       &zerolog.Logger{},
			},
			args: args{context.Background()},
			mocks: func(t *testing.T) *mocks.Admin {
				m := mocks.NewAdmin(t)

				m.On("GetBrokers", mock.Anything).
					Return([]client.BrokerInfo{
						{ID: 1, Rack: "rack1"},
						{ID: 2, Rack: "rack2"},
					}, nil)

				m.On("GetTopic", mock.Anything, mock.Anything).
					Return(client.TopicInfo{
						Name: mockTopic,
						Partitions: []client.PartitionAssignment{{
							ID:       1,
							Leader:   0,
							Replicas: []int{},
						}},
					}, nil)

				m.On("UpdateTopicConfig", mock.Anything, mock.Anything, mock.Anything).
					Return(errors.New("Some error"))

				return m
			},
			metrics: []metrics{
				{name: "topicCreationFailed", metric: topicCreationFailed, value: 0},
				{name: "describeClusterError", metric: describeClusterError, value: 0},
				{name: "describeTopicError", metric: describeTopicError, value: 0},
				{name: "alterTopicAssignmentsError", metric: alterTopicAssignmentsError, value: 0},
				{name: "alterTopicConfigurationError", metric: alterTopicConfigurationError, value: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create all mock calls
			tt.fields.admin = tt.mocks(t)

			s := &topicService{
				initialized:  tt.fields.initialized,
				admin:        tt.fields.admin,
				canaryConfig: tt.fields.canaryConfig,
				logger:       tt.fields.logger,
			}

			_, _ = s.Reconcile(tt.args.ctx)
			for _, m := range tt.metrics {
				t.Run(m.name, func(t *testing.T) {
					count := testutil.CollectAndCount(m.metric)

					if m.value == 0 {
						assert.Equal(
							t, 0,
							count, "Unexpected number of metric vectors",
						)
						return
					}

					assert.Equal(
						t, 1,
						count, "Unexpected number of metric vectors",
					)

					assert.Equal(t,
						float64(m.value),
						testutil.ToFloat64(m.metric),
					)

					// Reset metrics after each loop
					m.metric.Reset()
				})
			}
		})
	}

}
