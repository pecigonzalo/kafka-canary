package client

import (
	"context"
	"errors"
	"testing"

	"github.com/pecigonzalo/kafka-canary/internal/client/mocks"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewBrokerAdmin(t *testing.T) {
	type args struct {
		config ConnectorConfig
	}
	tests := []struct {
		name      string
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:      "Default",
			args:      args{ConnectorConfig{}},
			assertion: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewBrokerAdmin(tt.args.config)
			tt.assertion(t, err)
		})
	}
}

func TestBrokerAdmin_GetTopic(t *testing.T) {
	mockKafkaAdminClient := mocks.NewKafkaAdminClient(t)

	mockKafkaAdminClient.
		On("Metadata", mock.Anything, mock.Anything).
		Return(&kafka.MetadataResponse{
			Topics:  mockGetTopics(),
			Brokers: mockGetBrokers(),
		}, nil)

	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      TopicInfo
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:   "Default",
			fields: fields{client: mockKafkaAdminClient},
			args:   args{context.Background(), mockTopicName},
			want: TopicInfo{Name: mockTopicName, Partitions: []PartitionAssignment{
				{ID: 0, Replicas: []int{0, 1, 2}},
				{ID: 1, Replicas: []int{0, 1, 2}},
				{ID: 2, Replicas: []int{0, 1, 2}},
			}},
			assertion: assert.NoError,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			got, err := c.GetTopic(tt.args.ctx, tt.args.name)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBrokerAdmin_CreateTopic(t *testing.T) {
	mockKafkaAdminClient := mocks.NewKafkaAdminClient(t)
	mockKafkaAdminClientWithError := mocks.NewKafkaAdminClient(t)

	mockKafkaAdminClient.
		On("CreateTopics", mock.Anything, mock.Anything).
		Return(&kafka.CreateTopicsResponse{}, nil)

	mockKafkaAdminClientWithError.
		On("CreateTopics", mock.Anything, mock.Anything).
		Return(&kafka.CreateTopicsResponse{
			Errors: map[string]error{mockTopicName: errors.New("Some error")},
		}, nil)

	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx         context.Context
		name        string
		assignments []PartitionAssignment
		configs     map[string]string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:   "Default",
			fields: fields{client: mockKafkaAdminClient},
			args: args{context.Background(), mockTopicName,
				[]PartitionAssignment{
					{ID: 0, Replicas: []int{0, 1, 2}},
				},
				map[string]string{},
			},
			assertion: assert.NoError,
		},
		{
			name:      "CatchResponseError",
			fields:    fields{client: mockKafkaAdminClientWithError},
			args:      args{context.Background(), mockTopicName, []PartitionAssignment{}, map[string]string{}},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			tt.assertion(t, c.CreateTopic(tt.args.ctx, tt.args.name, tt.args.assignments, tt.args.configs))
		})
	}
}

func TestBrokerAdmin_UpdateTopicConfig(t *testing.T) {
	mockKafkaAdminClient := mocks.NewKafkaAdminClient(t)
	mockKafkaAdminClientWithError := mocks.NewKafkaAdminClient(t)

	mockKafkaAdminClient.
		On("IncrementalAlterConfigs", mock.Anything, mock.Anything).
		Return(&kafka.IncrementalAlterConfigsResponse{}, nil)

	mockKafkaAdminClientWithError.
		On("IncrementalAlterConfigs", mock.Anything, mock.Anything).
		Return(&kafka.IncrementalAlterConfigsResponse{
			Resources: []kafka.IncrementalAlterConfigsResponseResource{
				{Error: errors.New("Some error")},
			},
		}, nil)

	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx     context.Context
		name    string
		configs map[string]string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:   "Default",
			fields: fields{client: mockKafkaAdminClient},
			args: args{context.Background(), mockTopicName, map[string]string{
				"this": "that",
			}},
			assertion: assert.NoError,
		},
		{
			name:   "CatchResponseError",
			fields: fields{client: mockKafkaAdminClientWithError},
			args: args{context.Background(), mockTopicName, map[string]string{
				"this": "that",
			}},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			tt.assertion(t, c.UpdateTopicConfig(tt.args.ctx, tt.args.name, tt.args.configs))
		})
	}
}

func TestBrokerAdmin_GetBrokers(t *testing.T) {
	mockKafkaAdminClient := mocks.NewKafkaAdminClient(t)

	mockKafkaAdminClient.
		On("Metadata", mock.Anything, mock.Anything).
		Return(&kafka.MetadataResponse{
			Topics:  mockGetTopics(),
			Brokers: mockGetBrokers(),
		}, nil)

	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      []BrokerInfo
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:   "Default",
			fields: fields{client: mockKafkaAdminClient},
			args:   args{context.Background()},
			want: []BrokerInfo{
				{ID: 0, Rack: "rack1"},
				{ID: 1, Rack: "rack2"},
				{ID: 2, Rack: "rack3"},
			},
			assertion: assert.NoError,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			got, err := c.GetBrokers(tt.args.ctx)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBrokerAdmin_AddParitions(t *testing.T) {
	mockKafkaAdminClient := mocks.NewKafkaAdminClient(t)
	mockKafkaAdminClientWithError := mocks.NewKafkaAdminClient(t)

	mockKafkaAdminClient.
		On("Metadata", mock.Anything, mock.Anything).
		Return(&kafka.MetadataResponse{
			Topics:  mockGetTopics(),
			Brokers: mockGetBrokers(),
		}, nil)

	mockKafkaAdminClient.
		On("CreatePartitions", mock.Anything, mock.Anything).
		Return(&kafka.CreatePartitionsResponse{}, nil)

	mockKafkaAdminClientWithError.
		On("Metadata", mock.Anything, mock.Anything).
		Return(&kafka.MetadataResponse{
			Topics:  mockGetTopics(),
			Brokers: mockGetBrokers(),
		}, nil)

	mockKafkaAdminClientWithError.
		On("CreatePartitions", mock.Anything, mock.Anything).
		Return(
			&kafka.CreatePartitionsResponse{
				Errors: map[string]error{mockTopicName: errors.New("Some error")},
			}, nil,
		)

	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx         context.Context
		name        string
		assignments []PartitionAssignment
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:   "Default",
			fields: fields{client: mockKafkaAdminClient},
			args: args{context.Background(), mockTopicName, []PartitionAssignment{
				{ID: 0, Replicas: []int{0, 1, 2}},
			}},
			assertion: assert.NoError,
		},
		{
			name:      "CatchResponseError",
			fields:    fields{client: mockKafkaAdminClientWithError},
			args:      args{context.Background(), mockTopicName, []PartitionAssignment{}},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			tt.assertion(t, c.AddParitions(tt.args.ctx, tt.args.name, tt.args.assignments))
		})
	}
}

func TestBrokerAdmin_AssignPartitions(t *testing.T) {
	mockKafkaAdminClient := mocks.NewKafkaAdminClient(t)
	mockKafkaAdminClientWithError := mocks.NewKafkaAdminClient(t)

	mockKafkaAdminClient.
		On("AlterPartitionReassignments", mock.Anything, mock.Anything).
		Return(&kafka.AlterPartitionReassignmentsResponse{}, nil)

	mockKafkaAdminClientWithError.
		On("AlterPartitionReassignments", mock.Anything, mock.Anything).
		Return(&kafka.AlterPartitionReassignmentsResponse{Error: errors.New("Some error")}, nil)

	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx         context.Context
		name        string
		assignments []PartitionAssignment
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:   "Default",
			fields: fields{client: mockKafkaAdminClient},
			args: args{context.Background(), mockTopicName, []PartitionAssignment{
				{ID: 0, Replicas: []int{0, 1, 2}},
			}},
			assertion: assert.NoError,
		},
		{
			name:      "CatchResponseError",
			fields:    fields{client: mockKafkaAdminClientWithError},
			args:      args{context.Background(), mockTopicName, []PartitionAssignment{}},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			tt.assertion(t, c.AssignPartitions(tt.args.ctx, tt.args.name, tt.args.assignments))
		})
	}
}

func TestBrokerAdmin_RunLeaderElection(t *testing.T) {
	mockKafkaAdminClient := mocks.NewKafkaAdminClient(t)
	mockKafkaAdminClientWithError := mocks.NewKafkaAdminClient(t)

	mockKafkaAdminClient.
		On("ElectLeaders", mock.Anything, mock.Anything).
		Return(&kafka.ElectLeadersResponse{}, nil)

	mockKafkaAdminClientWithError.
		On("ElectLeaders", mock.Anything, mock.Anything).
		Return(&kafka.ElectLeadersResponse{Error: errors.New("Some error")}, nil)

	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx        context.Context
		name       string
		partitions []int
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:      "Default",
			fields:    fields{client: mockKafkaAdminClient},
			args:      args{context.Background(), mockTopicName, []int{0, 1, 2}},
			assertion: assert.NoError,
		},
		{
			name:      "CatchResponseError",
			fields:    fields{client: mockKafkaAdminClientWithError},
			args:      args{context.Background(), mockTopicName, []int{0, 1, 2}},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			tt.assertion(t, c.RunLeaderElection(tt.args.ctx, tt.args.name, tt.args.partitions))
		})
	}
}
