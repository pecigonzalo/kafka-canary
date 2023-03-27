package client

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
)

const (
	mockTopicName = "fake-topic"
)

func getMockBrokers() []kafka.Broker {
	return []kafka.Broker{
		{ID: 0, Rack: "rack1"},
		{ID: 1, Rack: "rack2"},
		{ID: 2, Rack: "rack3"},
	}
}

func getMockTopics() []kafka.Topic {
	return []kafka.Topic{
		{
			Name: mockTopicName,
			Partitions: []kafka.Partition{
				{Topic: mockTopicName, ID: 0, Replicas: getMockBrokers()},
				{Topic: mockTopicName, ID: 1, Replicas: getMockBrokers()},
				{Topic: mockTopicName, ID: 2, Replicas: getMockBrokers()},
			},
		},
	}
}

// TODO: Can we make the mock more DRY?
type mockKafkaClient struct {
	err                                 error
	metadataResponse                    *kafka.MetadataResponse
	createTopicsResponse                *kafka.CreateTopicsResponse
	incrementalAlterConfigsResponse     *kafka.IncrementalAlterConfigsResponse
	createPartitionsResponse            *kafka.CreatePartitionsResponse
	alterPartitionReassignmentsResponse *kafka.AlterPartitionReassignmentsResponse
	electLeadersResponse                *kafka.ElectLeadersResponse
}

func newMockKafkaClient(err error, metadataResponse *kafka.MetadataResponse) *mockKafkaClient {
	mock := &mockKafkaClient{
		err: nil,
		metadataResponse: &kafka.MetadataResponse{
			Topics:  getMockTopics(),
			Brokers: getMockBrokers(),
		},
		createTopicsResponse: &kafka.CreateTopicsResponse{
			Errors: map[string]error{},
		},
		incrementalAlterConfigsResponse: &kafka.IncrementalAlterConfigsResponse{
			Resources: []kafka.IncrementalAlterConfigsResponseResource{
				{
					ResourceName: mockTopicName,
					ResourceType: kafka.ResourceTypeTopic,
				},
			},
		},
		createPartitionsResponse:            &kafka.CreatePartitionsResponse{},
		alterPartitionReassignmentsResponse: &kafka.AlterPartitionReassignmentsResponse{},
		electLeadersResponse:                &kafka.ElectLeadersResponse{},
	}

	if err != nil {
		mock.err = err
	}

	if metadataResponse != nil {
		mock.metadataResponse = metadataResponse
	}

	return mock
}

func (m *mockKafkaClient) Metadata(ctx context.Context, req *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	if m.err != nil {
		return &kafka.MetadataResponse{}, m.err
	}
	return m.metadataResponse, nil
}

func (m *mockKafkaClient) CreateTopics(ctx context.Context, req *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error) {
	if m.err != nil {
		return &kafka.CreateTopicsResponse{}, m.err
	}
	return m.createTopicsResponse, nil
}

func (m *mockKafkaClient) IncrementalAlterConfigs(ctx context.Context, req *kafka.IncrementalAlterConfigsRequest) (*kafka.IncrementalAlterConfigsResponse, error) {
	if m.err != nil {
		return &kafka.IncrementalAlterConfigsResponse{}, m.err
	}
	return m.incrementalAlterConfigsResponse, nil
}

func (m *mockKafkaClient) CreatePartitions(ctx context.Context, req *kafka.CreatePartitionsRequest) (*kafka.CreatePartitionsResponse, error) {
	if m.err != nil {
		return &kafka.CreatePartitionsResponse{}, m.err
	}
	return m.createPartitionsResponse, nil
}

func (m *mockKafkaClient) AlterPartitionReassignments(ctx context.Context, req *kafka.AlterPartitionReassignmentsRequest) (*kafka.AlterPartitionReassignmentsResponse, error) {
	if m.err != nil {
		return &kafka.AlterPartitionReassignmentsResponse{}, m.err
	}
	return m.alterPartitionReassignmentsResponse, nil
}

func (m *mockKafkaClient) ElectLeaders(ctx context.Context, req *kafka.ElectLeadersRequest) (*kafka.ElectLeadersResponse, error) {
	if m.err != nil {
		return &kafka.ElectLeadersResponse{}, m.err
	}
	return m.electLeadersResponse, nil
}

func TestNewBrokerAdmin(t *testing.T) {
	type args struct {
		config ConnectorConfig
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"Default", args{ConnectorConfig{}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewBrokerAdmin(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBrokerAdmin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestBrokerAdmin_GetTopic(t *testing.T) {
	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    TopicInfo
		wantErr bool
	}{
		{
			"Default",
			fields{client: newMockKafkaClient(nil, nil)},
			args{context.Background(), mockTopicName},
			TopicInfo{Name: mockTopicName, Partitions: []PartitionAssignment{
				{ID: 0, Replicas: []int{0, 1, 2}},
				{ID: 1, Replicas: []int{0, 1, 2}},
				{ID: 2, Replicas: []int{0, 1, 2}},
			}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			got, err := c.GetTopic(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("BrokerAdmin.GetTopic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BrokerAdmin.GetTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBrokerAdmin_CreateTopic(t *testing.T) {
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
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Default",
			fields{client: newMockKafkaClient(nil, nil)},
			args{context.Background(), mockTopicName, []PartitionAssignment{{ID: 0, Replicas: []int{0, 1, 2}}}, map[string]string{}},
			false,
		},
		{
			"CatchResponseError",
			fields{client: &mockKafkaClient{
				createTopicsResponse: &kafka.CreateTopicsResponse{Errors: map[string]error{
					mockTopicName: errors.New("Some error"),
				}},
			}},
			args{context.Background(), mockTopicName, []PartitionAssignment{}, map[string]string{}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			if err := c.CreateTopic(tt.args.ctx, tt.args.name, tt.args.assignments, tt.args.configs); (err != nil) != tt.wantErr {
				t.Errorf("BrokerAdmin.CreateTopic() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBrokerAdmin_UpdateTopicConfig(t *testing.T) {
	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx     context.Context
		name    string
		configs map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Default",
			fields{client: newMockKafkaClient(nil, nil)},
			args{context.Background(), mockTopicName, map[string]string{
				"this": "that",
			}},
			false,
		},
		{
			"CatchResponseError",
			fields{client: &mockKafkaClient{
				incrementalAlterConfigsResponse: &kafka.IncrementalAlterConfigsResponse{
					Resources: []kafka.IncrementalAlterConfigsResponseResource{
						{Error: errors.New("Some error")},
					},
				},
			}},
			args{context.Background(), mockTopicName, map[string]string{
				"this": "that",
			}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			if err := c.UpdateTopicConfig(tt.args.ctx, tt.args.name, tt.args.configs); (err != nil) != tt.wantErr {
				t.Errorf("BrokerAdmin.UpdateTopicConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBrokerAdmin_GetBrokers(t *testing.T) {
	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []BrokerInfo
		wantErr bool
	}{
		{
			"Default",
			fields{client: newMockKafkaClient(nil, nil)},
			args{context.Background()},
			[]BrokerInfo{
				{ID: 0, Rack: "rack1"},
				{ID: 1, Rack: "rack2"},
				{ID: 2, Rack: "rack3"},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			got, err := c.GetBrokers(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("BrokerAdmin.GetBrokers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BrokerAdmin.GetBrokers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBrokerAdmin_AddParitions(t *testing.T) {
	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx         context.Context
		name        string
		assignments []PartitionAssignment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Default",
			fields{client: newMockKafkaClient(nil, nil)},
			args{context.Background(), mockTopicName, []PartitionAssignment{
				{ID: 0, Replicas: []int{0, 1, 2}},
			}},
			false,
		},
		{
			"CatchResponseError",
			fields{client: &mockKafkaClient{
				metadataResponse: &kafka.MetadataResponse{
					Topics:  getMockTopics(),
					Brokers: getMockBrokers(),
				},
				createPartitionsResponse: &kafka.CreatePartitionsResponse{
					Errors: map[string]error{
						mockTopicName: errors.New("Some error"),
					},
				},
			}},
			args{context.Background(), mockTopicName, []PartitionAssignment{}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			if err := c.AddParitions(tt.args.ctx, tt.args.name, tt.args.assignments); (err != nil) != tt.wantErr {
				t.Errorf("BrokerAdmin.AddParitions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBrokerAdmin_AssignPartitions(t *testing.T) {
	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx         context.Context
		name        string
		assignments []PartitionAssignment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Default",
			fields{client: newMockKafkaClient(nil, nil)},
			args{context.Background(), mockTopicName, []PartitionAssignment{
				{ID: 0, Replicas: []int{0, 1, 2}},
			}},
			false,
		},
		{
			"CatchResponseError",
			fields{client: &mockKafkaClient{
				metadataResponse: &kafka.MetadataResponse{
					Topics:  getMockTopics(),
					Brokers: getMockBrokers(),
				},
				alterPartitionReassignmentsResponse: &kafka.AlterPartitionReassignmentsResponse{
					Error: errors.New("Some error"),
				},
			}},
			args{context.Background(), mockTopicName, []PartitionAssignment{}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			if err := c.AssignPartitions(tt.args.ctx, tt.args.name, tt.args.assignments); (err != nil) != tt.wantErr {
				t.Errorf("BrokerAdmin.AssignPartitions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBrokerAdmin_RunLeaderElection(t *testing.T) {
	type fields struct {
		client KafkaAdminClient
	}
	type args struct {
		ctx        context.Context
		name       string
		partitions []int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Default",
			fields{client: newMockKafkaClient(nil, nil)},
			args{context.Background(), mockTopicName, []int{0, 1, 2}},
			false,
		},
		{
			"CatchResponseError",
			fields{client: &mockKafkaClient{
				metadataResponse: &kafka.MetadataResponse{
					Topics:  getMockTopics(),
					Brokers: getMockBrokers(),
				},
				electLeadersResponse: &kafka.ElectLeadersResponse{
					Error: errors.New("Some error"),
				},
			}},
			args{context.Background(), mockTopicName, []int{0, 1, 2}},
			true,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BrokerAdmin{
				client: tt.fields.client,
			}
			if err := c.RunLeaderElection(tt.args.ctx, tt.args.name, tt.args.partitions); (err != nil) != tt.wantErr {
				t.Errorf("BrokerAdmin.RunLeaderElection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
