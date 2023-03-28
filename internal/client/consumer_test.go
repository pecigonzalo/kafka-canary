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

func TestNewConsumerClient(t *testing.T) {
	type args struct {
		config          ConnectorConfig
		topic           string
		consumerGroupID string
	}
	tests := []struct {
		name      string
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "Default",
			args: args{
				ConnectorConfig{BrokerAddrs: []string{"broker-1:9098"}},
				mockTopicName,
				"test-group",
			},
			assertion: assert.NoError,
		},
		{
			name: "BadMachanism",
			args: args{
				ConnectorConfig{
					BrokerAddrs: []string{"broker-1:9098"},
					SASL: SASLConfig{
						Enabled:   true,
						Mechanism: "INVALID",
					},
				},
				mockTopicName,
				"test-group",
			},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConsumerClient(tt.args.config, tt.args.topic, tt.args.consumerGroupID)
			tt.assertion(t, err)
		})
	}
}

func TestConsumerClient_Fetch(t *testing.T) {
	kafkaMessage := mockGetKafkaMessage()

	mockReaderClient := mocks.NewKafkaReaderClient(t)
	mockReaderClientWithError := mocks.NewKafkaReaderClient(t)

	mockReaderClient.
		On("FetchMessage", mock.Anything).
		Return(*kafkaMessage, nil)

	mockReaderClientWithError.
		On("FetchMessage", mock.Anything).
		Return(kafka.Message{}, errors.New("Some error"))

	type fields struct {
		reader KafkaReaderClient
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      *Message
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:      "Default",
			fields:    fields{mockReaderClient},
			args:      args{context.Background()},
			want:      mockGetMessage(),
			assertion: assert.NoError,
		},
		{
			name:      "Error",
			fields:    fields{mockReaderClientWithError},
			args:      args{context.Background()},
			want:      nil,
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConsumerClient{
				reader: tt.fields.reader,
			}
			got, err := c.Fetch(tt.args.ctx)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConsumerClient_Commit(t *testing.T) {
	message := mockGetMessage()
	messages := []*Message{
		message,
	}

	mockReaderClient := mocks.NewKafkaReaderClient(t)
	mockReaderClientWithError := mocks.NewKafkaReaderClient(t)

	mockReaderClient.
		On("CommitMessages", mock.Anything, mock.Anything).
		Return(nil)

	mockReaderClientWithError.
		On("CommitMessages", mock.Anything, mock.Anything).
		Return(errors.New("Some error"))

	type fields struct {
		reader KafkaReaderClient
	}
	type args struct {
		ctx  context.Context
		msgs []*Message
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:      "Default",
			fields:    fields{mockReaderClient},
			args:      args{context.Background(), messages},
			assertion: assert.NoError,
		},
		{
			name:      "Error",
			fields:    fields{mockReaderClientWithError},
			args:      args{context.Background(), messages},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConsumerClient{
				reader: tt.fields.reader,
			}
			tt.assertion(t, c.Commit(tt.args.ctx, tt.args.msgs...))
		})
	}
}

func TestConsumerClient_Close(t *testing.T) {
	mockReaderClient := mocks.NewKafkaReaderClient(t)
	mockReaderClientWithError := mocks.NewKafkaReaderClient(t)

	mockReaderClient.
		On("Close").
		Return(nil)

	mockReaderClientWithError.
		On("Close").
		Return(errors.New("Some error"))

	type fields struct {
		reader KafkaReaderClient
	}
	tests := []struct {
		name      string
		fields    fields
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:      "Default",
			fields:    fields{mockReaderClient},
			assertion: assert.NoError,
		},
		{
			name:      "Error",
			fields:    fields{mockReaderClientWithError},
			assertion: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConsumerClient{
				reader: tt.fields.reader,
			}
			tt.assertion(t, c.Close())
		})
	}
}
