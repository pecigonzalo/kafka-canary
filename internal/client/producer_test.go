package client

import (
	"context"
	"errors"
	"testing"

	"github.com/pecigonzalo/kafka-canary/internal/client/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewProducerClient(t *testing.T) {
	type args struct {
		config ConnectorConfig
		topic  string
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
			},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewProducerClient(tt.args.config, tt.args.topic)
			tt.assertion(t, err)
		})
	}
}

func TestProducerClient_Write(t *testing.T) {
	message := mockGetMessage()
	messages := []Message{
		*message,
	}

	mockWriterClient := mocks.NewKafkaWriterClient(t)
	mockWriterClientWithError := mocks.NewKafkaWriterClient(t)

	mockWriterClient.
		On("WriteMessages", mock.Anything, mock.Anything).
		Return(nil)

	mockWriterClientWithError.
		On("WriteMessages", mock.Anything, mock.Anything).
		Return(errors.New("Some error"))

	type fields struct {
		writer KafkaWriterClient
	}
	type args struct {
		ctx  context.Context
		msgs []Message
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:      "Default",
			fields:    fields{writer: mockWriterClient},
			args:      args{context.Background(), messages},
			assertion: assert.NoError,
		},
		{
			name:      "Error",
			fields:    fields{writer: mockWriterClientWithError},
			args:      args{context.Background(), messages},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ProducerClient{
				writer: tt.fields.writer,
			}
			tt.assertion(t, c.Write(tt.args.ctx, tt.args.msgs...))
		})
	}
}

func TestProducerClient_Close(t *testing.T) {
	mockWriterClient := mocks.NewKafkaWriterClient(t)
	mockWriterClientWithError := mocks.NewKafkaWriterClient(t)

	mockWriterClient.
		On("Close", mock.Anything, mock.Anything).
		Return(nil)

	mockWriterClientWithError.
		On("Close", mock.Anything, mock.Anything).
		Return(errors.New("Some error"))

	type fields struct {
		writer KafkaWriterClient
	}
	tests := []struct {
		name      string
		fields    fields
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:      "Default",
			fields:    fields{writer: mockWriterClient},
			assertion: assert.NoError,
		},
		{
			name:      "Error",
			fields:    fields{writer: mockWriterClientWithError},
			assertion: assert.Error,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ProducerClient{
				writer: tt.fields.writer,
			}
			tt.assertion(t, c.Close())
		})
	}
}
