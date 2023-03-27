package client

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

type mockKafkaWriterClient struct {
	err     error
	message *kafka.Message
}

func newMockWriterClient(err error, message *kafka.Message) *mockKafkaWriterClient {
	mock := &mockKafkaWriterClient{
		err:     err,
		message: getMockKafkaMessage(),
	}

	if message != nil {
		mock.message = message
	}

	return mock
}

func (m *mockKafkaWriterClient) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.err != nil {
		return m.err
	}
	return nil
}

func (m *mockKafkaWriterClient) Close() error {
	if m.err != nil {
		return m.err
	}
	return nil
}

func TestNewProducerClient(t *testing.T) {
	type args struct {
		config ConnectorConfig
		topic  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"Default",
			args{
				ConnectorConfig{BrokerAddrs: []string{"broker-1:9098"}},
				mockTopicName,
			},
			false,
		},
		{
			"BadMachanism",
			args{
				ConnectorConfig{
					BrokerAddrs: []string{"broker-1:9098"},
					SASL: SASLConfig{
						Enabled:   true,
						Mechanism: "INVALID",
					},
				},
				mockTopicName,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewProducerClient(tt.args.config, tt.args.topic)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducerClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestProducerClient_Write(t *testing.T) {
	message := getMockMessage()
	messages := []Message{
		*message,
	}

	type fields struct {
		writer KafkaWriterClient
	}
	type args struct {
		ctx  context.Context
		msgs []Message
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Default",
			fields{newMockWriterClient(nil, nil)},
			args{context.Background(), messages},
			false,
		},
		{
			"Error",
			fields{newMockWriterClient(errors.New("Some error"), nil)},
			args{context.Background(), messages},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ProducerClient{
				writer: tt.fields.writer,
			}
			if err := c.Write(tt.args.ctx, tt.args.msgs...); (err != nil) != tt.wantErr {
				t.Errorf("ProducerClient.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProducerClient_Close(t *testing.T) {
	type fields struct {
		writer KafkaWriterClient
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Default",
			fields{newMockWriterClient(nil, nil)},
			false,
		},
		{
			"Error",
			fields{newMockWriterClient(errors.New("Some error"), nil)},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ProducerClient{
				writer: tt.fields.writer,
			}
			if err := c.Close(); (err != nil) != tt.wantErr {
				t.Errorf("ProducerClient.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
