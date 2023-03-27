package client

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
)

func getMockKafkaMessage() *kafka.Message {
	return &kafka.Message{
		Topic:     mockTopicName,
		Partition: 0,
		Offset:    1,
		Value:     []byte("Fake Message"),
	}
}

func getMockMessage() *Message {
	kafkaMessage := getMockKafkaMessage()
	return &Message{
		Topic:     kafkaMessage.Topic,
		Partition: kafkaMessage.Partition,
		Offset:    kafkaMessage.Offset,
		Value:     kafkaMessage.Value,
	}
}

type mockKafkaReaderClient struct {
	err     error
	message *kafka.Message
}

func newMockReaderClient(err error, message *kafka.Message) *mockKafkaReaderClient {
	mock := &mockKafkaReaderClient{
		err:     err,
		message: getMockKafkaMessage(),
	}

	if message != nil {
		mock.message = message
	}

	return mock
}

func (m *mockKafkaReaderClient) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.err != nil {
		return kafka.Message{}, m.err
	}
	return *getMockKafkaMessage(), nil
}

func (m *mockKafkaReaderClient) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.err != nil {
		return m.err
	}
	return nil
}

func (m *mockKafkaReaderClient) Close() error {
	if m.err != nil {
		return m.err
	}
	return nil
}

func TestNewConsumerClient(t *testing.T) {
	type args struct {
		config          ConnectorConfig
		topic           string
		consumerGroupID string
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
				"test-group",
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
				"test-group",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConsumerClient(tt.args.config, tt.args.topic, tt.args.consumerGroupID)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsumerClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConsumerClient_Fetch(t *testing.T) {
	type fields struct {
		reader KafkaReaderClient
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Message
		wantErr bool
	}{
		{
			"Default",
			fields{newMockReaderClient(nil, nil)},
			args{context.Background()},
			getMockMessage(),
			false,
		},
		{
			"Error",
			fields{newMockReaderClient(errors.New("Some error"), nil)},
			args{context.Background()},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConsumerClient{
				reader: tt.fields.reader,
			}
			got, err := c.Fetch(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConsumerClient.Fetch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConsumerClient.Fetch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsumerClient_Commit(t *testing.T) {
	message := getMockMessage()
	messages := []*Message{
		message,
	}

	type fields struct {
		reader KafkaReaderClient
	}
	type args struct {
		ctx  context.Context
		msgs []*Message
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Default",
			fields{newMockReaderClient(nil, nil)},
			args{context.Background(), messages},
			false,
		},
		{
			"Error",
			fields{newMockReaderClient(errors.New("Some error"), nil)},
			args{context.Background(), messages},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConsumerClient{
				reader: tt.fields.reader,
			}
			if err := c.Commit(tt.args.ctx, tt.args.msgs...); (err != nil) != tt.wantErr {
				t.Errorf("ConsumerClient.Commit() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConsumerClient_Close(t *testing.T) {
	type fields struct {
		reader KafkaReaderClient
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Default",
			fields{newMockReaderClient(nil, nil)},
			false,
		},
		{
			"Error",
			fields{newMockReaderClient(errors.New("Some error"), nil)},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConsumerClient{
				reader: tt.fields.reader,
			}
			if err := c.Close(); (err != nil) != tt.wantErr {
				t.Errorf("ConsumerClient.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
