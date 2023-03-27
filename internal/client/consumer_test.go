package client

import (
	"context"
	"reflect"
	"testing"
)

type mockConsumerClient struct {
	err error
}

func (m *mockConsumerClient) Fetch(ctx context.Context) (*Message, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockConsumerClient) Commit(ctx context.Context, msg ...*Message) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockConsumerClient) Close() error {
	if m.err != nil {
		return m.err
	}
	return nil
}

func TestNewConsumerClient(t *testing.T) {
	type args struct {
		config          ConnectorConfig
		topic           string
		clientID        string
		consumerGroupID string
	}
	tests := []struct {
		name    string
		args    args
		want    *ConsumerClient
		wantErr bool
	}{
		{
			"Default",
			args{
				ConnectorConfig{BrokerAddrs: []string{"broker-1:9098"}},
				mockTopicName,
				"test-client",
				"test-group",
			},
			&ConsumerClient{},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConsumerClient(tt.args.config, tt.args.topic, tt.args.clientID, tt.args.consumerGroupID)
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
			fields{},
			args{},
			&Message{},
			false,
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
		// TODO: Add test cases.
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
		// TODO: Add test cases.
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
