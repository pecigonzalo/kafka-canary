package client

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Fetch(ctx context.Context) (*Message, error)
	Commit(ctx context.Context, msg ...*Message) error
	Close() error
}

var _ Consumer = (*ConsumerClient)(nil)

type ConsumerClient struct {
	reader KafkaReaderClient
}

func NewConsumerClient(config ConnectorConfig, topic string, clientID string, consumerGroupID string) (*ConsumerClient, error) {
	connector, err := NewConnector(config)
	if err != nil {
		return nil, err
	}
	connector.Dialer.ClientID = clientID

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               connector.Config.BrokerAddrs,
		Dialer:                connector.Dialer,
		GroupID:               consumerGroupID,
		Topic:                 topic,
		StartOffset:           kafka.LastOffset,
		WatchPartitionChanges: true,
	})

	return &ConsumerClient{
		reader: reader,
	}, nil
}

func (c *ConsumerClient) Fetch(ctx context.Context) (*Message, error) {
	kafkaMessage, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}

	return &Message{
		Topic:     kafkaMessage.Topic,
		Partition: kafkaMessage.Partition,
		Offset:    kafkaMessage.Offset,
		Value:     kafkaMessage.Value,
	}, nil
}

func (c *ConsumerClient) Commit(ctx context.Context, msgs ...*Message) error {
	var kafkaMessages []kafka.Message
	for _, m := range msgs {
		kafkaMessages = append(kafkaMessages, kafka.Message{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
		})
	}

	if err := c.reader.CommitMessages(ctx, kafkaMessages...); err != nil {
		return err
	}

	return nil
}

func (c *ConsumerClient) Close() error {
	return c.reader.Close()
}
