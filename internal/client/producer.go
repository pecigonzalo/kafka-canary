package client

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Write(ctx context.Context, msgs ...Message) error
	Close() error
}

var _ Producer = (*ProducerClient)(nil)

type ProducerClient struct {
	writer KafkaWriterClient
}

func NewProducerClient(config ConnectorConfig, topic string) (*ProducerClient, error) {
	connector, err := NewConnector(config)
	if err != nil {
		return nil, err
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(connector.Config.BrokerAddrs...),
		Transport: connector.KafkaClient.Transport,
		Topic:     topic,
	}

	return &ProducerClient{
		writer: writer,
	}, nil
}

func (c *ProducerClient) Write(ctx context.Context, msgs ...Message) error {
	var kafkaMessages []kafka.Message
	for _, m := range msgs {
		kafkaMessages = append(kafkaMessages, kafka.Message{
			Partition: m.Partition,
			Value:     m.Value,
		})
	}

	if err := c.writer.WriteMessages(ctx, kafkaMessages...); err != nil {
		return err
	}

	return nil
}

func (c ProducerClient) Close() error {
	return c.writer.Close()
}
