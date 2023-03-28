package client

import (
	kafka "github.com/segmentio/kafka-go"
)

const (
	mockTopicName = "fake-topic"
)

func mockGetBrokers() []kafka.Broker {
	return []kafka.Broker{
		{ID: 0, Rack: "rack1"},
		{ID: 1, Rack: "rack2"},
		{ID: 2, Rack: "rack3"},
	}
}

func mockGetTopics() []kafka.Topic {
	return []kafka.Topic{
		{
			Name: mockTopicName,
			Partitions: []kafka.Partition{
				{Topic: mockTopicName, ID: 0, Replicas: mockGetBrokers()},
				{Topic: mockTopicName, ID: 1, Replicas: mockGetBrokers()},
				{Topic: mockTopicName, ID: 2, Replicas: mockGetBrokers()},
			},
		},
	}
}
func mockGetKafkaMessage() *kafka.Message {
	return &kafka.Message{
		Topic:     mockTopicName,
		Partition: 0,
		Offset:    1,
		Value:     []byte("Fake Message"),
	}
}

func mockGetMessage() *Message {
	kafkaMessage := mockGetKafkaMessage()
	return &Message{
		Topic:     kafkaMessage.Topic,
		Partition: kafkaMessage.Partition,
		Offset:    kafkaMessage.Offset,
		Value:     kafkaMessage.Value,
	}
}
