package client

// Message is a simpler internal representation of kafka.Message
// Topic, Partition and Offset are the required fields for commiting a message
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Value     []byte
}

// TopicInfo represents the information stored about a topic.
type TopicInfo struct {
	Name       string                `json:"name"`
	Partitions []PartitionAssignment `json:"partitions"`
}

// PartitionAssignment contains the actual or desired assignment of
// replicas in a topic partition.
type PartitionAssignment struct {
	ID       int   `json:"id"`
	Leader   int   `json:"leader"`
	Replicas []int `json:"replicas"`
}

// BrokerInfo represents the information stored about a broker.
type BrokerInfo struct {
	ID   int    `json:"id"`
	Rack string `json:"rack"`
}
