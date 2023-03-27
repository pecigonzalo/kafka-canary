package client

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
