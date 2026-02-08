package main

type TopicMetadata struct {
	UUID       [16]byte
	Partitions []PartitionMetadata
}

type PartitionMetadata struct {
	ID          int32
	Leader      int32
	LeaderEpoch int32
	Replicas    []int32
	ISR         []int32
}

// Global in-memory map: topic_name -> TopicMetadata
var topicMap = make(map[string]*TopicMetadata)
