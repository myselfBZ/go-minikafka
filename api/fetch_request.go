package api

import (
	"bytes"
	"encoding/binary"
)


type FetchRequest struct{
    headers *RequestHeader    

    ReplicaID int32
    MaxWaitTime int32
    MinBytes int32
    MaxBytes int32           
    IsolationLevel int8      
    topics []consumerTopic
}


type consumerPartition struct{
    index int32
    fetchOffset int32
    maxBytes int32
}

type consumerTopic struct{
    name string
    partitions []consumerPartition
    maxBytes int32
}


func (f *FetchRequest) Deserialize(buff *bytes.Buffer) error {
    // Read fixed fields first
    binary.Read(buff, binary.BigEndian, &f.ReplicaID)
    binary.Read(buff, binary.BigEndian, &f.MaxWaitTime)
    binary.Read(buff, binary.BigEndian, &f.MinBytes)

    var topicCount uint32
    binary.Read(buff, binary.BigEndian, &topicCount)

    for i := 0; i < int(topicCount); i++ {
        var topic consumerTopic

        var topicNameLen uint16
        binary.Read(buff, binary.BigEndian, &topicNameLen)

        topicName := make([]byte, topicNameLen)
        buff.Read(topicName)
        topic.name = string(topicName)

        var partitionCount uint32
        binary.Read(buff, binary.BigEndian, &partitionCount)

        for j := 0; j < int(partitionCount); j++ {
            var partition consumerPartition

            binary.Read(buff, binary.BigEndian, &partition.index)      // Partition ID
            binary.Read(buff, binary.BigEndian, &partition.fetchOffset) // FetchOffset
            binary.Read(buff, binary.BigEndian, &partition.maxBytes)    // MaxBytes

            topic.partitions = append(topic.partitions, partition)
        }

        f.topics = append(f.topics, topic)
    }
    return nil
}

func (fr *FetchRequest) Headers() *RequestHeader {
    return fr.headers
}
