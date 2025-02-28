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
    SessionId int32          
    SessionEpoch int32       
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

// FetchRequest {
//     ReplicaId: int32          // -1 for normal consumers, broker ID for replicas
//     MaxWaitTime: int32        // How long to wait for messages (in ms)
//     MinBytes: int32           // Minimum bytes to fetch before returning
//     MaxBytes: int32           // Maximum bytes to fetch (optional)
//     IsolationLevel: int8      // 0 = Read Uncommitted, 1 = Read Committed
//     SessionId: int32          // Session ID for incremental fetch (optional)
//     SessionEpoch: int32       // Epoch for incremental fetch (optional)
//
//     Topics: [
//         {
//             TopicName: string
//             Partitions: [
//                 {
//                     Partition: int32
//                     FetchOffset: int64    // The offset from where to start reading
//                     MaxBytes: int32       // Max bytes to fetch for this partition
//                 }
//             ]
//         }
//     ]
// }

func (f *FetchRequest) Deserialize(buff *bytes.Buffer) {
    // Read fixed fields first
    binary.Read(buff, binary.BigEndian, &f.ReplicaID)
    binary.Read(buff, binary.BigEndian, &f.MaxWaitTime)
    binary.Read(buff, binary.BigEndian, &f.MinBytes)
    binary.Read(buff, binary.BigEndian, &f.MaxBytes)
    binary.Read(buff, binary.BigEndian, &f.IsolationLevel)
    binary.Read(buff, binary.BigEndian, &f.SessionId)
    binary.Read(buff, binary.BigEndian, &f.SessionEpoch)

    // Read number of topics
    var topicCount uint32
    binary.Read(buff, binary.BigEndian, &topicCount)

    // Read each topic
    for i := 0; i < int(topicCount); i++ {
        var topic consumerTopic

        // Read topic name length
        var topicNameLen uint16
        binary.Read(buff, binary.BigEndian, &topicNameLen)

        // Read topic name
        topicName := make([]byte, topicNameLen)
        buff.Read(topicName)
        topic.name = string(topicName)

        // Read partition count
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
}



func (fr *FetchRequest) Headers() *RequestHeader {
    return fr.headers
}
