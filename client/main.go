package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

func produceRequest() []byte {
    var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, uint32(38)) // Request Length
	binary.Write(&buf, binary.BigEndian, uint16(0))  // API Key (Produce)
	binary.Write(&buf, binary.BigEndian, uint16(0))  // API Version
	binary.Write(&buf, binary.BigEndian, uint32(123)) // Correlation ID
	binary.Write(&buf, binary.BigEndian, uint16(3))   // Client ID Length
	buf.Write([]byte("app"))                         // Client ID

	// Request Body
	binary.Write(&buf, binary.BigEndian, uint16(1))  // Required Acks
	binary.Write(&buf, binary.BigEndian, uint32(5000)) // Timeout
	binary.Write(&buf, binary.BigEndian, uint32(2))  // Number of Topics
	binary.Write(&buf, binary.BigEndian, uint16(8))  // Topic Name Length
	buf.Write([]byte("my_topic"))                    // Topic Name
	binary.Write(&buf, binary.BigEndian, uint32(1))  // Number of Partitions
	binary.Write(&buf, binary.BigEndian, uint32(0))  // Partition Index
	binary.Write(&buf, binary.BigEndian, uint32(24)) // Message Set Size

	// Message Set
    // 2 messages in one partition
	binary.Write(&buf, binary.BigEndian, uint64(123))  // Offset
	binary.Write(&buf, binary.BigEndian, uint32(12)) // Message Size
	binary.Write(&buf, binary.BigEndian, uint32(0))  // CRC (fake, should be computed)
	binary.Write(&buf, binary.BigEndian, uint8(0))   // Magic Byte
	binary.Write(&buf, binary.BigEndian, uint8(0))   // Attributes
	binary.Write(&buf, binary.BigEndian, int32(-1))  // Key Length (-1 = no key)
	binary.Write(&buf, binary.BigEndian, uint32(12)) // Value Length
	buf.Write([]byte("Hello Kafka!"))                // Value

	binary.Write(&buf, binary.BigEndian, uint64(124))  // Offset
	binary.Write(&buf, binary.BigEndian, uint32(12)) // Message Size
	binary.Write(&buf, binary.BigEndian, uint32(0))  // CRC (fake, should be computed)
	binary.Write(&buf, binary.BigEndian, uint8(0))   // Magic Byte
	binary.Write(&buf, binary.BigEndian, uint8(0))   // Attributes
	binary.Write(&buf, binary.BigEndian, int32(-1))  // Key Length (-1 = no key)
	binary.Write(&buf, binary.BigEndian, uint32(14)) // Value Length
	buf.Write([]byte("Hello Kafka 1!"))                // Value

    // second topic
	binary.Write(&buf, binary.BigEndian, uint16(9))  // Topic Name Length
	buf.Write([]byte("my_topic1"))                    // Topic Name
	binary.Write(&buf, binary.BigEndian, uint32(1))  // Number of Partitions
	binary.Write(&buf, binary.BigEndian, uint32(0))  // Partition Index
	binary.Write(&buf, binary.BigEndian, uint32(12)) // Message Set Size

    // message
	binary.Write(&buf, binary.BigEndian, uint64(124))  // Offset
	binary.Write(&buf, binary.BigEndian, uint32(12)) // Message Size
	binary.Write(&buf, binary.BigEndian, uint32(0))  // CRC (fake, should be computed)
	binary.Write(&buf, binary.BigEndian, uint8(0))   // Magic Byte
	binary.Write(&buf, binary.BigEndian, uint8(0))   // Attributes
	binary.Write(&buf, binary.BigEndian, int32(-1))  // Key Length (-1 = no key)
    msg := "Hello Kafka from the second topic partition index 0"
	binary.Write(&buf, binary.BigEndian, uint32(len(msg))) // Value Length
	buf.Write([]byte(msg))                // Value
    
    return buf.Bytes()
}

type FetchRequest struct {
	ReplicaID   int32
	MaxWaitTime int32
	MinBytes    int32
	Topics      []FetchTopic
}

// FetchTopic represents a topic inside a FetchRequest
type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

// FetchPartition represents a partition inside a FetchRequest
type FetchPartition struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

func (r *FetchRequest) Encode() []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, uint32(38)) // Request Length
	binary.Write(&buf, binary.BigEndian, uint16(1))  // API Key (Produce)
	binary.Write(&buf, binary.BigEndian, uint16(0))  // API Version
	binary.Write(&buf, binary.BigEndian, uint32(123)) // Correlation ID
	binary.Write(&buf, binary.BigEndian, uint16(3))   // Client ID Length
	buf.Write([]byte("app"))                         // Client ID
	binary.Write(&buf, binary.BigEndian, r.ReplicaID)

	binary.Write(&buf, binary.BigEndian, r.MaxWaitTime)

	binary.Write(&buf, binary.BigEndian, r.MinBytes)

	binary.Write(&buf, binary.BigEndian, uint32(len(r.Topics)))

	for _, topic := range r.Topics {

        binary.Write(&buf, binary.BigEndian, int16(len(topic.Name)))
		buf.Write([]byte(topic.Name))

		binary.Write(&buf, binary.BigEndian, int32(len(topic.Partitions)))

		for _, part := range topic.Partitions {
			binary.Write(&buf, binary.BigEndian, part.Partition)
			binary.Write(&buf, binary.BigEndian, part.FetchOffset)
			binary.Write(&buf, binary.BigEndian, part.MaxBytes)
		}
	}

	return buf.Bytes()
}



func main() {
	addr := "localhost:6969"

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Failed to connect to Kafka:", err)
		return
	}

    req := &FetchRequest{
        ReplicaID:   -1,
        MaxWaitTime: 5000,
        MinBytes:    1,
        Topics: []FetchTopic{
            {
                Name: "test-topic",
                Partitions: []FetchPartition{
                    {
                        Partition:   0,
                        FetchOffset: 0,
                        MaxBytes:    1048576, // 1MB
                    },
                },
            },
        },
    }

	defer conn.Close()

    conn.Write(req.Encode())


}

