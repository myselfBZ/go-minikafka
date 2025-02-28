package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)


func main() {
	addr := "localhost:6969"

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Failed to connect to Kafka:", err)
		return
	}

    var buf bytes.Buffer

	defer conn.Close()

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
    
    conn.Write(buf.Bytes())


}

