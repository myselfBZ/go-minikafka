package main

import (
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
	defer conn.Close()

	header := []byte{
		0x00, 0x00, 0x00, 0x12,  // header length
		0x00, 0x00,   // API key         
		0x00, 0x00,   // API version          
		0x00, 0x00, 0x30, 0x39, // correlation id
        0x00, 0x0B,            // client id length 
		'm', 'y', '-', 'p', 'r', 'o', 'd', 'u', 'c', 'e', 'r', // Client ID
	}

	_, err = conn.Write(header)

	if err != nil {
		fmt.Println("Failed to send data:", err)
		return
	}

	fmt.Println("Kafka header sent successfully!")
}

