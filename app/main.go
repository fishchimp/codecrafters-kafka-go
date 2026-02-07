package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	
	// Send response: 8 bytes total
	// 4 bytes for message_size (0)
	// 4 bytes for correlation_id (7)
	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], 0)      // message_size: 0
	binary.BigEndian.PutUint32(response[4:8], 7)      // correlation_id: 7
	
	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response: ", err.Error())
		os.Exit(1)
	}
}
