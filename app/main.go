package main

import (
	"encoding/binary"
	"fmt"
	"io"
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

	// Read request header v2: message_size (4) + api_key (2) + api_version (2) + correlation_id (4)
	header := make([]byte, 12)
	if _, err = io.ReadFull(conn, header); err != nil {
		fmt.Println("Error reading request: ", err.Error())
		os.Exit(1)
	}

	correlationID := binary.BigEndian.Uint32(header[8:12])

	// Send response: 8 bytes total (message_size + correlation_id)
	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], 0) // message_size: 0
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response: ", err.Error())
		os.Exit(1)
	}
}
