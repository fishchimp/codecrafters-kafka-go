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

	// Parse request_api_version (2 bytes at offset 6-7)
	requestAPIVersion := binary.BigEndian.Uint16(header[6:8])
	fmt.Printf("Parsed request_api_version: %d\n", requestAPIVersion)

	// Compute error_code: 35 if not in 0-4, else 0
	var errorCode uint16
	if requestAPIVersion > 4 {
		errorCode = 35
	} else {
		errorCode = 0
	}

	correlationID := binary.BigEndian.Uint32(header[8:12])

	// Send response: 10 bytes total (message_size + correlation_id + error_code)
	response := make([]byte, 10)
	binary.BigEndian.PutUint32(response[0:4], 2) // message_size: can be any 4-byte value
	binary.BigEndian.PutUint32(response[4:8], correlationID)
	binary.BigEndian.PutUint16(response[8:10], errorCode)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response: ", err.Error())
		os.Exit(1)
	}
}
