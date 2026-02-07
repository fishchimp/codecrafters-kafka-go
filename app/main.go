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

	// Build ApiVersions v4 response body:
	// error_code (2 bytes)
	// api_keys compact array length 0x02 (1 element)
	// api_key 18, min 0, max 4, element tag buffer 0x00
	// throttle_time_ms (4 bytes zero)
	// response tag buffer 0x00

	responseBody := make([]byte, 0, 11)
	// error_code
	responseBody = append(responseBody, byte(errorCode>>8), byte(errorCode))
	// api_keys compact array length (0x02 means 1 element, zigzag encoding for compact array length: n+1)
	responseBody = append(responseBody, 0x02)
	// api_key 18
	responseBody = append(responseBody, 0x00, 0x12)
	// min_version 0
	responseBody = append(responseBody, 0x00, 0x00)
	// max_version 4
	responseBody = append(responseBody, 0x00, 0x04)
	// element tag buffer
	responseBody = append(responseBody, 0x00)
	// throttle_time_ms (4 bytes zero)
	responseBody = append(responseBody, 0x00, 0x00, 0x00, 0x00)
	// response tag buffer
	responseBody = append(responseBody, 0x00)

	// Prepend message_size (4 bytes) and correlation_id (4 bytes), then write header + body
	bodyLength := len(responseBody)
	messageSize := uint32(4 + bodyLength) // 4 bytes for correlation_id + body

	
	binary.BigEndian.PutUint32(header[0:4], messageSize)
	binary.BigEndian.PutUint32(header[4:8], correlationID)

	// Write header then body
	if _, err = conn.Write(header); err != nil {
		fmt.Println("Error writing response header: ", err.Error())
		os.Exit(1)
	}
	if _, err = conn.Write(responseBody); err != nil {
		fmt.Println("Error writing response body: ", err.Error())
		os.Exit(1)
	}
}
