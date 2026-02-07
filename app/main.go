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

	for {
		// Read message_size (4 bytes) first
		sizeBuf := make([]byte, 4)
		if _, err = io.ReadFull(conn, sizeBuf); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading message_size: ", err.Error())
			break
		}

		requestMessageSize := binary.BigEndian.Uint32(sizeBuf)

		if requestMessageSize < 8 {
			fmt.Println("Invalid message_size (too small): ", requestMessageSize)
			break
		}

		payload := make([]byte, requestMessageSize)
		if _, err = io.ReadFull(conn, payload); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading request payload: ", err.Error())
			break
		}

		// Parse api_version (2 bytes at offset 2-3)
		requestAPIVersion := binary.BigEndian.Uint16(payload[2:4])
		fmt.Printf("Parsed request_api_version: %d\n", requestAPIVersion)

		// Compute error_code: 35 if not in 0-4, else 0
		var errorCode uint16
		if requestAPIVersion > 4 {
			errorCode = 35
		} else {
			errorCode = 0
		}

		// Parse correlation_id (4 bytes at offset 4-7)
		correlationID := binary.BigEndian.Uint32(payload[4:8])

		// Build ApiVersions v4 response body:
		responseBody := []byte{
			byte(errorCode >> 8), byte(errorCode), // error_code (2)
			0x02, // api_keys compact array length (1 element, zigzag encoded)
			0x00, 0x12, // api_key 18
			0x00, 0x00, // min_version 0
			0x00, 0x04, // max_version 4
			0x00, // element tag buffer
			0x00, 0x00, 0x00, 0x00, // throttle_time_ms
			0x00, // response tag buffer
		}

		// Calculate message_size: everything after the message_size field
		messageSize := uint32(19) // Total bytes after message_size field

		// Prepend message_size (4 bytes) and correlation_id (4 bytes)
		header := make([]byte, 8)
		binary.BigEndian.PutUint32(header[0:4], messageSize) // message_size
		binary.BigEndian.PutUint32(header[4:8], correlationID) // correlation_id

		// Write header then body
		if _, err = conn.Write(header); err != nil {
			fmt.Println("Error writing response header: ", err.Error())
			break
		}
		if _, err = conn.Write(responseBody); err != nil {
			fmt.Println("Error writing response body: ", err.Error())
			break
		}
	}
}
