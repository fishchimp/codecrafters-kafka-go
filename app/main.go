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

		// Read the next 8 bytes to get the rest of the header (api_key + api_version + correlation_id)
		headerRest := make([]byte, 8)
		if _, err = io.ReadFull(conn, headerRest); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading request header: ", err.Error())
			break
		}

		// Combine sizeBuf and headerRest for compatibility with existing code
		header := append(sizeBuf, headerRest...)

		// Parse message_size
		requestMessageSize := binary.BigEndian.Uint32(header[0:4])

		// Allocate or reuse a buffer of length message_size
		payload := make([]byte, requestMessageSize)
		copy(payload[0:12], header) // already read first 12 bytes
		if requestMessageSize > 12 {
			if _, err = io.ReadFull(conn, payload[12:]); err != nil {
				if err == io.EOF {
					break
				}
				fmt.Println("Error reading request payload: ", err.Error())
				break
			}
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

		// Build response body (exact bytes, 19 bytes)
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
		// This includes: correlation_id (4) + error_code (2) + array_length (1) +
		// api_key (2) + min_version (2) + max_version (2) + element_tag (1) + throttle_time (4) + response_tag (1)
		messageSize := uint32(19) // Total bytes after message_size field

		// Prepend message_size (4 bytes) and correlation_id (4 bytes)
		// Build response header
		header = make([]byte, 8)
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
