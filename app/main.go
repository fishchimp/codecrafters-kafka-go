package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

const clusterMetadataLogPath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

func main() {
	fmt.Println("Logs from your program will appear here!")
	if err := loadClusterMetadataLog(clusterMetadataLogPath); err != nil {
		fmt.Printf("Initial metadata load failed: %v\n", err)
	}

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		// Read message_size (4 bytes) first
		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, sizeBuf); err != nil {
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
		if _, err := io.ReadFull(conn, payload); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading request payload: ", err.Error())
			break
		}

		// Parse api_key (2 bytes at offset 0-1)
		requestAPIKey := binary.BigEndian.Uint16(payload[0:2])
		// Parse api_version (2 bytes at offset 2-3)
		requestAPIVersion := binary.BigEndian.Uint16(payload[2:4])
		fmt.Printf("Parsed request_api_key: %d, request_api_version: %d\n", requestAPIKey, requestAPIVersion)

		// Parse correlation_id (4 bytes at offset 4-7)
		correlationID := binary.BigEndian.Uint32(payload[4:8])

		// Compute start of request body (after header v2)
		bodyIdx := 8
		if len(payload) < bodyIdx+2 {
			fmt.Println("Payload too short for client_id length")
			break
		}
		clientIDLen := int16(binary.BigEndian.Uint16(payload[bodyIdx : bodyIdx+2]))
		bodyIdx += 2
		if clientIDLen >= 0 {
			if len(payload) < bodyIdx+int(clientIDLen) {
				fmt.Println("Payload too short for client_id")
				break
			}
			bodyIdx += int(clientIDLen)
		}
		// Skip request header TAG_BUFFER (uvarint length + bytes)
		tagLen := 0
		for i := 0; i < 5; i++ { // uvarint max 5 bytes for 32-bit
			if len(payload) <= bodyIdx {
				fmt.Println("Payload too short for header TAG_BUFFER")
				break
			}
			b := payload[bodyIdx]
			bodyIdx++
			tagLen |= int(b&0x7F) << (7 * i)
			if b&0x80 == 0 {
				break
			}
		}
		if len(payload) < bodyIdx+tagLen {
			fmt.Println("Payload too short for header TAG_BUFFER data")
			break
		}
		bodyIdx += tagLen

		// Parse topic name if DescribeTopicPartitions (API key 75)
		var topicName string
		var topicMeta *TopicMetadata
		var topicFound bool
		if requestAPIKey == 75 {
			// topics array starts at bodyIdx
			idx := bodyIdx
			if len(payload) <= idx {
				fmt.Println("Payload too short for topics array")
				break
			}
			topicsLenByte := payload[idx]
			idx++
			topicsCount := int(topicsLenByte) - 1
			if topicsLenByte != 0x02 || topicsCount != 1 {
				fmt.Printf("Unexpected topics compact array length: %d\n", topicsLenByte)
				break
			}
			// Parse topic_name as COMPACT_STRING
			if len(payload) <= idx {
				fmt.Println("Payload too short for topic_name length")
				break
			}
			topicNameLenByte := payload[idx]
			idx++
			topicNameLen := int(topicNameLenByte) - 1
			if topicNameLen < 0 || len(payload) < idx+topicNameLen {
				fmt.Println("Invalid topic name length")
				break
			}
			topicName = string(payload[idx : idx+topicNameLen])
			idx += topicNameLen
			// Skip topic TAG_BUFFER
			if len(payload) <= idx {
				fmt.Println("Payload too short for topic TAG_BUFFER")
				break
			}
			idx++ // skip topic TAG_BUFFER (should be 0x00)
			// Skip request TAG_BUFFER
			if len(payload) <= idx {
				fmt.Println("Payload too short for request TAG_BUFFER")
				break
			}
			idx++ // skip request TAG_BUFFER (should be 0x00)
			fmt.Printf("Parsed topic name: %s\n", topicName)

			// Lookup topic metadata
			topicMeta, topicFound = topicMap[topicName]
			if !topicFound {
				// Retry once in case metadata log wasn't ready during startup.
				if err := loadClusterMetadataLog(clusterMetadataLogPath); err != nil {
					fmt.Printf("Metadata reload failed: %v\n", err)
				}
				topicMeta, topicFound = topicMap[topicName]
			}
			if !topicFound {
				// Last-resort tolerant scan for this topic in metadata log.
				meta, ok, err := lookupTopicMetadataFromLog(clusterMetadataLogPath, topicName)
				if err != nil {
					fmt.Printf("Metadata direct lookup failed: %v\n", err)
				} else if ok && meta != nil {
					topicMap[topicName] = meta
					topicMeta, topicFound = meta, true
				}
			}
		}

		if requestAPIKey == 75 {
			// Build DescribeTopicPartitions v0 response body
			responseBody := make([]byte, 0)
			responseBody = append(responseBody, 0x00, 0x00, 0x00, 0x00) // throttle_time_ms = 0
			responseBody = append(responseBody, 0x02)                   // topics COMPACT_ARRAY with 1 element

			// Topic element:
			var errorCode uint16 = 3 // unknown topic
			var topicID [16]byte
			var partitionsArray []byte
			if topicFound && topicMeta != nil {
				errorCode = 0
				topicID = topicMeta.UUID
				p := PartitionMetadata{
					ID:          0,
					Leader:      1,
					LeaderEpoch: 0,
					Replicas:    []int32{1},
					ISR:         []int32{1},
				}
				if len(topicMeta.Partitions) > 0 {
					p = topicMeta.Partitions[0]
				}
				partitionsArray = make([]byte, 0)
				partitionsArray = append(partitionsArray, 0x02)      // length byte (1 element)
				partitionsArray = appendInt16(partitionsArray, 0)    // error_code
				partitionsArray = appendInt32(partitionsArray, p.ID) // partition_index

				leaderID := p.Leader
				if leaderID == 0 && len(p.Replicas) > 0 {
					leaderID = p.Replicas[0]
				}
				if leaderID == 0 {
					leaderID = 1
				}
				partitionsArray = appendInt32(partitionsArray, leaderID)      // leader_id
				partitionsArray = appendInt32(partitionsArray, p.LeaderEpoch) // leader_epoch

				replicas := p.Replicas
				if len(replicas) == 0 {
					replicas = []int32{leaderID}
				}
				isr := p.ISR
				if len(isr) == 0 {
					isr = []int32{leaderID}
				}
				partitionsArray = appendCompactInt32Array(partitionsArray, replicas) // replica_nodes
				partitionsArray = appendCompactInt32Array(partitionsArray, isr)      // isr_nodes
				partitionsArray = append(partitionsArray, 0x01, 0x01, 0x01)          // eligible_leader_replicas, last_known_elr, offline_replicas
				partitionsArray = append(partitionsArray, 0x00)                      // TAG_BUFFER
			} else {
				partitionsArray = []byte{0x01} // empty partitions array
			}

			responseBody = append(responseBody, byte(errorCode>>8), byte(errorCode)) // error_code
			topicNameLen := len(topicName)
			responseBody = append(responseBody, byte(topicNameLen+1))
			responseBody = append(responseBody, []byte(topicName)...)
			responseBody = append(responseBody, topicID[:]...) // topic_id
			responseBody = append(responseBody, 0x00)          // is_internal = 0
			responseBody = append(responseBody, partitionsArray...)
			responseBody = append(responseBody, 0x00, 0x00, 0x00, 0x00) // topic_authorized_operations
			responseBody = append(responseBody, 0x00)                   // topic TAG_BUFFER
			responseBody = append(responseBody, 0xFF)                   // next_cursor = NULLABLE_INT8 = -1
			responseBody = append(responseBody, 0x00)                   // response TAG_BUFFER

			messageSize := uint32(5 + len(responseBody)) // header v1 (5) + body
			sizeOut := make([]byte, 4)
			binary.BigEndian.PutUint32(sizeOut, messageSize)
			if _, err := conn.Write(sizeOut); err != nil {
				fmt.Println("Error writing message size: ", err.Error())
				break
			}

			// Header v1: correlation_id (4 bytes) + TAG_BUFFER (1 byte)
			header := make([]byte, 5)
			binary.BigEndian.PutUint32(header[0:4], correlationID)
			header[4] = 0x00 // TAG_BUFFER is empty
			if _, err := conn.Write(header); err != nil {
				fmt.Println("Error writing response header: ", err.Error())
				break
			}
			if _, err := conn.Write(responseBody); err != nil {
				fmt.Println("Error writing response body: ", err.Error())
				break
			}
		} else {
			// ApiVersions (or other) response
			// Compute error_code: 35 if not in 0-4, else 0
			var errorCode uint16
			if requestAPIVersion > 4 {
				errorCode = 35
			} else {
				errorCode = 0
			}
			// Build ApiVersions v4 response body:
			responseBody := []byte{
				byte(errorCode >> 8), byte(errorCode), // error_code (2)
				0x03,       // api_keys compact array length (2 elements, zigzag encoded)
				0x00, 0x12, // api_key 18
				0x00, 0x00, // min_version 0
				0x00, 0x04, // max_version 4
				0x00,       // element tag buffer
				0x00, 0x4B, // api_key 75
				0x00, 0x00, // min_version 0
				0x00, 0x00, // max_version 0
				0x00,                   // element tag buffer
				0x00, 0x00, 0x00, 0x00, // throttle_time_ms
				0x00, // response tag buffer
			}
			messageSize := uint32(4 + len(responseBody)) // header v0 (correlation_id only) + body
			sizeOut := make([]byte, 4)
			binary.BigEndian.PutUint32(sizeOut, messageSize)
			if _, err := conn.Write(sizeOut); err != nil {
				fmt.Println("Error writing message size: ", err.Error())
				break
			}
			header := make([]byte, 4)
			binary.BigEndian.PutUint32(header[0:4], correlationID)
			if _, err := conn.Write(header); err != nil {
				fmt.Println("Error writing response header: ", err.Error())
				break
			}
			if _, err := conn.Write(responseBody); err != nil {
				fmt.Println("Error writing response body: ", err.Error())
				break
			}
		}
	}
}
