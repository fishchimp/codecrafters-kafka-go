package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
)

const fallbackClusterMetadataLogPath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

var clusterMetadataLogPaths []string

type topicResult struct {
	meta  *TopicMetadata
	found bool
}

func main() {
	fmt.Println("Logs from your program will appear here!")
	clusterMetadataLogPaths = discoverClusterMetadataLogPaths(os.Args)
	if err := loadClusterMetadataLogs(clusterMetadataLogPaths); err != nil {
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

		// Parse topics if DescribeTopicPartitions (API key 75)
		var requestedTopics []string
		topicResults := make(map[string]topicResult)
		var responsePartitionLimit int32
		if requestAPIKey == 75 {
			idx := bodyIdx
			topicsNPlus, err := readUvarint(payload, &idx)
			if err != nil || topicsNPlus == 0 {
				fmt.Println("Invalid topics compact array length")
				break
			}
			topicsCount := int(topicsNPlus - 1)
			requestedTopics = make([]string, 0, topicsCount)

			for i := 0; i < topicsCount; i++ {
				topicName, ok := readCompactString(payload, &idx)
				if !ok {
					fmt.Println("Invalid topic_name in topics array")
					break
				}
				if !skipTaggedFields(payload, &idx) {
					fmt.Println("Invalid topic TAG_BUFFER")
					break
				}
				requestedTopics = append(requestedTopics, topicName)
			}
			if len(requestedTopics) != topicsCount {
				break
			}

			if len(payload) < idx+4 {
				fmt.Println("Payload too short for response_partition_limit")
				break
			}
			responsePartitionLimit = int32(binary.BigEndian.Uint32(payload[idx : idx+4]))
			idx += 4

			if len(payload) <= idx {
				fmt.Println("Payload too short for cursor")
				break
			}
			cursorPresent := int8(payload[idx])
			idx++
			if cursorPresent != -1 {
				if _, ok := readCompactString(payload, &idx); !ok {
					fmt.Println("Invalid cursor topic_name")
					break
				}
				if len(payload) < idx+4 {
					fmt.Println("Payload too short for cursor partition_index")
					break
				}
				idx += 4
				if !skipTaggedFields(payload, &idx) {
					fmt.Println("Invalid cursor TAG_BUFFER")
					break
				}
			}

			if !skipTaggedFields(payload, &idx) {
				fmt.Println("Invalid request TAG_BUFFER")
				break
			}
			fmt.Printf("Parsed topic names: %v\n", requestedTopics)

			needsReload := false
			for _, topicName := range requestedTopics {
				meta, found := topicMap[topicName]
				topicResults[topicName] = topicResult{meta: meta, found: found}
				if !found || meta == nil || len(meta.Partitions) == 0 {
					needsReload = true
				}
			}
			if needsReload {
				clusterMetadataLogPaths = discoverClusterMetadataLogPaths(os.Args)
				if err := loadClusterMetadataLogs(clusterMetadataLogPaths); err != nil {
					fmt.Printf("Metadata reload failed: %v\n", err)
				}
				for _, topicName := range requestedTopics {
					meta, found := topicMap[topicName]
					topicResults[topicName] = topicResult{meta: meta, found: found}
				}
			}
			for _, topicName := range requestedTopics {
				res := topicResults[topicName]
				if res.found && res.meta != nil && len(res.meta.Partitions) > 0 {
					continue
				}
				meta, ok, err := lookupTopicMetadataFromLogs(clusterMetadataLogPaths, topicName)
				if err != nil {
					fmt.Printf("Metadata direct lookup failed for topic %s: %v\n", topicName, err)
					continue
				}
				if ok && meta != nil {
					topicMap[topicName] = meta
					topicResults[topicName] = topicResult{meta: meta, found: true}
				}
			}
			for _, topicName := range requestedTopics {
				res := topicResults[topicName]
				if res.found && res.meta != nil {
					fmt.Printf("Loaded topic metadata: %s partitions=%d\n", topicName, len(res.meta.Partitions))
				}
			}
			_ = responsePartitionLimit
		}

		if requestAPIKey == 75 {
			responseBody := make([]byte, 0)
			responseBody = append(responseBody, 0x00, 0x00, 0x00, 0x00) // throttle_time_ms = 0

			sortedTopics := make([]string, len(requestedTopics))
			copy(sortedTopics, requestedTopics)
			sort.Strings(sortedTopics)

			responseBody = append(responseBody, byte(len(sortedTopics)+1)) // topics COMPACT_ARRAY
			for _, topicName := range sortedTopics {
				res := topicResults[topicName]

				var errorCode uint16 = 3 // unknown topic
				var topicID [16]byte
				partitionsArray := []byte{0x01} // empty partitions array

				if res.found && res.meta != nil {
					errorCode = 0
					topicID = res.meta.UUID
					partitionByID := make(map[int32]PartitionMetadata)
					for _, p := range res.meta.Partitions {
						partitionByID[p.ID] = p
					}
					partitionIDs := make([]int32, 0, len(partitionByID))
					for id := range partitionByID {
						partitionIDs = append(partitionIDs, id)
					}
					sort.Slice(partitionIDs, func(i, j int) bool {
						return partitionIDs[i] < partitionIDs[j]
					})
					fmt.Printf("DescribeTopicPartitions %s partition_ids=%v\n", topicName, partitionIDs)

					partitionsArray = make([]byte, 0)
					partitionsArray = append(partitionsArray, byte(len(partitionIDs)+1)) // compact array len
					for _, partitionID := range partitionIDs {
						p := partitionByID[partitionID]
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
					}
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
			}
			responseBody = append(responseBody, 0xFF) // next_cursor = NULLABLE_INT8 = -1
			responseBody = append(responseBody, 0x00) // response TAG_BUFFER

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
