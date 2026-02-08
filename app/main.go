package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// TopicMetadata holds topic uuid and partitions.
type TopicMetadata struct {
	UUID       [16]byte
	Partitions []PartitionMetadata
}

type PartitionMetadata struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
}

// Global in-memory map: topic_name -> TopicMetadata.
var topicMap = make(map[string]*TopicMetadata)

// Reads cluster metadata log and populates topicMap.
func loadClusterMetadataLog(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Map from topic UUID -> name (filled by TopicRecord).
	topicByID := make(map[[16]byte]string)
	// Map from topic UUID -> metadata (for early PartitionRecords).
	topicByUUID := make(map[[16]byte]*TopicMetadata)

	for off := 0; off+12 <= len(data); {
		baseOffset := binary.BigEndian.Uint64(data[off : off+8])
		_ = baseOffset
		batchLen := int(binary.BigEndian.Uint32(data[off+8 : off+12]))
		if batchLen <= 0 || off+12+batchLen > len(data) {
			break
		}
		batchEnd := off + 12 + batchLen
		off += 12

		if off+49 > len(data) {
			break
		}
		partitionLeaderEpoch := binary.BigEndian.Uint32(data[off : off+4])
		_ = partitionLeaderEpoch
		magic := data[off+4]
		if magic != 2 {
			off = batchEnd
			continue
		}
		// Skip to recordsCount.
		recordsCount := binary.BigEndian.Uint32(data[off+45 : off+49])
		off += 49

		for i := uint32(0); i < recordsCount && off < batchEnd; i++ {
			recordLen, err := readVarintZigZag(data, &off)
			if err != nil || recordLen <= 0 {
				break
			}
			recordEnd := off + recordLen
			if recordEnd > batchEnd {
				break
			}
			// attributes
			off++
			// timestampDelta, offsetDelta
			if _, err := readVarintZigZag(data, &off); err != nil {
				break
			}
			if _, err := readVarintZigZag(data, &off); err != nil {
				break
			}
			// key
			keyLen, err := readVarintZigZag(data, &off)
			if err != nil {
				break
			}
			if keyLen >= 0 {
				if off+keyLen > recordEnd {
					break
				}
				off += keyLen
			}
			// value
			valLen, err := readVarintZigZag(data, &off)
			if err != nil {
				break
			}
			var val []byte
			if valLen >= 0 {
				if off+valLen > recordEnd {
					break
				}
				val = data[off : off+valLen]
				off += valLen
			}
			// headers
			hCount, err := readVarintZigZag(data, &off)
			if err != nil {
				break
			}
			for h := 0; h < hCount; h++ {
				hKeyLen, err := readVarintZigZag(data, &off)
				if err != nil {
					break
				}
				if hKeyLen > 0 {
					off += hKeyLen
				}
				hValLen, err := readVarintZigZag(data, &off)
				if err != nil {
					break
				}
				if hValLen > 0 {
					off += hValLen
				}
			}

			if len(val) > 0 {
				parseMetadataRecord(val, topicByID, topicByUUID)
			}

			off = recordEnd
		}
		off = batchEnd
	}
	return nil
}

func main() {
	fmt.Println("Logs from your program will appear here!")
	_ = loadClusterMetadataLog("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")

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

func readVarintZigZag(data []byte, idx *int) (int, error) {
	var u uint64
	var shift uint
	for {
		if *idx >= len(data) {
			return 0, io.ErrUnexpectedEOF
		}
		b := data[*idx]
		*idx++
		u |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 63 {
			return 0, io.ErrUnexpectedEOF
		}
	}
	// ZigZag decode
	v := int64(u>>1) ^ -int64(u&1)
	return int(v), nil
}

func readInt32(data []byte, idx *int) (int32, bool) {
	if *idx+4 > len(data) {
		return 0, false
	}
	v := int32(binary.BigEndian.Uint32(data[*idx : *idx+4]))
	*idx += 4
	return v, true
}

func readInt16(data []byte, idx *int) (int16, bool) {
	if *idx+2 > len(data) {
		return 0, false
	}
	v := int16(binary.BigEndian.Uint16(data[*idx : *idx+2]))
	*idx += 2
	return v, true
}

func readUUID(data []byte, idx *int) ([16]byte, bool) {
	var u [16]byte
	if *idx+16 > len(data) {
		return u, false
	}
	copy(u[:], data[*idx:*idx+16])
	*idx += 16
	return u, true
}

func readInt32Array(data []byte, idx *int) ([]int32, bool) {
	n, ok := readInt32(data, idx)
	if !ok || n < 0 {
		return nil, false
	}
	arr := make([]int32, n)
	for i := int32(0); i < n; i++ {
		v, ok := readInt32(data, idx)
		if !ok {
			return nil, false
		}
		arr[i] = v
	}
	return arr, true
}

func parseMetadataRecord(val []byte, topicByID map[[16]byte]string, topicByUUID map[[16]byte]*TopicMetadata) {
	if len(val) < 2 {
		return
	}
	rtype := -1
	version := -1
	idx := 0
	if (val[0] == 2 || val[0] == 3) && (val[1] == 0 || val[1] == 1) {
		rtype = int(val[0])
		version = int(val[1])
		idx = 2
	} else if len(val) >= 4 {
		rt := int(binary.BigEndian.Uint16(val[0:2]))
		ver := int(binary.BigEndian.Uint16(val[2:4]))
		if (rt == 2 || rt == 3) && (ver == 0 || ver == 1) {
			rtype = rt
			version = ver
			idx = 4
		}
	}
	if rtype == -1 || version != 0 {
		return
	}

	switch rtype {
	case 2: // TopicRecord
		nameLen, ok := readInt16(val, &idx)
		if !ok || nameLen < 0 {
			return
		}
		if idx+int(nameLen) > len(val) {
			return
		}
		name := string(val[idx : idx+int(nameLen)])
		idx += int(nameLen)
		uuid, ok := readUUID(val, &idx)
		if !ok {
			return
		}
		meta, ok := topicByUUID[uuid]
		if !ok {
			meta = &TopicMetadata{}
			topicByUUID[uuid] = meta
		}
		meta.UUID = uuid
		topicMap[name] = meta
		topicByID[uuid] = name
	case 3: // PartitionRecord
		partitionID, ok := readInt32(val, &idx)
		if !ok {
			return
		}
		topicID, ok := readUUID(val, &idx)
		if !ok {
			return
		}
		replicas, ok := readInt32Array(val, &idx)
		if !ok {
			return
		}
		isr, ok := readInt32Array(val, &idx)
		if !ok {
			return
		}
		if _, ok := readInt32Array(val, &idx); !ok { // removingReplicas
			return
		}
		if _, ok := readInt32Array(val, &idx); !ok { // addingReplicas
			return
		}
		leader, ok := readInt32(val, &idx)
		if !ok {
			return
		}
		if _, ok := readInt32(val, &idx); !ok { // leaderEpoch
			return
		}
		if _, ok := readInt32(val, &idx); !ok { // partitionEpoch
			return
		}
		// directories (array of UUIDs) - skip
		dirCount, ok := readInt32(val, &idx)
		if !ok || dirCount < 0 {
			return
		}
		for i := int32(0); i < dirCount; i++ {
			if _, ok := readUUID(val, &idx); !ok {
				return
			}
		}

		meta, ok := topicByUUID[topicID]
		if !ok {
			meta = &TopicMetadata{UUID: topicID}
			topicByUUID[topicID] = meta
		}
		if name, ok := topicByID[topicID]; ok {
			topicMap[name] = meta
		}
		meta.Partitions = append(meta.Partitions, PartitionMetadata{
			ID:       partitionID,
			Leader:   leader,
			Replicas: replicas,
			ISR:      isr,
		})
	}
}

func appendInt16(b []byte, v int16) []byte {
	return append(b, byte(v>>8), byte(v))
}

func appendInt32(b []byte, v int32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendCompactInt32Array(b []byte, arr []int32) []byte {
	if len(arr) == 0 {
		return append(b, 0x01)
	}
	if len(arr) > 126 {
		// not expected in this stage; fall back to empty
		return append(b, 0x01)
	}
	b = append(b, byte(len(arr)+1))
	for _, v := range arr {
		b = appendInt32(b, v)
	}
	return b
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
		}

		if requestAPIKey == 75 {
			// Build DescribeTopicPartitions v0 response body
			responseBody := make([]byte, 0)
			responseBody = append(responseBody, 0x00, 0x00, 0x00, 0x00) // throttle_time_ms = 0
			responseBody = append(responseBody, 0x02) // topics COMPACT_ARRAY with 1 element

			// Topic element:
			var errorCode uint16 = 3 // unknown topic
			var topicID [16]byte
			var partitionsArray []byte
			if topicFound && topicMeta != nil {
				errorCode = 0
				topicID = topicMeta.UUID
				partitionsArray = make([]byte, 0)
				partitionsArray = append(partitionsArray, byte(len(topicMeta.Partitions)+1)) // compact array length
				if len(topicMeta.Partitions) == 0 {
					partitionsArray = []byte{0x01}
				} else {
					for _, p := range topicMeta.Partitions {
						partitionsArray = appendInt16(partitionsArray, 0)    // error_code
						partitionsArray = appendInt32(partitionsArray, p.ID) // partition_index

						leaderID := p.Leader
						if leaderID <= 0 && len(p.Replicas) > 0 {
							leaderID = p.Replicas[0]
						}
						if leaderID <= 0 {
							leaderID = 1
						}

						partitionsArray = appendInt32(partitionsArray, leaderID) // leader_id
						partitionsArray = appendInt32(partitionsArray, 0)        // leader_epoch

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
						partitionsArray = append(partitionsArray, 0x01, 0x01, 0x01)           // eligible_leader_replicas, last_known_elr, offline_replicas
						partitionsArray = append(partitionsArray, 0x00)                       // TAG_BUFFER
					}
				}
			} else {
				partitionsArray = []byte{0x01} // empty partitions array
			}

			responseBody = append(responseBody, byte(errorCode>>8), byte(errorCode)) // error_code
			topicNameLen := len(topicName)
			responseBody = append(responseBody, byte(topicNameLen+1))
			responseBody = append(responseBody, []byte(topicName)...)
			responseBody = append(responseBody, topicID[:]...) // topic_id
			responseBody = append(responseBody, 0x00) // is_internal = 0
			responseBody = append(responseBody, partitionsArray...)
			responseBody = append(responseBody, 0x00, 0x00, 0x00, 0x00) // topic_authorized_operations
			responseBody = append(responseBody, 0x00) // topic TAG_BUFFER
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
				0x03, // api_keys compact array length (2 elements, zigzag encoded)
				0x00, 0x12, // api_key 18
				0x00, 0x00, // min_version 0
				0x00, 0x04, // max_version 4
				0x00, // element tag buffer
				0x00, 0x4B, // api_key 75
				0x00, 0x00, // min_version 0
				0x00, 0x00, // max_version 0
				0x00, // element tag buffer
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
