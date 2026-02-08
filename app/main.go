package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

// TopicMetadata holds topic uuid and partition ids
// uuid is 16 bytes (as []byte), partitions is a slice of int32
//
type TopicMetadata struct {
	UUID       [16]byte
	Partitions []int32
}

// Global in-memory map: topic_name -> TopicMetadata
var topicMap = make(map[string]*TopicMetadata)

// Reads cluster metadata log and populates topicMap
func loadClusterMetadataLog(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		head := make([]byte, 12) // offset(8) + size(4)
		_, err := io.ReadFull(f, head)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		recordSize := binary.BigEndian.Uint32(head[8:12])
		record := make([]byte, recordSize)
		_, err = io.ReadFull(f, record)
		if err != nil {
			return err
		}
		// Kafka log record: magic(1), attributes(1), timestampDelta(4), offsetDelta(4), keyLen(4), key, valueLen(4), value
		if len(record) < 18 {
			continue
		}
		keyLen := int(binary.BigEndian.Uint32(record[10:14]))
		if len(record) < 14+keyLen+4 {
			continue
		}
		key := record[14 : 14+keyLen]
		valLen := int(binary.BigEndian.Uint32(record[14+keyLen : 14+keyLen+4]))
		if len(record) < 14+keyLen+4+valLen {
			continue
		}
		val := record[14+keyLen+4 : 14+keyLen+4+valLen]
		// Topic record: key starts with "__topic__"
		if strings.HasPrefix(string(key), "__topic__") && len(val) >= 16 {
			// topic name after prefix
			name := string(key[len("__topic__") : ])
			var uuid [16]byte
			copy(uuid[:], val[:16])
			if _, ok := topicMap[name]; !ok {
				topicMap[name] = &TopicMetadata{UUID: uuid}
			} else {
				topicMap[name].UUID = uuid
			}
			continue
		}
		// Partition record: key starts with "__partition__" + topic + partition id
		if strings.HasPrefix(string(key), "__partition__") {
			rem := key[len("__partition__"):]
			// Find last '-' (topic-partition)
			sep := strings.LastIndexByte(string(rem), '-')
			if sep < 0 {
				continue
			}
			topic := string(rem[:sep])
			partID := rem[sep+1:]
			pid := int32(0)
			for _, b := range partID {
				if b < '0' || b > '9' {
					pid = -1
					break
				}
				pid = pid*10 + int32(b-'0')
			}
			if pid < 0 {
				continue
			}
			meta, ok := topicMap[topic]
			if !ok {
				meta = &TopicMetadata{}
				topicMap[topic] = meta
			}
			found := false
			for _, p := range meta.Partitions {
				if p == pid {
					found = true
					break
				}
			}
			if !found {
				meta.Partitions = append(meta.Partitions, pid)
			}
		}
	}
	return nil
}

func main() {
	fmt.Println("Logs from your program will appear here!")
	_ = loadClusterMetadataLog("00000000000000000000.log") // ignore error for now

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
				// Build partitions COMPACT_ARRAY with 1 partition
				partitionsArray = make([]byte, 0)
				partitionsArray = append(partitionsArray, 0x02) // length byte (1 element)
				// Partition element:
				partitionsArray = append(partitionsArray, 0x00, 0x00) // error_code = 0
				partitionsArray = append(partitionsArray, 0x00, 0x00, 0x00, 0x00) // partition_index = 0
				partitionsArray = append(partitionsArray, 0x00, 0x00, 0x00, 0x00) // leader_id = 0
				partitionsArray = append(partitionsArray, 0x01) // replicas COMPACT_ARRAY (1 element)
				partitionsArray = append(partitionsArray, 0x00, 0x00, 0x00, 0x00) // replica = 0
				partitionsArray = append(partitionsArray, 0x00) // element tag buffer
				partitionsArray = append(partitionsArray, 0x01) // isr COMPACT_ARRAY (1 element)
				partitionsArray = append(partitionsArray, 0x00, 0x00, 0x00, 0x00) // isr = 0
				partitionsArray = append(partitionsArray, 0x00) // element tag buffer
				partitionsArray = append(partitionsArray, 0x00) // partition tag buffer
			} else {
				// topic unknown, partitions empty
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

			messageSize := uint32(len(header) + len(responseBody)) // header v1 (5) + body
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
