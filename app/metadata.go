package main

import (
	"encoding/binary"
	"os"
)

func loadClusterMetadataLog(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Map from topic UUID -> name (for partition records seen before topic records).
	topicByID := make(map[[16]byte]string)
	topicByUUID := make(map[[16]byte]*TopicMetadata)

	for off := 0; off+12 <= len(data); {
		batchLen := int(binary.BigEndian.Uint32(data[off+8 : off+12]))
		if batchLen <= 0 || off+12+batchLen > len(data) {
			break
		}
		batchEnd := off + 12 + batchLen
		off += 12

		if off+49 > len(data) {
			break
		}
		magic := data[off+4]
		if magic != 2 {
			off = batchEnd
			continue
		}
		recordsCount := binary.BigEndian.Uint32(data[off+45 : off+49])
		off += 49

		for i := uint32(0); i < recordsCount && off < batchEnd; i++ {
			// Record length is unsigned varint (not zigzag).
			recordLenU, err := readUvarint(data, &off)
			if err != nil {
				break
			}
			recordLen := int(recordLenU)
			if recordLen <= 0 {
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
			var key []byte
			if keyLen >= 0 {
				if off+keyLen > recordEnd {
					break
				}
				key = data[off : off+keyLen]
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
				parseMetadataRecord(key, val, topicByID, topicByUUID)
			}

			off = recordEnd
		}
		off = batchEnd
	}
	return nil
}

func parseMetadataRecord(key []byte, val []byte, topicByID map[[16]byte]string, topicByUUID map[[16]byte]*TopicMetadata) {
	if len(val) < 2 && len(key) < 2 {
		return
	}
	rtype := -1
	version := -1
	idx := 0
	// Prefer type/version from record key if present.
	if len(key) >= 4 {
		rt := int(binary.BigEndian.Uint16(key[0:2]))
		ver := int(binary.BigEndian.Uint16(key[2:4]))
		if (rt == 2 || rt == 3) && (ver == 0 || ver == 1) {
			rtype = rt
			version = ver
			idx = 0
		}
	} else if len(key) == 2 {
		rt := int(binary.BigEndian.Uint16(key[0:2]))
		if rt == 2 || rt == 3 {
			rtype = rt
			version = 0
			idx = 0
		}
	} else if len(key) == 1 {
		rt := int(key[0])
		if rt == 2 || rt == 3 {
			rtype = rt
			version = 0
			idx = 0
		}
	}
	// Fallback to prefix in value if key didn't provide it.
	if rtype == -1 {
		if len(val) >= 2 && (val[0] == 2 || val[0] == 3) && (val[1] == 0 || val[1] == 1) {
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
	}
	if rtype == -1 || (version != 0 && version != 1) {
		return
	}

	switch rtype {
	case 2: // TopicRecord v0/v1
		var name string
		var ok bool
		if version == 0 {
			nameLen, ok := readInt16(val, &idx)
			if !ok || nameLen < 0 {
				return
			}
			if idx+int(nameLen) > len(val) {
				return
			}
			name = string(val[idx : idx+int(nameLen)])
			idx += int(nameLen)
		} else {
			name, ok = readCompactString(val, &idx)
			if !ok {
				return
			}
		}
		uuid, ok := readUUID(val, &idx)
		if !ok {
			return
		}
		if version == 1 {
			if !skipTaggedFields(val, &idx) {
				return
			}
		}
		meta, ok := topicByUUID[uuid]
		if !ok {
			meta = &TopicMetadata{}
			topicByUUID[uuid] = meta
		}
		meta.UUID = uuid
		topicMap[name] = meta
		topicByID[uuid] = name
	case 3: // PartitionRecord v0/v1
		partitionID, ok := readInt32(val, &idx)
		if !ok {
			return
		}
		topicID, ok := readUUID(val, &idx)
		if !ok {
			return
		}
		var replicas []int32
		var isr []int32
		if version == 0 {
			replicas, ok = readInt32Array(val, &idx)
			if !ok {
				return
			}
			isr, ok = readInt32Array(val, &idx)
			if !ok {
				return
			}
			if _, ok := readInt32Array(val, &idx); !ok { // removingReplicas
				return
			}
			if _, ok := readInt32Array(val, &idx); !ok { // addingReplicas
				return
			}
		} else {
			replicas, ok = readCompactInt32Array(val, &idx)
			if !ok {
				return
			}
			isr, ok = readCompactInt32Array(val, &idx)
			if !ok {
				return
			}
			if _, ok := readCompactInt32Array(val, &idx); !ok { // removingReplicas
				return
			}
			if _, ok := readCompactInt32Array(val, &idx); !ok { // addingReplicas
				return
			}
		}
		leader, ok := readInt32(val, &idx)
		if !ok {
			return
		}
		leaderEpoch, ok := readInt32(val, &idx)
		if !ok {
			return
		}
		if _, ok := readInt32(val, &idx); !ok { // partitionEpoch
			return
		}
		if version == 0 {
			dirCount, ok := readInt32(val, &idx)
			if !ok || dirCount < 0 {
				return
			}
			for i := int32(0); i < dirCount; i++ {
				if _, ok := readUUID(val, &idx); !ok {
					return
				}
			}
		} else {
			if _, ok := readCompactUUIDArray(val, &idx); !ok {
				return
			}
			if !skipTaggedFields(val, &idx) {
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
			ID:          partitionID,
			Leader:      leader,
			LeaderEpoch: leaderEpoch,
			Replicas:    replicas,
			ISR:         isr,
		})
	}
}
