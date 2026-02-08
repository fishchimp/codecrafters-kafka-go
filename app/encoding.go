package main

import (
	"encoding/binary"
	"io"
)

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
	v := int64(u>>1) ^ -int64(u&1)
	return int(v), nil
}

func readUvarint(data []byte, idx *int) (uint64, error) {
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
	return u, nil
}

func readInt16(data []byte, idx *int) (int16, bool) {
	if *idx+2 > len(data) {
		return 0, false
	}
	v := int16(binary.BigEndian.Uint16(data[*idx : *idx+2]))
	*idx += 2
	return v, true
}

func readInt32(data []byte, idx *int) (int32, bool) {
	if *idx+4 > len(data) {
		return 0, false
	}
	v := int32(binary.BigEndian.Uint32(data[*idx : *idx+4]))
	*idx += 4
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

func readCompactString(data []byte, idx *int) (string, bool) {
	nPlus, err := readUvarint(data, idx)
	if err != nil {
		return "", false
	}
	if nPlus == 0 {
		return "", false
	}
	n := int(nPlus - 1)
	if *idx+n > len(data) {
		return "", false
	}
	s := string(data[*idx : *idx+n])
	*idx += n
	return s, true
}

func readCompactInt32Array(data []byte, idx *int) ([]int32, bool) {
	nPlus, err := readUvarint(data, idx)
	if err != nil {
		return nil, false
	}
	if nPlus == 0 {
		return nil, false
	}
	n := int32(nPlus - 1)
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

func readCompactUUIDArray(data []byte, idx *int) (int, bool) {
	nPlus, err := readUvarint(data, idx)
	if err != nil {
		return 0, false
	}
	if nPlus == 0 {
		return 0, false
	}
	n := int(nPlus - 1)
	for i := 0; i < n; i++ {
		if _, ok := readUUID(data, idx); !ok {
			return 0, false
		}
	}
	return n, true
}

func skipTaggedFields(data []byte, idx *int) bool {
	numTags, err := readUvarint(data, idx)
	if err != nil {
		return false
	}
	for i := uint64(0); i < numTags; i++ {
		if _, err := readUvarint(data, idx); err != nil { // tag id
			return false
		}
		size, err := readUvarint(data, idx)
		if err != nil {
			return false
		}
		if *idx+int(size) > len(data) {
			return false
		}
		*idx += int(size)
	}
	return true
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
		return append(b, 0x01)
	}
	b = append(b, byte(len(arr)+1))
	for _, v := range arr {
		b = appendInt32(b, v)
	}
	return b
}
