/*
  userdata passed between group members

  Copyright 2016 MistSys
*/

package stable

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// data is the extra (UserData) information reported by each member. It contains the current partition assignments of the member
type data struct {
	version     uint8              // 1 is the current version
	assignments map[string][]int32 // map topic -> list of partitions currently assigned to the member
}

// marshal 'data' into a binary form, to be distributed
func (d *data) marshal() []byte {
	var buf = make([]byte, 1, 1+len(d.assignments)*10) // initial capacity is just a guess

	buf[0] = 1 // version 1

	buf = appendUint(buf, len(d.assignments))
	for topic, partitions := range d.assignments {
		buf = appendString(buf, topic)
		buf = appendInt32Slice(buf, partitions)
	}

	return buf
}

// unmarshal decodes what marshal encoded
func (d *data) unmarshal(buf []byte) error {
	if len(buf) < 1 {
		return errors.New("no data")
	}
	d.version = buf[0]
	if d.version != 1 {
		return fmt.Errorf("unknown version %d", d.version)
	}
	buf = buf[1:]

	buf, n, err := parseUint(buf)
	if err != nil {
		return err
	}
	nn := n
	if n > 1000 {
		nn = 1000 // avoid DoS from bad data
	}
	d.assignments = make(map[string][]int32, nn)

	for n > 0 {
		n--
		var topic string
		buf, topic, err = parseString(buf)
		if err != nil {
			return err
		}
		var partitions []int32
		buf, partitions, err = parseInt32Slice(buf)
		if err != nil {
			return err
		}
		d.assignments[topic] = partitions
	}

	return nil
}

// utility functions

func appendUint(buf []byte, x int) []byte {
	//dbgf("appendUint(%v)\n", x)
	// varint encoding, since it is small
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(x))
	return append(buf, tmp[:n]...)
}

func appendInt32(buf []byte, x int32) []byte {
	//dbgf("appendInt32(%v)\n", x)
	// varint encoding, since it is small
	var tmp [binary.MaxVarintLen32]byte
	n := binary.PutVarint(tmp[:], int64(x))
	return append(buf, tmp[:n]...)
}

func appendString(buf []byte, s string) []byte {
	//dbgf("appendString(%q)\n", s)
	// varint length, followed by the bytes of the string
	buf = appendUint(buf, len(s))
	return append(buf, s...)
}

func appendInt32Slice(buf []byte, xx []int32) []byte {
	//dbgf("appendInt32Slice(%v)\n", xx)
	buf = appendUint(buf, len(xx))
	for _, x := range xx {
		buf = appendInt32(buf, x)
	}
	return buf
}

func parseUint(buf []byte) ([]byte, int, error) {
	x, n := binary.Uvarint(buf)
	//dbgf("parseUint() %v, %v\n", x, n)
	if x == 0 && n <= 0 {
		// an error
		return buf, 0, errors.New("parse error")
	}
	return buf[n:], int(x), nil
}

func parseInt32(buf []byte) ([]byte, int32, error) {
	x, n := binary.Varint(buf)
	//dbgf("parseInt32() %v, %v\n", x, n)
	if x == 0 && n <= 0 {
		// an error
		return buf, 0, errors.New("parse error")
	}
	return buf[n:], int32(x), nil
}

func parseString(buf []byte) ([]byte, string, error) {
	buf, n, err := parseUint(buf)
	if err != nil {
		return buf, "", err
	}
	if len(buf) < n {
		return buf, "", fmt.Errorf("truncated string %d<%d; buf = % x", len(buf), n, buf)
	}
	//dbgf("parseString() %q\n", buf[:n])
	return buf[n:], string(buf[:n]), nil
}

func parseInt32Slice(buf []byte) ([]byte, []int32, error) {
	buf, n, err := parseUint(buf)
	if err != nil {
		return buf, nil, err
	}
	nn := n
	if n > 1000 {
		nn = 1000 // avoid DoS attach from 64-bit length causing huge allocation
	}
	s := make([]int32, 0, nn)
	for n > 0 {
		n--
		var x int32
		buf, x, err = parseInt32(buf)
		if err != nil {
			return buf, nil, err
		}
		s = append(s, x)
	}
	//dbgf("parseInt32Slice() %v\n", s)
	return buf, s, nil
}
