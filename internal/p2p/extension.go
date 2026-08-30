package p2p

import (
	"bytes"
	"fmt"
	"github.com/jackpal/bencode-go"
)

// ExtHandshake represents the payload for the BEP 10 extended handshake (msg ID 20, ext ID 0).
type ExtHandshake struct {
	M            map[string]int `bencode:"m"` // Mapping of extension name to ID
	MetadataSize int            `bencode:"metadata_size,omitempty"`
}

// ExtMetadataMsg represents the payload for a ut_metadata message (BEP 9).
type ExtMetadataMsg struct {
	MsgType   int `bencode:"msg_type"` // 0: request, 1: data, 2: reject
	Piece     int `bencode:"piece"`
	TotalSize int `bencode:"total_size,omitempty"`
}

// FormatExtHandshake creates a serialized extended handshake message.
func FormatExtHandshake() (*Message, error) {
	handshake := ExtHandshake{
		M: map[string]int{
			"ut_metadata": 1,
		},
	}
	var buf bytes.Buffer
	buf.WriteByte(0) // Extended Message ID 0 for handshake
	if err := bencode.Marshal(&buf, handshake); err != nil {
		return nil, err
	}
	return &Message{
		ID:      MsgExtended,
		Payload: buf.Bytes(),
	}, nil
}

// ParseExtHandshake parses an extended handshake message payload.
func ParseExtHandshake(payload []byte) (*ExtHandshake, error) {
	if len(payload) < 1 {
		return nil, fmt.Errorf("payload too short")
	}
	if payload[0] != 0 {
		return nil, fmt.Errorf("not an extended handshake")
	}

	var extHandshake ExtHandshake
	err := bencode.Unmarshal(bytes.NewReader(payload[1:]), &extHandshake)
	return &extHandshake, err
}

// FormatMetadataRequest creates a ut_metadata request message.
func FormatMetadataRequest(extID int, piece int) (*Message, error) {
	req := ExtMetadataMsg{
		MsgType: 0,
		Piece:   piece,
	}
	var buf bytes.Buffer
	buf.WriteByte(byte(extID))
	if err := bencode.Marshal(&buf, req); err != nil {
		return nil, err
	}
	return &Message{
		ID:      MsgExtended,
		Payload: buf.Bytes(),
	}, nil
}
