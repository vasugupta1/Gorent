package p2p

import (
	"fmt"
	"io"
)

// Handshake is a special message that a peer uses to identify itself.
type Handshake struct {
	Pstr     string
	InfoHash [20]byte
	PeerID   [20]byte
	Reserved [8]byte
}

// NewHandshake creates a new handshake with the extension bit set for BEP 10.
func NewHandshake(infoHash, peerID [20]byte) *Handshake {
	h := &Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: infoHash,
		PeerID:   peerID,
	}
	// Set the 20th bit from the right (byte index 5, bit index 4) to 1 to signal Extension Protocol (BEP 10).
	h.Reserved[5] |= 0x10
	return h
}

// Serialize serializes the handshake to a byte slice.
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, len(h.Pstr)+49)
	buf[0] = byte(len(h.Pstr))
	curr := 1
	curr += copy(buf[curr:], h.Pstr)
	curr += copy(buf[curr:], h.Reserved[:])
	curr += copy(buf[curr:], h.InfoHash[:])
	curr += copy(buf[curr:], h.PeerID[:])
	return buf
}

// ReadHandshake reads a handshake from an io.Reader.
func ReadHandshake(r io.Reader) (*Handshake, error) {
	lengthBuf := make([]byte, 1)
	_, err := io.ReadFull(r, lengthBuf)
	if err != nil {
		return nil, err
	}
	pstrlen := int(lengthBuf[0])

	if pstrlen == 0 {
		return nil, fmt.Errorf("pstrlen cannot be 0")
	}

	handshakeBuf := make([]byte, 48+pstrlen)
	_, err = io.ReadFull(r, handshakeBuf)
	if err != nil {
		return nil, err
	}

	var infoHash, peerID [20]byte
	var reserved [8]byte

	pstr := string(handshakeBuf[0:pstrlen])
	copy(reserved[:], handshakeBuf[pstrlen:pstrlen+8])
	copy(infoHash[:], handshakeBuf[pstrlen+8:pstrlen+28])
	copy(peerID[:], handshakeBuf[pstrlen+28:pstrlen+48])

	return &Handshake{
		Pstr:     pstr,
		InfoHash: infoHash,
		PeerID:   peerID,
		Reserved: reserved,
	}, nil
}
