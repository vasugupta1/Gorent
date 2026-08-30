package peer

import (
	"crypto/rand"
	"fmt"
)

// GeneratePeerID generates a random 20-byte peer ID.
// We use the Azureus-style encoding: '-', two-character client id, four-character version, '-', followed by random characters.
func GeneratePeerID() ([20]byte, error) {
	var peerID [20]byte
	copy(peerID[:], []byte("-GR0001-")) // GR for Gorent, 0001 for version 0.0.1
	_, err := rand.Read(peerID[8:])
	if err != nil {
		return [20]byte{}, fmt.Errorf("failed to generate random peer id: %w", err)
	}
	return peerID, nil
}

// Peer represents a remote BitTorrent peer.
type Peer struct {
	IP   string
	Port uint16
}

// String returns the IP:Port string of the peer.
func (p Peer) String() string {
	return fmt.Sprintf("%s:%d", p.IP, p.Port)
}
