package tracker

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jackpal/bencode-go"
	"github.com/vasugupta1/Gorent/internal/peer"
)

type HTTPTracker struct {
	URL string
}

type TrackerResponse struct {
	Interval int    `bencode:"interval"`
	Peers    string `bencode:"peers"`
}

// Announce sends an announce request to the tracker.
func (t *HTTPTracker) Announce(infoHash, peerID [20]byte, port uint16, uploaded, downloaded, left int) ([]peer.Peer, error) {
	u, err := url.Parse(t.URL)
	if err != nil {
		return nil, err
	}

	params := url.Values{
		"info_hash":  []string{string(infoHash[:])},
		"peer_id":    []string{string(peerID[:])},
		"port":       []string{strconv.Itoa(int(port))},
		"uploaded":   []string{strconv.Itoa(uploaded)},
		"downloaded": []string{strconv.Itoa(downloaded)},
		"left":       []string{strconv.Itoa(left)},
		"compact":    []string{"1"},
	}
	u.RawQuery = params.Encode()

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(u.String())
	if err != nil {
		return nil, fmt.Errorf("tracker request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tracker returned non-200 status: %d", resp.StatusCode)
	}

	var tr TrackerResponse
	if err := bencode.Unmarshal(resp.Body, &tr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tracker response: %w", err)
	}

	return unmarshalPeers([]byte(tr.Peers))
}

func unmarshalPeers(peersBin []byte) ([]peer.Peer, error) {
	const peerSize = 6 // 4 bytes for IP, 2 bytes for port
	if len(peersBin)%peerSize != 0 {
		return nil, fmt.Errorf("received malformed peers string")
	}

	numPeers := len(peersBin) / peerSize
	peers := make([]peer.Peer, numPeers)

	for i := 0; i < numPeers; i++ {
		offset := i * peerSize
		ip := fmt.Sprintf("%d.%d.%d.%d", peersBin[offset], peersBin[offset+1], peersBin[offset+2], peersBin[offset+3])
		port := binary.BigEndian.Uint16(peersBin[offset+4 : offset+6])
		peers[i] = peer.Peer{
			IP:   ip,
			Port: port,
		}
	}

	return peers, nil
}
