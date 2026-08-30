package tracker

import (
	"fmt"
	"net/url"

	"github.com/vasugupta1/Gorent/internal/peer"
)

type Tracker interface {
	Announce(infoHash, peerID [20]byte, port uint16, uploaded, downloaded, left int) ([]peer.Peer, error)
}

func NewTracker(trackerURL string) (Tracker, error) {
	u, err := url.Parse(trackerURL)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "http" || u.Scheme == "https" {
		return &HTTPTracker{URL: trackerURL}, nil
	} else if u.Scheme == "udp" {
		return &UDPTracker{URL: trackerURL}, nil
	}
	return nil, fmt.Errorf("unsupported tracker scheme: %s", u.Scheme)
}
