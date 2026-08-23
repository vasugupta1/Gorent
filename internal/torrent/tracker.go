package torrent

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jackpal/bencode-go"
	"github.com/vasugupta1/gorent/internal/peers"
)

type bencodeTrackerResp struct {
	Interval       int    `bencode:"interval"`
	Peers          string `bencode:"peers"`
	FailureReason  string `bencode:"failure reason,omitempty"`
	WarningMessage string `bencode:"warning message,omitempty"`
	Complete       int    `bencode:"complete,omitempty"`   // number of seeders
	Incomplete     int    `bencode:"incomplete,omitempty"` // number of leechers
}

func buildTrackerURL(tf *TorrentFile, peerId [20]byte, port uint16) (string, error) {
	base, err := url.Parse(tf.Announce)
	if err != nil {
		return "", err
	}

	urlParams := url.Values{
		"info_hash":  []string{string(tf.InfoHash[:])},
		"peer_id":    []string{string(peerId[:])},
		"port":       []string{strconv.Itoa(int(port))},
		"uploaded":   []string{"0"},
		"downloaded": []string{"0"},
		"left":       []string{strconv.Itoa(tf.Length)},
		"compact":    []string{"1"},
	}

	base.RawQuery = urlParams.Encode()
	return base.String(), nil
}

func requestPeersHttp(tf *TorrentFile, peerId [20]byte) ([]peers.Peer, error) {
	url, err := buildTrackerURL(tf, peerId, uint16(6881))
	if err != nil {
		return nil, err
	}

	c := &http.Client{Timeout: 15 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Request failed with status code %s", resp.Status)
	}

	trackerResponse := bencodeTrackerResp{}
	err = bencode.Unmarshal(resp.Body, &trackerResponse)
	if err != nil {
		return nil, err
	}

	if trackerResponse.FailureReason != "" {
		return nil, fmt.Errorf("tracker failure: %s", trackerResponse.FailureReason)
	}

	if trackerResponse.WarningMessage != "" {
		return nil, fmt.Errorf("tracker failure: %s", trackerResponse.FailureReason)
	}

	if trackerResponse.Complete > 0 || trackerResponse.Incomplete > 0 {
		fmt.Printf("HTTP Tracker Response - Seeders: %d, Leechers: %d, Interval: %ds\n",
			trackerResponse.Complete, trackerResponse.Incomplete, trackerResponse.Interval)
	}

	return peers.UnmarshalPeers([]byte(trackerResponse.Peers))

}

func RequestPeers(tf *TorrentFile, peerId [20]byte) ([]peers.Peer, error) {
	url, err := url.Parse(tf.Announce)
	if err != nil {
		return nil, err
	}
	switch url.Scheme {
	case "http", "https":
		return requestPeersHttp(tf, peerId)
	// case "udp":
	// 	return requestPeersUdp(tf, peerId)
	default:
		return nil, fmt.Errorf("not supported scheme, %s", url.Scheme)
	}
}
